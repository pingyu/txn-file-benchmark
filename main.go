package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	INSERT_BATCH_ROWS = 1024
)

var (
	numThreads *int    = flag.Int("threads", 1, "number of threads")
	rowBytes   *int    = flag.Int("row-size", 1024, "size of row")
	txnBytes   *int64  = flag.Int64("txn-size-mb", 8, "size of transaction")
	targetTPM  *int    = flag.Int("tpm", 0, "target tpm (transactions per minute)")
	dsn        *string = flag.String("dsn", "root@tcp(127.0.0.1:4000)/", "DSN, or multiple DSNs separate by comma")
	database   *string = flag.String("database", "test", "database")
	useTxnFile *bool   = flag.Bool("use-txn-file", false, "use txn file")
)

var (
	totalRows atomic.Int64
	totalTxns atomic.Int64
	dsns      []string
)

func initFlags() {
	flag.Parse()

	fmt.Printf("Number of Threads: %d\n", *numThreads)
	fmt.Printf("Size of Transaction (MB): %d\n", *txnBytes)
	fmt.Printf("Use Txn File: %v\n", *useTxnFile)
	fmt.Printf("Target QPS: %d\n", *targetTPM)

	*txnBytes = *txnBytes * 1024 * 1024
	dsns = strings.Split(*dsn, ",")
}

func main() {
	initFlags()

	ctx := context.Background()

	dbs := make([]*sql.DB, 0, len(dsns))
	for _, dsn := range dsns {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		dbs = append(dbs, db)
	}

	// Create database
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *database)
	_, err := dbs[0].ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	// Create tables
	for i := 0; i < *numThreads; i++ {
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.table_%d (k BIGINT AUTO_INCREMENT PRIMARY KEY, v VARBINARY(65535))", *database, i)

		_, err = dbs[0].ExecContext(ctx, query)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Start worker threads
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < *numThreads; i++ {
		i := i
		eg.Go(func() error {
			db := dbs[i%len(dbs)]
			workerThread(ctx, db, i)
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		log.Fatal(err)
	}
}

func workerThread(ctx context.Context, db *sql.DB, threadIdx int) {
	// fmt.Printf("Starting worker thread %d\n", threadIdx)

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rowCount := *txnBytes / int64(*rowBytes)
	rowValue := make([]byte, *rowBytes)
	sql := strings.Builder{}

	lastReportTime := time.Now()
	lastTime := time.Now()
	lastRows := int64(0)
	lastTxns := int64(0)

	report := func() {
		if threadIdx == 0 {
			if time.Since(lastReportTime).Seconds() >= 10 {
				lastReportTime = time.Now()

				currentTxns := totalTxns.Load()
				txns := currentTxns - lastTxns
				currentRows := totalRows.Load()
				rows := currentRows - lastRows
				if txns > 0 {
					elapsed := float32(time.Since(lastTime).Seconds())
					fmt.Printf("QPM: %v, rows/s %v\n", float32(txns)*60/elapsed, float32(rows)/elapsed)

					lastTime = time.Now()
					lastTxns = currentTxns
					lastRows = currentRows
				} else {
					fmt.Printf("Pending rows: %v\n", rows)
				}
			}
		}
	}

	doTxn := func() (err error) {
		defer func() {
			if err != nil {
				_, err1 := conn.ExecContext(ctx, "ROLLBACK")
				if err1 != nil {
					fmt.Fprintf(os.Stderr, "rollback failed, thread %d, err %v", threadIdx, err1)
				}
			}
		}()

		if *useTxnFile {
			_, err = conn.ExecContext(ctx, "BEGIN OPTIMISTIC")
		} else {
			_, err = conn.ExecContext(ctx, "BEGIN PESSIMISTIC")
		}
		if err != nil {
			return errors.Wrap(err, "begin failed")
		}

		for i := 0; i < int(rowCount); i += INSERT_BATCH_ROWS {
			sql.WriteString(fmt.Sprintf("INSERT INTO %s.table_%d (v) VALUES ", *database, threadIdx))
			for j := 0; j < INSERT_BATCH_ROWS; j++ {
				if j > 0 {
					sql.WriteString(",")
				}
				_, err = rand.Read(rowValue)
				if err != nil {
					return errors.Wrap(err, "generate random value failed")
				}
				sql.WriteString(fmt.Sprintf("(0x%s)", hex.EncodeToString(rowValue)))
			}

			_, err = conn.ExecContext(ctx, sql.String())
			sql.Reset()
			if err != nil {
				return errors.Wrap(err, "insert failed")
			}
			totalRows.Add(INSERT_BATCH_ROWS)
			report()
		}

		_, err = conn.ExecContext(ctx, "COMMIT")
		if err != nil {
			return errors.Wrap(err, "commit failed")
		}
		totalTxns.Add(1)
		report()

		return nil
	}

	for {
		// startTime := time.Now()
		err := doTxn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Do transaction failed, thread %d, err %v", threadIdx, err)
		}
	}
}
