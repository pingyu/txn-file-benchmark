package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func insertWorkload(targetTxns int) {
	ctx := context.Background()

	startTime := time.Now()

	dbs := make([]*sql.DB, 0, len(dsns))
	for _, dsn := range dsns {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		dbs = append(dbs, db)
	}

	// Set variable
	query := fmt.Sprintf("SET GLOBAL tidb_mem_quota_query=%d", TIDB_MEM_QUOTA_QUERY)
	_, err := dbs[0].ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	// Create database
	query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *database)
	_, err = dbs[0].ExecContext(ctx, query)
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
	targetTxnsPerThread := targetTxns / *numThreads
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < *numThreads; i++ {
		i := i
		eg.Go(func() error {
			db := dbs[i%len(dbs)]
			workerThread(ctx, db, i, targetTxnsPerThread)
			return nil
		})
	}
	err = eg.Wait()
	fmt.Printf("Total transactions: %v, elapsed: %v\n", totalTxns.Load(), time.Since(startTime))
	if err != nil {
		log.Fatal(err)
	}
}

func workerThread(ctx context.Context, db *sql.DB, threadIdx int, targetTxns int) {
	// fmt.Printf("Starting worker thread %d\n", threadIdx)

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rowCount := txnBytes / int64(*rowBytes)
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

	threadTxns := 0
	for {
		err := doTxn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Do transaction failed, thread %d, err %v\n", threadIdx, err)
			time.Sleep(1 * time.Second)
		} else {
			threadTxns++
			if targetTxns > 0 && threadTxns >= targetTxns {
				break
			}
		}
	}
}
