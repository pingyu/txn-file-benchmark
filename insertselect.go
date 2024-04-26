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

func insertSelectWorkload() {
	if *action == "prepare" {
		prepareTableForSelect()
	} else if *action == "run" {
		runInsertSelectWorkload()
	} else {
		log.Fatal("Unknown action type: ", *action)
	}
}

func prepareTableForSelect() {
	ctx := context.Background()

	db, err := sql.Open("mysql", dsns[0])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create database
	query := "CREATE DATABASE IF NOT EXISTS db_select"
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	tableName := fmt.Sprintf("db_select.table_select_%d", *txnMB)

	// Create table
	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (v VARBINARY(65535))", tableName)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	// Truncate table
	query = fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rowCount := txnBytes / int64(*rowBytes)
	rowValue := make([]byte, *rowBytes)
	sql := strings.Builder{}

	if *useTxnFile {
		_, err = conn.ExecContext(ctx, "BEGIN OPTIMISTIC")
	} else {
		_, err = conn.ExecContext(ctx, "BEGIN PESSIMISTIC")
	}
	if err != nil {
		log.Fatal(err, "begin failed")
	}

	for i := 0; i < int(rowCount); i += INSERT_BATCH_ROWS {
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (v) VALUES ", tableName))
		for j := 0; j < INSERT_BATCH_ROWS; j++ {
			if j > 0 {
				sql.WriteString(",")
			}
			_, err = rand.Read(rowValue)
			if err != nil {
				log.Fatal(err, "generate random value failed")
			}
			sql.WriteString(fmt.Sprintf("(0x%s)", hex.EncodeToString(rowValue)))
		}

		_, err = conn.ExecContext(ctx, sql.String())
		sql.Reset()
		if err != nil {
			log.Fatal(err, "insert failed")
		}
	}

	_, err = conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		log.Fatal(err, "commit failed")
	}
}

func runInsertSelectWorkload() {
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
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.table_%d (v VARBINARY(65535))", *database, i)

		_, err := dbs[0].ExecContext(ctx, query)
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
			insertSelectWorkerThread(ctx, db, i)
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		log.Fatal(err)
	}
}

func insertSelectWorkerThread(ctx context.Context, db *sql.DB, threadIdx int) {
	// fmt.Printf("Starting worker thread %d\n", threadIdx)

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rowCount := txnBytes / int64(*rowBytes)
	tableForSelect := fmt.Sprintf("db_select.table_select_%d", *txnMB)

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

		sql := fmt.Sprintf("INSERT INTO %s.table_%d (v) SELECT v FROM %s", *database, threadIdx, tableForSelect)
		_, err = conn.ExecContext(ctx, sql)
		if err != nil {
			return errors.Wrap(err, "insert failed")
		}
		totalRows.Add(rowCount)

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
			fmt.Fprintf(os.Stderr, "Do transaction failed, thread %d, err %v\n", threadIdx, err)
			time.Sleep(1 * time.Second)
		}
	}
}
