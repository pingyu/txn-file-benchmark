package main

import (
	"flag"
	"fmt"
	"strings"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
)

const (
	INSERT_BATCH_ROWS    = 1024
	TIDB_MEM_QUOTA_QUERY = 16 << 30 // 16GB
)

var (
	numThreads *int    = flag.Int("threads", 1, "number of threads")
	rowBytes   *int    = flag.Int("row-size", 1024, "size of row")
	txnMB      *int64  = flag.Int64("txn-size-mb", 8, "size of transaction")
	targetTxns *int    = flag.Int("txns", 0, "target transactions")
	dsn        *string = flag.String("dsn", "root@tcp(127.0.0.1:4000)/", "DSN, or multiple DSNs separate by comma")
	database   *string = flag.String("database", "test", "database")
	useTxnFile *bool   = flag.Bool("use-txn-file", false, "use txn file")
	workload   *string = flag.String("workload", "insert", "workload type [insert/insert-select]")
	action     *string = flag.String("action", "", "action type [prepare/run]")

	// targetTPM  *int    = flag.Int("tpm", 0, "target tpm (transactions per minute)")
)

var (
	totalRows atomic.Int64
	totalTxns atomic.Int64
	dsns      []string

	txnBytes int64
)

func initFlags() {
	flag.Parse()

	fmt.Printf("Number of Threads: %d\n", *numThreads)
	fmt.Printf("Size of Transaction (MB): %d\n", *txnMB)
	fmt.Printf("Use Txn File: %v\n", *useTxnFile)
	// fmt.Printf("Target QPS: %d\n", *targetTPM)
	fmt.Printf("Target Transactions: %d\n", *targetTxns)
	fmt.Printf("Workload Type: %s\n", *workload)
	fmt.Printf("Action Type: %s\n", *action)

	txnBytes = *txnMB * 1024 * 1024
	dsns = strings.Split(*dsn, ",")
}

func main() {
	initFlags()

	if *workload == "insert" {
		insertWorkload(*targetTxns)
	} else if *workload == "insert-select" {
		insertSelectWorkload(*targetTxns)
	} else {
		fmt.Printf("Unknown workload type: %s\n", *workload)
		return
	}
}
