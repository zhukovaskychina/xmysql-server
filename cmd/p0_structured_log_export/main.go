package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/observability/logging"
)

func main() {
	slowOutput := flag.String("slow-output", "", "path to write slow query JSON lines")
	errorOutput := flag.String("error-output", "", "path to write error JSON lines")
	flag.Parse()

	if *slowOutput == "" || *errorOutput == "" {
		fmt.Fprintln(os.Stderr, "both -slow-output and -error-output are required")
		os.Exit(2)
	}

	if err := writeSlowQuerySample(*slowOutput); err != nil {
		fmt.Fprintf(os.Stderr, "write slow query sample: %v\n", err)
		os.Exit(1)
	}

	if err := writeErrorSample(*errorOutput); err != nil {
		fmt.Fprintf(os.Stderr, "write error sample: %v\n", err)
		os.Exit(1)
	}
}

func writeSlowQuerySample(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := logging.NewEncoder(file)
	return encoder.WriteSlowQuery(logging.NewSlowQueryEvent(
		"trace-p0d-runtime-slow-001",
		101,
		9001,
		"test",
		"select_user_by_status",
		1520*time.Millisecond,
		120000,
		50,
		"",
		"slow",
	))
}

func writeErrorSample(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := logging.NewEncoder(file)
	return encoder.WriteError(logging.NewErrorEvent(
		"ERROR",
		"trace-p0d-runtime-error-001",
		"engine",
		"insert",
		"ER_DUP_ENTRY",
		"duplicate_key",
		"duplicate key conflict",
		12*time.Millisecond,
		103,
		9003,
	))
}
