package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func mustExec(db *sql.DB, q string) error {
	_, err := db.Exec(q)
	return err
}

func runSetupRedo(db *sql.DB, dbName, tableName string) error {
	if err := mustExec(db, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		return err
	}
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	if err := mustExec(db, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, val VARCHAR(64))", tableName)); err != nil {
		return err
	}
	return nil
}

func runVerifyRedo(db *sql.DB, dbName, tableName string) error {
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return fmt.Errorf("redo verify failed: database %s not accessible: %w", dbName, err)
	}
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName))
	if err != nil {
		return fmt.Errorf("redo verify failed: table %s.%s not queryable: %w", dbName, tableName, err)
	}
	defer rows.Close()
	return nil
}

func runHoldUndo(dsn, dbName, tableName string, holdSeconds int) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (id,val) VALUES (2,'undo_uncommitted')", tableName)); err != nil {
		return err
	}
	fmt.Println("UNDO_HOLD_READY")
	// 故意不提交，让外部脚本 kill 掉服务端进程模拟崩溃
	time.Sleep(time.Duration(holdSeconds) * time.Second)
	_ = tx.Rollback()
	return nil
}

func runVerifyUndo(db *sql.DB, dbName, tableName string) error {
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	var cnt int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id=2", tableName)).Scan(&cnt); err != nil {
		return err
	}
	if cnt != 0 {
		return fmt.Errorf("undo verify failed: expected 0 row, got %d", cnt)
	}
	return nil
}

func runRaceCommit(dsn, dbName, tableName string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (id,val) VALUES (3,'half_commit_race')", tableName)); err != nil {
		return err
	}
	fmt.Println("HALF_COMMIT_READY")
	time.Sleep(1200 * time.Millisecond)
	if err := tx.Commit(); err != nil {
		// 提交失败在本场景允许（由外部 kill 干扰）
		fmt.Printf("HALF_COMMIT_COMMIT_ERR: %v\n", err)
		return nil
	}
	fmt.Println("HALF_COMMIT_COMMIT_OK")
	return nil
}

func runVerifyHalfCommit(db *sql.DB, dbName, tableName string) error {
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	var cnt int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id=3", tableName)).Scan(&cnt); err != nil {
		return err
	}
	// 半提交竞态允许 0 或 1，重点验证恢复后可查询、无损坏
	if cnt < 0 || cnt > 1 {
		return fmt.Errorf("half-commit verify failed: invalid row count %d", cnt)
	}
	fmt.Printf("HALF_COMMIT_FINAL_COUNT=%d\n", cnt)
	return nil
}

func runVerifyShowTablesWhere(db *sql.DB, dbName string) error {
	if err := mustExec(db, fmt.Sprintf("USE %s", dbName)); err != nil {
		return err
	}
	rows, err := db.Query("SHOW TABLES WHERE 1 = 0")
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("show tables where verify failed: expected 0 rows, got %d", count)
	}
	return nil
}

func main() {
	var (
		dsn         string
		mode        string
		holdSeconds int
		dbName      string
		tableName   string
	)
	flag.StringVar(&dsn, "dsn", "root:root@tcp(127.0.0.1:3309)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true", "mysql dsn")
	flag.StringVar(&mode, "mode", "", "setup_redo|verify_redo|hold_undo|verify_undo|race_commit|verify_half_commit|verify_show_tables_where")
	flag.IntVar(&holdSeconds, "hold-seconds", 30, "seconds to hold uncommitted tx")
	flag.StringVar(&dbName, "db", "drill_recovery_db", "database name for drill")
	flag.StringVar(&tableName, "table", "drill_txn", "table name for drill")
	flag.Parse()

	if mode == "" {
		fmt.Fprintln(os.Stderr, "mode is required")
		os.Exit(2)
	}

	if mode == "hold_undo" || mode == "race_commit" {
		var err error
		if mode == "hold_undo" {
			err = runHoldUndo(dsn, dbName, tableName, holdSeconds)
		} else {
			err = runRaceCommit(dsn, dbName, tableName)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "mode=%s failed: %v\n", mode, err)
			os.Exit(1)
		}
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "ping failed: %v\n", err)
		os.Exit(1)
	}

	switch mode {
	case "setup_redo":
		err = runSetupRedo(db, dbName, tableName)
	case "verify_redo":
		err = runVerifyRedo(db, dbName, tableName)
	case "verify_undo":
		err = runVerifyUndo(db, dbName, tableName)
	case "verify_half_commit":
		err = runVerifyHalfCommit(db, dbName, tableName)
	case "verify_show_tables_where":
		err = runVerifyShowTablesWhere(db, dbName)
	default:
		err = fmt.Errorf("unknown mode: %s", mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "mode=%s failed: %v\n", mode, err)
		os.Exit(1)
	}
	fmt.Printf("mode=%s ok\n", mode)
}
