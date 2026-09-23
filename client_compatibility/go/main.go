package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type result struct {
	Client string            `json:"client"`
	Cases  map[string]string `json:"cases"`
}

func main() {
	dsn := os.Getenv("XMYSQL_CLIENT_DSN")
	if dsn == "" || !strings.Contains(dsn, "@tcp(") {
		fmt.Fprintln(os.Stderr, "XMYSQL_CLIENT_DSN is required")
		os.Exit(2)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fail(err)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		fail(err)
	}
	out := result{Client: "go-mysql-driver", Cases: map[string]string{}}
	check := func(name, query string) {
		if _, err := db.Exec(query); err != nil {
			fail(fmt.Errorf("%s: %w", name, err))
		}
		out.Cases[name] = "PASS"
	}
	check("connection-auth", "SELECT 1")
	check("database-ddl-dml", "CREATE DATABASE IF NOT EXISTS client_matrix")
	check("database-ddl-dml-table", "CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32))")
	check("database-ddl-dml-write", "INSERT INTO client_matrix.matrix_rows(id, label) VALUES (1, 'one') ON DUPLICATE KEY UPDATE label=VALUES(label)")
	stmt, err := db.Prepare("SELECT ? + 1")
	if err != nil {
		fail(err)
	}
	defer stmt.Close()
	var prepared int
	if err = stmt.QueryRow(41).Scan(&prepared); err != nil || prepared != 42 {
		fail(fmt.Errorf("prepared-statements: value=%d err=%v", prepared, err))
	}
	out.Cases["prepared-statements"] = "PASS"
	tx, err := db.Begin()
	if err != nil {
		fail(err)
	}
	if _, err = tx.Exec("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (2, 'tx')"); err != nil {
		fail(err)
	}
	if err = tx.Rollback(); err != nil {
		fail(err)
	}
	out.Cases["transactions"] = "PASS"
	var nullable any
	var integer int64
	var utf8 string
	if err = db.QueryRow("SELECT NULL, CAST(42 AS SIGNED), _utf8mb4'兼容'").Scan(&nullable, &integer, &utf8); err != nil || nullable != nil || integer != 42 || utf8 != "兼容" {
		fail(fmt.Errorf("null-and-types: nullable=%v integer=%d utf8=%q err=%v", nullable, integer, utf8, err))
	}
	out.Cases["null-and-types"] = "PASS"
	var charsetValue string
	if err = db.QueryRow("SELECT _utf8mb4'兼容'").Scan(&charsetValue); err != nil || charsetValue != "兼容" {
		fail(fmt.Errorf("charset: value=%q err=%v", charsetValue, err))
	}
	out.Cases["charset"] = "PASS"
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'").Scan(&count); err != nil || count == 0 {
		fail(fmt.Errorf("metadata: count=%d err=%v", count, err))
	}
	out.Cases["metadata"] = "PASS"
	rows, err := db.Query("SELECT 1 AS first_value; SELECT 2 AS second_value")
	if err != nil {
		fail(fmt.Errorf("multi-result-and-error: query failed: %w", err))
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "first_value" {
		rows.Close()
		fail(fmt.Errorf("multi-result-and-error: first columns=%v err=%v", columns, err))
	}
	var firstValue int64
	if !rows.Next() || rows.Scan(&firstValue) != nil || firstValue != 1 {
		rows.Close()
		fail(fmt.Errorf("multi-result-and-error: first value=%d err=%v", firstValue, rows.Err()))
	}
	if !rows.NextResultSet() {
		err := rows.Err()
		rows.Close()
		fail(fmt.Errorf("multi-result-and-error: second result set unavailable: %v", err))
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "second_value" {
		rows.Close()
		fail(fmt.Errorf("multi-result-and-error: second columns=%v err=%v", columns, err))
	}
	var secondValue int64
	if !rows.Next() || rows.Scan(&secondValue) != nil || secondValue != 2 {
		rows.Close()
		fail(fmt.Errorf("multi-result-and-error: second value=%d err=%v", secondValue, rows.Err()))
	}
	if err = rows.Close(); err != nil {
		fail(fmt.Errorf("multi-result-and-error: close failed: %w", err))
	}
	if _, err = db.Exec("SELECT * FROM table_that_does_not_exist"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "table") {
		fail(fmt.Errorf("multi-result-and-error: expected table error, got %v", err))
	}
	out.Cases["multi-result-and-error"] = "PASS"
	if err = db.Ping(); err != nil {
		fail(err)
	}
	out.Cases["reconnect"] = "PASS"
	encoded, _ := json.Marshal(out)
	fmt.Println(string(encoded))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
