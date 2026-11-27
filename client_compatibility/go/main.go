package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
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
	if caFile := os.Getenv("XMYSQL_CLIENT_TLS_CA"); caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			fail(fmt.Errorf("read TLS CA: %w", err))
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			fail(errors.New("parse TLS CA: no certificates found"))
		}
		if err := mysqlDriver.RegisterTLSConfig("xmysql", &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, ServerName: "localhost"}); err != nil {
			fail(fmt.Errorf("register TLS config: %w", err))
		}
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
	// database/sql Ping is implemented by go-sql-driver/mysql with a real
	// COM_PING round trip, so keep this protocol case separate from SQL SELECT 1.
	out.Cases["protocol-ping"] = "PASS"
	var rows *sql.Rows
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
	rows, err = db.Query("SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' ORDER BY TABLE_NAME, ORDINAL_POSITION")
	if err != nil {
		fail(fmt.Errorf("metadata-shape: query failed: %w", err))
	}
	columns, err := rows.Columns()
	if err != nil || len(columns) != 4 || strings.ToLower(columns[0]) != "table_schema" || strings.ToLower(columns[1]) != "table_name" || strings.ToLower(columns[2]) != "column_name" || strings.ToLower(columns[3]) != "ordinal_position" {
		rows.Close()
		fail(fmt.Errorf("metadata-shape: columns=%v err=%v", columns, err))
	}
	if !rows.Next() {
		rows.Close()
		fail(fmt.Errorf("metadata-shape: no rows returned"))
	}
	var schema, table, column string
	var ordinal int64
	if err = rows.Scan(&schema, &table, &column, &ordinal); err != nil || schema != "client_matrix" || table == "" || column == "" || ordinal < 1 {
		rows.Close()
		fail(fmt.Errorf("metadata-shape: row=%q.%q.%q ordinal=%d err=%v", schema, table, column, ordinal, err))
	}
	if err = rows.Close(); err != nil {
		fail(fmt.Errorf("metadata-shape: close failed: %w", err))
	}
	out.Cases["metadata-shape"] = "PASS"
	check("auto-increment-and-result-metadata", "CREATE TABLE IF NOT EXISTS client_matrix.auto_rows(id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(32))")
	insertResult, err := db.Exec("INSERT INTO client_matrix.auto_rows(label) VALUES ('go-client')")
	if err != nil {
		fail(fmt.Errorf("auto-increment-and-result-metadata: insert failed: %w", err))
	}
	insertID, err := insertResult.LastInsertId()
	if err != nil || insertID <= 0 {
		fail(fmt.Errorf("auto-increment-and-result-metadata: last insert id=%d err=%v", insertID, err))
	}
	affected, err := insertResult.RowsAffected()
	if err != nil || affected != 1 {
		fail(fmt.Errorf("auto-increment-and-result-metadata: affected=%d err=%v", affected, err))
	}
	out.Cases["auto-increment-and-result-metadata"] = "PASS"
	tx, err = db.Begin()
	if err != nil {
		fail(fmt.Errorf("savepoints: begin failed: %w", err))
	}
	if _, err = tx.Exec("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300001, 'savepoint-before')"); err != nil {
		fail(fmt.Errorf("savepoints: first insert failed: %w", err))
	}
	if _, err = tx.Exec("SAVEPOINT client_matrix_sp"); err != nil {
		fail(fmt.Errorf("savepoints: create failed: %w", err))
	}
	if _, err = tx.Exec("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300002, 'savepoint-after')"); err != nil {
		fail(fmt.Errorf("savepoints: second insert failed: %w", err))
	}
	if _, err = tx.Exec("ROLLBACK TO SAVEPOINT client_matrix_sp"); err != nil {
		fail(fmt.Errorf("savepoints: rollback failed: %w", err))
	}
	if _, err = tx.Exec("RELEASE SAVEPOINT client_matrix_sp"); err != nil {
		fail(fmt.Errorf("savepoints: release failed: %w", err))
	}
	if err = tx.Commit(); err != nil {
		fail(fmt.Errorf("savepoints: commit failed: %w", err))
	}
	var savepointCount int
	if err = db.QueryRow("SELECT COUNT(*) FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)").Scan(&savepointCount); err != nil || savepointCount != 1 {
		fail(fmt.Errorf("savepoints: count=%d err=%v", savepointCount, err))
	}
	if _, err = db.Exec("DELETE FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)"); err != nil {
		fail(fmt.Errorf("savepoints: cleanup failed: %w", err))
	}
	out.Cases["savepoints"] = "PASS"
	if _, err = db.Exec("SET @client_matrix_value = 41"); err != nil {
		fail(fmt.Errorf("session-state: set failed: %w", err))
	}
	var sessionValue int64
	if err = db.QueryRow("SELECT @client_matrix_value + 1").Scan(&sessionValue); err != nil || sessionValue != 42 {
		fail(fmt.Errorf("session-state: value=%d err=%v", sessionValue, err))
	}
	out.Cases["session-state"] = "PASS"
	rows, err = db.Query("SELECT 1 AS first_col; SELECT 2 AS second_col")
	if err != nil {
		fail(fmt.Errorf("multi-result-and-error: query failed: %w", err))
	}
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "first_col" {
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
	if columns, err := rows.Columns(); err != nil || len(columns) != 1 || columns[0] != "second_col" {
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
	var mysqlErr *mysqlDriver.MySQLError
	if _, err = db.Exec("SELECT * FROM client_matrix_missing_table"); err == nil || !errors.As(err, &mysqlErr) || mysqlErr.Number != 1146 {
		fail(fmt.Errorf("negative-error-code: expected MySQL error 1146, got %v", err))
	}
	out.Cases["negative-error-code"] = "PASS"
	check("extended-types-metadata", "CREATE TABLE IF NOT EXISTS client_matrix.type_rows(id BIGINT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, text_value TEXT, blob_value BLOB, created_at TIMESTAMP)")
	rows, err = db.Query("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' AND TABLE_NAME='type_rows' ORDER BY ORDINAL_POSITION")
	if err != nil {
		fail(fmt.Errorf("extended-types-metadata: query failed: %w", err))
	}
	expectedTypes := []string{"bigint", "decimal", "double", "text", "blob", "timestamp"}
	for _, expectedType := range expectedTypes {
		if !rows.Next() {
			fail(fmt.Errorf("extended-types-metadata: missing type %s", expectedType))
		}
		var dataType string
		if err = rows.Scan(&dataType); err != nil || strings.ToLower(dataType) != expectedType {
			fail(fmt.Errorf("extended-types-metadata: got %q, want %q, err=%v", dataType, expectedType, err))
		}
	}
	if rows.Next() {
		fail(fmt.Errorf("extended-types-metadata: returned more columns than expected"))
	}
	if err = rows.Err(); err != nil {
		fail(fmt.Errorf("extended-types-metadata: rows failed: %w", err))
	}
	if err = rows.Close(); err != nil {
		fail(fmt.Errorf("extended-types-metadata: close failed: %w", err))
	}
	out.Cases["extended-types-metadata"] = "PASS"
	var decimalValue string
	var doubleValue float64
	var binaryValue []byte
	var dateValue time.Time
	check("wire-value-types-table", "CREATE TABLE IF NOT EXISTS client_matrix.wire_rows(id INT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, binary_value BLOB, date_value DATE)")
	check("wire-value-types-reset", "DELETE FROM client_matrix.wire_rows WHERE id = 1")
	check("wire-value-types-write", "INSERT INTO client_matrix.wire_rows(id, decimal_value, double_value, binary_value, date_value) VALUES (1, 12.34, 1.5, _binary'xy', '2026-09-28')")
	if err = db.QueryRow("SELECT decimal_value, double_value, binary_value, date_value FROM client_matrix.wire_rows WHERE id = 1").Scan(&decimalValue, &doubleValue, &binaryValue, &dateValue); err != nil || decimalValue != "12.34" || doubleValue != 1.5 || string(binaryValue) != "xy" || dateValue.Format("2006-01-02") != "2026-09-28" {
		fail(fmt.Errorf("wire-value-types: decimal=%q double=%v binary=%q date=%v err=%v", decimalValue, doubleValue, binaryValue, dateValue, err))
	}
	out.Cases["wire-value-types"] = "PASS"
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	firstConn, err := db.Conn(context.Background())
	if err != nil {
		fail(fmt.Errorf("multi-session-pool: first connection failed: %w", err))
	}
	secondConn, err := db.Conn(context.Background())
	if err != nil {
		firstConn.Close()
		fail(fmt.Errorf("multi-session-pool: second connection failed: %w", err))
	}
	var firstPoolValue, secondPoolValue int64
	if err = firstConn.QueryRowContext(context.Background(), "SELECT 1").Scan(&firstPoolValue); err != nil || firstPoolValue != 1 {
		fail(fmt.Errorf("multi-session-pool: first query value=%d err=%v", firstPoolValue, err))
	}
	if err = secondConn.QueryRowContext(context.Background(), "SELECT 2").Scan(&secondPoolValue); err != nil || secondPoolValue != 2 {
		fail(fmt.Errorf("multi-session-pool: second query value=%d err=%v", secondPoolValue, err))
	}
	out.Cases["multi-session-pool"] = "PASS"
	if err = secondConn.Close(); err != nil {
		fail(fmt.Errorf("multi-session-pool: second connection close failed: %w", err))
	}
	if err = firstConn.Close(); err != nil {
		fail(fmt.Errorf("multi-session-pool: first connection close failed: %w", err))
	}
	if err = db.Close(); err != nil {
		fail(fmt.Errorf("reconnect: initial close failed: %w", err))
	}
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		fail(fmt.Errorf("reconnect: reopen failed: %w", err))
	}
	if err = db.Ping(); err != nil {
		fail(fmt.Errorf("reconnect: ping failed: %w", err))
	}
	var reconnectValue int64
	if err = db.QueryRow("SELECT 1").Scan(&reconnectValue); err != nil || reconnectValue != 1 {
		fail(fmt.Errorf("reconnect: value=%d err=%v", reconnectValue, err))
	}
	out.Cases["reconnect"] = "PASS"
	if os.Getenv("XMYSQL_CLIENT_AUTH_PLUGINS") == "1" {
		runAuthPluginCase(db, dsn, out)
	}
	encoded, _ := json.Marshal(out)
	fmt.Println(string(encoded))
}

func runAuthPluginCase(rootDB *sql.DB, rootDSN string, out result) {
	password := os.Getenv("XMYSQL_CLIENT_AUTH_PLUGIN_PASSWORD")
	if password == "" {
		bytes := make([]byte, 24)
		if _, err := rand.Read(bytes); err != nil {
			fail(fmt.Errorf("auth-plugins: generate ephemeral password: %w", err))
		}
		password = fmt.Sprintf("%x", bytes)
	}
	accounts := []struct {
		user   string
		plugin string
	}{
		{user: "xmysql_cache_client", plugin: "caching_sha2_password"},
		{user: "xmysql_sha_client", plugin: "sha256_password"},
	}
	for _, account := range accounts {
		if _, err := rootDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account.user)); err != nil {
			fail(fmt.Errorf("auth-plugins: drop %s: %w", account.user, err))
		}
		createSQL := fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED WITH %s BY '%s'", account.user, account.plugin, password)
		if _, err := rootDB.Exec(createSQL); err != nil {
			fail(fmt.Errorf("auth-plugins: create %s: %w", account.user, err))
		}
		if _, err := rootDB.Exec(fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%%'", account.user)); err != nil {
			fail(fmt.Errorf("auth-plugins: grant %s: %w", account.user, err))
		}
	}
	defer func() {
		for _, account := range accounts {
			_, _ = rootDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account.user))
		}
	}()
	for _, account := range accounts {
		pluginDB, err := sql.Open("mysql", authPluginDSN(rootDSN, account.user, password))
		if err != nil {
			fail(fmt.Errorf("auth-plugins: open %s: %w", account.user, err))
		}
		var value int
		err = pluginDB.QueryRow("SELECT 1").Scan(&value)
		closeErr := pluginDB.Close()
		if err != nil || closeErr != nil || value != 1 {
			fail(fmt.Errorf("auth-plugins: connect %s value=%d query=%v close=%v", account.user, value, err, closeErr))
		}
	}
	out.Cases["auth-plugins"] = "PASS"
}

func authPluginDSN(base, user, password string) string {
	at := strings.Index(base, "@tcp(")
	if at < 0 {
		return base
	}
	rest := base[at:]
	databaseMarker := strings.Index(rest, ")/")
	if databaseMarker < 0 {
		return user + ":" + password + rest
	}
	query := ""
	if queryOffset := strings.Index(rest[databaseMarker+2:], "?"); queryOffset >= 0 {
		query = rest[databaseMarker+2+queryOffset:]
	}
	return user + ":" + password + rest[:databaseMarker+2] + query
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
