//go:build reconnectfixture

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type reconnectResult struct {
	Client string            `json:"client"`
	Cases  map[string]string `json:"cases"`
}

func main() {
	dsn := os.Getenv("XMYSQL_RECONNECT_DSN")
	readyPath := os.Getenv("XMYSQL_RECONNECT_READY_FILE")
	serverReadyPath := os.Getenv("XMYSQL_RECONNECT_SERVER_READY_FILE")
	if dsn == "" || readyPath == "" || serverReadyPath == "" {
		fail(fmt.Errorf("XMYSQL_RECONNECT_DSN, XMYSQL_RECONNECT_READY_FILE and XMYSQL_RECONNECT_SERVER_READY_FILE are required"))
	}
	timeoutSeconds := 30
	if raw := os.Getenv("XMYSQL_RECONNECT_TIMEOUT_SECONDS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			timeoutSeconds = parsed
		}
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fail(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		fail(fmt.Errorf("open physical connection: %w", err))
	}
	var initial int64
	if err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&initial); err != nil || initial != 1 {
		fail(fmt.Errorf("initial query value=%d err=%v", initial, err))
	}
	if err = os.WriteFile(readyPath, []byte("ready\n"), 0644); err != nil {
		fail(fmt.Errorf("write ready marker: %w", err))
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		if _, err = os.Stat(serverReadyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			fail(fmt.Errorf("server ready marker did not appear"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	var stale int64
	if err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&stale); err == nil {
		fail(fmt.Errorf("old physical connection unexpectedly succeeded after server restart: %d", stale))
	}
	// The driver may already close the physical connection as soon as the
	// server restart is observed. That is the expected stale-connection state;
	// do not turn a best-effort cleanup error into a compatibility failure.
	_ = conn.Close()
	if err = db.Close(); err != nil {
		fail(fmt.Errorf("close old database handle: %w", err))
	}
	newDB, err := sql.Open("mysql", dsn)
	if err != nil {
		fail(fmt.Errorf("reopen database handle: %w", err))
	}
	defer newDB.Close()
	if err = newDB.Ping(); err != nil {
		fail(fmt.Errorf("reconnect ping: %w", err))
	}
	var recovered int64
	if err = newDB.QueryRow("SELECT 1").Scan(&recovered); err != nil || recovered != 1 {
		fail(fmt.Errorf("reconnect query value=%d err=%v", recovered, err))
	}
	recoveryCase := "reconnect-after-server-restart"
	if os.Getenv("XMYSQL_RECONNECT_FAULT_MODE") == "network" {
		recoveryCase = "reconnect-after-network-fault"
	}
	encoded, _ := json.Marshal(reconnectResult{Client: "go-mysql-driver", Cases: map[string]string{"old-connection-failure": "PASS", recoveryCase: "PASS"}})
	fmt.Println(string(encoded))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
