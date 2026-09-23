package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	port := flag.Int("port", 3308, "MySQL port")
	mode := flag.String("mode", "ping", "setup, write, read, readonly, write-after-promote")
	value := flag.Int("value", 1, "row value")
	flag.Parse()

	dsn := fmt.Sprintf("root:root@1234@tcp(127.0.0.1:%d)/?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true&interpolateParams=true", *port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "ping":
		fmt.Println("PING_OK")
	case "setup":
		exec(db, "CREATE DATABASE IF NOT EXISTS cluster_app")
		exec(db, "CREATE TABLE IF NOT EXISTS cluster_app.items (id INT PRIMARY KEY, value VARCHAR(64))")
		fmt.Println("SETUP_OK")
	case "write", "write-after-promote":
		exec(db, fmt.Sprintf("INSERT INTO cluster_app.items(id, value) VALUES (%d, 'v%d')", *value, *value))
		fmt.Println("WRITE_OK")
	case "read":
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM cluster_app.items").Scan(&count); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("COUNT=%d\n", count)
	case "readonly":
		_, err := db.Exec(fmt.Sprintf("INSERT INTO cluster_app.items(id, value) VALUES (%d, 'readonly')", *value))
		if err == nil {
			log.Fatal("READONLY_WRITE_UNEXPECTEDLY_SUCCEEDED")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "read-only") {
			log.Fatalf("unexpected readonly error: %v", err)
		}
		fmt.Println("READONLY_REJECTED")
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func exec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("%s: %v", query, err)
	}
}
