package main

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
)

func main() {
	dsn := "root:@tcp(127.0.0.1:3309)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}
	fmt.Println("Ping OK")

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		panic(err)
	}
	fmt.Println("SELECT 1 ->", one)
}
