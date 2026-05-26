package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := "root:root@1234@tcp(127.0.0.1:3309)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		fmt.Printf("ping失败: %v\n", err)
		return
	}
	fmt.Println("Ping OK")

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}
	fmt.Println("SELECT 1 ->", one)
}
