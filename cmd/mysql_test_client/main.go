package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// runSingleColumnQuery 执行单列查询并打印结果
func runSingleColumnQuery(db *sql.DB, query string) {
	var value string
	if err := db.QueryRow(query).Scan(&value); err != nil {
		fmt.Printf("[FAIL] %s\n  Error: %v\n", query, err)
		return
	}
	fmt.Printf("[OK]   %s\n  Result: %s\n", query, value)
}

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

	// 基础测试
	runSingleColumnQuery(db, "SELECT 1")

	// 版本相关
	runSingleColumnQuery(db, "SELECT @@version")
	runSingleColumnQuery(db, "SELECT @@version_comment")

	// 用户相关
	runSingleColumnQuery(db, "SELECT USER()")
	runSingleColumnQuery(db, "SELECT CURRENT_USER()")

	// 会话/系统变量（接近 JDBC 初始化会用到的变量）
	runSingleColumnQuery(db, "SELECT @@session.auto_increment_increment")
	runSingleColumnQuery(db, "SELECT @@session.time_zone")
	runSingleColumnQuery(db, "SELECT @@system_time_zone")
}
