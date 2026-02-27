package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

func main() {
	fmt.Println(" 测试XMySQL服务器系统变量修复")

	// 连接到XMySQL服务器
	dsn := "root:123456@tcp(127.0.0.1:3309)/demo_db"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf(" 连接失败: %v", err)
	}
	defer db.Close()

	fmt.Println(" 连接成功")

	// 测试系统变量查询
	testQueries := []string{
		"SELECT @@auto_increment_increment",
		"SELECT @@character_set_client",
		"SELECT @@version",
		"SELECT @@auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client",
		// mysql-connector-java的完整查询
		"SELECT @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buffer_length, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout",
	}

	for i, query := range testQueries {
		logger.Debugf("\n 测试查询 %d: %s\n", i+1, query)

		rows, err := db.Query(query)
		if err != nil {
			logger.Debugf(" 查询失败: %v\n", err)
			continue
		}

		// 获取列名
		columns, err := rows.Columns()
		if err != nil {
			logger.Debugf(" 获取列名失败: %v\n", err)
			rows.Close()
			continue
		}

		logger.Debugf(" 列名: %v\n", columns)

		// 读取结果
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rowCount := 0
		for rows.Next() {
			rowCount++
			err := rows.Scan(valuePtrs...)
			if err != nil {
				logger.Debugf(" 扫描行失败: %v\n", err)
				continue
			}

			logger.Debugf(" 行 %d: ", rowCount)
			for i, val := range values {
				if val == nil {
					logger.Debugf("%s=NULL ", columns[i])
				} else {
					switch v := val.(type) {
					case []byte:
						logger.Debugf("%s='%s' ", columns[i], string(v))
					default:
						logger.Debugf("%s='%v' ", columns[i], v)
					}
				}
			}
			fmt.Println()
		}

		if rowCount == 0 {
			fmt.Println(" 没有返回任何行")
		}

		rows.Close()
	}

	fmt.Println("\n🏁 测试完成")
}
