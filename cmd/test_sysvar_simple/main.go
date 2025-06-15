package main

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println(" 测试SystemVariablesManager功能")

	// 创建配置
	cfg := &conf.Cfg{
		InnodbDataDir: "data",
		DataDir:       "data",
		Port:          3309,
		BaseDir:       "/usr/local/mysql/",
	}

	// 创建存储管理器（这会初始化SystemVariablesManager）
	fmt.Println(" 创建存储管理器...")
	storageManager := manager.NewStorageManager(cfg)
	if storageManager == nil {
		fmt.Println(" 创建存储管理器失败")
		return
	}

	// 获取系统变量管理器
	fmt.Println(" 获取系统变量管理器...")
	sysVarManager := storageManager.GetSystemVariablesManager()
	if sysVarManager == nil {
		fmt.Println(" 获取系统变量管理器失败")
		return
	}

	// 获取系统变量分析器
	fmt.Println(" 获取系统变量分析器...")
	sysVarAnalyzer := storageManager.GetSystemVariableAnalyzer()
	if sysVarAnalyzer == nil {
		fmt.Println(" 获取系统变量分析器失败")
		return
	}

	// 创建测试会话
	sessionID := "test_session"
	logger.Debugf(" 创建测试会话: %s\n", sessionID)
	sysVarManager.CreateSession(sessionID)

	// 测试获取auto_increment_increment
	fmt.Println("\n 测试auto_increment_increment变量:")
	value, err := sysVarManager.GetVariable(sessionID, "auto_increment_increment", manager.SessionScope)
	if err != nil {
		logger.Debugf(" 获取auto_increment_increment失败: %v\n", err)
	} else {
		logger.Debugf(" auto_increment_increment = %v (类型: %T)\n", value, value)
	}

	// 测试获取character_set_client
	fmt.Println("\n 测试character_set_client变量:")
	value, err = sysVarManager.GetVariable(sessionID, "character_set_client", manager.SessionScope)
	if err != nil {
		logger.Debugf(" 获取character_set_client失败: %v\n", err)
	} else {
		logger.Debugf(" character_set_client = %v (类型: %T)\n", value, value)
	}

	// 测试mysql-connector-java查询分析
	fmt.Println("\n 测试mysql-connector-java查询分析:")
	mysqlConnectorQuery := `SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection`

	// 检查是否为系统变量查询
	isSystemVarQuery := sysVarAnalyzer.IsSystemVariableQuery(mysqlConnectorQuery)
	logger.Debugf(" 是否为系统变量查询: %v\n", isSystemVarQuery)

	if isSystemVarQuery {
		// 分析查询
		query, err := sysVarAnalyzer.AnalyzeSystemVariableQuery(mysqlConnectorQuery)
		if err != nil {
			logger.Debugf(" 分析查询失败: %v\n", err)
		} else {
			logger.Debugf(" 查询分析成功，找到 %d 个变量:\n", len(query.Variables))
			for i, varInfo := range query.Variables {
				logger.Debugf("   %d. 变量: %s, 作用域: %s, 别名: %s\n", i+1, varInfo.Name, varInfo.Scope, varInfo.Alias)
			}

			// 生成结果
			fmt.Println("\n 生成查询结果:")
			columns, rows, err := sysVarAnalyzer.GenerateSystemVariableResult(sessionID, query)
			if err != nil {
				logger.Debugf(" 生成结果失败: %v\n", err)
			} else {
				logger.Debugf(" 结果生成成功:\n")
				logger.Debugf("   列: %v\n", columns)
				if len(rows) > 0 {
					logger.Debugf("   行: %v\n", rows[0])

					// 检查每个值的类型
					fmt.Println("\n 详细值分析:")
					for i, col := range columns {
						if i < len(rows[0]) {
							val := rows[0][i]
							if val == nil {
								logger.Debugf("   %s: NULL\n", col)
							} else {
								logger.Debugf("   %s: %v (类型: %T)\n", col, val, val)
							}
						}
					}
				}
			}
		}
	}

	// 清理
	logger.Debugf("\n🧹 清理会话: %s\n", sessionID)
	sysVarManager.DestroySession(sessionID)

	fmt.Println("\n🏁 测试完成")
}
