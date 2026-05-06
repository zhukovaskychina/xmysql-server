package main

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println(" 测试 tx_read_only 系统变量修复")
	fmt.Println(strings.Repeat("=", 60))

	// 1. 创建系统变量管理器
	sysVarManager := manager.NewSystemVariablesManager()

	// 2. 创建测试会话
	sessionID := "test-session-001"
	sysVarManager.CreateSession(sessionID)

	// 3. 测试获取 tx_read_only 变量
	fmt.Println("\n 测试 tx_read_only 系统变量:")

	// 测试会话级别
	value, err := sysVarManager.GetVariable(sessionID, "tx_read_only", manager.SessionScope)
	if err != nil {
		logger.Debugf(" 获取会话级 tx_read_only 失败: %v\n", err)
	} else {
		logger.Debugf(" 会话级 tx_read_only: %v (类型: %T)\n", value, value)

		// 检查是否为整数类型
		if intVal, ok := value.(int64); ok {
			logger.Debugf("   ✓ 正确的整数类型: %d\n", intVal)
			if intVal == 0 {
				logger.Debugf("   ✓ 默认值正确: 0 (可读写)\n")
			} else {
				logger.Debugf("    非预期值: %d\n", intVal)
			}
		} else {
			logger.Debugf("    错误的类型，期望 int64，实际: %T\n", value)
		}
	}

	// 测试全局级别
	globalValue, err := sysVarManager.GetVariable(sessionID, "tx_read_only", manager.GlobalScope)
	if err != nil {
		logger.Debugf(" 获取全局级 tx_read_only 失败: %v\n", err)
	} else {
		logger.Debugf(" 全局级 tx_read_only: %v (类型: %T)\n", globalValue, globalValue)
	}

	// 4. 测试设置 tx_read_only 为只读
	fmt.Println("\n 测试设置 tx_read_only 为只读:")
	err = sysVarManager.SetVariable(sessionID, "tx_read_only", int64(1), manager.SessionScope)
	if err != nil {
		logger.Debugf(" 设置 tx_read_only 失败: %v\n", err)
	} else {
		logger.Debugf(" 成功设置 tx_read_only = 1\n")

		// 验证设置结果
		newValue, err := sysVarManager.GetVariable(sessionID, "tx_read_only", manager.SessionScope)
		if err != nil {
			logger.Debugf(" 重新获取 tx_read_only 失败: %v\n", err)
		} else {
			logger.Debugf(" 设置后的值: %v (类型: %T)\n", newValue, newValue)
			if intVal, ok := newValue.(int64); ok && intVal == 1 {
				logger.Debugf("   ✓ 设置成功: 1 (只读)\n")
			}
		}
	}

	// 5. 测试恢复为可读写
	fmt.Println("\n 测试恢复 tx_read_only 为可读写:")
	err = sysVarManager.SetVariable(sessionID, "tx_read_only", int64(0), manager.SessionScope)
	if err != nil {
		logger.Debugf(" 恢复 tx_read_only 失败: %v\n", err)
	} else {
		logger.Debugf(" 成功恢复 tx_read_only = 0\n")

		// 验证恢复结果
		finalValue, err := sysVarManager.GetVariable(sessionID, "tx_read_only", manager.SessionScope)
		if err != nil {
			logger.Debugf(" 最终获取 tx_read_only 失败: %v\n", err)
		} else {
			logger.Debugf(" 恢复后的值: %v (类型: %T)\n", finalValue, finalValue)
			if intVal, ok := finalValue.(int64); ok && intVal == 0 {
				logger.Debugf("   ✓ 恢复成功: 0 (可读写)\n")
			}
		}
	}

	// 6. 模拟JDBC驱动的使用场景
	fmt.Println("\n 模拟JDBC驱动使用场景:")
	fmt.Println("   查询: SELECT @@session.tx_read_only")

	value, err = sysVarManager.GetVariable(sessionID, "tx_read_only", manager.SessionScope)
	if err != nil {
		logger.Debugf(" JDBC查询失败: %v\n", err)
	} else {
		logger.Debugf(" JDBC查询结果: %v\n", value)

		// 模拟JDBC驱动的getInt()调用
		if intVal, ok := value.(int64); ok {
			logger.Debugf("   ✓ JDBC getInt()调用成功: %d\n", int(intVal))
			logger.Debugf("   ✓ 不会再出现 'Invalid value for getInt() - 'OFF'' 错误\n")
		} else {
			logger.Debugf("    JDBC getInt()调用会失败，值类型: %T\n", value)
		}
	}

	// 7. 验证其他相关变量
	fmt.Println("\n 验证其他事务相关变量:")

	// 检查 autocommit
	autocommitValue, err := sysVarManager.GetVariable(sessionID, "autocommit", manager.SessionScope)
	if err != nil {
		logger.Debugf(" 获取 autocommit 失败: %v\n", err)
	} else {
		logger.Debugf(" autocommit: %v (类型: %T)\n", autocommitValue, autocommitValue)
	}

	// 检查 tx_isolation
	isolationValue, err := sysVarManager.GetVariable(sessionID, "tx_isolation", manager.SessionScope)
	if err != nil {
		logger.Debugf(" 获取 tx_isolation 失败: %v\n", err)
	} else {
		logger.Debugf(" tx_isolation: %v (类型: %T)\n", isolationValue, isolationValue)
	}

	fmt.Println("\n🎉 tx_read_only 系统变量修复验证完成!")
	fmt.Println("现在JDBC连接应该能够正确处理事务只读状态查询了!")
}
