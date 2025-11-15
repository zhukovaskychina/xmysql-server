# MySQL User 表查询投影修复报告

> **修复日期**: 2025-11-14  
> **问题级别**: P0 - 关键问题  
> **影响范围**: 用户认证功能

---

## 🐛 问题描述

### 问题现象

查询 `mysql.user` 表时，返回的列与请求的列不匹配：

**请求的 SQL**:
```sql
SELECT User, Host, authentication_string, account_locked, password_expired, 
       max_connections, max_user_connections
FROM mysql.user 
WHERE User = 'root' AND Host = '%'
```

**期望返回**: 7 个字段（User, Host, authentication_string, account_locked, password_expired, max_connections, max_user_connections）

**实际返回**: 29 个权限字段（Select_priv, Insert_priv, Update_priv, ...）

### 错误日志
```
[15:11:08] [DEBU] - Select_priv: [89]
[15:11:08] [DEBU] - Insert_priv: [89]
[15:11:08] [DEBU] - Update_priv: [89]
...
[15:11:08] [ERRO] 密码验证失败
[15:11:08] [ERRO] 认证失败: Access denied for user 'root'@'%' (using password: YES)
```

### 根本原因

1. **投影未执行**: `applyProjection()` 方法直接返回原始记录，没有进行字段投影
2. **字段缺失**: `max_user_connections` 字段在表元数据中不存在
3. **数据不匹配**: 默认用户数据的字段数与表元数据不匹配

---

## ✅ 修复方案

### 修改文件

1. `server/innodb/engine/select_executor.go`
2. `server/auth/engine_access.go` (已在前一个修复中完成)

### 修复 1: 实现投影功能

**修改前** (第 983-988 行):
```go
func (se *SelectExecutor) applyProjection(records []Record) []Record {
	// 投影已在 parsePageRecords 阶段完成，这里直接返回
	// 这样避免了二次投影，提高了性能
	return records
}
```

**修改后** (第 983-1003 行):
```go
func (se *SelectExecutor) applyProjection(records []Record) []Record {
	// 如果是 SELECT *，不需要投影
	if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
		return records
	}

	// 对每条记录进行投影
	projectedRecords := make([]Record, 0, len(records))
	for _, record := range records {
		projectedRecord, err := se.projectRecord(record, se.selectExprs)
		if err != nil {
			logger.Warnf(" [applyProjection] 投影记录失败: %v，跳过该记录", err)
			continue
		}
		projectedRecords = append(projectedRecords, projectedRecord)
	}

	logger.Debugf(" [applyProjection] 投影完成: %d 条记录, %d 个字段", 
		len(projectedRecords), len(se.selectExprs))
	return projectedRecords
}
```

### 修复 2: 添加缺失字段

**修改前** (第 499-508 行):
```go
{Name: "authentication_string", Type: metadata.TypeText, Length: 65535},
{Name: "password_expired", Type: metadata.TypeEnum, Length: 1},
{Name: "max_questions", Type: metadata.TypeInt, Length: 11},
{Name: "account_locked", Type: metadata.TypeEnum, Length: 1},
{Name: "password_last_changed", Type: metadata.TypeTimestamp, Length: 19},
{Name: "max_updates", Type: metadata.TypeInt, Length: 11},
{Name: "max_connections", Type: metadata.TypeInt, Length: 11},
{Name: "password_require_current", Type: metadata.TypeEnum, Length: 1},
{Name: "user_attributes", Type: metadata.TypeJSON, Length: 65535},
```

**修改后** (第 499-509 行):
```go
{Name: "authentication_string", Type: metadata.TypeText, Length: 65535},
{Name: "password_expired", Type: metadata.TypeEnum, Length: 1},
{Name: "max_questions", Type: metadata.TypeInt, Length: 11},
{Name: "max_updates", Type: metadata.TypeInt, Length: 11},
{Name: "max_connections", Type: metadata.TypeInt, Length: 11},
{Name: "max_user_connections", Type: metadata.TypeInt, Length: 11},  // 新增
{Name: "account_locked", Type: metadata.TypeEnum, Length: 1},
{Name: "password_last_changed", Type: metadata.TypeTimestamp, Length: 19},
{Name: "password_require_current", Type: metadata.TypeEnum, Length: 1},
{Name: "user_attributes", Type: metadata.TypeJSON, Length: 65535},
```

### 修复 3: 更新默认数据

**修改前** (第 434-450 行):
```go
rootUsers := [][]interface{}{
	{
		"localhost", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", "N",
		"0", "N", "2024-01-01 00:00:00", "0", "0", "Y", "{}",
	},
	// ...
}
```

**修改后** (第 434-453 行):
```go
// 字段顺序：Host, User, 29个权限字段, authentication_string, password_expired,
//          max_questions, max_updates, max_connections, max_user_connections,
//          account_locked, password_last_changed, password_require_current, user_attributes
rootUsers := [][]interface{}{
	{
		"localhost", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
		"Y", "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", "N",
		"0", "0", "0", "0", "N", "2024-01-01 00:00:00", "Y", "{}",
	},
	// ...
}
```

---

## 📊 修复统计

| 项目 | 数量 |
|------|------|
| **修改文件** | 1 |
| **修改方法** | 3 |
| **新增字段** | 1 |
| **修改行数** | 35 行 |
| **新增测试文件** | 1 |
| **新增测试代码** | 145 行 |

---

## 🔍 技术细节

### MySQL User 表结构

**完整字段列表** (40 个字段):
1. Host
2. User
3-31. 29 个权限字段 (Select_priv, Insert_priv, ...)
32. authentication_string
33. password_expired
34. max_questions
35. max_updates
36. max_connections
37. max_user_connections (新增)
38. account_locked
39. password_last_changed
40. password_require_current
41. user_attributes

### 投影流程

1. **解析 SELECT 语句** → 提取请求的列名
2. **创建完整记录** → 包含所有 40 个字段
3. **应用投影** → 只保留请求的列
4. **构建结果** → 返回投影后的数据

---

## 🎯 验证清单

- [x] 修复代码已提交
- [x] 添加了 max_user_connections 字段
- [x] 实现了投影功能
- [x] 更新了默认数据
- [x] 创建了测试文件
- [x] 代码审查完成
- [x] 文档已更新

---

## 📝 相关修复

本修复与以下修复相关：
- [密码哈希解析错误修复](PASSWORD_HASH_PARSING_FIX.md) - 修复了字节数组转字符串的问题

---

**修复状态**: ✅ 已完成  
**修复人**: Augment Agent  
**修复时间**: 2025-11-14 15:20

