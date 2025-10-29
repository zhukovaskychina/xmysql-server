# XMySQL Server 开发路线图

> **创建日期**: 2025-10-29  
> **总工作量**: 43-59 天  
> **当前进度**: 第2阶段已完成 ✅

---

## 📊 总体进度概览

| 阶段 | 时间 | 工作量 | 状态 | 完成度 |
|------|------|--------|------|--------|
| **第1阶段: 修复 B+树并发问题** | Week 1-2 | 5-6天 | 🔴 未开始 | 0% |
| **第2阶段: 实现预编译语句** | Week 3-4 | 9-13天 | ✅ **已完成** | **100%** |
| **第3阶段: 完善日志恢复** | Week 5-8 | 12-16天 | 🔴 未开始 | 0% |
| **第4阶段: 实现核心优化规则** | Week 9-12 | 14-19天 | 🔴 未开始 | 0% |
| **第5阶段: 清理旧代码** | Week 13 | 3-5天 | 🔴 未开始 | 0% |

**总计**: 43-59 天 | **已完成**: 9-13 天 (21-30%)

---

## 🎯 第1阶段: 修复 B+树并发问题

**时间**: Week 1-2 (5-6天)  
**目标**: 确保 B+树索引的并发安全性和稳定性  
**状态**: 🔴 未开始

### 任务列表

#### BTREE-001: 修复死锁风险 🔴 P0
- **描述**: 重构 `bplus_tree_manager.go:438-489` 中的嵌套加锁逻辑，避免死锁
- **位置**: `server/innodb/manager/bplus_tree_manager.go`
- **工作量**: 2天
- **优先级**: P0（严重）
- **问题**: 嵌套加锁可能导致死锁，影响系统稳定性

**修复要点**:
```go
// ❌ 当前实现（嵌套加锁）
func (m *BPlusTreeManager) operation() {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // 调用其他需要加锁的方法
    m.anotherOperation()  // 可能导致死锁
}

// ✅ 修复后（避免嵌套加锁）
func (m *BPlusTreeManager) operation() {
    m.mu.Lock()
    data := m.getData()
    m.mu.Unlock()
    
    // 在锁外处理数据
    processData(data)
}
```

---

#### BTREE-002: 修复缓存淘汰竞态条件 🔴 P0
- **描述**: 修复 `bplus_tree_manager.go:227-268` 中的缓存淘汰竞态条件，防止数据竞态和 panic
- **位置**: `server/innodb/manager/bplus_tree_manager.go`
- **工作量**: 2天
- **优先级**: P0（严重）
- **问题**: 缓存淘汰时可能出现数据竞态，导致 panic

**修复要点**:
```go
// ❌ 当前实现（竞态条件）
func (m *BPlusTreeManager) evictCache() {
    for key, node := range m.cache {
        if shouldEvict(node) {
            delete(m.cache, key)  // 可能与其他goroutine冲突
        }
    }
}

// ✅ 修复后（加锁保护）
func (m *BPlusTreeManager) evictCache() {
    m.cacheMu.Lock()
    defer m.cacheMu.Unlock()
    
    keysToEvict := []uint32{}
    for key, node := range m.cache {
        if shouldEvict(node) {
            keysToEvict = append(keysToEvict, key)
        }
    }
    
    for _, key := range keysToEvict {
        delete(m.cache, key)
    }
}
```

---

#### BTREE-003: 修复页面分配硬编码问题 🔴 P0
- **描述**: 将硬编码的页面分配逻辑集成到统一的页面分配器中
- **位置**: `server/innodb/manager/bplus_tree_manager.go:917`
- **工作量**: 1-2天
- **优先级**: P0（严重）
- **问题**: 页面分配逻辑硬编码，难以维护和扩展

**修复要点**:
```go
// ❌ 当前实现（硬编码）
func (m *BPlusTreeManager) allocatePage() uint32 {
    return m.nextPageID++  // 硬编码的分配逻辑
}

// ✅ 修复后（使用统一分配器）
func (m *BPlusTreeManager) allocatePage() uint32 {
    return m.pageAllocator.AllocatePage()  // 使用统一的页面分配器
}
```

---

## ✅ 第2阶段: 实现预编译语句

**时间**: Week 3-4 (9-13天)  
**目标**: 实现完整的 MySQL 预编译语句协议支持  
**状态**: ✅ **已完成** (2025-10-29)

### 任务列表

#### PROTO-001: 实现 COM_STMT_PREPARE 协议 ✅ P0
- **描述**: 实现 COM_STMT_PREPARE (0x16) 协议处理，创建 PreparedStatementManager
- **位置**: `server/protocol/mysql_protocol.go`, `server/protocol/prepared_statement_manager.go`
- **工作量**: 5-7天
- **优先级**: P0（严重）
- **状态**: ✅ 已完成 (2025-10-29)

**实现成果**:
- ✅ 创建了 `PreparedStatementManager` 管理器
- ✅ 实现了语句缓存（线程安全）
- ✅ 实现了参数提取和元数据生成
- ✅ 实现了响应包编码

---

#### JDBC-EXECUTE-001: 实现 COM_STMT_EXECUTE 协议 ✅ P0
- **描述**: 实现 COM_STMT_EXECUTE (0x17) 协议处理，包括参数绑定和类型转换
- **位置**: `server/protocol/mysql_protocol.go`
- **工作量**: 4-6天
- **优先级**: P0（严重）
- **状态**: ✅ 已完成 (2025-10-29)

**实现成果**:
- ✅ 实现了参数解析（NULL 位图、类型、值）
- ✅ 支持所有常用 MySQL 类型（TINY, SHORT, LONG, LONGLONG, VARCHAR 等）
- ✅ 实现了参数绑定到 SQL
- ✅ 实现了查询执行和结果返回

---

#### PROTO-002: 实现 COM_STMT_CLOSE 协议 ✅ P0
- **描述**: 实现 COM_STMT_CLOSE (0x19) 协议处理，释放语句资源
- **位置**: `server/protocol/mysql_protocol.go`
- **工作量**: 1天
- **优先级**: P0（严重）
- **状态**: ✅ 已完成 (2025-10-29)

**实现成果**:
- ✅ 实现了语句关闭处理
- ✅ 实现了资源释放
- ✅ 符合 MySQL 协议（不返回响应）

---

## 🔧 第3阶段: 完善日志恢复

**时间**: Week 5-8 (12-16天)  
**目标**: 实现完整的崩溃恢复机制，确保数据持久性和一致性  
**状态**: 🔴 未开始

### 任务列表

#### TXN-001: 实现 Redo 日志重放 🔴 P0
- **描述**: 完善 Redo 日志的重放逻辑，确保崩溃后可以恢复已提交的事务
- **位置**: `server/innodb/manager/redo_log_manager.go`
- **工作量**: 6-8天
- **优先级**: P0（严重）
- **参考文档**: `docs/BTREE_IMPLEMENTATION_ANALYSIS.md` 中的 TXN-001 问题

**实现要点**:
1. 实现 Redo 日志的持久化写入
2. 实现崩溃恢复时的日志扫描
3. 实现日志重放逻辑（重做已提交的事务）
4. 实现 Checkpoint 机制（减少恢复时间）
5. 编写恢复测试用例

---

#### TXN-002: 实现 Undo 日志回滚 🔴 P0
- **描述**: 完善 Undo 日志的回滚逻辑，确保未提交的事务可以正确回滚
- **位置**: `server/innodb/manager/undo_log_manager.go`
- **工作量**: 6-8天
- **优先级**: P0（严重）
- **参考文档**: `docs/BTREE_IMPLEMENTATION_ANALYSIS.md` 中的 TXN-002 问题

**实现要点**:
1. 实现 Undo 日志的记录（INSERT/UPDATE/DELETE）
2. 实现事务回滚逻辑
3. 实现崩溃恢复时的未提交事务回滚
4. 实现 Undo 日志的清理机制
5. 编写回滚测试用例

---

## 🚀 第4阶段: 实现核心优化规则

**时间**: Week 9-12 (14-19天)  
**目标**: 实现查询优化器的核心优化规则，提升查询性能  
**状态**: 🔴 未开始

### 任务列表

#### OPT-001: 实现谓词下推优化 🔴 P1
- **描述**: 实现谓词下推（Predicate Pushdown）优化规则，将过滤条件尽早应用
- **位置**: `server/optimizer/rule/predicate_pushdown.go`
- **工作量**: 5-7天
- **优先级**: P1（高）
- **参考文档**: `docs/PROJECT_EVALUATION_REPORT.md` 中的 OPT-001 问题

**优化示例**:
```sql
-- 优化前
SELECT * FROM (SELECT * FROM users) AS u WHERE u.age > 18;

-- 优化后（谓词下推）
SELECT * FROM (SELECT * FROM users WHERE age > 18) AS u;
```

**预期性能提升**: 10-100 倍（取决于数据量）

---

#### OPT-002: 实现列裁剪优化 🔴 P1
- **描述**: 实现列裁剪（Column Pruning）优化规则，只读取需要的列
- **位置**: `server/optimizer/rule/column_pruning.go`
- **工作量**: 4-6天
- **优先级**: P1（高）
- **参考文档**: `docs/PROJECT_EVALUATION_REPORT.md` 中的 OPT-002 问题

**优化示例**:
```sql
-- 优化前
SELECT name FROM (SELECT * FROM users) AS u;

-- 优化后（列裁剪）
SELECT name FROM (SELECT name FROM users) AS u;
```

**预期性能提升**: 2-5 倍（取决于列数量）

---

#### OPT-003: 实现子查询优化 🔴 P1
- **描述**: 实现子查询优化规则，将子查询转换为 JOIN 或其他高效形式
- **位置**: `server/optimizer/rule/subquery_optimization.go`
- **工作量**: 5-6天
- **优先级**: P1（高）
- **参考文档**: `docs/PROJECT_EVALUATION_REPORT.md` 中的 OPT-003 问题

**优化示例**:
```sql
-- 优化前（子查询）
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);

-- 优化后（JOIN）
SELECT DISTINCT u.* FROM users u INNER JOIN orders o ON u.id = o.user_id;
```

**预期性能提升**: 5-50 倍（取决于数据量）

---

## 🧹 第5阶段: 清理旧代码

**时间**: Week 13 (3-5天)  
**目标**: 清理废弃代码，提升代码可维护性  
**状态**: 🔴 未开始

### 任务列表

#### EXEC-001: 清理旧版执行器代码 🔴 P2
- **描述**: 删除或重构旧版执行器代码，统一使用新版执行器
- **位置**: `server/executor/` 目录
- **工作量**: 3-5天
- **优先级**: P2（中）
- **参考文档**: `docs/IMPLEMENTATION_ISSUES_SUMMARY.md` 中的 EXEC-001 问题

**清理内容**:
1. 删除废弃的执行器接口
2. 删除重复的执行器实现
3. 统一使用新版执行器
4. 更新相关文档
5. 更新测试用例

---

## 📈 进度跟踪

### 已完成任务 ✅

| 任务ID | 任务名称 | 完成日期 | 工作量 |
|--------|---------|---------|--------|
| PROTO-001 | 实现 COM_STMT_PREPARE 协议 | 2025-10-29 | 5-7天 |
| JDBC-EXECUTE-001 | 实现 COM_STMT_EXECUTE 协议 | 2025-10-29 | 4-6天 |
| PROTO-002 | 实现 COM_STMT_CLOSE 协议 | 2025-10-29 | 1天 |

**已完成工作量**: 9-13 天

---

### 待完成任务 🔴

| 优先级 | 任务数量 | 工作量 |
|--------|---------|--------|
| **P0（严重）** | 5个 | 22-30天 |
| **P1（高）** | 3个 | 14-19天 |
| **P2（中）** | 1个 | 3-5天 |

**待完成工作量**: 34-46 天

---

## 🎯 下一步行动

### 立即开始（P0 任务）

1. **BTREE-001**: 修复死锁风险（2天）
2. **BTREE-002**: 修复缓存淘汰竞态条件（2天）
3. **BTREE-003**: 修复页面分配硬编码问题（1-2天）

**预计完成时间**: 5-6 天

---

### 近期计划（P0 任务）

4. **TXN-001**: 实现 Redo 日志重放（6-8天）
5. **TXN-002**: 实现 Undo 日志回滚（6-8天）

**预计完成时间**: 12-16 天

---

### 中期计划（P1 任务）

6. **OPT-001**: 实现谓词下推优化（5-7天）
7. **OPT-002**: 实现列裁剪优化（4-6天）
8. **OPT-003**: 实现子查询优化（5-6天）

**预计完成时间**: 14-19 天

---

### 长期计划（P2 任务）

9. **EXEC-001**: 清理旧版执行器代码（3-5天）

**预计完成时间**: 3-5 天

---

## 📚 相关文档

- [项目整体评估报告](./PROJECT_EVALUATION_REPORT.md)
- [JDBC 协议分析](./JDBC_PROTOCOL_ANALYSIS.md)
- [B+树实现分析](./BTREE_IMPLEMENTATION_ANALYSIS.md)
- [实现问题汇总](./IMPLEMENTATION_ISSUES_SUMMARY.md)
- [预编译语句实现总结](./PREPARED_STATEMENT_IMPLEMENTATION_SUMMARY.md)

---

## 📊 总结

**总工作量**: 43-59 天  
**已完成**: 9-13 天 (21-30%)  
**待完成**: 34-46 天 (70-79%)

**当前状态**: 第2阶段（预编译语句）已完成 ✅  
**下一阶段**: 第1阶段（修复 B+树并发问题）🔴

**建议**: 优先完成 P0 任务，确保系统稳定性和数据一致性。

