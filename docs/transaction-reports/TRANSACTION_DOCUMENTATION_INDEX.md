# 事务与日志（Redo / Undo / 恢复）文档索引

> **2026-04**：`transaction-reports/`（当前 9 个 Markdown 文件）收录 Redo/Undo 修复报告、Savepoint、长事务检测、**崩溃恢复**与日志优化总结等；与 `mvcc-reports/`（可见性、Gap/Next-Key）、`innodb/` 内核说明有交叉。本页为 **`transaction-reports/`** 推荐入口。

---

## 权威与主线

| 文档 | 说明 |
|------|------|
| [TXN_002_ROLLBACK_FIX_REPORT.md](./TXN_002_ROLLBACK_FIX_REPORT.md) | **TXN-002**：Undo / 回滚修复（唯一维护正文） |
| [CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md](./CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md) | **崩溃恢复**：ARIES 三阶段与集成说明 |

## 其它 TXN / 日志报告

| 文档 | 说明 |
|------|------|
| [TXN_001_REDO_LOG_FIX_REPORT.md](./TXN_001_REDO_LOG_FIX_REPORT.md) | TXN-001：Redo 日志相关修复 |
| [TXN-003_SAVEPOINT_IMPLEMENTATION_REPORT.md](./TXN-003_SAVEPOINT_IMPLEMENTATION_REPORT.md) | Savepoint 实现报告 |
| [TXN-004_LONG_TRANSACTION_DETECTION_REPORT.md](./TXN-004_LONG_TRANSACTION_DETECTION_REPORT.md) | 长事务检测 |
| [log-optimization-summary.md](./log-optimization-summary.md) | LOG-003/007/008 等日志优化总结 |

---

## 其它入口

- MVCC / 锁：[../mvcc-reports/MVCC_DOCUMENTATION_INDEX.md](../mvcc-reports/MVCC_DOCUMENTATION_INDEX.md)  
- 规划导航：[../planning/DEVELOPMENT_ROADMAP.md](../planning/DEVELOPMENT_ROADMAP.md)  
- 路线图任务：[../planning/CAPABILITY_PRIORITY_INDEX_20260715.md](../planning/CAPABILITY_PRIORITY_INDEX_20260715.md)
- 全库文档表：[../README.md](../README.md)
