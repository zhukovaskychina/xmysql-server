# MVCC / 锁 文档索引

> **2026-04**：`mvcc-reports/` 含架构分析、ReadView 修复、Gap/Next-Key 设计与总结等；与 `docs/innodb/MVCC_README.md`、`transaction-reports/` 有交叉。本页为 **`mvcc-reports/`** 推荐入口。

---

## 架构与演进

| 文档 | 说明 |
|------|------|
| [MVCC_ARCHITECTURE_ANALYSIS.md](./MVCC_ARCHITECTURE_ANALYSIS.md) | 双包 MVCC 重复实现等问题（**分析/审计**） |
| [MVCC_REFACTORING_PROGRESS.md](./MVCC_REFACTORING_PROGRESS.md) | format/mvcc 统一等**重构进度** |
| [MVCC_STORE_PACKAGE_EVALUATION.md](./MVCC_STORE_PACKAGE_EVALUATION.md) | store 包 MVCC 相关评估 |

## 测试与集成

| 文档 | 说明 |
|------|------|
| [MVCC_INTEGRATION_TEST_SUMMARY.md](./MVCC_INTEGRATION_TEST_SUMMARY.md) | 集成测试总结 |

## Gap / Next-Key 锁

| 文档 | 说明 |
|------|------|
| [txn-gap-nextkey-locks.md](./txn-gap-nextkey-locks.md) | **主文档**：Gap / Next-Key / 插入意向锁等实现说明 |
| [txn-gap-nextkey-locks-summary.md](./txn-gap-nextkey-locks-summary.md) | **→ 跳转** 至 `txn-gap-nextkey-locks.md` |
| [LOCK_001_GAP_LOCK_FIX_REPORT.md](./LOCK_001_GAP_LOCK_FIX_REPORT.md) | LOCK-001：Gap 锁相关修复报告（与上互补） |

## ReadView 修复

| 文档 | 说明 |
|------|------|
| [MVCC_001_READVIEW_FIX_REPORT.md](./MVCC_001_READVIEW_FIX_REPORT.md) | MVCC-001 ReadView 修复报告 |

---

## 其它入口

- 内核说明：[../innodb/MVCC_README.md](../innodb/MVCC_README.md)  
- 事务与日志报告：[../transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md](../transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md)  
