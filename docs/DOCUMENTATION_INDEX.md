# XMySQL Server 文档索引

> **最后更新**: 2026-04-12  
> **文档总数**: 120+ 个 Markdown 文件

---

## 📁 文档结构

### 1. 📊 任务报告 (`reports/`)

#### P0 任务报告 (`reports/p0-tasks/`) - 15 个文件

**关键问题修复**：日志恢复、MVCC、锁管理、存储、索引、查询优化器

重要文档：

- `P0_P1_OVERALL_COMPLETION_REPORT.md` - P0+P1 总体完成报告
- `P0_PHASE1_COMPLETION_REPORT.md` - Phase 1 完成报告
- `P0_TASK1_FINAL_REPORT.md` - 日志恢复最终报告
- `P0_TASK2.2_GAP_LOCK_COMPLETION_REPORT.md` - Gap 锁完成报告
- `P0_TASK3_STORAGE_AND_INDEX_COMPLETION_REPORT.md` - 存储和索引完成报告
- `P0_TASK4_QUERY_OPTIMIZER_COMPLETION_REPORT.md` - 查询优化器完成报告

#### P1 任务报告 (`reports/p1-tasks/`) - 13 个文件

**功能增强**：Extent 迁移、Page 统一、FSP Header 优化

重要文档：

- `P1_PHASE2_COMPLETION_REPORT.md` - Phase 2 完成报告
- `P1_2_COMPLETION_REPORT.md` - Page 迁移完成报告
- `P1_3_COMPLETION_REPORT.md` - FSP Header 优化完成报告

#### P2 任务报告 (`reports/p2-tasks/`) - 6 个文件

**性能优化**：Extent 位图、Bitmap Manager、Page 内容访问

#### P3 任务报告 (`reports/p3-tasks/`) - 5 个文件

**架构优化**：架构分析、命名规范、代码风格

#### 阶段报告 (`reports/phases/`) - 4 个文件

- `PHASE3_COMPLETION_SUMMARY.md` - 第3阶段完成总结
- `PHASE4_COMPLETION_SUMMARY.md` - 第4阶段完成总结
- `PHASE4_OPTIMIZER_RULES_ANALYSIS.md` - 优化器规则分析
- `PHASE5_COMPLETION_SUMMARY.md` - 第5阶段完成总结

#### 阶段报告 (`reports/stages/`) - 9 个文件

- `STAGE2_COMPLETION_REPORT.md` - Stage 2 完成报告
- `STAGE3_COMPLETION_REPORT.md` - Stage 3 完成报告
- `STAGE4_DEADLOCK_DETECTION_REPORT.md` - 死锁检测报告
- `STAGE_5_OPTIMIZATION_COMPLETION_REPORT.md` - 优化完成报告

#### 任务报告 (`reports/tasks/`) - 14 个文件

具体任务的修复和实现报告

---

### 2. 🌲 B+树相关 (`btree-reports/` + `btree/`)

**推荐入口**: `btree/BTREE_DOCUMENTATION_INDEX.md`

`**btree-reports/`**（4 个文件）

- `BTREE-005_SPLIT_OPTIMIZATION_REPORT.md` - 分裂优化报告
- `BTREE_006_CACHE_LIMIT_FIX_REPORT.md` - 缓存限制修复报告
- `BTREE_FIXES_VERIFICATION_REPORT.md` - 修复验证报告
- `BTREE_IMPLEMENTATION_SUMMARY.md` - 实现总结

`**btree/`**: 架构、任务清单、实现摘要/分析等（约 11 个文件；阅读顺序见索引）

---

### 3. 💾 存储相关 (`storage-reports/` + `storage/`)

**推荐入口**: `storage/STORAGE_DOCUMENTATION_INDEX.md`

`**storage-reports/`**（9 个文件；含 `STORAGE-001_FIX_SUMMARY` 与 `STORAGE-001_CONCURRENCY_FIX_SUMMARY` 两篇**同名编号、不同专题**）

- `STORAGE-001_FIX_SUMMARY.md` - 脏页刷新等（STORAGE-001）
- `STORAGE-001_CONCURRENCY_FIX_SUMMARY.md` - 表空间扩展并发
- `STORAGE-001_DIRTY_PAGE_FLUSH_VERIFICATION.md` - 脏页刷新验证
- `STORAGE-003_TABLESPACE_DEFRAGMENTATION_REPORT.md` - 表空间碎片整理
- `STORAGE_ARCHITECTURE.md` - 存储架构
- `STORAGE_MODULE_ARCHITECTURE_ANALYSIS.md` - 存储模块架构分析
- `STORAGE_MODULE_REFACTORING_PLAN.md` - 模块重构计划
- `STORAGE_OPTIMIZATION_TASKS.md` - 存储优化任务
- `STORAGE_WRAPPER_STORE_ANALYSIS_REPORT.md` - Wrapper/Store 分析

`**storage/`**: 实现长文、STG 专题与索引（详见 `STORAGE_DOCUMENTATION_INDEX.md`）

---

### 4. 🔄 事务相关 (`transaction-reports/`) - 9 个文件

- `TRANSACTION_DOCUMENTATION_INDEX.md` - **推荐入口**（Redo / Undo / 恢复 / 日志优化）
- `TXN_001_REDO_LOG_FIX_REPORT.md` - Redo 日志修复报告
- `TXN_002_ROLLBACK_FIX_REPORT.md` - **TXN-002 Undo/回滚权威报告**
- `TXN_002_UNDO_LOG_FIX_REPORT.md` / `TXN_002_REAPPLY_COMPLETION_REPORT.md` - 已合并为跳转页 → 上文
- `TXN-003_SAVEPOINT_IMPLEMENTATION_REPORT.md` - Savepoint 实现报告
- `TXN-004_LONG_TRANSACTION_DETECTION_REPORT.md` - 长事务检测报告
- `CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md` - 崩溃恢复实现总结
- `log-optimization-summary.md` - 日志优化总结

---

### 5. 🔒 MVCC 和锁 (`mvcc-reports/`) - 9 个文件

- `MVCC_DOCUMENTATION_INDEX.md` - **推荐入口**
- `MVCC_001_READVIEW_FIX_REPORT.md` - ReadView 修复报告
- `MVCC_ARCHITECTURE_ANALYSIS.md` - MVCC 架构分析
- `MVCC_INTEGRATION_TEST_SUMMARY.md` - MVCC 集成测试总结
- `MVCC_REFACTORING_PROGRESS.md` - MVCC 重构进度
- `MVCC_STORE_PACKAGE_EVALUATION.md` - store 包 MVCC 相关评估
- `LOCK_001_GAP_LOCK_FIX_REPORT.md` - Gap 锁修复报告
- `txn-gap-nextkey-locks.md` - Gap/Next-Key 锁**主文档**
- `txn-gap-nextkey-locks-summary.md` - 已合并为跳转页 → 上文

---

### 6. 🌐 协议相关 (`protocol/` + `protocol-reports/`)

**推荐入口**: `protocol/PROTOCOL_DOCUMENTATION_INDEX.md`（JDBC 两文阅读顺序、与 `protocol-reports/` 对照表）

`**protocol/`**（6 个 Markdown，含索引）

- `JDBC_CONNECTION_FIX_SUMMARY.md` → `JDBC_CONNECTION_COMMUNICATION_FIX.md`（**请按序联读**）
- `SET_NAMES_PROTOCOL_ANALYSIS.md` / `SET_NAMES_UTF8_FIX_SUMMARY.md`
- `TX_READ_ONLY_FIX_SUMMARY.md`

`**protocol-reports/`**（5 个文件）

- `PROTO-003_PASSWORD_VERIFICATION_FIX.md` - 密码验证修复
- `PROTO-004_COLUMN_TYPE_MAPPING_FIX.md` - 列类型映射修复
- `PROTO-005_ERROR_CODE_MAPPING_REPORT.md` - 错误码映射报告
- `JDBC_PROTOCOL_ANALYSIS.md` - JDBC 协议深度分析
- `PREPARED_STATEMENT_IMPLEMENTATION_SUMMARY.md` - **→ 跳转** `development/NET-001-PREPARED-STATEMENT-SUMMARY.md`（预处理权威）

---

### 7. ⚙️ 执行器相关 (`executor-reports/`) - 4 个文件

- `EXECUTOR_DOCUMENTATION_INDEX.md` - **推荐入口**
- `EXECUTOR_ARCHITECTURE_REFACTOR_PLAN.md` - 执行器架构重构计划
- `EXECUTOR_REFACTOR_COMPLETION_REPORT.md` - 执行器重构完成报告（**权威**）
- `EXECUTOR_REFACTOR_SUMMARY.md` - 已合并为跳转页 → 上文

---

### 8. 🔍 索引相关 (`index-reports/`) - 1 个文件

- `INDEX_001_SECONDARY_INDEX_VERIFICATION_REPORT.md` - 二级索引验证报告

---

### 9. 📋 规划文档 (`planning/`) - 6+ 个文件

- `DEVELOPMENT_ROADMAP.md` - **规划导航入口**（链到任务清单、16 周计划、生产 P0、TODO 主清单等）
- `P0_PRODUCTION_TASKS.md` / `P0_PRODUCTION_CHECKLIST.md` - 生产上线任务与验收（见 `DEVELOPMENT_ROADMAP` 表）
- `PRIORITY_TASK_LIST.md` - 优先任务导航短页（→ 生产文档与 `DEVELOPMENT_ROADMAP_TASKS`）
- `TODO_DETAILED_CHECKLIST.md` - **代码内 TODO 主清单**（含文末统计附录）
- `TODO_EXECUTION_PLAN.md` / `TODO_STATISTICS_REPORT.md` / `TODO_SUMMARY.md` - **已合并为跳转页** → 上文 + `development/开发计划.md` / `DEVELOPMENT_ROADMAP_TASKS.md`

**说明**：**任务 ID、完成度、缺口**以 `development/DEVELOPMENT_ROADMAP_TASKS.md` 与根目录 `未实现功能梳理.md` 为准；规划目录内同名长文已迁入跳转或 `TODO_DETAILED_CHECKLIST` 附录。

---

### 10. 🔬 分析文档 (`analysis/`) - 4 个文件

- `PROJECT_COMPREHENSIVE_ANALYSIS.md` - 项目综合分析
- `PROJECT_EVALUATION_REPORT.md` - 项目评估报告
- `REMAINING_ISSUES_ANALYSIS.md` - 剩余问题分析
- `MISSING_FEATURES_LIST.md` - 缺失功能列表

---

### 11. 📝 总结文档 (`summaries/`) - 1 个文件

- `IMPLEMENTATION_ISSUES_SUMMARY.md` - 实现问题总结

---

### 12. 🏗️ 其他专题文档

#### 架构 (`architecture/`) - 1 个文件

- `architecture_responsibility_separation.md` - 架构职责分离

#### 开发 (`development/`) - 10+ 个文件

- `DEVELOPMENT_ROADMAP_TASKS.md` - 开发路线图任务（与代码同步的完成度）
- `NET-001-PREPARED-STATEMENT-SUMMARY.md` - **预处理语句（COM_STMT_*）权威说明**
- `PHASE3-P1-TASKS-COMPLETION-SUMMARY.md` - Phase 3 P1 任务完成总结
- 等

#### 实现 (`implementation/`) - 4 个文件

- `CREATE_DATABASE.md` - CREATE DATABASE 设计与实现（**2026-04** 与 `schema_manager` / `executor` 路径对齐）
- `DML_IMPLEMENTATION.md` - DML 实现
- `IMPLEMENTATION_SUMMARY.md` - 实现总结
- `OLD_INTERFACE_REMOVAL_SUMMARY.md` - 旧接口移除总结

#### 查询优化器 (`query-optimizer/`) - 3 个文件

- `query_optimizer_p0_implementation.md` - P0 功能实现（**含 CNF**，原 `CNF_CONVERTER_IMPLEMENTATION.md` 已并入此文）
- `query_optimizer_integration_summary.md` - 集成总结
- `QUERY_ENGINE_ANALYSIS.md` - 查询引擎分析

#### Volcano 模型 (`volcano/`) - 7 个文件

- `VOLCANO_DOCUMENTATION_INDEX.md` - **推荐入口**
- `VOLCANO_MODEL_IMPLEMENTATION.md` - Volcano 模型实现
- `VOLCANO_CLEANUP_FINAL_REPORT.md` - 清理最终报告
- `VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md` / `VOLCANO_MODEL_REFACTOR_SUMMARY.md` - 已合并为跳转页，见索引
- 等

---

## 📚 快速导航

### 按主题查找


| 主题          | 目录                                                                 | 文件数 |
| ----------- | ------------------------------------------------------------------ | --- |
| **P0 关键问题** | `reports/p0-tasks/`                                                | 15  |
| **P1 功能增强** | `reports/p1-tasks/`                                                | 13  |
| **B+树**     | `btree/`（`BTREE_DOCUMENTATION_INDEX.md`）、`btree-reports/`          | 15  |
| **存储引擎**    | `storage/`（`STORAGE_DOCUMENTATION_INDEX.md`）、`storage-reports/`    | 21+ |
| **事务和日志**   | `transaction-reports/`（索引 `TRANSACTION_DOCUMENTATION_INDEX.md`）    | 9   |
| **MVCC 和锁** | `mvcc-reports/`（索引 `MVCC_DOCUMENTATION_INDEX.md`）                  | 9   |
| **协议**      | `protocol/`（`PROTOCOL_DOCUMENTATION_INDEX.md`）、`protocol-reports/` | 11  |
| **执行器**     | `executor-reports/`（`EXECUTOR_DOCUMENTATION_INDEX.md`）             | 4   |
| **查询优化器**   | `query-optimizer/`                                                 | 3   |
| **规划**      | `planning/`                                                        | 6   |


---

## 🎯 推荐阅读顺序

### 新手入门

1. `README.md` - 项目概述
2. `analysis/PROJECT_COMPREHENSIVE_ANALYSIS.md` - 项目综合分析
3. `planning/DEVELOPMENT_ROADMAP.md` - 开发路线图

### 了解架构

1. `architecture/architecture_responsibility_separation.md` - 架构职责分离
2. `storage/STORAGE_DOCUMENTATION_INDEX.md` → `storage-reports/STORAGE_ARCHITECTURE.md` - 存储架构
3. `btree/BTREE_DOCUMENTATION_INDEX.md` → `btree/BTREE_ARCHITECTURE_OVERVIEW.md` - B+树架构概述
4. `protocol/PROTOCOL_DOCUMENTATION_INDEX.md` - 协议与 JDBC 文档入口

### 查看进度

1. `reports/p0-tasks/P0_P1_OVERALL_COMPLETION_REPORT.md` - P0+P1 总体完成报告
2. `reports/phases/PHASE5_COMPLETION_SUMMARY.md` - 最新阶段完成总结

---

**文档整理完成时间**: 2026-04-12