# XMySQL Server 文档中心

本目录包含 XMySQL Server 项目的所有技术文档，按功能模块分类组织。

## 📁 文档目录结构

### 🏗️ architecture - 架构设计

系统架构设计与职责分离相关文档

- [architecture_responsibility_separation.md](./architecture/architecture_responsibility_separation.md) - 架构职责分离说明

### 🌲 btree - B+树索引

B+树索引实现的完整文档集

- [BTREE_DOCUMENTATION_INDEX.md](./btree/BTREE_DOCUMENTATION_INDEX.md) - B+树文档索引（推荐从这里开始）
- [BTREE_ARCHITECTURE_OVERVIEW.md](./btree/BTREE_ARCHITECTURE_OVERVIEW.md) - B+树架构概览
- [BTREE_QUICKSTART_GUIDE.md](./btree/BTREE_QUICKSTART_GUIDE.md) - 快速上手指南
- [BTREE_IMPLEMENTATION_SUMMARY.md](./btree/BTREE_IMPLEMENTATION_SUMMARY.md) - 实现总结 V1
- [BTREE_IMPLEMENTATION_SUMMARY_V2.md](./btree/BTREE_IMPLEMENTATION_SUMMARY_V2.md) - 实现总结 V2
- [BTREE_IMPLEMENTATION_ANALYSIS.md](./btree/BTREE_IMPLEMENTATION_ANALYSIS.md) - 实现分析
- [BTREE_IMPLEMENTATION_REPORT.md](./btree/BTREE_IMPLEMENTATION_REPORT.md) - 实现报告
- [BTREE_CORE_IMPLEMENTATION_PLAN.md](./btree/BTREE_CORE_IMPLEMENTATION_PLAN.md) - 核心实现计划
- [BTREE_TASK_CHECKLIST.md](./btree/BTREE_TASK_CHECKLIST.md) - 任务检查清单
- [BTREE_PROGRESS_UPDATE.md](./btree/BTREE_PROGRESS_UPDATE.md) - 进度更新
- [BTREE_ISSUES_QUICK_REFERENCE.md](./btree/BTREE_ISSUES_QUICK_REFERENCE.md) - 问题快速参考

### 👤 client - 客户端

客户端工具相关文档

- [CLIENT_README.md](./client/CLIENT_README.md) - 客户端使用说明

### 📅 development - 开发计划

项目开发路线图与任务管理

- [未实现功能梳理.md](./未实现功能梳理.md) - 与代码同步的缺口与优先级总览
- [PROJECT_STATUS_AND_NEXT_STEPS.md](./development/PROJECT_STATUS_AND_NEXT_STEPS.md) - 项目状态与下一步计划
- [DEVELOPMENT_ROADMAP_TASKS.md](./development/DEVELOPMENT_ROADMAP_TASKS.md) - 开发路线图任务
- [ROADMAP_README.md](./development/ROADMAP_README.md) - 路线图说明
- [TASKS_SUMMARY.md](./development/TASKS_SUMMARY.md) - 任务总结
- [JDBC_DDL_TEST_RESULTS.md](./development/JDBC_DDL_TEST_RESULTS.md) - JDBC DDL 测试结果

### 🔧 implementation - 功能实现

各类功能实现文档

- [IMPLEMENTATION_SUMMARY.md](./implementation/IMPLEMENTATION_SUMMARY.md) - 实现总结
- [OLD_INTERFACE_REMOVAL_SUMMARY.md](./implementation/OLD_INTERFACE_REMOVAL_SUMMARY.md) - 旧接口移除总结
- [query_optimizer_p0_implementation.md](./query-optimizer/query_optimizer_p0_implementation.md) - 查询优化器 P0（含 **CNF**，原 `CNF_CONVERTER_IMPLEMENTATION.md` 已合并至此）
- [DML_IMPLEMENTATION.md](./implementation/DML_IMPLEMENTATION.md) - DML 语句实现
- [CREATE_DATABASE.md](./implementation/CREATE_DATABASE.md) - CREATE DATABASE 设计与实现（**2026-04** 与 `schema_manager` / `executor` 路径对齐）

### 💾 innodb - InnoDB 引擎

InnoDB 存储引擎内部机制文档

- [storage_architecture_doc.md](./innodb/storage_architecture_doc.md) - 存储架构文档
- [WRAPPER_README.md](./innodb/WRAPPER_README.md) - Wrapper 层说明
- [MVCC_README.md](./innodb/MVCC_README.md) - MVCC 机制说明
- [IBD_README.md](./innodb/IBD_README.md) - IBD 文件格式说明
- [PAGES_README.md](./innodb/PAGES_README.md) - 页面管理说明
- [inode_README.md](./innodb/inode_README.md) - Inode 节点说明
- [MYSQL_USER_RECORD_FORMAT.md](./innodb/MYSQL_USER_RECORD_FORMAT.md) - MySQL 用户记录格式
- [mysql_user_initialization_process.md](./innodb/mysql_user_initialization_process.md) - MySQL 用户初始化流程

### 📡 protocol - 协议实现

MySQL 协议相关文档

- [PROTOCOL_DOCUMENTATION_INDEX.md](./protocol/PROTOCOL_DOCUMENTATION_INDEX.md) - **协议与 JDBC 索引（推荐入口）**
- [SET_NAMES_PROTOCOL_ANALYSIS.md](./protocol/SET_NAMES_PROTOCOL_ANALYSIS.md) - SET NAMES 协议分析
- [SET_NAMES_UTF8_FIX_SUMMARY.md](./protocol/SET_NAMES_UTF8_FIX_SUMMARY.md) - SET NAMES UTF8 修复总结
- [TX_READ_ONLY_FIX_SUMMARY.md](./protocol/TX_READ_ONLY_FIX_SUMMARY.md) - 事务只读修复总结
- [JDBC_CONNECTION_FIX_SUMMARY.md](./protocol/JDBC_CONNECTION_FIX_SUMMARY.md) - JDBC 连接修复总结（**先读**）
- [JDBC_CONNECTION_COMMUNICATION_FIX.md](./protocol/JDBC_CONNECTION_COMMUNICATION_FIX.md) - JDBC 连接通信修复（**后读**）
- `protocol-reports/PREPARED_STATEMENT_IMPLEMENTATION_SUMMARY.md` 已合并为跳转页 → `development/NET-001-PREPARED-STATEMENT-SUMMARY.md`，见索引
- 会话 / `COM_INIT_DB` / `USE` 与 `docs/` 根目录补充说明：见索引 **「`docs/` 根目录」** 小节，或直接 [SESSION_STATE_AND_PROTOCOL.md](./SESSION_STATE_AND_PROTOCOL.md)
- [SHOW_DROP_IMPLEMENTATION.md](./SHOW_DROP_IMPLEMENTATION.md) — SHOW / DROP DATABASE 实现（亦见协议索引根目录表）

### 🔍 query-optimizer - 查询优化器

查询优化器设计与实现

- [QUERY_ENGINE_ANALYSIS.md](./query-optimizer/QUERY_ENGINE_ANALYSIS.md) - 查询引擎分析
- [query_optimizer_integration_summary.md](./query-optimizer/query_optimizer_integration_summary.md) - 查询优化器集成总结
- [query_optimizer_p0_implementation.md](./query-optimizer/query_optimizer_p0_implementation.md) - 查询优化器 P0 实现

### 💽 storage - 存储引擎

存储引擎核心实现文档

- [STORAGE_DOCUMENTATION_INDEX.md](./storage/STORAGE_DOCUMENTATION_INDEX.md) - **存储文档索引（推荐入口）**
- [STORAGE_ENGINE_FINAL_REPORT.md](./storage/STORAGE_ENGINE_FINAL_REPORT.md) - 存储引擎最终报告（状态权威）
- [STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md](./storage/STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md) - 存储引擎实现拆解（长文）
- [STORAGE_ENGINE_INTEGRATION.md](./storage/STORAGE_ENGINE_INTEGRATION.md) - 存储引擎集成（DML / B+ 树）
- `STORAGE_ENGINE_COMPLETION_SUMMARY.md`、`STORAGE_ENGINE_INTEGRATION_SUMMARY.md` 已合并为跳转页，见索引
- [STG-012_EXTENT_REUSE_IMPLEMENTATION.md](./storage/STG-012_EXTENT_REUSE_IMPLEMENTATION.md) - 区段重用实现
- [STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md](./storage/STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md) - 动态格式实现
- [PERSISTENCE_IMPLEMENTATION_SUMMARY.md](./storage/PERSISTENCE_IMPLEMENTATION_SUMMARY.md) - 持久化实现总结
- [storage_architecture_clarification.md](./storage/storage_architecture_clarification.md) - 存储架构说明
- [system_space_manager_final_architecture.md](./storage/system_space_manager_final_architecture.md) - 系统表空间管理器最终架构
- [ibdata1_independent_tablespace_mapping.md](./storage/ibdata1_independent_tablespace_mapping.md) - ibdata1 独立表空间映射

### 🌋 volcano - 火山模型执行器

火山模型执行器相关文档

- [VOLCANO_DOCUMENTATION_INDEX.md](./volcano/VOLCANO_DOCUMENTATION_INDEX.md) - **文档索引（推荐入口）**
- [VOLCANO_MODEL_IMPLEMENTATION.md](./volcano/VOLCANO_MODEL_IMPLEMENTATION.md) - 火山模型实现（主文档）
- [VOLCANO_MODEL_ANALYSIS_SUMMARY.md](./volcano/VOLCANO_MODEL_ANALYSIS_SUMMARY.md) - 代码重复与双接口分析
- [VOLCANO_CLEANUP_FINAL_REPORT.md](./volcano/VOLCANO_CLEANUP_FINAL_REPORT.md) - 清理最终报告
- [VOLCANO_CLEANUP_CHANGELOG.md](./volcano/VOLCANO_CLEANUP_CHANGELOG.md) - 清理变更日志
- [VOLCANO_MODEL_CLEANUP_PLAN.md](./volcano/VOLCANO_MODEL_CLEANUP_PLAN.md) - 清理计划
- `VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md`、`VOLCANO_MODEL_REFACTOR_SUMMARY.md` 已合并为跳转页，见索引

### ⚙️ executor-reports - 执行器重构报告

- [EXECUTOR_DOCUMENTATION_INDEX.md](./executor-reports/EXECUTOR_DOCUMENTATION_INDEX.md) - **索引（推荐入口）**
- [EXECUTOR_REFACTOR_COMPLETION_REPORT.md](./executor-reports/EXECUTOR_REFACTOR_COMPLETION_REPORT.md) - 重构完成报告（权威）
- [EXECUTOR_ARCHITECTURE_REFACTOR_PLAN.md](./executor-reports/EXECUTOR_ARCHITECTURE_REFACTOR_PLAN.md) - 重构计划
- `EXECUTOR_REFACTOR_SUMMARY.md` 已合并为跳转页，见索引

### 🔄 transaction-reports - 事务与日志（Redo / Undo / 恢复）

- [TRANSACTION_DOCUMENTATION_INDEX.md](./transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md) - **索引（推荐入口）**
- [CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md](./transaction-reports/CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md) - 崩溃恢复实现总结
- [TXN_002_ROLLBACK_FIX_REPORT.md](./transaction-reports/TXN_002_ROLLBACK_FIX_REPORT.md) - TXN-002 Undo/回滚（权威）
- `TXN_002_UNDO_LOG_FIX_REPORT.md`、`TXN_002_REAPPLY_COMPLETION_REPORT.md` 为跳转页，见索引

### 🔒 mvcc-reports - MVCC 与锁

- [MVCC_DOCUMENTATION_INDEX.md](./mvcc-reports/MVCC_DOCUMENTATION_INDEX.md) - **索引（推荐入口）**
- [txn-gap-nextkey-locks.md](./mvcc-reports/txn-gap-nextkey-locks.md) - Gap / Next-Key 锁主文档
- [MVCC_ARCHITECTURE_ANALYSIS.md](./mvcc-reports/MVCC_ARCHITECTURE_ANALYSIS.md) - MVCC 架构分析
- `txn-gap-nextkey-locks-summary.md` 已合并为跳转页，见索引

## 📖 快速导航

### 新手入门

1. 先阅读 [项目主 README](../README.md) 了解项目概况
2. 查看 [项目状态与下一步计划](./development/PROJECT_STATUS_AND_NEXT_STEPS.md) 了解当前进度
3. 阅读 [架构职责分离](./architecture/architecture_responsibility_separation.md) 理解系统架构

### 核心模块学习路径

- **B+树索引**: 从 [B+树文档索引](./btree/BTREE_DOCUMENTATION_INDEX.md) 开始
- **存储引擎**: 从 [存储文档索引](./storage/STORAGE_DOCUMENTATION_INDEX.md) 进入 [存储引擎最终报告](./storage/STORAGE_ENGINE_FINAL_REPORT.md)
- **查询执行**: 从 [Volcano 文档索引](./volcano/VOLCANO_DOCUMENTATION_INDEX.md) 进入 [火山模型实现](./volcano/VOLCANO_MODEL_IMPLEMENTATION.md)；执行器重构闭环见 [执行器文档索引](./executor-reports/EXECUTOR_DOCUMENTATION_INDEX.md)
- **事务与崩溃恢复**: 从 [事务文档索引](./transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md) 进入 [崩溃恢复总结](./transaction-reports/CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md)
- **MVCC / 锁**: 从 [MVCC 文档索引](./mvcc-reports/MVCC_DOCUMENTATION_INDEX.md) 进入 [Gap/Next-Key 主文档](./mvcc-reports/txn-gap-nextkey-locks.md)
- **协议层**: 从 [协议文档索引](./protocol/PROTOCOL_DOCUMENTATION_INDEX.md) 进入 `protocol/` 与 `protocol-reports/`

### 开发者指南

- [规划导航](./planning/DEVELOPMENT_ROADMAP.md)（指向 16 周计划、114 项任务、P0 上线文档）
- [未实现功能梳理](./未实现功能梳理.md)
- [开发任务清单](./development/DEVELOPMENT_ROADMAP_TASKS.md)
- [任务总结](./development/TASKS_SUMMARY.md)
- [实现总结](./implementation/IMPLEMENTATION_SUMMARY.md)

## 🔄 文档更新

文档最后整理时间: 2026-04-12（CREATE DATABASE 正文恢复、协议根目录文档纳入索引、执行器/存储「重估」归档说明已与代码对齐）

---

**注意**: 本文档中心会随项目开发持续更新，建议定期查看最新版本。