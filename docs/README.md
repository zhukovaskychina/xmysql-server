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
- [PROJECT_STATUS_AND_NEXT_STEPS.md](./development/PROJECT_STATUS_AND_NEXT_STEPS.md) - 项目状态与下一步计划
- [DEVELOPMENT_ROADMAP_TASKS.md](./development/DEVELOPMENT_ROADMAP_TASKS.md) - 开发路线图任务
- [ROADMAP_README.md](./development/ROADMAP_README.md) - 路线图说明
- [TASKS_SUMMARY.md](./development/TASKS_SUMMARY.md) - 任务总结
- [JDBC_DDL_TEST_RESULTS.md](./development/JDBC_DDL_TEST_RESULTS.md) - JDBC DDL 测试结果

### 🔧 implementation - 功能实现
各类功能实现文档
- [IMPLEMENTATION_SUMMARY.md](./implementation/IMPLEMENTATION_SUMMARY.md) - 实现总结
- [OLD_INTERFACE_REMOVAL_SUMMARY.md](./implementation/OLD_INTERFACE_REMOVAL_SUMMARY.md) - 旧接口移除总结
- [CNF_CONVERTER_IMPLEMENTATION.md](./implementation/CNF_CONVERTER_IMPLEMENTATION.md) - CNF 转换器实现
- [DML_IMPLEMENTATION.md](./implementation/DML_IMPLEMENTATION.md) - DML 语句实现
- [CREATE_DATABASE_ANALYSIS.md](./implementation/CREATE_DATABASE_ANALYSIS.md) - CREATE DATABASE 分析
- [CREATE_DATABASE_IMPLEMENTATION_COMPLETE.md](./implementation/CREATE_DATABASE_IMPLEMENTATION_COMPLETE.md) - CREATE DATABASE 实现完成
- [CREATE_DATABASE_MYSQL_STANDARD_IMPLEMENTATION.md](./implementation/CREATE_DATABASE_MYSQL_STANDARD_IMPLEMENTATION.md) - CREATE DATABASE MySQL 标准实现

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
- [SET_NAMES_PROTOCOL_ANALYSIS.md](./protocol/SET_NAMES_PROTOCOL_ANALYSIS.md) - SET NAMES 协议分析
- [SET_NAMES_UTF8_FIX_SUMMARY.md](./protocol/SET_NAMES_UTF8_FIX_SUMMARY.md) - SET NAMES UTF8 修复总结
- [TX_READ_ONLY_FIX_SUMMARY.md](./protocol/TX_READ_ONLY_FIX_SUMMARY.md) - 事务只读修复总结
- [JDBC_CONNECTION_COMMUNICATION_FIX.md](./protocol/JDBC_CONNECTION_COMMUNICATION_FIX.md) - JDBC 连接通信修复
- [JDBC_CONNECTION_FIX_SUMMARY.md](./protocol/JDBC_CONNECTION_FIX_SUMMARY.md) - JDBC 连接修复总结

### 🔍 query-optimizer - 查询优化器
查询优化器设计与实现
- [QUERY_ENGINE_ANALYSIS.md](./query-optimizer/QUERY_ENGINE_ANALYSIS.md) - 查询引擎分析
- [query_optimizer_integration_summary.md](./query-optimizer/query_optimizer_integration_summary.md) - 查询优化器集成总结
- [query_optimizer_p0_implementation.md](./query-optimizer/query_optimizer_p0_implementation.md) - 查询优化器 P0 实现

### 💽 storage - 存储引擎
存储引擎核心实现文档
- [STORAGE_ENGINE_FINAL_REPORT.md](./storage/STORAGE_ENGINE_FINAL_REPORT.md) - 存储引擎最终报告
- [STORAGE_ENGINE_COMPLETION_SUMMARY.md](./storage/STORAGE_ENGINE_COMPLETION_SUMMARY.md) - 存储引擎完成总结
- [STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md](./storage/STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md) - 存储引擎实现总结
- [STORAGE_ENGINE_INTEGRATION.md](./storage/STORAGE_ENGINE_INTEGRATION.md) - 存储引擎集成
- [STORAGE_ENGINE_INTEGRATION_SUMMARY.md](./storage/STORAGE_ENGINE_INTEGRATION_SUMMARY.md) - 存储引擎集成总结
- [STG-012_EXTENT_REUSE_IMPLEMENTATION.md](./storage/STG-012_EXTENT_REUSE_IMPLEMENTATION.md) - 区段重用实现
- [STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md](./storage/STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md) - 动态格式实现
- [PERSISTENCE_IMPLEMENTATION_SUMMARY.md](./storage/PERSISTENCE_IMPLEMENTATION_SUMMARY.md) - 持久化实现总结
- [storage_architecture_clarification.md](./storage/storage_architecture_clarification.md) - 存储架构说明
- [system_space_manager_final_architecture.md](./storage/system_space_manager_final_architecture.md) - 系统表空间管理器最终架构
- [ibdata1_independent_tablespace_mapping.md](./storage/ibdata1_independent_tablespace_mapping.md) - ibdata1 独立表空间映射

### 🌋 volcano - 火山模型执行器
火山模型执行器相关文档
- [VOLCANO_CLEANUP_FINAL_REPORT.md](./volcano/VOLCANO_CLEANUP_FINAL_REPORT.md) - 火山模型清理最终报告
- [VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md](./volcano/VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md) - 火山模型清理实现总结
- [VOLCANO_CLEANUP_CHANGELOG.md](./volcano/VOLCANO_CLEANUP_CHANGELOG.md) - 火山模型清理变更日志
- [VOLCANO_MODEL_IMPLEMENTATION.md](./volcano/VOLCANO_MODEL_IMPLEMENTATION.md) - 火山模型实现
- [VOLCANO_MODEL_REFACTOR_SUMMARY.md](./volcano/VOLCANO_MODEL_REFACTOR_SUMMARY.md) - 火山模型重构总结
- [VOLCANO_MODEL_ANALYSIS_SUMMARY.md](./volcano/VOLCANO_MODEL_ANALYSIS_SUMMARY.md) - 火山模型分析总结
- [VOLCANO_MODEL_CLEANUP_PLAN.md](./volcano/VOLCANO_MODEL_CLEANUP_PLAN.md) - 火山模型清理计划

## 📖 快速导航

### 新手入门
1. 先阅读 [项目主 README](../README.md) 了解项目概况
2. 查看 [项目状态与下一步计划](./development/PROJECT_STATUS_AND_NEXT_STEPS.md) 了解当前进度
3. 阅读 [架构职责分离](./architecture/architecture_responsibility_separation.md) 理解系统架构

### 核心模块学习路径
- **B+树索引**: 从 [B+树文档索引](./btree/BTREE_DOCUMENTATION_INDEX.md) 开始
- **存储引擎**: 查看 [存储引擎最终报告](./storage/STORAGE_ENGINE_FINAL_REPORT.md)
- **查询执行**: 阅读 [火山模型实现](./volcano/VOLCANO_MODEL_IMPLEMENTATION.md)
- **协议层**: 参考 [协议实现文档](./protocol/)

### 开发者指南
- [开发路线图](./development/DEVELOPMENT_ROADMAP_TASKS.md)
- [任务总结](./development/TASKS_SUMMARY.md)
- [实现总结](./implementation/IMPLEMENTATION_SUMMARY.md)

## 🔄 文档更新
文档最后整理时间: 2025-10-28

---
**注意**: 本文档中心会随项目开发持续更新，建议定期查看最新版本。
