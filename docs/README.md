# XMySQL Server 文档中心

> 最后整理：2026-11-13
>
> 本文是 `docs/` 的唯一导航入口。历史一次性计划、旧评估报告和重复任务清单已删除，
> 需要追溯时请使用 `git log -- docs/<path>`。

---

## 1. 当前权威入口

判断项目现状、优先级和上线条件时，只以下列文档为准：

| 主题 | 文档 |
|---|---|
| 能力优先级总索引 | [CAPABILITY_PRIORITY_INDEX_20260715.md](planning/CAPABILITY_PRIORITY_INDEX_20260715.md) |
| P0 生产阻塞能力 | [P0_CAPABILITY_BACKLOG_20260715.md](planning/P0_CAPABILITY_BACKLOG_20260715.md) |
| P1 重要能力 | [P1_CAPABILITY_BACKLOG_20260715.md](planning/P1_CAPABILITY_BACKLOG_20260715.md) |
| P2/P3/未来能力 | [PX_CAPABILITY_BACKLOG_20260715.md](planning/PX_CAPABILITY_BACKLOG_20260715.md) |
| MySQL 兼容性剩余边界 | [COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md](planning/COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md) |
| SQL 正则 → AST 迁移进展 | [2026-10-18-sql-regex-ast-migration-progress.md](planning/2026-10-18-sql-regex-ast-migration-progress.md) |
| 模块级缺口与历史估算 | [未实现功能梳理.md](未实现功能梳理.md) |
| 路线图导航 | [DEVELOPMENT_ROADMAP.md](planning/DEVELOPMENT_ROADMAP.md) |

## 2. 当前状态摘要（2026-11-13）

- 基础 CRUD、JDBC DML、预处理语句、事务提交/回滚/Savepoint、核心 DDL 与元数据已在聚焦路径跑通；
  2026-07-22 的 JDBC 聚焦矩阵为 77 个用例、0 失败、0 错误。
- SQL 结构解析的正则迁移已基本完成：`server/` 中剩余 35 个 `regexp` 调用点均为值语义
  （LIKE/REGEXP 求值、账户主机通配、日期/时区/UUID/WKT 解析、标识符校验、digest 归一化），
  不再承担 SQL 结构识别。详见迁移进展文档。
- MySQL 兼容性仍在推进：P0 有门禁证据；P1 的完整 I_S/P_S 运行时与权限语义、P2 的复制/XA/GTID
  统一提交与故障拓扑、P4 的官方 MySQL 全量互操作仍为 `partial`；FULLTEXT 延期，非 InnoDB 引擎
  明确不做。
- 项目尚未通过生产发布评审：外部 crash-kill、并发压测、活体指标、回滚演练和 owner sign-off
  仍需补齐。不要用历史“完成度百分比”判断生产就绪。

## 3. 架构与内核

| 领域 | 入口 |
|---|---|
| 架构职责分离 | [architecture/architecture_responsibility_separation.md](architecture/architecture_responsibility_separation.md) |
| InnoDB 存储机制 | [innodb/storage_architecture_doc.md](innodb/storage_architecture_doc.md)、[innodb/IBD_README.md](innodb/IBD_README.md)、[innodb/PAGES_README.md](innodb/PAGES_README.md) |
| 存储引擎 | [storage/STORAGE_DOCUMENTATION_INDEX.md](storage/STORAGE_DOCUMENTATION_INDEX.md) |
| B+Tree | [btree/BTREE_DOCUMENTATION_INDEX.md](btree/BTREE_DOCUMENTATION_INDEX.md) |
| 事务与崩溃恢复 | [transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md](transaction-reports/TRANSACTION_DOCUMENTATION_INDEX.md) |
| MVCC 与锁 | [mvcc-reports/MVCC_DOCUMENTATION_INDEX.md](mvcc-reports/MVCC_DOCUMENTATION_INDEX.md) |
| 火山模型执行器 | [volcano/VOLCANO_DOCUMENTATION_INDEX.md](volcano/VOLCANO_DOCUMENTATION_INDEX.md) |
| 执行器重构 | [executor-reports/EXECUTOR_DOCUMENTATION_INDEX.md](executor-reports/EXECUTOR_DOCUMENTATION_INDEX.md) |
| 查询优化器 | [query-optimizer/QUERY_ENGINE_ANALYSIS.md](query-optimizer/QUERY_ENGINE_ANALYSIS.md) |

## 4. 协议、客户端与兼容性

- 协议总索引：[protocol/PROTOCOL_DOCUMENTATION_INDEX.md](protocol/PROTOCOL_DOCUMENTATION_INDEX.md)
- 会话与 `COM_INIT_DB`：[SESSION_STATE_AND_PROTOCOL.md](SESSION_STATE_AND_PROTOCOL.md)
- SHOW / DROP 实现：[SHOW_DROP_IMPLEMENTATION.md](SHOW_DROP_IMPLEMENTATION.md)
- 客户端工具：[client/CLIENT_README.md](client/CLIENT_README.md)
- MySQL 8.4 兼容矩阵：[compatibility/mysql84_compatibility_matrix.md](compatibility/mysql84_compatibility_matrix.md)
- 兼容范围：[compatibility/mysql84_command_support.md](compatibility/mysql84_command_support.md)、[planning/GLOBAL_COMPATIBILITY_SCOPE_20260925.md](planning/GLOBAL_COMPATIBILITY_SCOPE_20260925.md)
- 预处理语句：[development/NET-001-PREPARED-STATEMENT-SUMMARY.md](development/NET-001-PREPARED-STATEMENT-SUMMARY.md)
- JDBC 集成测试修复计划：[planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md](planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md)

## 5. 开发规范与质量

- 编码规范：[CODING_STANDARDS.md](CODING_STANDARDS.md)
- 代码评审清单：[CODE_REVIEW_CHECKLIST.md](CODE_REVIEW_CHECKLIST.md)
- 快速参考：[QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- 具体缺陷修复记录：[bugfix/](bugfix/)
- 功能实现说明：[implementation/](implementation/)
- JDBC / DataGrip / 索引等专项：[development/](development/)

## 6. 运维与发布

- 数据恢复：[operations/data_recovery_runbook.md](operations/data_recovery_runbook.md)
- 灰度发布：[operations/gray_release_runbook.md](operations/gray_release_runbook.md)
- 回滚：[operations/rollback_runbook.md](operations/rollback_runbook.md)
- 全链路演练模板：[operations/full_chain_drill_record_template.md](operations/full_chain_drill_record_template.md)
- P0 证据套件操作指南：[operations/p0_evidence_suite_operator_guide.md](operations/p0_evidence_suite_operator_guide.md)
- 指标目录与使用：[observability/metrics_catalog.md](observability/metrics_catalog.md)
- 结构化日志：[observability/structured_logging_usage.md](observability/structured_logging_usage.md)
- P0 上线任务与验收：[planning/P0_PRODUCTION_TASKS.md](planning/P0_PRODUCTION_TASKS.md)、[planning/P0_PRODUCTION_CHECKLIST.md](planning/P0_PRODUCTION_CHECKLIST.md)、[planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md](planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md)
- 风险登记：[planning/P0_RISK_REGISTER.md](planning/P0_RISK_REGISTER.md)

## 7. 历史报告

以下目录保留当时的实现/验证快照，用于追溯，不代表当前结论：

- [btree-reports/](btree-reports/)、[storage-reports/](storage-reports/)、[transaction-reports/](transaction-reports/)
- [mvcc-reports/](mvcc-reports/)、[executor-reports/](executor-reports/)、[index-reports/](index-reports/)
- [protocol-reports/](protocol-reports/)、[reports/](reports/)

阅读历史报告时，优先对照第 1 节的当前权威文档；冲突时以权威文档和当前源码为准。

## 8. 维护规则

1. 新增一次性计划或阶段性报告时，文件名带日期，并在开头写明它取代/被谁取代。
2. 当前结论只维护在第 1 节列出的文档中，历史报告不回头改写。
3. 删除文档前先确认其结论已被权威文档覆盖；删除后通过 git 历史追溯。
