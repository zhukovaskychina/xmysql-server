# 开发路线图（导航）

> 2026-11-13：本页只做导航，不再维护阶段百分比、任务数量或人天估算。
> 当前状态与优先级以第 1 节文档为准；历史路线图已删除，可通过 git 历史追溯。

## 当前权威入口

| 用途 | 文档 |
|---|---|
| 能力优先级总索引 | [CAPABILITY_PRIORITY_INDEX_20260715.md](CAPABILITY_PRIORITY_INDEX_20260715.md) |
| P0 生产阻塞能力 | [P0_CAPABILITY_BACKLOG_20260715.md](P0_CAPABILITY_BACKLOG_20260715.md) |
| P1 重要能力 | [P1_CAPABILITY_BACKLOG_20260715.md](P1_CAPABILITY_BACKLOG_20260715.md) |
| P2/P3/未来能力 | [PX_CAPABILITY_BACKLOG_20260715.md](PX_CAPABILITY_BACKLOG_20260715.md) |
| MySQL 兼容性剩余边界 | [COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md](COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md) |
| SQL 正则 → AST 迁移 | [2026-10-18-sql-regex-ast-migration-progress.md](2026-10-18-sql-regex-ast-migration-progress.md) |
| 模块级缺口与历史估算 | [../未实现功能梳理.md](../未实现功能梳理.md) |
| 文档总导航 | [../README.md](../README.md) |

## 上线与证据

- [P0_PRODUCTION_TASKS.md](P0_PRODUCTION_TASKS.md)（历史任务拆解，保留追溯）
- [P0_PRODUCTION_CHECKLIST.md](P0_PRODUCTION_CHECKLIST.md)
- [P0_PRODUCTION_DEPLOYMENT_PLAN.md](P0_PRODUCTION_DEPLOYMENT_PLAN.md)
- [P0_EVIDENCE_RUN_20260722.md](P0_EVIDENCE_RUN_20260722.md)
- [P0_RISK_REGISTER.md](P0_RISK_REGISTER.md)
- [GAP_06_SECURITY_DEFAULT_POSTURE.md](GAP_06_SECURITY_DEFAULT_POSTURE.md)

## 当前实施顺序

1. 每个合并候选都保持 2026-07-22 的聚焦矩阵为绿。
2. 补齐 P1 发布证据：外部 crash drill、并发负载、指标演练、回滚演练和 owner sign-off。
3. 推进 P1 兼容性广度：完整 I_S/P_S 运行时与权限语义、FK/CHECK 完整语义、更多 SQL 兼容路径。
4. 推进 P2/P4：统一 storage/WAL/native-binlog/GTID 物理提交点、完整崩溃/网络分区/晋升拓扑和官方 MySQL 互操作。
5. P1/P* 存储压力：大规模 B+Tree 分裂/合并/复用、损坏注入、长时间 purge/restart。
6. 生产就绪声明只引用当前或更新的证据文档，不使用历史完成度百分比。
