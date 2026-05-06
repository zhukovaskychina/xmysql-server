# 执行器（Executor / Volcano）文档索引

> **2026-04**：执行器相关叙述分散在「重构计划 / 实施总结 / 完成报告」与 `docs/volcano/`。本页为 **`executor-reports/`** 入口；火山算子与清理见 [Volcano 文档索引](../volcano/VOLCANO_DOCUMENTATION_INDEX.md)。

---

## 本目录文档


| 文档                                                                                 | 说明                                                              |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| [EXECUTOR_ARCHITECTURE_REFACTOR_PLAN.md](./EXECUTOR_ARCHITECTURE_REFACTOR_PLAN.md) | EXEC-001：**历史计划归档**（文首 2026-04 重估说明；闭环见完成报告） |
| [EXECUTOR_REFACTOR_COMPLETION_REPORT.md](./EXECUTOR_REFACTOR_COMPLETION_REPORT.md) | **权威**：重构完成报告（闭环、指标、与 `executor.go` / `volcano_executor.go` 关系） |
| [EXECUTOR_REFACTOR_SUMMARY.md](./EXECUTOR_REFACTOR_SUMMARY.md)                     | **→ 跳转** 至完成报告（原「实施总结」与完成报告重复）                                  |


---

## 相关目录


| 位置                                                                                                       | 说明                         |
| -------------------------------------------------------------------------------------------------------- | -------------------------- |
| [../volcano/](../volcano/)                                                                               | `Operator` 接口、算子实现、清理与最终报告 |
| [../implementation/OLD_INTERFACE_REMOVAL_SUMMARY.md](../implementation/OLD_INTERFACE_REMOVAL_SUMMARY.md) | 旧接口移除与迁移说明                 |


## 代码入口

- `server/innodb/engine/executor.go` — SQL 入口与分派  
- `server/innodb/engine/volcano_executor.go` — 火山算子树