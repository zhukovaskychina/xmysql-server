# Superpowers 实施计划

本目录存放使用 **writing-plans** 技能生成的、可被 agent 或人工按步骤执行的实施计划。

## 当前计划

| 文件 | 范围 | 说明 |
|------|------|------|
| [2025-03-16-xmysql-phase1-p0-implementation-plan.md](./2025-03-16-xmysql-phase1-p0-implementation-plan.md) | Phase 1（第 1～4 周） | P0 核心突破：表达式优化、LSN/ReadView、谓词下推/列裁剪、版本链/可见性、B+ 树分裂与 Redo 重放、RC/RR/Gap 锁、Undo 回滚、B+ 树合并 |
| [2025-03-16-xmysql-phase2-implementation-plan.md](./2025-03-16-xmysql-phase2-implementation-plan.md) | Phase 2（第 5～8 周） | P0 收尾 + P1 启动：统计/选择性、二级索引、连接顺序/Next-Key/模糊检查点、Compact/BLOB/HashJoin、脏页刷新/预处理语句（**已完成**） |
| [2025-03-16-xmysql-phase3-implementation-plan.md](./2025-03-16-xmysql-phase3-implementation-plan.md) | Phase 3（第 9～12 周） | P1 重要功能：索引合并/子查询优化/Purge、日志组提交/Undo 段/自适应哈希、压缩/Dynamic/HashAgg、自适应刷新/并行扫描/子查询执行 |
| [2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md](./2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md) | Engine / Buffer Pool 收敛 | 修复 engine/buffer_pool 编译与关键执行器用例；B1.2/C1 专项（**E1/B1.1 已完成**） |
| [2026-03-17-xmysql-remaining-development-plan.md](./2026-03-17-xmysql-remaining-development-plan.md) | 未开发项开发计划 | 已知失败修复（R1）、收敛剩余 B1.2/C1（R2）、Plan Null 语义（R3 可选）、Phase 4 剩余（R4） |
| [2026-03-17-query-opt-002-001-tdd-plan.md](./2026-03-17-query-opt-002-001-tdd-plan.md) | 查询优化 OPT-002/OPT-001 | TDD：覆盖索引检测（Chunk 1 ✅）、索引条件下推完善（Chunk 2 ✅） |

## 与开发计划的关系

- 高层阶段与周次见 [docs/development/开发计划.md](../development/开发计划.md)。
- 本目录下的计划将 16 周拆成可执行的 Chunk，每 Chunk 内为带 checkbox 的 bite-sized 任务与步骤（TDD、小步提交）。
- Phase 2～4 的详细步骤可后续按同样格式追加到本目录（如 `2025-XX-XX-xmysql-phase2-p0-p1-plan.md`）。

## 收敛状态

- Phase 1/2 的**包级结论、跳过用例、已知失败、验收范围**见 **[CONVERGENCE.md](./CONVERGENCE.md)**。回归或排期修复时以该文档为准。

## 执行方式

- **有 subagent 时**：使用 **superpowers:subagent-driven-development**，按 Chunk 或按 Task 派发子 agent，每 Chunk 完成后做一次 review。
- **无 subagent 时**：使用 **superpowers:executing-plans** 在当前会话中按步骤执行，并在每个 Chunk 结束时做检查点。
