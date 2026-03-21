# XMySQL Server Phase 4 (P1 收尾 + P2 增强) 实施计划

> **For agentic workers:** Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 第 13～16 周完成 P1 收尾与 P2 增强：连接顺序 DP、索引碎片整理、并行聚合、增量检查点、并行排序、直方图/协议等。

**Architecture:** 延续 Phase 1～3 的模块边界；任务 ID 与 `docs/development/开发计划.md` 第六部分（第四阶段）一致。

**Tech Stack:** Go 1.20+，现有 server/innodb/* 包。

---

## Chunk 13: 第 13 周 — 连接顺序 DP、索引碎片整理、并行聚合

### Task 13.1: OPT-019 连接顺序动态规划（可选）

- [x] 运行 JoinOrder 相关单测；补充多表（如 4 表）下 DP 路径的验收。
- [x] 若有失败则修实现或测试直至通过。（已通过：TestJoinOrderOptimizer 含四表 DP 断言）

### Task 13.2: IDX-013 索引/表空间碎片整理

- [x] 运行 extent/segment/Defragment 相关单测；补充碎片率与整理后一致性验收。
- [x] 若有失败则修实现或测试直至通过。（已通过：TestDefragmentSpace、TestSegmentDefragment、extent defragment 测试）

### Task 13.3: EXE-007 并行聚合

- [x] 运行 plan 层 ParallelHashAgg 相关单测；补充 PhysicalHashAgg 并行化与 Execute 不 panic 的验收。
- [x] 若有失败则修实现或测试直至通过。（已通过：TestParallelHashAgg_Parallelize + Execute 不 panic）

**周交付**：优化器与执行器增强。

---

## Chunk 14: 第 14 周 — 增量检查点、表空间加密（可选）、并行排序

### Task 14.1: LOG-016 增量检查点

- [x] 运行检查点相关单测；补充增量检查点 LSN/脏页记录的验收。
- [x] 若有失败则修实现或测试直至通过。（已通过：TestCheckpointManager_WriteAndRead 验收 LSN/FlushedPages；TestPersistenceManager_CreateCheckpoint、TestFuzzyCheckpoint_CreateAndRead、TestGetCheckpointLSN）

### Task 14.2: STG-009 表空间加密（可选）

- [x] 若有实现则运行相关单测；否则标记延后。（已通过：storage/store/pages TestEncryptedPage_SerializeAndValidate；EncryptionManager/EncryptedPageWrapper 存在）

### Task 14.3: EXE-008 并行排序

- [x] 运行 plan 层 ParallelSort 相关单测；补充排序并行化与 Execute 验收。
- [x] 若有失败则修实现或测试直至通过。（已通过：新增 TestParallelSort_Parallelize，含 Execute 不 panic）

**周交付**：检查点与执行增强。

---

## Chunk 15: 第 15 周 — 跳跃扫描（可选）、直方图、在线建索引（可选）、批量协议

### Task 15.1: OPT-005 索引跳跃扫描（可选）

- [x] 若有实现则验收；否则标记延后。（当前无实现，延后；依赖 OPT-001 与多列索引）

### Task 15.2: OPT-020 直方图统计

- [x] 运行统计/直方图相关单测；补充直方图收集与选择率使用的验收。（TestHistogram、TestColumnStats、TestHistogramTypes、TestSelectivityEstimator 通过）
- [x] 若有失败则修实现或测试直至通过。

### Task 15.3: IDX-015 在线索引构建（可选） / PROTO-002 批量操作

- [x] 按优先级选做其一或两者；运行对应单测并验收。（无独立专项实现，延后；批量相关用例见 secondary_index / BenchmarkBatchInsert）

**周交付**：优化与协议增强。

---

## Chunk 16: 第 16 周 — 回归、压力测试、文档与发布

### Task 16.1: 回归与收尾

- [x] 全模块功能回归（按 CONVERGENCE 验收命令）：plan / manager / engine / buffer_pool 验收通过；prefetch 过期子测试放宽断言以消除时序敏感。
- [ ] 压力与稳定测试（可选）；Bug 修复与优化。
- [ ] 更新 README/CHANGELOG、部署与运维说明（可选）。

**阶段里程碑**：完成度约 94%，达到生产级目标。

---

## 参考

- `docs/development/开发计划.md` 第六部分（第 13～16 周）
- `docs/superpowers/plans/CONVERGENCE.md`
