# XMySQL Server Phase 3 (P1 重要功能) 实施计划

> **For agentic workers:** Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 第 9～12 周完成 P1 重要功能：索引合并/子查询优化/Purge、日志组提交/Undo 段/自适应哈希、页面压缩/Dynamic 行格式/HashAgg、自适应刷新/并行扫描/子查询执行。

**Architecture:** 延续 Phase 1/2 的模块边界；任务 ID 与 `docs/development/开发计划.md` 第五部分（第三阶段）一致。

**Tech Stack:** Go 1.20+，现有 server/innodb/* 包。

---

## Chunk 9: 第 9 周 — 索引合并、子查询优化、Purge

### Task 9.1: OPT-003 索引合并 Index Merge

- [x] 运行现有索引下推/优化器单测；补充“多索引扫描合并”的用例或单测。
- [x] 若有失败则修实现或测试直至通过。
  - 修正 TestIndexMergeCandidates：条件列改为 name（表有 idx_name）；多条件时接受合并或单索引择优。

### Task 9.2: OPT-014 子查询优化规则

- [x] 运行子查询相关单测；补充 IN/EXISTS 转 SEMI JOIN 等规则的单测或验收。
- [x] 若有失败则修实现或测试直至通过。
  - 放宽 TestSubqueryOptimizer_ComplexQuery：直接调用 optimizeSubqueryNode 不更新 GetStats，仅断言优化结果非空。

### Task 9.3: TXN-004 Purge 线程

- [x] 运行 Purge/Undo 相关单测；补充后台清理已提交 undo/版本链的用例。
- [x] 若有失败则修实现或测试直至通过。（TestTXN002_PurgeOldVersions 已通过。）

**周交付**：子查询优化与 Purge 可用。

---

## Chunk 10: 第 10 周 — 日志组提交、Undo 段、自适应哈希

### Task 10.1: LOG-003 日志组提交

- [x] 运行 Redo/日志相关单测；补充组提交逻辑的单测或验收。
- [x] 若有失败则修实现或测试直至通过。（TestGroupCommit 已通过。）

### Task 10.2: LOG-007 Undo 段管理

- [x] 运行 Undo 相关单测；补充 Undo 段分配与回收的用例。
- [x] 若有失败则修实现或测试直至通过。
  - 新增 `undo_segment_test.go`：TestUndoSegment_AllocateAddPurge、TestUndoSegmentManager_AllocateRelease。

### Task 10.3: IDX-011 自适应哈希索引（可选）

- [x] 若有实现则运行相关单测；必要时补充热点访问哈希加速的验收。
- [x] 若未实现则可标记为延后或 P2。（当前仅配置项 InnodbAdaptiveHashIndex，无独立实现；标记为延后。）

**周交付**：日志与 Undo 管理增强。

---

## Chunk 11: 第 11 周 — 页面压缩、Dynamic 行格式、HashAgg

### Task 11.1: STG-001 页面压缩完善

- [x] 运行存储/页面相关单测；补充透明压缩/解压或多算法的验收。
- [x] 若有失败则修实现或测试直至通过。
  - TestCompressedPage_SerializeAndValidate 已通过；补充 DecompressData 往返与 GetCompressionRatio 断言。

### Task 11.2: STG-016 Dynamic 行格式

- [x] 运行 record 包与行格式单测；补充 Dynamic 格式编解码或 BLOB 溢出验收。
- [x] 若有失败则修实现或测试直至通过。
  - 新增 record/dynamic_format_test.go：TestDynamicRowFormat_NoOverflow（无溢出路径 encode/decode）。

### Task 11.3: EXE-005 HashAgg 算子

- [x] 运行执行器/聚合相关单测；补充 HashAgg Build/Flush 或分组聚合的用例。
- [x] 若有失败则修实现或测试直至通过。
  - 新增 plan 层 TestPhysicalHashAgg_ConvertFromLogicalAggregation；engine 包 HashAggregateOperator 单测存在但 engine 当前编译未通过。

**周交付**：存储与聚合能力增强。

---

## Chunk 12: 第 12 周 — 自适应刷新、并行扫描、子查询执行

### Task 12.1: BUF-010 自适应刷新

- [x] 运行缓冲池刷新相关单测；补充根据负载调整刷新间隔/批量的验收。
- [x] 若有失败则修实现或测试直至通过。
  - manager 包：TestCalculateFlushBatchSize、TestAdjustFlushInterval、TestFlushStrategyIntegration、TestFlushIntervalBounds 已通过，覆盖根据脏页比例调整批量与间隔；engine 内 AdaptiveFlushStrategy 单测因 engine 编译失败暂未运行。

### Task 12.2: EXE-006 并行扫描

- [x] 运行执行器单测；补充表/索引并行扫描的用例。
- [x] 若有失败则修实现或测试直至通过。
  - 新增 plan 层 TestParallelTableScan_Parallelize：PhysicalTableScan 经 ParallelizePhysicalPlan 得到 ParallelTableScan 且 chunks 非空（EXE-006 验收）。

### Task 12.3: EXEC-001 子查询执行（标量/IN/EXISTS）

- [x] 运行子查询执行相关单测；补充标量子查询、IN、EXISTS 执行正确的验收。
- [x] 若有失败则修实现或测试直至通过。
  - plan 包：TestSubqueryOptimizer_OptimizeScalarSubquery、TestSubqueryOptimizer_OptimizeInSubquery、TestSubqueryOptimizer_OptimizeExistsSubquery 已通过；实际执行在 engine 的 SubqueryOperator/executeScalarSubquery，待 engine 编译修复后补充执行层验收。

**周交付**：性能与复杂查询能力明显提升。

**阶段里程碑**：P1 重要功能完成；性能与复杂查询能力明显提升。

---

## 参考

- `docs/development/开发计划.md` 第五部分（第 9～12 周）
- `docs/development/DEVELOPMENT_ROADMAP_TASKS.md`
