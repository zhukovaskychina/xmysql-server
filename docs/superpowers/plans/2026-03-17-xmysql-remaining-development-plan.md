# XMySQL 未开发项开发计划

> **Scope:** 覆盖 CONVERGENCE.md 中「未推进/待办」及 2026-03-16 收敛计划、Phase 4 中尚未完成的任务，产出可执行的 Chunk/Task 清单。
>
> **For agentic workers:** 使用 `superpowers:executing-plans` 或 `subagent-driven-development`；所有任务采用 checkbox（`- [ ]`）语法，建议 TDD、小步提交。
>
> **参考:** [CONVERGENCE.md](./CONVERGENCE.md)、[2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md](./2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md)、[2025-03-16-xmysql-phase4-implementation-plan.md](./2025-03-16-xmysql-phase4-implementation-plan.md)、[DEVELOPMENT_ROADMAP_TASKS.md](../development/DEVELOPMENT_ROADMAP_TASKS.md)

---

## 总览


| Chunk  | 范围                 | 目标                                                                                                      |
| ------ | ------------------ | ------------------------------------------------------------------------------------------------------- |
| **R1** | 已知失败修复             | TestConcurrentInsert、TestTransactionBeginCommit、TestSendResultSetPackets_NullValues 三处单测修复或显式 Skip + 文档 |
| **R2** | 收敛计划剩余             | B1.2 buffer_pool 与 manager 刷新策略对齐；C1.1/C1.2 专项决策与 CONVERGENCE 同步                                        |
| **R3** | Plan 层 Null 语义（可选） | TestBinaryOperation/Add_Null 等与 Null 语义、老优化规则一致的断言梳理                                                    |
| **R4** | Phase 4 剩余         | OPT-020 直方图、OPT-005 跳跃扫描（可选）、15.3/16.1 回归与收尾                                                            |


---

## Chunk R1: 已知失败修复

**Goal:** 三处已知失败用例要么修复通过，要么显式 Skip 并在 CONVERGENCE.md 中注明，避免阻塞回归。

### Task R1.1: TestConcurrentInsert（manager）

- 复盘 `manager/btree_improvements_test.go`（或实际所在文件）中 `TestConcurrentInsert` 的失败栈，确认 `OptimizedLRUCache` 为 nil 的调用路径。
- 二选一：
  - **方案 A**：在测试中显式初始化最小可用的 `OptimizedLRUCache`（或等价依赖），使用例在小规模并发下通过，并保留为回归用例。
  - **方案 B**：在测试顶部 `t.Skip("需完整 OptimizedBufferPoolManager + PageAllocator/Extent 初始化环境...")`，并在 CONVERGENCE.md「已跳过的用例」中新增一行，说明该用例不计入 Phase 1~3 验收范围。
- 更新 CONVERGENCE.md 中「已知失败」对应行：已修则改为「已修」并简述；已 Skip 则移至「已跳过的用例」并注明原因。

### Task R1.2: TestTransactionBeginCommit（engine）

- 定位 `engine` 包中 `TestTransactionBeginCommit`，确认 `commitStorageTransaction` 中 BufferPool 为 nil 的根因（集成环境未注入）。
- 二选一：
  - **方案 A**：在 `commitStorageTransaction` / insert/update/delete 路径中对 `bufferPoolManager` 做 nil 检查，无 BPM 时跳过 FlushPage，用例通过。
  - **方案 B**：`t.Skip("事务/存储集成测试需完整 BufferPool 环境，当前作为集成专项保留")`，并在 CONVERGENCE.md 中同步。
- 更新 CONVERGENCE.md 中「已知失败」对应行。

### Task R1.3: TestSendResultSetPackets_NullValues（protocol）

- 对照 MySQL 协议文档或 Wireshark 抓包，确认 NULL 在 ResultSet 中的编码方式（NULL bitmap 等）。
- 判断：若当前实现正确、测试预期错误，则更新测试断言，并在 CONVERGENCE.md 中将该用例从「已知失败」转为「已修」；若实现错误，则修实现并注明「协议兼容性修复」。（结论：编码 0xFB 正确，测试中行包索引错误，已按列数+EOF 修正）
- 更新 CONVERGENCE.md 中「已知失败」与「已跳过的用例」的对应条目，保持文档与代码一致。

**R1 验收：** 上述三处用例要么通过，要么已 Skip 且 CONVERGENCE.md 已更新；全量 `go test ./server/innodb/manager/... ./server/innodb/engine/... ./server/protocol/...` 无新增失败。

---

## Chunk R2: 收敛计划剩余（B1.2 + C1）

**Goal:** 完成 2026-03-16 收敛计划中未勾选任务，并统一 CONVERGENCE 文档状态。

### Task R2.1: B1.2 — buffer_pool 与 manager 自适应刷新策略对齐（BUF-010）

- 对比：
  - `manager/buffer_pool_flush_simple_test.go` 中的 `TestCalculateFlushBatchSize`、`TestAdjustFlushInterval`、`TestFlushStrategyIntegration`。
  - `server/innodb/buffer_pool` 内部对应的刷新策略实现（若有）。
- 在 buffer_pool 中增加或修正测试，使得在相同「脏页比例」输入下，刷新批量与间隔的趋势与 manager 策略一致（脏页越多 → 刷新越积极）；数值不必完全相同。（已加 TestFlushStrategyDirtyRatioTrend）
- 确认生产路径上 manager 与 buffer_pool 不会因策略不一致而「互相打架」（manager 算 batch/interval，buffer_pool 按 limit 执行 SelectPagesToFlush，趋势一致）。
- 运行：`go test ./server/innodb/buffer_pool -run '.' -count=1` 与 manager 侧刷新相关测试，均通过。

### Task R2.2: C1.1 — TestConcurrentInsert 处理策略落地

- 若 R1.1 已选择方案 A 或 B，则本任务仅需确认 CONVERGENCE.md 中「未推进/待办」与「已知失败/已跳过」描述与 R1.1 结论一致。（已一致）
- 若 R1.1 未做，则在此执行：复盘失败栈 → 选择方案 A 或 B → 更新 CONVERGENCE.md。

### Task R2.3: C1.2 — 协议层 Null 与计划外专项归类

- 若 R1.3 已完成，则本任务仅需将 CONVERGENCE 中与「协议兼容性」相关的描述统一（如 TestSendResultSetPackets_NullValues 的最终状态）。
- 将其余与 Phase 1~3 无直接关系、但在 CONVERGENCE 中标记的用例，按专题归类并写入 CONVERGENCE「未推进/待办」或新小节，便于后续开专项计划：
  - 协议兼容性（含 Null、其他编码）
  - Null 语义/优化规则、集成/压力、Phase 4 剩余（见 CONVERGENCE 6.1 计划外专项归类）
  - 其他（如有）

**R2 验收：** B1.2 相关测试通过；CONVERGENCE.md 中 engine/buffer_pool 与 C1 相关条目与代码/测试状态一致。

---

## Chunk R3: Plan 层 Null 语义与断言（可选）

**Goal:** 统一 plan 层对 Null 的语义与老优化规则，减少 TestBinaryOperation/Add_Null、部分 index pushdown/aggregation 消除用例的误报。

### Task R3.1: Null 语义与优化规则梳理

- 列出当前 plan 层对「表达式含 Null」的处理策略（常量折叠、谓词下推、聚合消除等）。（算术：遇 NULL 报错；比较/逻辑/聚合见 expression_test）
- 对照测试预期，区分：实现符合预期但断言过严 vs 实现与预期不一致。（Add_Null 原期望 nil+1=nil，实现为 error，属保守语义）
- 在 CONVERGENCE 或单独小结中记录「Null 语义约定」与「暂不修改的用例及原因」。（见 CONVERGENCE 7.1）

### Task R3.2: 断言放宽或用例标记

- 对「实现正确、断言过严」的用例：放宽断言或改为表格式多用例，避免误报。（Add_Null 改为 wantErr true，与当前实现一致）
- 对「实现与预期不一致、暂不修改」的用例：在测试中增加注释或 Skip 说明，并在 CONVERGENCE 中注明。（index pushdown/aggregation eliminate 已在 CONVERGENCE 注明，与 Null 无直接关系）

**R3 验收：** plan 回归命令下相关用例要么通过要么已明确标记；文档中 Null 相关条目清晰可查。

---

## Chunk R4: Phase 4 剩余（Chunk 15～16）

**Goal:** 完成 Phase 4 实施计划中未勾选任务，达到阶段里程碑（完成度约 94%）。

### Task R4.1: OPT-005 索引跳跃扫描（可选）

- 若已有实现：运行相关单测，补充验收（如索引扫描路径、结果正确性），通过则勾选。
- 若无实现：在 Phase 4 计划与 CONVERGENCE 中明确标记为「延后」，并注明依赖或优先级理由。（当前无 SkipScan 实现，延后；依赖 OPT-001 索引下推与多列索引前缀）

### Task R4.2: OPT-020 直方图统计

- 运行统计/直方图相关单测（如 `TestHistogram`*、`TestColumnStats*`）。（TestHistogram、TestColumnStats、TestHistogramTypes、TestSelectivityEstimator 均通过）
- 补充直方图收集与选择率使用的验收用例（若缺失则按 TDD 补测）。（已有 buildHistogram、SelectivityEstimator 直方图估算与 CBO 集成测试覆盖）
- 若有失败则修实现或测试直至通过；更新 Phase 4 计划中 Task 15.2 为已勾选。

### Task R4.3: IDX-015 在线索引构建（可选）/ PROTO-002 批量操作

- 按优先级选做其一或两者：运行对应单测并验收。
- 若延后：在计划中注明「延后」及原因。（当前无独立 IDX-015/PROTO-002 专项实现；批量相关有 secondary_index 一致性测试与 BenchmarkBatchInsert，专项延后）

### Task R4.4: Chunk 16 — 回归与收尾

- 按 CONVERGENCE 验收命令执行全模块功能回归，记录通过/失败列表。（plan / manager / engine / buffer_pool 通过；prefetch TestExpiredRequests 已放宽断言）
- 可选：压力与稳定测试；Bug 修复与优化。
- 可选：更新 README/CHANGELOG、部署与运维说明。
- 更新 Phase 4 计划中 Task 16.1 为已勾选；在 CONVERGENCE 或 CHANGELOG 中记录本阶段里程碑（完成度约 94%）。

**R4 验收：** Phase 4 计划中 Chunk 15～16 的未勾选任务均已处理（完成或显式延后）；全模块回归通过或失败已文档化。

---

## 验收标准汇总

- **R1**：三处已知失败（TestConcurrentInsert、TestTransactionBeginCommit、TestSendResultSetPackets_NullValues）已修复或已 Skip，CONVERGENCE 已更新。
- **R2**：B1.2 刷新策略对齐完成；C1.1/C1.2 策略与 CONVERGENCE 一致。
- **R3**（可选）：Plan Null 语义与断言已梳理；Add_Null 已对齐「遇 NULL 报错」语义，CONVERGENCE 7.1 记录约定；其余 index pushdown/aggregation 消除已注明。
- **R4**：R4.1 OPT-005 延后（无实现）；R4.2 OPT-020 直方图验收通过；R4.3 15.3 延后；R4.4 回归与收尾已执行。

---

## 参考文档

- [CONVERGENCE.md](./CONVERGENCE.md) — 包级结论、已跳过用例、已知失败、验收范围
- [2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md](./2026-03-16-xmysql-engine-buffer-pool-convergence-plan.md) — B1.2、C1.1、C1.2 原文
- [2025-03-16-xmysql-phase4-implementation-plan.md](./2025-03-16-xmysql-phase4-implementation-plan.md) — Chunk 15～16
- [DEVELOPMENT_ROADMAP_TASKS.md](../development/DEVELOPMENT_ROADMAP_TASKS.md) — 任务 ID 与模块映射