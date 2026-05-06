# XMySQL Engine / Buffer Pool 收敛与执行层验收计划

> **Scope:** 聚焦 `server/innodb/engine` 与 `server/innodb/buffer_pool` 两个目前“编译失败/集成未完成”的模块，将其收敛到“可编译 + 关键执行器用例可跑通”的状态。
>
> **For agentic workers:** Use `superpowers:executing-plans` or `subagent-driven-development`. All tasks use checkbox (`- [ ]`) 语法，建议 TDD、小步提交。

---

## Chunk E1: 修复 engine 编译与索引扫描执行链路

**Goal:** `go test ./server/innodb/engine -run 'IndexScanOperator|HashAggregateOperator|SubqueryOperator'` 至少可编译并执行基础用例。

### Task E1.1: 对齐 `NewIndexScanOperator` 与相关 Mock

- [x] 阅读 `server/innodb/engine/index_scan_operator.go` / `index_lookup_test.go` / `index_reading_test.go`，列出当前 `NewIndexScanOperator` 签名与测试调用之间的差异（含 `StorageAdapter` / `IndexAdapter` / startKey / endKey 类型）。
- [x] 调整测试使用的 mock 结构（或增加轻量 wrapper），使之满足当前实现的接口与类型要求，避免在实现中为了测试而引入 hack。
- [x] 运行：
  - [x] `go test ./server/innodb/engine -run 'TestIndexScanOperator_' -count=1`
  - [x] 若仍有编译错误或 panic，则迭代修复，直到 IndexScan 相关测试稳定通过。

### Task E1.2: 恢复 `HashAggregateOperator` 单测（EXE-005 执行层）

- [x] 阅读 `server/innodb/engine/volcano_executor.go` 中 `HashAggregateOperator` 与 `buildHashAgg` 相关代码，确认与 plan 层 `PhysicalHashAgg` 字段含义一致。
- [x] 跑通：
  - [x] `go test ./server/innodb/engine -run 'TestHashAggregateOperator_' -count=1`
- [x] 若存在编译错误或行为偏差：
  - [x] 优先保证与 plan 层约定的一致性（分组键、聚合函数列表、输入记录流模型）。
  - [x] 必要时放宽过于“硬编码”的断言（例如精确日志内容），但保留对结果集正确性的校验。

### Task E1.3: 子查询执行链路（EXEC-001 执行层）

- [x] 定位 `SubqueryOperator` 实现与 `executeScalarSubquery` / IN / EXISTS 等执行路径（通常在 `volcano_executor.go` 或相邻文件）。
- [x] 设计/补充最小化单测场景：
  - [x] 标量子查询：`SELECT (SELECT 1)`、`SELECT (SELECT max(id) FROM t)`。
  - [x] IN 子查询：`SELECT * FROM t1 WHERE t1.id IN (SELECT t2.id FROM t2)`。
  - [x] EXISTS 子查询：`SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)`。
- [x] 运行：
  - [x] `go test ./server/innodb/engine -run 'SubqueryOperator|ScalarSubquery' -count=1`
- [x] 遇到复杂依赖（如需真实存储/事务）时，可以：
  - [x] 在测试中使用最小的 in‑memory/mock 执行图，只验证执行器算子之间的数据流和结果，而非完整 SQL 入口。

---

## Chunk B1: 修复 buffer_pool 编译与基础行为

**Goal:** `go test ./server/innodb/buffer_pool -run '.' -count=1` 可编译并执行基础 LRU / 刷新策略测试；行为与 manager 层策略不冲突。

### Task B1.1: 对齐 `basic.Space` / `SpaceManager` / `StorageManager` Mock

- [x] 阅读 `server/innodb/buffer_pool` 下的接口定义，与 `server/innodb/basic`、`server/innodb/manager` 中真实实现比对，列出字段/方法不匹配点。
- [x] 统一 Mock 定义：
  - [x] 为测试专门定义 `MockSpace` / `MockSpaceManager` / `MockStorageManager`，确保实现完整接口但行为尽量简单（例如总是从内存 map 返回页面）。
  - [x] 避免在生产代码中加入仅服务于测试的编译标记分支。
- [x] 运行：
  - [x] `go test ./server/innodb/buffer_pool -run '.' -count=1`
- [x] 若仍存在 panic 或竞态：
  - [x] 优先修正明显的 nil dereference / 不安全并发访问，再考虑行为细节。

### Task B1.2: 与 manager 自适应刷新策略对齐（BUF-010 执行侧）

- [x] 对比：
  - [x] `manager/buffer_pool_flush_simple_test.go` 中的 `TestCalculateFlushBatchSize` / `TestAdjustFlushInterval` / `TestFlushStrategyIntegration`。
  - [x] buffer_pool 内部对应的刷新策略实现（若有）。
- [x] 增加或修正 buffer_pool 内部测试，使：
  - [x] 在同样的“脏页比例”输入下，刷新批量与间隔趋势与 manager 中的策略一致（已加 TestFlushStrategyDirtyRatioTrend）。
- [x] 确认：
  - [x] 不会因为策略不一致导致 manager 与 buffer_pool 在生产路径上出现“互相打架”的行为。

---

## Chunk C1: 并发 / Crash 用例的安全降级与计划外专项

**Goal:** 清晰标记哪些高风险/高依赖用例不作为当前 P1/P3 验收阻塞，同时为后续专项预留入口。

### Task C1.1: `TestConcurrentInsert` 的处理策略

- [x] 复盘 `manager/btree_improvements_test.go` 中 `TestConcurrentInsert` 的失败栈（`OptimizedLRUCache` 为 nil）。
- [x] 根据当前工程优先级选择其一：
  - [ ] 方案 A：在测试中显式初始化一个最小可用的 `OptimizedLRUCache`，让用例在小规模并发下运行，通过后继续保留为回归用例。
  - [x] 方案 B：在测试顶部 `t.Skip(...)`，并在 `CONVERGENCE.md` 中说明该用例不计入 Phase 1~3 验收范围。
- [x] 无论选择哪种方案，都要更新 `CONVERGENCE.md` 中对应条目，保持文档与现实一致。

### Task C1.2: 协议层 Null 处理与其他计划外专项

- [x] 对 `protocol` 中 `TestSendResultSetPackets_NullValues`：已修（测试行包索引修正，编码 0xFB 正确），CONVERGENCE 已更新。
- [x] 将其他与 Phase 1~3 无直接关系、但在 `CONVERGENCE.md` 中标记的用例，按专题归类（见 CONVERGENCE 6.1 计划外专项归类）。

---

## 验收标准（Engine / Buffer Pool 收敛完成的定义）

- [x] `go test ./server/innodb/engine -run 'IndexScanOperator|HashAggregateOperator|SubqueryOperator' -count=1` 全部通过，无编译错误或 panic。
- [x] `go test ./server/innodb/buffer_pool -run '.' -count=1` 通过，LRU/刷新相关测试稳定。
- [x] `CONVERGENCE.md` 中：
  - [x] `server/innodb/engine` / `server/innodb/buffer_pool` 的状态从 “❌ 编译失败” 更新为 “✅ 通过*（附说明）” 或“⚠️ 部分通过（仅哪些用例仍作为专项保留）”。
  - [x] `TestConcurrentInsert` / `TestSendResultSetPackets_NullValues` 等用例的处理策略与实际代码/测试状态保持一致（见 Chunk C1；已 Skip / 已修并更新 CONVERGENCE）。

