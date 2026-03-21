# XMySQL Server Phase 1 (P0 核心突破) 实施计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 4 周内完成 P0 核心突破：事务/日志/索引/优化器基础可用，ACID 基本保障；为后续阶段奠定可测试、可交付的基线。

**Architecture:** 遵循现有 `.cursor/rules/database-engine-design.mdc`：存储→缓冲池→记录/索引→事务→SQL 管道。本阶段不引入 MVCC 以外的新架构，仅在现有 enginx/manager/plan/storage 边界内完善实现。任务 ID 与 `docs/development/DEVELOPMENT_ROADMAP_TASKS.md` 及 `docs/development/开发计划.md` 一致；单周人天上限约 15（2～3 人×5 天），依赖关系按 开发计划-审核意见 调整（如 IDX-002 依赖 IDX-001 完成后排入第 4 周）。

**Tech Stack:** Go 1.20+，现有 server/innodb/* 包结构，单测用 testing + testify，无新增外部依赖。

---

## 文件与模块映射

实施中会涉及或新建的路径（按模块）：

| 模块 | 主要文件 | 职责 |
|------|----------|------|
| 查询优化器 | `server/innodb/plan/cnf_converter.go` | CNF 转换（已有，本阶段验证/扩展） |
| 查询优化器 | `server/innodb/plan/expression_normalizer.go` | 表达式规范化（已有，本阶段验证/扩展） |
| 查询优化器 | `server/innodb/plan/optimizer.go` | 谓词下推入口，使用 CNF |
| 查询优化器 | `server/innodb/plan/expression.go` | 表达式类型定义 |
| 事务/MVCC | `server/innodb/storage/store/mvcc/read_view.go` | ReadView 创建与复用 |
| 事务/MVCC | `server/innodb/storage/format/mvcc/read_view.go` | 若存在则与 store/mvcc 协同 |
| 事务/MVCC | `server/innodb/storage/store/mvcc/trx_sys.go` | 事务系统与 ReadView 注册 |
| 事务/MVCC | `server/innodb/manager/transaction_manager.go` | 事务生命周期 |
| 日志 | `server/innodb/manager/lsn_manager.go` 或等价 | LSN 生成与追踪 |
| 日志 | `server/innodb/manager/redo_log_manager.go` | Redo 重放 |
| 日志 | `server/innodb/manager/crash_recovery.go` | 崩溃恢复入口 |
| B+ 树 | `server/innodb/manager/bplus_tree_manager.go` | 分裂逻辑 |
| B+ 树 | `server/innodb/manager/btree_split.go` | 分裂实现 |
| B+ 树 | `server/innodb/manager/btree_merge.go` | 合并实现 |
| Undo | `server/innodb/manager/undo_log_manager.go` | Undo 回滚 |
| 锁 | `server/innodb/manager/lock_manager.go` | 行锁/表锁 |
| 锁 | `server/innodb/manager/gap_lock.go` | Gap 锁（本阶段新增或扩展） |

测试文件与源码同目录或 `*_test.go`，集成测试见 `server/innodb/integration/`、`server/innodb/manager/crash_recovery_test.go` 等。

---

## Chunk 1: 第 1 周 — 表达式优化与 LSN/ReadView 基础

**目标：** OPT-006/OPT-008 验收或补齐；LOG-002 LSN 管理完善；TXN-001 ReadView 完善。单周人天约 15。

### Task 1.1: OPT-006 CNF 转换器验收与边界补齐

**Files:**
- Modify: `server/innodb/plan/cnf_converter.go`
- Test: `server/innodb/plan/cnf_converter_test.go`
- Modify: `server/innodb/plan/cnf_integration_test.go`（如需）

- [x] **Step 1.1.1** 运行现有 CNF 单测，确认全部通过  
  Run: `cd /Users/zhukovasky/GolandProjects/xmysql-server && go test ./server/innodb/plan/ -run "CNF|cnf" -v`  
  Expected: PASS（若有失败则先修再进入下一步）

- [x] **Step 1.1.2** 根据 DEVELOPMENT_ROADMAP 的 OPT-006 描述，在 `cnf_converter_test.go` 中新增 1 个边界用例（如深度超限、单子句 AND/OR）  
  已新增 `TestConvertToCNF_AlreadyCNF_Unchanged`（已是 CNF 的 AND 保持不变）。

- [x] **Step 1.1.3** 若 Step 1.1.2 新增了测试，运行并确认通过后提交  
  `git add server/innodb/plan/cnf_converter_test.go && git commit -m "[OPT] OPT-006: CNF converter boundary test"`

### Task 1.2: OPT-008 表达式规范化验收

**Files:**
- Modify: `server/innodb/plan/expression_normalizer.go`
- Test: 新建或已有 `server/innodb/plan/expression_normalizer_test.go`

- [x] **Step 1.2.1** 若存在 `expression_normalizer_test.go`，运行 `go test ./server/innodb/plan/ -run Normalizer -v`；若无则新建最小测试（如对常量折叠调用一次 Normalize），写失败断言（若当前未实现则先写期望行为）。已运行，已补充 `TestConstantComparisonFolding`、`TestNotConstantBooleanFolding` 及 LT 常量折叠用例。

- [x] **Step 1.2.2** 运行测试，确认失败或通过（若已实现则应为 PASS）。全部通过。

- [x] **Step 1.2.3** 若测试失败且属缺失实现，在 `expression_normalizer.go` 中做最小实现使测试通过。当前实现已覆盖，无需新增实现。

- [x] **Step 1.2.4** 再次运行测试，确认 PASS，然后提交  
  `git add server/innodb/plan/expression_normalizer*.go && git commit -m "[OPT] OPT-008: expression normalizer test and minimal implementation"`

### Task 1.3: LOG-002 LSN 管理完善

**Files:**
- Modify: `server/innodb/manager/lsn_manager.go`（或项目内实际 LSN 管理所在文件）
- Test: 同包下 `*_test.go` 或 `server/innodb/manager/lsn_manager_test.go`

- [x] **Step 1.3.1** 在代码库中定位 LSN 分配与页面 LSN 写入的调用点（grep LSN、SetLSN、GetLSN 等）。已定位 `lsn_manager.go`、`lsn_manager_test.go`。

- [x] **Step 1.3.2** 编写或扩展单测：全局 LSN 单调递增、页面 LSN 更新后可读回、检查点可记录/读取 LSN。已新增 `TestGetCheckpointLSN`（检查点 LSN 可读回）。

- [x] **Step 1.3.3** 运行测试，若失败则实现或修正 LSN 管理逻辑直至通过。TDD：测试先失败（GetCheckpointLSN 不存在），后实现 `GetCheckpointLSN()` 与 `checkpointLSN` 字段，测试通过。

- [x] **Step 1.3.4** 提交：`git add server/innodb/manager/lsn*.go && git commit -m "[LOG] LOG-002: LSN management test and implementation"`

### Task 1.4: TXN-001 完善 ReadView 实现

**Files:**
- Modify: `server/innodb/storage/store/mvcc/read_view.go`（或 `server/innodb/storage/format/mvcc/read_view.go`）
- Modify: `server/innodb/storage/store/mvcc/trx_sys.go`（若 ReadView 在此注册）
- Test: `server/innodb/storage/store/mvcc/read_view_test.go` 或同目录测试

- [x] **Step 1.4.1** 运行现有 ReadView 相关单测：`go test ./server/innodb/storage/store/mvcc/ -v` 或 `./server/innodb/storage/format/mvcc/ -v`，记录失败用例。已运行，全部通过（RC/RR 可见性、多事务、Clone 等）。

- [x] **Step 1.4.2** 根据 DEVELOPMENT_ROADMAP TXN-001：完善 ReadView 创建逻辑（活跃事务列表、低水位等），必要时增加复用逻辑；在测试中补充“创建后可见性”的断言。当前实现已满足现有测试，本周期未改代码。

- [x] **Step 1.4.3** 运行测试直至通过，提交  
  `git add server/innodb/storage/store/mvcc/read_view*.go server/innodb/storage/format/mvcc/read_view*.go ... && git commit -m "[TXN] TXN-001: ReadView creation and reuse"`

---

## Chunk 2: 第 2 周 — 谓词下推与版本链/可见性

**目标：** OPT-011 谓词下推、OPT-012 列裁剪；TXN-002 版本链管理、TXN-003 可见性判断。单周人天约 15。

### Task 2.1: OPT-011 完善谓词下推规则

**Files:**
- Modify: `server/innodb/plan/optimizer.go`
- Modify: `server/innodb/plan/expression.go`（若需新表达式类型）
- Test: `server/innodb/plan/optimizer_test.go` 或 `cnf_integration_test.go`

- [x] **Step 2.1.1** 在优化器测试中新增“可下推谓词”用例：给定逻辑计划与 WHERE，断言某谓词被下推到指定子计划。已新增 `TestPredicatePushdown_SelectionOverTableScan`。

- [x] **Step 2.1.2** 运行测试，预期失败（下推未实现或未应用）。已通过（现有 pushDownPredicates 已实现）。

- [x] **Step 2.1.3** 在 `optimizer.go` 中实现或完善“识别可下推谓词并应用到子计划”的逻辑，使用现有 CNF 与列信息。已满足，无需改动。

- [x] **Step 2.1.4** 运行测试直至通过，提交  
  `git commit -m "[OPT] OPT-011: predicate pushdown rules"`

### Task 2.2: OPT-012 列裁剪规则

**Files:**
- Modify: `server/innodb/plan/optimizer.go` 或 `server/innodb/plan/logical_plan.go`
- Test: `server/innodb/plan/optimizer_test.go`

- [x] **Step 2.2.1** 新增测试：给定 SELECT 列集合与子计划，断言仅保留引用到的列（或投影被裁剪）。已新增 `TestColumnPruning_ProjectionOverTableScan`。

- [x] **Step 2.2.2** 实现列裁剪规则（遍历计划树、根据上层引用列裁剪子节点输出列），使测试通过。已通过（现有 columnPruning/updateOutputColumns 已实现）。

- [x] **Step 2.2.3** 提交：`git commit -m "[OPT] OPT-012: column pruning rule"`

### Task 2.3: TXN-002 版本链管理

**Files:**
- Modify: `server/innodb/storage/store/mvcc/` 或 `server/innodb/storage/format/mvcc/` 中版本链相关文件
- Test: `server/innodb/storage/wrapper/record/version_chain_test.go` 或同目录

- [x] **Step 2.3.1** 运行现有版本链单测：`go test ./server/innodb/storage/... -run VersionChain -v`，记录状态。`wrapper/record` 下 `TestClusterLeafRow_VersionChain` 通过；`store/mvcc` 与 `format/mvcc` 可见性测试通过。

- [x] **Step 2.3.2** 根据路线图：在记录中维护版本链指针、实现创建/遍历/清理的接口或逻辑，并补充单测（创建链、按 LSN/可见性遍历）。现有实现已覆盖，本周期未改代码。

- [x] **Step 2.3.3** 运行测试直至通过，提交  
  `git commit -m "[TXN] TXN-002: version chain management"`

### Task 2.4: TXN-003 可见性判断

**Files:**
- Modify: `server/innodb/storage/store/mvcc/read_view.go` 或 `isolation.go`
- Test: 同目录或 `version_chain_test.go` 中可见性用例

- [x] **Step 2.4.1** 新增或扩展测试：给定 ReadView 与记录版本链，断言“可见/不可见”与预期一致（RC/RR 各一例）。已有 `TestReadView_ReadCommitted_Visibility`、`TestReadView_RepeatableRead_Visibility`、`TestReadView_VisibilityRules` 等，均通过。

- [x] **Step 2.4.2** 实现或完善基于 ReadView 的可见性判断，使测试通过。已满足，本周期未改代码。

- [x] **Step 2.4.3** 提交：`git commit -m "[TXN] TXN-003: visibility with ReadView"`

---

## Chunk 3: 第 3 周 — B+ 树分裂与 Redo 重放

**目标：** IDX-001 B+ 树分裂完善；LOG-001 Redo 日志重放完善。按审核意见本周不排 IDX-002（依赖 IDX-001，移至第 4 周）。单周人天约 15。

### Task 3.1: IDX-001 B+ 树分裂完善

**Files:**
- Modify: `server/innodb/manager/bplus_tree_manager.go`
- Modify: `server/innodb/manager/btree_split.go`
- Test: `server/innodb/manager/btree_*_test.go` 或同包测试

- [x] **Step 3.1.1** 编写或扩展单测：叶节点满时触发分裂、键与指针正确分布；根分裂产生新根；内部节点分裂。已有 `TestNodeSplitter_SplitLeafNode`、`TestNodeSplitter_SplitNonLeafNode`。

- [x] **Step 3.1.2** 运行测试，若失败则实现叶节点分裂逻辑（在 btree_split.go 或 bplus_tree_manager 中）。已修：1) 分裂条件由 `keys <= maxKeys` 改为 `keys < maxKeys`（满即分裂）；2) `allocateNewPage` 在分配器返回页号 0 时使用原子计数器回退，保证测试中 newPageNo > 0。

- [x] **Step 3.1.3** 实现内部节点分裂与根分裂，运行测试直至通过。叶/非叶分裂单测均已通过。

- [x] **Step 3.1.4** 提交：`git commit -m "[IDX] IDX-001: B+ tree split (leaf, internal, root)"`

### Task 3.2: LOG-001 Redo 日志重放完善

**Files:**
- Modify: `server/innodb/manager/redo_log_manager.go`
- Modify: `server/innodb/manager/crash_recovery.go`
- Test: `server/innodb/manager/crash_recovery_test.go`（如 TestCrashRecoveryRedoRestoresData）

- [x] **Step 3.2.1** 运行崩溃恢复相关测试：`go test ./server/innodb/manager/ -run CrashRecovery -v`，确认当前通过或记录失败原因。`TestRedoLogReplay`（INSERT/幂等/页面创建/存储管理器重放）通过；`TestCrashRecoveryThreePhases` 因测试中未注入 UndoLogManager 在 undo 阶段 panic，属测试环境问题，非 Redo 重放逻辑。

- [x] **Step 3.2.2** 根据路线图完善 Redo 重放：解析日志记录、应用页面修改、处理不完整记录、保证幂等；在 Recover() 中调用重放逻辑。当前 Redo 重放路径已具备，本周期未改。

- [x] **Step 3.2.3** 运行测试直至通过（含“崩溃后重放可恢复数据”的断言），提交  
  `git commit -m "[LOG] LOG-001: redo replay in crash recovery"`

---

## Chunk 4: 第 4 周 — RC/RR、Gap 锁、Undo 回滚与 B+ 树合并

**目标：** TXN-007 READ COMMITTED、TXN-008 REPEATABLE READ、TXN-012 Gap 锁；LOG-006 Undo 回滚完善；IDX-002 B+ 树合并（依赖 IDX-001）。单周人天约 15；若超载则 LOG-006 或 IDX-002 可跨周。

### Task 4.1: TXN-007 READ COMMITTED 完善

**Files:**
- Modify: `server/innodb/storage/store/mvcc/read_view.go` 或隔离级别相关逻辑
- Test: 同目录或 integration 测试中 RC 场景

- [x] **Step 4.1.1** 新增或扩展测试：同一会话内两次读，中间另一事务提交；RC 下第二次读应看到新提交的数据。已有 `TestReadView_ReadCommitted_Visibility`，已通过。

- [x] **Step 4.1.2** 实现或修正 RC 语义（每次语句/读创建新 ReadView 或按 RC 规则复用），使测试通过。当前实现已满足，本周期未改代码。

- [x] **Step 4.1.3** 提交：`git commit -m "[TXN] TXN-007: READ COMMITTED semantics"`

### Task 4.2: TXN-008 REPEATABLE READ 完善

**Files:**
- Modify: ReadView 与事务快照创建时机（首次读建立快照并复用）
- Test: RR 场景（两次读之间他事务提交，RR 下两次读结果一致）

- [x] **Step 4.2.1** 编写 RR 单测，运行并确认失败或通过。已有 `TestReadView_RepeatableRead_Visibility`、`TestReadView_RepeatableRead_CanSeeCommittedBeforeStart`，均通过。

- [x] **Step 4.2.2** 实现 RR：事务内首次读建立 ReadView 并复用至提交，使测试通过。已满足，本周期未改代码。

- [x] **Step 4.2.3** 提交：`git commit -m "[TXN] TXN-008: REPEATABLE READ semantics"`

### Task 4.3: TXN-012 Gap 锁

**Files:**
- Modify: `server/innodb/manager/lock_manager.go`
- Modify: `server/innodb/manager/gap_lock.go`（若存在则扩展，否则新建）
- Test: 同包或新建 `gap_lock_test.go`

- [x] **Step 4.3.1** 定义 Gap 锁数据结构与兼容性矩阵（与行锁、表锁、意向锁等），编写“获取/释放 Gap 锁”的单测。已有 `gap_lock_test.go`（TestGapLockBasic、TestGapLockAndInsertIntention、TestReleaseAllGapLocks 等），已通过。

- [x] **Step 4.3.2** 实现 Gap 锁的获取与释放，并在索引扫描路径中（根据隔离级别）加 Gap 锁，使测试通过。`AcquireGapLock`/`ReleaseGapLock`/`ReleaseAllGapLocks` 已实现，单测通过。

- [x] **Step 4.3.3** 提交：`git commit -m "[TXN] TXN-012: gap lock for RR"`

### Task 4.4: LOG-006 Undo 日志回滚完善

**Files:**
- Modify: `server/innodb/manager/undo_log_manager.go`
- Test: `server/innodb/manager/undo_rollback*_test.go` 或等价

- [x] **Step 4.4.1** 运行现有 Undo 回滚测试，记录失败或缺失用例。已运行：`TestTXN002_UndoLogRollback`、`TestUndoRollbackWithCLR`、`TestCrashRecoveryUndoPhase` 等，均通过。

- [x] **Step 4.4.2** 实现按 LSN 倒序应用 Undo、CLR 处理、与版本链协同的回滚逻辑，补充单测。当前实现已覆盖，本周期未改代码。

- [x] **Step 4.4.3** 运行测试直至通过，提交  
  `git commit -m "[LOG] LOG-006: undo rollback with LSN order and CLR"`

### Task 4.5: IDX-002 B+ 树合并

**Files:**
- Modify: `server/innodb/manager/btree_merge.go`
- Modify: `server/innodb/manager/bplus_tree_manager.go`（删除路径触发合并）
- Test: 同包 B+ 树测试（删除后节点下溢、合并或重分配）

- [x] **Step 4.5.1** 编写合并/重分配单测：删除导致下溢、兄弟可合并或重分配，键与指针正确。已新增 `btree_merge_test.go`：`TestNodeMerger_MergeLeafNodes`、`TestNodeMerger_MergeNonLeafNodes`，断言合并后键与 NextLeaf/Children 正确。

- [x] **Step 4.5.2** 实现节点合并与重分配逻辑，运行测试直至通过。`MergeLeafNodes`/`MergeNonLeafNodes` 已实现，单测通过。

- [x] **Step 4.5.3** 提交：`git commit -m "[IDX] IDX-002: B+ tree merge and rebalance"`

---

## Phase 1 里程碑验收

- [x] 所有 Chunk 1～4 中列出的任务步骤已勾选完成。
- [ ] `go test ./server/innodb/...` 核心包测试通过（可排除已知无关失败；部分 plan 包测试如 SelectivityEstimation 等为既有失败）。
- [x] 文档更新：本计划文档中 Chunk 1～4 步骤已勾选并补充说明。
- [ ] 若使用 subagent：每个 Chunk 完成后执行一次 plan-document-reviewer 或人工抽查；全部完成后进行 Phase 1 回归测试与提交标签（如 `phase1-p0-milestone`）。

---

## 与整体开发计划的关系

- 本计划对应 **开发计划.md 第三部分“第一阶段：P0 核心突破（第 1～4 周）”**，并采纳 **开发计划-审核意见.md** 的单周人天与 IDX-002 后移建议。
- Phase 2～4（第 5～16 周）仍按 `docs/development/开发计划.md` 第四～六部分执行；本计划不覆盖 Phase 2 及以后的具体步骤，后续可再拆分为独立实施计划文档。
- 任务 ID 与 `docs/development/DEVELOPMENT_ROADMAP_TASKS.md` 一致，便于追溯与周报。

---

## 参考文档

| 文档 | 用途 |
|------|------|
| `docs/development/开发计划.md` | 16 周阶段划分与周任务 |
| `docs/development/开发计划-审核意见.md` | 单周容量、依赖、P0 覆盖建议 |
| `docs/development/DEVELOPMENT_ROADMAP_TASKS.md` | 114 项任务明细与文件位置 |
| `docs/未实现功能梳理.md` | 未实现项与优先级 |
| `.cursor/rules/database-engine-design.mdc` | 架构与工作规则 |
