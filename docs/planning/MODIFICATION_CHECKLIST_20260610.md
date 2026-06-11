# XMySQL Server 修改清单（2026-06-10）

> 本清单用于指导后续代码修改、测试补齐和验收证据归档。它不替代 `P0_PRODUCTION_CHECKLIST.md`，而是把“下一步具体改哪里、怎么验收”整理成工程执行入口。

## 0. 当前基线

- 当前结论：不具备生产灰度条件。
- 当前代码扫描：`server/**/*.go` 内仍有 `172` 个 TODO/FIXME/未实现/stub/跳过测试类命中，分布在 `82` 个文件。
- 测试命令必须使用 Go 1.24.3：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ...`
  - 不使用默认 `/usr/local/bin/go`，当前默认版本是 Go 1.16.2，会导致测试二进制加载问题。
- 本清单优先处理能降低正确性风险、能写测试验证、能支撑 P0 验收的项。

## 1. P0：发布与测试基线

### P0-01 固定 Go 工具链与本地/CI 一致性

- [x] 修改 `.github/workflows/go.yml`，确保 CI 使用 Go 1.24.x。
- [x] 补充开发文档，明确本地测试统一使用 `/Users/zhukovasky/sdk/go1.24.3/bin/go` 或等价 Go 1.24.x。
- [x] 检查脚本中的 `go test`、`go run` 是否显式使用同一工具链或通过 `PATH` 注入。

涉及路径：

- `.github/workflows/go.yml`
- `README.md`
- `scripts/*.sh`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go version
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/basic ./server/innodb/manager ./server/innodb/record -count=1
```

完成标准：

- CI、本地、脚本文档的 Go 版本一致。
- 不再出现 Go 1.16 链接或 `missing LC_UUID load command` 类问题。

### P0-02 建立可重复的发布包测试集

- [x] 定义“发布包集”，避免每次只跑零散包。
- [x] 将发布包集写入脚本 `scripts/p0_release_package_tests.sh`。
- [x] 输出测试报告到 `reports/p0_release_tests/`。
- [x] 明确 `go test ./...` 当前策略：默认以固定包集为发布门禁，允许通过 `P0_RELEASE_TEST_SCOPE=full` 触发全量 `go test ./...`，并通过 `P0_RELEASE_ENFORCE_FULL_TEST=1` 强制全量门禁；当前阶段为“全量失败不阻塞发布证据”。

建议首批包集：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test \
  ./server/conf \
  ./server/dispatcher \
  ./server/net \
  ./server/protocol \
  ./server/innodb/basic \
  ./server/innodb/record \
  ./server/innodb/manager \
  ./server/innodb/engine \
  -count=1
```

涉及路径：

- `scripts/p0_release_package_tests.sh`
- `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- `reports/p0_release_tests/`

完成标准：

- 发布包集测试可重复执行。
- 报告中包含命令、Go 版本、通过/失败包、耗时和失败摘要。
- 真实报告样例：`reports/p0_release_tests/20260610_*/summary.log`。

## 2. P0：关键路径错误处理与 fallback 清理

### 2026-06-11 进展补充

- P0-03 已完成子项（本轮）：
  - `unified_executor.go` 的 `SELECT/INSERT/UPDATE/DELETE` 关键路径改为结构化错误。
  - `storage_adapter.go` 的 `schema.SchemaName` 字段问题修复。
  - `index_transaction_adapter.go` 的 `fmt` `%w` 包装符问题修复。
  - `executor.go` 的错误构造参数对齐。
  - `unified_executor_test.go` 增加/调整 `ExecutionError` + `ErrorCode` 断言。

- 当前状态：
  - `go1.24.3/bin/go test ./server/innodb/engine -run 'TestUnifiedExecutor|Test.*Error' -count=1` 通过。
  - `go1.24.3/bin/go test ./server/innodb/engine -count=1` 通过。

- 剩余：`executor.go`、`dml_operators.go`、`storage_integrated_*` 仍有较多非结构化返回路径需继续整理。

### P0-03 清理执行器错误掩盖路径

- 当前进展：`storage_adapter.go` 与 `index_transaction_adapter.go` 已完成关键错误返回结构化改造（metadata 读取、页面读写、主键回表、索引范围扫描、索引记录读取、事务/锁参数校验）。

- [ ] 梳理 `recover()` 包装是否吞掉真实错误。
- [ ] 将文本匹配类错误判断改成结构化错误类型或错误码。
- [ ] 为失败路径补测试：存储失败、表不存在、唯一键冲突、事务回滚失败。
- [ ] 日志中输出模块、SQL、表名、事务 ID、错误码。

重点路径：

- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/dml_operators.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -count=1
```

完成标准：

- 关键失败路径不 silent fallback。
- 失败路径单测能证明错误会被明确返回。

### P0-04 DML 元数据与类型校验补齐

- [x] `DMLExecutor.validateValueType` 从简单判断升级为按 `metadata.DataType` 校验。
- [x] `dml_executor.go` 中临时 TableID 获取改为来自 TableManager 或数据字典。
- [x] `storage_integrated_dml_executor.go` 从真实数据字典读取表元数据。
- [x] 增加 INSERT/UPDATE 类型错误、长度错误、NULL 约束、默认值测试。
- [x] 二级索引同步不依赖临时 ID，使用真实表存储映射并有回归验证。

重点路径：

- `server/innodb/engine/dml_executor.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/metadata/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(DML|Insert|Update|Column|Type|Constraint)' -count=1
```

完成标准：

- 类型不匹配能返回结构化错误。
- DML 不再依赖临时 TableID。
- 二级索引同步拿到的元数据与实际表定义一致。

### P0-05 统一记录头事务 ID 编码策略

- [x] `server/innodb/manager/bplus_tree_manager.go` 中 `RecordRowAdapter.SetTransactionId` 写入 `[5:13]`。
- [x] `server/innodb/record/unified_record.go` 中 `UnifiedRecordImpl.SetTransactionId` 写入 `[5:13]`。
- [x] `server/innodb/manager/enhanced_btree_index.go` 的 `SimpleRow.SetTransactionId` 改为写入记录头 `[5:13]`。
- [x] `SpecialRow`、`SecondaryIndexLeafRow`、`SecondaryIndexInternalRow` 的 `SetTransactionId` 语义已确认（辅助索引与特殊行不存储事务ID，调用后不改动记录字节）。
- [x] 给事务 ID 编码位置补文档，避免不同记录实现各写各的。

补充说明：

- 聚簇/统一记录及 `RecordRowAdapter` 使用记录头偏移 `[5:13]` Little Endian 存储事务ID。
- `SpecialRow` 及辅助索引行（`SecondaryIndexLeafRow`、`SecondaryIndexInternalRow`）不存储事务ID；该语义在实现测试与脚本中已明确。

待检查路径：

- `server/innodb/record/special_rows.go`
- `server/innodb/storage/wrapper/record/*.go`
- `server/innodb/manager/enhanced_btree_adapter.go`
- `server/innodb/manager/enhanced_btree_index.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/record ./server/innodb/manager -run 'Test.*TransactionId' -count=1
```

完成标准：

- 普通记录、统一记录、BTree 适配器事务 ID 编码策略一致。
- 记录头前 5 字节不被事务 ID 写入覆盖。

## 3. P0：崩溃恢复与一致性证据

### P0-06 崩溃恢复演练标准化

- [x] 固定 redo、undo、半提交三类场景输入数据。
- [x] 演练脚本执行真实进程 `start -> crash -> restart -> verify`。
- [x] 每轮输出恢复前快照、恢复后快照、差异报告。
- [x] 连续执行至少 3 轮，并归档报告。

涉及路径：

- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- `reports/p0_b_audit/`

验收命令：

```bash
CR_PROC_REPORT_DIR=./reports B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh
```

完成标准：

- redo/undo/半提交恢复均有可复现报告。
- 重复演练输出一致。

### P0-07 灰度演练写入阶段阻塞修复

- [x] 修复灰度演练阶段 1 中 `table mysql.t1 not found in storage mapping`。
- [x] 将只读、写入、回退三阶段串成一条脚本。
- [x] 记录每阶段耗时。
- [x] 失败时输出可定位日志路径。

涉及路径：

- `scripts/p0_e_backup_snapshot.sh`
- `scripts/p0_e_backup_snapshot.sh`（新增 `MODE=canary`）
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `reports/p0_e_backups/`
- `server/innodb/manager/`
- `server/innodb/engine/`

验收命令：

```bash
P0E_MODE=canary bash scripts/p0_e_backup_snapshot.sh
./scripts/p0_e_backup_snapshot.sh list
```

完成标准：

- 灰度演练阶段 0/1/2 均通过。
- 有失败复盘和重演通过记录。

## 4. P0：可观测性最小闭环

### P0-08 慢查询日志落地

- [x] 明确 `slow_query_log`、阈值、输出路径配置。
- [x] 执行 SQL 时记录 SQL、耗时、影响行数、连接 ID、事务 ID、错误码。
- [x] 增加慢查询样例测试或集成脚本。

涉及路径：

- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`
- `server/conf/`
- `docs/planning/P0_D_OBSERVABILITY_READINESS_TEMPLATE.md`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log' -count=1
```

完成标准：

- 可配置开启/关闭慢查询日志。
- 样例 SQL 能产生可审计慢查询记录。

### P0-09 指标与告警导出

- [ ] 导出 QPS、错误率、连接数、活跃事务数。
- [ ] 导出延迟 P50/P95/P99。
- [ ] 导出 checkpoint、redo、undo、锁等待核心指标。
- [ ] 提供最小告警规则和触发演练记录。

涉及路径：

- `server/innodb/manager/checkpoint_monitor.go`
- `server/innodb/manager/transaction_manager.go`
- `server/net/`
- `server/conf/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1
```

完成标准：

- 指标可通过统一接口读取。
- 告警规则有触发和恢复记录。

## 5. P1：SQL 执行能力补齐

### P1-01 子查询执行与优化

- [ ] 标量子查询。
- [ ] `IN` 子查询。
- [ ] `EXISTS` 子查询。
- [ ] 相关子查询。
- [ ] `IN -> SEMI JOIN` 优化。

涉及路径：

- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/apply_operator*.go`
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/join_order_optimizer_helpers.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/plan -run 'Test.*(Subquery|Apply|Semi|Exists)' -count=1
```

完成标准：

- 四类子查询均有执行器测试。
- 至少一类子查询优化有计划层测试。

### P1-02 窗口函数与窗口帧

- [ ] 补齐 `ROWS BETWEEN ... AND ...`。
- [ ] 补齐 `RANGE BETWEEN ... AND ...`。
- [ ] 验证 `ROW_NUMBER`、`RANK`、`SUM/AVG OVER`。

涉及路径：

- `server/innodb/engine/window_function_executor.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*Window' -count=1
```

完成标准：

- 窗口帧边界行为有测试。
- 空分区、单行分区、多行分区均覆盖。

### P1-03 HAVING、DISTINCT、LIMIT OFFSET

- [ ] `HAVING` 在聚合之后过滤。
- [ ] `DISTINCT` 去重规则与 NULL 行为。
- [ ] `LIMIT offset, count` 和 `LIMIT count OFFSET offset`。

涉及路径：

- `server/innodb/engine/select_executor.go`
- `server/innodb/engine/volcano_executor.go`
- `server/innodb/plan/`
- `server/innodb/sqlparser/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/sqlparser -run 'Test.*(Having|Distinct|Limit|Offset)' -count=1
```

完成标准：

- 解析、计划、执行三层测试均覆盖。

## 6. P1：优化器与统计信息

### P1-04 代价估算从桩实现升级

- [ ] `physical_plan.go` 中 Hash Join、Merge Join、Hash Agg、Stream Agg 代价估算落地。
- [ ] `cost_estimator.go` 中选择率估算补真实规则。
- [ ] 增加行数、基数、过滤率测试。

涉及路径：

- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/selectivity_estimator.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/plan -run 'Test.*(Cost|Selectivity|Physical)' -count=1
```

完成标准：

- 不同连接/聚合方式能产生可解释代价。
- 选择率估算不再只有固定常量。

### P1-05 统计信息采集真实化

- [ ] B+ 树叶子节点遍历统计。
- [ ] 页面解析抽样。
- [ ] 全表扫描统计。
- [ ] 自动更新统计信息。

涉及路径：

- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/plan/statistics_collector_enhanced.go`
- `server/innodb/manager/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/plan ./server/innodb/manager -run 'Test.*(Statistics|Collector|Histogram)' -count=1
```

完成标准：

- 统计信息来自真实页或真实扫描路径。
- 统计结果能影响计划选择。

## 7. P1：存储页与空间管理

### P1-06 页面读写 TODO 收敛

- [ ] `page_impl.go` 实现实际磁盘读写或明确接入 buffer pool。
- [ ] 各 wrapper 的 `ReadFromDisk` / `WriteToDisk` 不再只是注释或空实现。
- [ ] 文件头、文件尾序列化落地。

涉及路径：

- `server/innodb/storage/wrapper/page/page_impl.go`
- `server/innodb/storage/wrapper/page/*.go`
- `server/innodb/storage/wrapper/page_wrapper.go`
- `server/innodb/storage/io/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/storage/wrapper/page ./server/innodb/storage/io -count=1
```

完成标准：

- 页面序列化、读、写、校验能形成闭环。
- 异常页输入返回明确错误。

### P1-07 MVCC 页面序列化与 IO

- [ ] `mvcc_page.go` 页面序列化。
- [ ] `mvcc_page.go` 页面反序列化。
- [ ] MVCC 页读写磁盘路径。

涉及路径：

- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/mvcc/`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/storage/wrapper/mvcc ./server/innodb/storage/store/mvcc -count=1
```

完成标准：

- 序列化后再反序列化，版本链信息不丢。
- 可见性测试覆盖读已提交和可重复读。

## 8. P1：索引维护能力

### P1-08 索引重建、优化、一致性检查

- [ ] 实现索引重建逻辑。
- [ ] 实现索引优化逻辑。
- [ ] 实现索引一致性检查逻辑。
- [ ] `SHOW INDEX` 能返回可验证统计。

涉及路径：

- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/manager/index_manager.go`
- `server/innodb/manager/enhanced_btree_manager.go`

验收命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Test.*(Index|Rebuild|Consistency|ShowIndex)' -count=1
```

完成标准：

- 二级索引和主表数据可做一致性校验。
- 重建后查询结果不变。

## 9. P2：长期能力

### P2-01 协议兼容性扩展

- [ ] `COM_STMT_SEND_LONG_DATA`。
- [ ] Batch Execute。
- [ ] `COM_FIELD_LIST`。
- [ ] 多结果集。
- [ ] binlog dump / GTID。

涉及路径：

- `server/net/`
- `server/protocol/`
- `jdbc_client/`

完成标准：

- JDBC 兼容测试能覆盖协议路径。
- 未支持能力返回明确错误，不挂起连接。

### P2-02 高级 SQL 与对象能力

- [ ] 存储过程 `CALL`。
- [ ] 触发器。
- [ ] 视图和物化视图。
- [ ] 分区表。
- [ ] 在线 DDL。

涉及路径：

- `server/innodb/sqlparser/`
- `server/innodb/engine/`
- `server/innodb/metadata/`

完成标准：

- 每类能力先定义支持矩阵。
- 未实现语法返回明确错误。

## 10. 建议执行顺序

1. P0-01：工具链统一。
2. P0-02：发布包测试集。
3. P0-03：执行器错误处理。
4. P0-04：DML 元数据与类型校验。
5. P0-06：崩溃恢复演练。
6. P0-08 / P0-09：可观测性。
7. P0-07：灰度写入阻塞。
8. P1-04 / P1-05：优化器和统计信息。
9. P1-06 / P1-07：页面 IO 与 MVCC 页。
10. P1-08：索引维护。

## 11. 每项修改的最低交付要求

每个清单项完成时必须同时提交：

- 代码修改。
- 失败先行的测试或可复现脚本。
- 通过的验证命令输出。
- 如果影响 P0，上线验收清单中的证据路径。
- 如果修改行为，补对应文档或支持矩阵。

## 12. 参考入口

- `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- `docs/planning/P0_PRODUCTION_TASKS.md`
- `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`
- `docs/planning/PRODUCTION_GAP_LIST.md`
- `docs/未实现功能梳理.md`
