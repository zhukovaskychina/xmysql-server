# XMySQL Server 修改清单（2026-06-10）

> 目标：明确下一步必须改的事项，给出可执行动作和验收方式。

## 一、当前状态（可复核）

- 代码扫描口径：`rg -n "TODO|FIXME|not implemented|stub|unimplemented" server --glob '*.go'`
- 命中数：`173`
- 涉及文件：`78`
- 状态：基础风险链条已部分修复，但关键发布风险项仍未全部收口。

## 二、必须先改（P0）

### P0-03：执行器错误可见性

- 风险：部分失败分支返回错误字符串或上下文不足，问题定位仍不够直接；`strings` 文本判断仍有残留风险。
- 影响范围：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 动作：
  - 统一失败返回为结构化错误（含 SQL、schema、table、txnID、阶段信息）。
  - 禁止以字符串包含判断关键错误分支。
  - 统一处理提交失败、回滚失败、存储层失败和唯一键冲突。
  - 为每类失败加单元测试，断言错误分类字段。
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML|Transaction|Index|Storage)' -count=1`

### P0-06：崩溃恢复演练标准化

- 风险：基础逻辑有改动但演练证据未形成固定、可复放闭环。
- 影响范围：
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `server/innodb/manager/crash_recovery.go`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 动作：
  - 固定三类场景：`redo`、`undo`、`half-commit`。
  - 每轮输出：恢复前快照、恢复后快照、差异报告。
  - 连续至少 3 轮执行并保留历史比对记录。
- 验收：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07：灰度写入阶段阻塞修复

- 风险：历史阻断仍可复现（例如写入阶段映射找不到表）。
- 影响范围：
  - `scripts/p0_e_backup_snapshot.sh`
  - `server/innodb/manager/`（映射/启动阶段）
  - `server/innodb/engine/`（执行与 DML）
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- 动作：
  - 统一只读/写入/回退 3 阶段脚本入口。
  - 写入失败必须输出阶段号、schema、表名、错误码、日志路径。
  - 修复 `storage mapping` 关键路径并验证至少一次完整通过。
- 验收：
  - `./scripts/p0_e_backup_snapshot.sh list`
  - 完成 `backup/read/write/restore` 全流程。

### P0-08：慢查询日志

- 风险：查询执行链里缺少统一慢查询采集，问题难以追踪。
- 影响范围：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 动作：
  - 加入 `slow_query_log` 开关与耗时阈值。
  - 记录字段：`sql`、`cost_ms`、`rows_affected`、`conn_id`、`txn_id`、`schema`、`table`、`error_code`。
- 验收：
  - `go test` 覆盖慢查询路径
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Slow.*|Test.*Log' -count=1`

### P0-09：指标与告警导出

- 风险：尚未形成可接入监控的指标输出与告警演练。
- 影响范围：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 动作：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99。
  - 输出 checkpoint/redo/undo/锁等待核心指标。
  - 增加至少一条可触发告警并可恢复的演练路径。
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## 三、P1（下一阶段）

- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/metadata/convert.go`

每一项至少补一条失败路径/边界条件测试。

## 四、P2（延后）

- `server/innodb/storage/wrapper/page/*.go` 的磁盘读写/解析 TODO
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/mvcc/*`

## 五、TODO/FIXME 热点文件 Top 10（用于安排人力）

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/parallel.go`（8）
3. `server/innodb/engine/index_reading_test.go`（8）
4. `server/innodb/plan/physical_plan.go`（6）
5. `server/innodb/metadata/convert.go`（6）
6. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
7. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
8. `server/innodb/plan/statistics_collector_helpers.go`（5）
9. `server/innodb/manager/page.go`（5）
10. `server/innodb/manager/crash_recovery.go`（5）

> 备注：数字基于当前 `server` 目录文本匹配结果，主要用于优先级排序，不等于功能优先级。

## 六、执行顺序建议

1. 先收口 `P0-03`
2. 再把 `P0-06` 做成 3 轮可复放的恢复演练
3. 接着修 `P0-07`
4. 并行补齐 `P0-08` 与 `P0-09`
5. 再处理 P1、P2
