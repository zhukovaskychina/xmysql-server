# XMySQL 修改清单（截至 2026-06-11）

基线：`docs/planning/MODIFICATION_CHECKLIST_20260610.md` + 当前仓库扫描结果  
范围：`server/**/*.go`、`scripts/*.sh`、相关 docs 与 reports 输出文件

## 先看结论

当前仍有三类阻塞优先级最高问题：

1. P0 失败可见性仍有大量非结构化错误路径（P0-03）；
2. 崩溃恢复和灰度演练仍未形成稳定闭环（P0-06、P0-07）；
3. 可观测性未形成最小可交付闭环（P0-08、P0-09）。

另外，P1 的子查询、窗口、优化器、页与索引能力目前仍是“功能可见但不完整”状态，需要在 P0 全绿后推进。

## P0：必须先改

### P0-03 执行器失败可见性与错误闭环

- [ ] 继续清理 `server/innodb/engine/executor.go`
  - 关键点：将剩余文本型失败返回统一到 `ExecutionError`；保留 cause，不再只靠字符串判断。
  - 风险：上层判断逻辑会把真实错误当成功路径或返回不一致 code。
  - 关注点：`schema is nil`、`execute ... failed`、`return nil` 这类边界返回。

- [ ] 继续清理 `server/innodb/engine/dml_operators.go`
  - 关键点：`findDuplicateRecord not fully implemented` 与若干 nil 返回的 DML 关键函数补齐真实行为，统一失败码。
  - 风险：唯一索引冲突、索引更新失败、事务回滚失败可见性不足。

- [ ] 继续清理 `server/innodb/engine/storage_integrated_dml_executor.go`
  - 关键点：INSERT/UPDATE/DELETE 的 `fmt.Errorf` 全链路改成结构化错误（含 module/stage/sql/schema/table/txn/error_code）。
  - 风险：`P0-07` 的灰度映射阻塞在失败恢复时难定位。

- [ ] 继续清理 `server/innodb/engine/storage_adapter.go`
  - 关键点：元数据、读写、解析失败继续使用统一错误封装；补充 cause 链。
  - 风险：storage 层问题会被上层吃掉导致行为不透明。

- [ ] 继续清理 `server/innodb/engine/storage_integrated_dml_helper.go`
  - 关键点：序列化/反序列化、扫描与查找空条件返回行为补齐边界处理与错误码。
  - 风险：空条件或错列数场景下行为不可控。

- [ ] 继续清理 `server/innodb/engine/storage_integrated_index_helper.go`
  - 关键点：`validateIndexKey`、重建、优化、索引一致性 TODO 补完整最低可用行为。
  - 风险：索引可见性与一致性校验缺失。

- [ ] 继续清理 `server/innodb/engine/index_transaction_adapter.go`
  - 关键点：补齐错误码路径；`lockManager` 不可用时不应静默跳过；补齐单资源释放能力。
  - 风险：并发路径会出现锁状态不一致。

- [ ] 继续补齐 `server/innodb/engine/unified_executor.go` 与 `server/innodb/engine/show_executor.go`
  - `show_executor.go` 当前 SHOW 相关返回空结果/文本提示，需接真实元数据并标准化错误返回。
  - 风险：元数据查询不真实，影响运维命令可用性。

验收：
- `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML|Show)' -count=1`
- 用 `errors.As(err, *engine.ExecutionError)` 在失败路径断言 `ErrorCode`。

### P0-06 崩溃恢复演练标准化

- [ ] 固定 redo / undo / half-commit 场景输入。
- [ ] `start -> crash -> restart -> verify` 脚本执行真实进程演练。
- [ ] 每轮输出 `before.json`、`after.json`、`diff.json`、日志路径、耗时。
- [ ] 最少 3 轮复放，报告统一落盘。

涉及：`scripts/crash_recovery_process_drill.sh`、`scripts/p0_b_recovery_audit.sh`、`docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`、`reports/p0_b_audit/`

### P0-07 灰度演练写入阻塞修复

- [ ] 修复 `table mysql.t1 not found in storage mapping` 根因并保证 write-stage 可恢复。
- [ ] read / write / restore 合并为单条可复放脚本。
- [ ] 每阶段记录 schema/table/stage/error_code/log_path/duration_ms。

涉及：`scripts/p0_e_backup_snapshot.sh`、`docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`、`server/innodb/manager/`、`server/innodb/engine/`

### P0-08 慢查询日志

- [ ] 完成配置链路：`slow_query_log`、阈值、路径。
- [ ] SQL 执行阶段打点并落盘：`sql,sql_fingerprint,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage,trace_id`。
- [ ] 补一个可复现样例脚本与验证。

### P0-09 指标与告警导出

- [ ] 导出最小指标：QPS、错误率、连接数、活跃事务。
- [ ] 导出 P50/P95/P99、redo、undo、锁等待核心指标。
- [ ] 增加最小告警规则与可触发演练记录。

涉及：`server/innodb/manager/transaction_manager.go`、`server/innodb/manager/checkpoint_monitor.go`、`server/net/`、`server/conf/`

## P1：功能完整性（P0 绿后）

- [ ] 子查询执行：`IN` / `EXISTS` / `相关子查询` / `IN -> SEMI JOIN`。
  - 路径：`server/innodb/engine/subquery_executor*.go`、`server/innodb/plan/physical_plan.go`
- [ ] 窗口函数：`ROWS/RANGE BETWEEN` 与 `ROW_NUMBER`、`RANK`、`SUM/AVG OVER`。
  - 路径：`server/innodb/engine/window_function_executor.go`
- [ ] HAVING / DISTINCT / LIMIT OFFSET 完整链路。
  - 路径：`server/innodb/engine/select_executor.go`、`server/innodb/engine/volcano_executor.go`
- [ ] 优化器真实化：代价估算与选择率规则从桩实现升级。
  - 路径：`server/innodb/plan/physical_plan.go`、`cost_estimator.go`、`selectivity_estimator.go`
- [ ] 统计信息采集真实化与更新机制。
  - 路径：`server/innodb/plan/statistics_collector_helpers.go`
- [ ] 页面 IO 与 MVCC 页序列化闭环。
  - 路径：`server/innodb/storage/wrapper/page/*.go`、`server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- [ ] 索引重建/优化/一致性检查/SHOW INDEX 统计返回可验证。
  - 路径：`server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/enhanced_btree_manager.go`

## P2：协议与高级能力（可延后）

- [ ] 协议能力（`COM_STMT_SEND_LONG_DATA`、`COM_FIELD_LIST`、多结果集、Batch 等）明确支持矩阵与行为一致性。
- [ ] 存储过程/触发器/视图/分区/在线DDL 的功能范围与错误返回策略定义。

## 当前工程风险热点（不一定全部修改，供排期参考）

- `server/innodb/engine`：`fmt.Errorf` 和 `return nil` 混杂在关键 DML/执行链路，已是高优先修复区。
- `server/innodb/engine/show_executor.go`：SHOW 路径当前是简化实现。
- `server/innodb/engine/index_transaction_adapter.go`：锁管理在 `lockManager` 缺失时可能静默跳过。
- `scripts` 多数 p0 演练脚本已存在，但证据输出尚未标准化到统一 schema。

## 执行顺序建议（今天可执行）

1. 先把 P0-03、P0-06、P0-07 完成到可复验状态；
2. 同时补 P0-08、P0-09；
3. P0 全绿后再排 P1；
4. 每完成一条，补 `go test` 失败/成功证据和脚本报告。
