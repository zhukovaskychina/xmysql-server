# 修改清单（当前可执行版，2026-06-11）

> 目标：明确“项目还需要改什么”，并给出可直接排期的执行清单。  
> 扫描口径：`TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|待实现|stub`

## 0. 当前基线

- 参考主清单：`docs/planning/MODIFICATION_CHECKLIST_20260610.md`
- 该清单里当前未勾选项：`57`，已完成：`17`
- 当前最优先问题仍是发布前 P0 闭环项（P0-03 / P0-06 / P0-07 / P0-08 / P0-09）
- 在 `server/innodb/engine` 内可复用的错误类型已就绪（`ExecutionError` 已定义），但实际链路接入比例仍不足

## 1. P0（必须先改）

### P0-03 执行器失败可见性与错误分类（未闭环）

影响文件：
- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/dml_executor.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/engine/index_transaction_adapter.go`

改动项：
- 用 `ExecutionError` 替换关键路径字符串错误返回（尤其是执行、事务、读写失败路径）
- 去掉关键失败分支对错误文本的匹配判断，改为错误码/错误类型判断
- commit/rollback/prepare/关键 DML 失败要上抛到上层，不允许只打日志后继续
- 日志和错误返回中必须包含：`module / stage / schema / table / txn_id / error_code / sql / cause`
- 补齐失败路径单测，至少覆盖：表不存在、存储读取失败、重复键、提交失败、回滚失败

验收建议：
- `go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|DML|Index|ExecuteSelect)' -count=1`

### P0-06 崩溃恢复演练闭环（未闭环）

影响文件/脚本：
- `server/innodb/manager/crash_recovery.go`
- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

改动项：
- 固化 `redo / undo / half-commit` 三类场景
- 每类至少 `3` 轮 `start -> crash -> restart -> verify`
- 统一报告字段（前后快照、差异、耗时、日志路径、场景参数）
- 输出可复放目录结构与归档方式

验收建议：
- `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07 灰度写入阶段阻塞修复（未闭环）

影响文件：
- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`、`server/innodb/manager/`

改动项：
- 修复 `storage mapping` 阶段阻塞（尤其是表映射缺失导致的只读/写入卡住）
- 串联 read/write/restore 阶段链路，形成一条可复放脚本
- 日志补齐：`stage / schema / table / duration_ms / error_code / log_path`

验收建议：
- `./scripts/p0_e_backup_snapshot.sh list`
- `go test ./server/innodb/engine ./server/innodb/manager -run 'Test.*(Crash|Recovery|Write|Mapping|Backup|Restore|Gray)' -count=1`

### P0-08 慢查询日志闭环（未闭环）

影响文件：
- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

改动项：
- 补齐慢查询开关链路：`slow_query_log`、`slow_query_log_file`、`long_query_time_ms`
- 在慢查询日志中必带字段：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg / stage`
- 增加可复放的慢查询样例场景

验收建议：
- `go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log' -count=1`

### P0-09 指标与告警（未闭环）

影响文件：
- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

改动项：
- 导出指标：QPS、错误率、连接数、活跃事务数、P50/P95/P99
- 同时输出 checkpoint / redo / undo / 锁等待
- 补齐最小告警规则和演练记录

验收建议：
- `go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## 2. 高频 TODO 文件（按扫描命中先做）

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/parallel.go`（8）
3. `server/innodb/engine/index_reading_test.go`（8）
4. `server/innodb/plan/statistics_collector_helpers.go`（7）
5. `server/innodb/plan/physical_plan.go`（6）
6. `server/innodb/metadata/convert.go`（6）
7. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
8. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
9. `server/innodb/plan/join_order_optimizer_helpers.go`（5）
10. `server/innodb/manager/page.go`（5）

## 3. P1 / P2（P0 绿灯后）

- P1：`server/innodb/engine/subquery_executor*.go`、`server/innodb/engine/cte_executor.go`、`server/innodb/engine/window_function_executor.go`、`server/innodb/plan/{physical_plan.go,parallel.go,cost_estimator.go,statistics_collector_helpers.go}`、`server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/index_manager.go`、`server/innodb/manager/enhanced_btree_manager.go`
- P2：`server/innodb/storage/wrapper/page/*.go`、`server/innodb/storage/store/pages/compressed_page.go`、`server/innodb/storage/store/mvcc/*`

## 4. 建议执行顺序

1. 先闭环 P0-03（核心稳定性）
2. 并行推进 P0-06 与 P0-07
3. 同步推进 P0-08 与 P0-09
4. P0 全部过后再进入 P1
