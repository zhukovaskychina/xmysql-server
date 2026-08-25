# XMySQL Server 修改清单（自动汇总，2026-06-11）

## 现状基线

扫描口径（当前会话）：
`rg -n "TODO|FIXME|not fully implemented|not implemented|unimplemented|placeholder|暂未实现|暂时返回|stub|暂时" server/innodb/{engine,manager,storage} --glob '*.go'`

- engine：`28`
- manager：`57`
- storage：`64`
- 合计：`149`

说明：该统计只用于趋势判断，不代表全部功能完成度。以下清单仅列出本轮影响功能正确性、可恢复性、可观测性、发布前稳定性的优先项。

## P0（本轮必须改）

### P0-03 执行链路错误可见性与失败可恢复性

- `server/innodb/engine/dml_operators.go`
  - `findDuplicateRecord` 仍是明示未实现返回。
  - `insertRow` / `updateRecord` / `parseInsertRows` / `valueToInterface` 为简化实现，实际语义与唯一键校验、Undo/索引同步、数据校验未闭环。
- `server/innodb/engine/index_transaction_adapter.go`
  - `ReleaseLock` 仅打印并调用 `ReleaseLocks`，不是单资源释放。
  - `lockManager == nil` 时直接返回成功，可能掩盖并发相关错误。
- `server/innodb/engine/executor.go`
  - `CREATE DATABASE` 路径标记“暂时简化实现”。
  - 部分关键失败路径仍有未统一封装的错误返回，建议统一为 `ExecutionError`，保留 `module/stage/schema/table/txn/error_code`。

### P0-06 崩溃恢复演练闭环

- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

缺项：三类场景（redo / undo / half-commit）每类至少 3 轮可复放证据，包含恢复前后快照、差异和时耗。

### P0-07 写入阶段阻塞修复

- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`

缺项：修复 `table ... not found in storage mapping` 阶段性阻塞，并闭环 read / write / restore。

### P0-08 与 P0-09 可观测性最小闭环

- `server/conf/`, `server/session/`, `server/dispatcher/`, `server/innodb/engine/`
  - 慢查询开关/路径/阈值链路未形成一致字段输出。
- `server/innodb/manager/`, `server/net/`, `server/conf/`
  - 关键指标与告警字段仍缺少持续导出与复现演练。

## P1（本轮后续）

- 查询能力与执行器：`server/innodb/engine/cte_executor.go`、`engine/window_function_executor.go`、`plan/physical_plan.go`、`plan/parallel.go`、`plan/cost_estimator.go`。
- 索引与一致性：`engine/storage_integrated_index_helper.go` 的重建/优化/一致性 TODO 与 `manager/index_manager.go`。
- 存储页实现：`storage/wrapper/page/*.go`、`storage/wrapper/mvcc/*.go` 中的读写/序列化 TODO。

## P2（可延后）

- 压缩页、空间管理器和 MVCC 高复杂度路径中的非阻塞能力（`storage/store/pages/compressed_page.go`、`storage/store/pages/allocated_page.go`、`storage/store/mvcc/*`）。
- 解析器、查询优化长期扩展项中遗留 TODO 不影响当前 P0 收敛。

## 本次建议执行顺序

1. 先完成 `P0-03`（文件优先级：`dml_operators.go` -> `index_transaction_adapter.go` -> `executor.go`）。
2. 同步推进 `P0-06` 与 `P0-07` 的脚本和日志闭环。
3. 并行补 `P0-08`、`P0-09`。
4. P0 全绿后再做 P1。

## 推荐验收命令

```bash
go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|DML|Index)' -count=1
CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh
./scripts/p0_e_backup_snapshot.sh list
```
