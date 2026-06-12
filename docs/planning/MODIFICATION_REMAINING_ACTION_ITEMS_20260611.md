# 修改清单（待改项汇总，2026-06-11）

> 目标：给出“还缺什么、先改什么、改完如何验收”。

## 一、当前基线（最新扫描）

- `server/*.go` 目录内关键 TODO/FIXME/placeholder/未实现类标记约 `137` 个。
- 引擎关键文件 `fmt.Errorf` 占比仍高（说明大部分错误路径仍依赖字符串）：
  - `server/innodb/engine/executor.go`：90
  - `server/innodb/engine/unified_executor.go`：54
  - `server/innodb/engine/dml_operators.go`：57
  - `server/innodb/engine/storage_integrated_dml_executor.go`：54
  - `server/innodb/engine/dml_executor.go`：67
  - `server/innodb/engine/storage_integrated_dml_helper.go`：30
  - `server/innodb/engine/storage_adapter.go`：12
  - `server/innodb/engine/index_transaction_adapter.go`：29
  - `server/innodb/engine/storage_integrated_index_helper.go`：17
- `ExecutionError` 体系仅在 `server/innodb/engine/execution_error.go` 与极少数调用点出现，尚未形成执行链路主干闭环。
- 引擎内仍有明确占位/未实现注释：
  - `window_function_executor.go`（窗口帧）
  - `subquery/cte/volcano` 相关路径（复杂查询支持缺口）
  - `storage_integrated_checkpoint.go`（写阻塞相关）
  - 若干 manager/page/wrapper 页面读写与校验 TODO

## 二、优先级清单（建议按顺序）

### P0：先改（影响可用性/稳定性）

#### P0-01 执行器错误分类与可观测性
- 目标：错误不要靠字符串分支，统一返回 `ExecutionError`，并携带 `ErrorCode/Schema/Table/SQL/TxnID`。
- 文件（优先）:
  - `server/innodb/engine/execution_error.go`
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/index_transaction_adapter.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 关键动作：
  - 将事务 begin/commit/rollback、读写、元数据缺失、唯一键冲突、算子未打开路径改为 `ExecutionError`。
  - 关键失败用例改为 `errors.As(err, *ExecutionError)`，并按 `ErrorCode` 断言。

#### P0-02 崩溃恢复演练
- 目标：固定 redo/undo/半提交三类场景，形成可重复脚本。
- 文件：
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `server/innodb/manager/crash_recovery.go`（若有改动点）
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 关键动作：每类场景至少 3 轮 `start -> crash -> restart -> verify`，输出前后快照与差异。

#### P0-03 灰度写入阻塞修复（P0_E 相关）
- 目标：消除 `table ... not found in storage mapping` 阶段阻塞。
- 文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 关键动作：把只读/写入/回退三阶段串成可复放流程，输出阶段耗时与错误码。

#### P0-04 观测能力最小闭环
- 目标：慢查询、指标和告警具备最小可用能力。
- 文件：
  - `server/conf/`, `server/session/`, `server/dispatcher/`
  - `server/innodb/engine/`, `server/innodb/manager/`, `server/net/`
- 关键动作：
  - `slow_query_log` 和 `long_query_time_ms` 生效。
  - 关键指标（QPS/错误率/连接数/活跃事务/P99/redo/undo/锁等待）可读。
  - 告警触发与恢复流程可复现。

### P1：本阶段可并行推进

#### P1-01 复杂 SQL 基线补齐
- 文件：
  - `server/innodb/engine/subquery_executor*.go`
  - `server/innodb/engine/cte_executor.go`
  - `server/innodb/engine/window_function_executor.go`
- 子项：子查询、CTE、窗口帧（ROWS/RANGE）至少有基础执行和回归。

#### P1-02 优化器/计划
- 文件：
  - `server/innodb/plan/physical_plan.go`
  - `server/innodb/plan/cost_estimator.go`
  - `server/innodb/plan/statistics_collector_helpers.go`
- 子项：代价估算与统计采集去桩。

#### P1-03 索引与一致性
- 文件：
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/manager/index_manager.go`
  - `server/innodb/manager/enhanced_btree_manager.go`
- 子项：索引重建/优化/一致性检查。

### P2：后续稳定性增强

#### P2-01 存储页/IO
- `server/innodb/storage/wrapper/page`（Read/Write/序列化/校验和）
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/manager/page.go` 与 `page_allocator/space_manager` 等未实现逻辑

#### P2-02 记录格式与系统能力
- `server/innodb/storage/wrapper/record/*` 尚未完成的记录实现
- 协议与高级 SQL 能力可按优先级后置

## 三、建议本周执行清单（10 项）

1. `server/innodb/engine/execution_error.go`
2. `server/innodb/engine/executor.go`
3. `server/innodb/engine/storage_adapter.go`
4. `server/innodb/engine/storage_integrated_dml_helper.go`
5. `server/innodb/engine/dml_operators.go`
6. `server/innodb/engine/storage_integrated_dml_executor.go`
7. `server/innodb/engine/index_transaction_adapter.go`
8. `server/innodb/engine/storage_integrated_index_helper.go`
9. `scripts/crash_recovery_process_drill.sh`
10. `server/innodb/engine/window_function_executor.go`

## 四、验收命令（建议）

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Index|DML|Recovery|Window|Subquery|CTE)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/plan ./server/innodb/manager -run 'Test.*(Cost|Selectivity|Index|Stats|Metrics)' -count=1
rg -n "ExecutionError\(|errors\.As\(" server/innodb/engine | head -n 200
rg -n "TODO:|FIXME|not implemented|暂未实现|未实现|stub" server/innodb/engine server/innodb/manager server/innodb/storage --glob '*.go' > /tmp/todo_after.txt
wc -l /tmp/todo_after.txt
```

