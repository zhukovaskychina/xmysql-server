# 修改清单（单一执行版，2026-06-11）

生成时间：2026-06-11（本地扫描快照）

## 1. 变更基线

- 关键词扫描口径：`TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|待实现|not implemented in test|not implemented yet|stub`
- 范围：
  - `server/**/*.go`
  - `server/**/*.sh`
  - `docs/**/*.md`
  - `scripts/**/*.sh`
- 结果：
  - `server/**/*.go`：约 `187` 处（约 `95` 个文件）
  - `docs/scripts`（按上述口径）：约 `228` 处

> 当前仓库也存在大量临时产物与历史报告文件，清单按“功能阻塞优先级”聚焦，不把它们重复展开。

## 2. P0（先做，阻塞发布）

### [ ] P0-03 执行器失败可见性与错误分类（未闭环）

- 涉及文件：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/dml_executor.go`
  - `server/innodb/engine/index_transaction_adapter.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 待改项：
  - 关键路径返回错误的 `fmt.Errorf` 替换为结构化 `ExecutionError`
  - `P0-03` 成功判据字段必须包含：`module/stage/schema/table/txn_id/error_code/sql/cause`
  - 去除关键失败分支的错误字符串文本判断
  - `commit/rollback` 等关键返回值必须从调用端检查，不允许仅记录日志
  - 补充失败路径单测，使用 `errors.As(err, *engine.ExecutionError)` + `ErrorCode` 断言
- 示例场景：
  - 表映射缺失
  - 存储读取/写入失败
  - begin/commit/rollback 失败
  - 重复键冲突

验收命令：

```bash
go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|DDL|DML|Index)' -count=1
```

### [ ] P0-06 崩溃恢复演练闭环（未闭环）

- 涉及文件/脚本：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 待改项：
  - 固定三类场景：`redo` / `undo` / `half-commit`
  - 每类复现至少 3 次并形成 `start -> crash -> restart -> verify` 报告链
  - 统一演练目录、文件命名、对账字段，保证可复放和可审计

### [ ] P0-07 灰度写入阶段阻塞修复（未闭环）

- 涉及文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 待改项：
  - 修复 `storage mapping` 阶段阻塞（如“table not found”）
  - 构建 read / write / restore 三阶段脚本闭环
  - 日志补齐：`stage/schema/table/error_code/log_path/duration_ms`

### [ ] P0-08 慢查询日志闭环（未闭环）

- 涉及文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 待改项：
  - `slow_query_log` / `slow_query_log_file` / `long_query_time_ms` 配置链路补齐
  - 日志字段至少包含：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg / stage`
  - 增加复放场景测试

验收命令：

```bash
go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log' -count=1
```

### [ ] P0-09 指标与告警导出（未闭环）

- 涉及文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 待改项：
  - 补齐基础指标：QPS、错误率、连接数、活跃事务、P50/P95/P99、redo/undo/锁等待
  - 增加可复现的告警规则与验证用例
- 验收命令：

```bash
go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1
```

## 3. P1（P0 完成后排期）

- `server/innodb/engine/subquery_executor*.go`、`server/innodb/engine/cte_executor.go`：子查询/CTE
- `server/innodb/engine/window_function_executor.go`：窗口函数
- `server/innodb/plan/physical_plan.go`、`server/innodb/plan/parallel.go`、`server/innodb/plan/cost_estimator.go`：优化器与并行
- `server/innodb/plan/statistics_collector_helpers.go`：统计信息闭环
- `server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/index_manager.go`、`server/innodb/manager/enhanced_btree_manager.go`：索引一致性与重建/优化路径

## 4. P2（可延后）

- `server/innodb/storage/wrapper/page/page_impl.go`
- `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`
- `server/innodb/storage/store/pages/compressed_page.go`
- `server/innodb/storage/store/mvcc/*`

## 5. 当前高频 TODO 文件（优先清理）

`server/innodb/manager/space_expansion_concurrent_test.go`（10）、
`server/innodb/plan/parallel.go`（8）、
`server/innodb/engine/index_reading_test.go`（8）、
`server/innodb/plan/statistics_collector_helpers.go`（7）、
`server/innodb/plan/physical_plan.go`（6）、
`server/innodb/metadata/convert.go`（6）、
`server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）、
`server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）、  
`server/innodb/plan/join_order_optimizer_helpers.go`（5）、
`server/innodb/manager/page.go`（5）

## 6. 今日推荐顺序（建议）

1. 先关闭 P0-03（错误链路一口气接完）
2. 再并行处理 P0-06、P0-07（恢复和灰度链路互斥少、收口快）
3. 同步补齐 P0-08、P0-09（观测链路）
4. P0 通过后推进 P1
