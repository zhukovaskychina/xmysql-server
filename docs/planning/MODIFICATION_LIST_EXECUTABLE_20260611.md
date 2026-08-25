# 修改清单（可执行版，2026-06-11）

> 目的：把“项目还差哪些要改”落成一个可直接执行的任务单。  
> 说明：状态按 `未开始 / 进行中 / 已完成`，负责人先填 `待分配`，后续可替换成实际 owner。

## 一、P0（阻塞项）

### P0-03 执行器错误可见性与失败闭环

1. [ ] `server/innodb/engine/execution_error.go`  
   - **改动**：确保所有关键失败路径返回统一的 `ExecutionError`，统一字段含义（`error_code/stage/schema/table/txn_id/cause/sql`），去掉关键路径中的文本分支判断。  
   - **验收**：关键单测中使用 `errors.As(err, &engine.ExecutionError)` + 校验 `ErrorCode`。  
   - **负责人 / 状态**：待分配 / 未开始

2. [ ] `server/innodb/engine/executor.go`  
   - **改动**：`SELECT/INSERT/UPDATE/DELETE` 关键分支错误返回使用结构化错误；保留错误上下文。  
   - **验收**：`go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|DDL|DML)' -count=1`，失败路径不再只返回通用文本。  
   - **负责人 / 状态**：待分配 / 未开始

3. [ ] `server/innodb/engine/unified_executor.go`  
   - **改动**：`WHERE`、`INSERT/UPDATE/DELETE`、`ORDER BY`/`LIMIT`/`SHOW` 关键分支去除“简化返回”与未结构化错误；统一上送错误码。  
   - **验收**：新增失败路径用例覆盖 `NoTable/Unsupported/WhereNotSupported/NoRootOperator`。  
   - **负责人 / 状态**：待分配 / 未开始

4. [ ] `server/innodb/engine/storage_integrated_dml_executor.go`、`server/innodb/engine/dml_operators.go`、`storage_integrated_dml_helper.go`、`storage_integrated_index_helper.go`  
   - **改动**：把简化/占位逻辑替换为真实分支：
     - `findDuplicateRecord`、`insertRow`、`updateRecord`、`deleteOldRecord`、`insertNewRecord`、`deleteRecord` 等核心行为不再返回空实现。  
     - `WHERE` 空条件和“按整页当一行”行为改为真实扫描/过滤。  
     - 索引更新失败/主键冲突的错误码完整输出。  
   - **验收**：`go test ./server/innodb/engine -run 'Test.*(DML|Insert|Update|Delete|Index|Duplicate|Where)' -count=1`。  
   - **负责人 / 状态**：待分配 / 未开始

### P0-06 崩溃恢复演练闭环

5. [ ] `server/innodb/manager/crash_recovery.go`  
   - **改动**：`redo / undo / half-commit` 重放路径补齐验证，去掉“直接应用日志”式简化分支（或给出明确证据注记）。  
   - **验收**：每类场景复现脚本输出 `before.json`、`after.json`、`diff.json`、耗时和日志路径。  
   - **负责人 / 状态**：待分配 / 未开始

6. [ ] `scripts/crash_recovery_drill.sh`、`scripts/crash_recovery_process_drill.sh`、`scripts/p0_b_recovery_audit.sh`、`docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`  
   - **改动**：输出命名固定、轮次固定、连续执行（≥3轮）、失败可重放。  
   - **验收**：`CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh` 通过且每轮有报告文件。  
   - **负责人 / 状态**：待分配 / 未开始

### P0-07 灰度写入阻塞

7. [ ] `server/innodb/manager/table_storage_mapping.go`、`server/innodb/manager/create_table*` 相关链路  
   - **改动**：确认 DDL 创建后立即注册 `schema.table -> storage mapping`，修复 `table mysql.t1 not found in storage mapping`。  
   - **验收**：`create table` 后立即执行一次 `read/write` 测试通过。  
   - **负责人 / 状态**：待分配 / 未开始

8. [ ] `scripts/p0_e_backup_snapshot.sh`、`docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`  
   - **改动**：把 read/write/restore 三阶段串成可复用链路，日志带 `stage/schema/table/error_code/log_path/duration_ms`。  
   - **验收**：`./scripts/p0_e_backup_snapshot.sh list` 显示三阶段通过且有失败复盘文件。  
   - **负责人 / 状态**：待分配 / 未开始

### P0-08 慢查询日志

9. [ ] `server/conf/`、`server/session/`、`server/dispatcher/`、`server/innodb/engine/`  
   - **改动**：完善慢查询配置链路（`slow_query_log/slow_query_log_file/long_query_time_ms`）并打通 SQL 执行打点。  
   - **验收**：存在结构化 slow log 条目，字段至少包含 `sql,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage`。  
   - **负责人 / 状态**：待分配 / 未开始

### P0-09 指标与告警

10. [ ] `server/innodb/manager/transaction_manager.go`、`server/innodb/manager/checkpoint_monitor.go`、`server/net/`、`server/conf/`  
    - **改动**：导出 QPS/错误率/活跃连接/活跃事务、P50/P95/P99、redo/undo/锁等待；补一个可触发可恢复告警演练。  
    - **验收**：`go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`，有演练证据。  
    - **负责人 / 状态**：待分配 / 未开始

## 二、P1（中优先）

11. [ ] `server/innodb/engine/cte_executor.go`  
    - **改动**：修正递归/依赖图逻辑 TODO，去掉递归无保护简化假设。  
    - **验收**：增加递归 CTE 与循环引用检测测试。  
    - **负责人 / 状态**：待分配 / 未开始

12. [ ] `server/innodb/engine/subquery_executor_test.go` / 相关子查询执行链路  
    - **改动**：将“joinConds 空则全部匹配”的简化假设替换为可验证逻辑（至少标量和 EXISTS）。  
    - **验收**：新增子查询正确性用例，减少误匹配。  
    - **负责人 / 状态**：待分配 / 未开始

13. [ ] `server/innodb/engine/window_function_executor.go`  
    - **改动**：实现窗口帧 `ROWS/RANGE BETWEEN ... AND ...`，补齐 `ROW_NUMBER`/`RANK`/滑动窗口场景。  
    - **验收**：`go test ./server/innodb/engine -run 'Test.*Window' -count=1`。  
    - **负责人 / 状态**：待分配 / 未开始

14. [ ] `server/innodb/plan/physical_plan.go`、`cost_estimator.go`、`statistics_collector_helpers.go`、`join_order_optimizer_helpers.go`  
    - **改动**：代价估算、选择率、统计收集从固定值/简化表达升级到真实估算逻辑。  
    - **验收**：优化器相关单测断言成本字段有变化并可解释。  
    - **负责人 / 状态**：待分配 / 未开始

## 三、P2（可延后）

15. [ ] `server/protocol/password_validator.go`、`charset_manager.go`  
    - **改动**：补齐密码验证和字符集转换校验（当前多处为简化实现）。  
    - **验收**：协议安全类测试新增覆盖。  
    - **负责人 / 状态**：待分配 / 未开始

16. [ ] `server/innodb/storage/wrapper/page/*.go`、`storage/wrapper/mvcc/mvcc_page.go`  
    - **改动**：页面读写、序列化、反序列化逐步替换简化/占位逻辑。  
    - **验收**：页面级读写 + 序列化一致性测试通过。  
    - **负责人 / 状态**：待分配 / 未开始

---

## 四、优先执行顺序（建议）

1. 先把 P0-03 关了（这是当前主要阻塞）。  
2. 并行推进 P0-06 与 P0-07。  
3. 同步补 P0-08 与 P0-09 的观测闭环。  
4. 再排 P1/ P2。

