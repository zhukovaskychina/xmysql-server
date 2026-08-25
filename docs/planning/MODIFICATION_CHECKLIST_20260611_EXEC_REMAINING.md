# XMySQL Server 修改清单（执行版本，2026-06-11）

> 目标：明确「继续完成 `MODIFICATION_CHECKLIST_20260610.md`」的下一个可执行步骤。

## 一、当前状态（已验收）

- Go 工具链：继续使用 `go1.24.3`。
- `server/innodb/engine` 包测试通过：
  - `go1.24.3/bin/go test ./server/innodb/engine -count=1`
  - `go1.24.3/bin/go test ./server/innodb/engine -run 'TestUnifiedExecutor|Test.*Error' -count=1`

## 二、P0-03 执行器错误可见性（进行中）

### 已完成（本阶段已落地）

- [x] `server/innodb/engine/unified_executor.go`
  - `SELECT` 路径改为结构化错误。
  - `stmt.SQL` 字段访问问题修复为 `sqlparser.String(stmt)`。
  - 避免将 `nil` 作为错误原因传入 `NewExecutionErrorWithCause`。

- [x] `server/innodb/engine/storage_adapter.go`
  - `metadata.Table` 的 schema 名获取改为 `schema.Schema.Name`（避免不存在的 `SchemaName` 字段）。

- [x] `server/innodb/engine/executor.go`
  - `NewExecutionErrorWithCause` 调用中的格式化参数问题修复。

- [x] `server/innodb/engine/index_transaction_adapter.go`
  - `fmt` 包 `%w` 包装符替换为 `%v`，消除 vet 报错。

- [x] `server/innodb/engine/unified_executor_test.go`
  - 统一失败路径断言改为 `errors.As(...,*ExecutionError)`。
  - 覆盖 `ExecutionErrorCode`（如 `E_OPTIMIZER`、`E_VALIDATION`）。

### 下一步（建议优先）

- [ ] `server/innodb/engine/executor.go` 的剩余入口路径继续补齐结构化错误（如多处 `errors.As` 未覆盖的分支）。
- [ ] `server/innodb/engine/dml_operators.go` 的剩余失败路径继续覆盖 `E_STORAGE_* / E_DUPLICATE_KEY / E_VALIDATION`。
- [ ] `server/innodb/engine/storage_integrated_dml_executor.go`、`storage_integrated_dml_helper.go`、`storage_integrated_index_helper.go` 继续替换主路径 `fmt.Errorf`。
- [ ] 补充更多失败用例：表不存在、主键回表失败、索引同步失败、存储映射缺失。

建议命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1
```

## 三、P0-06 崩溃恢复演练（待执行）

- [ ] `scripts/crash_recovery_drill.sh`
- [ ] `scripts/crash_recovery_process_drill.sh`
- [ ] `scripts/p0_b_recovery_audit.sh`
- [ ] 生成 `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

建议命令：

```bash
CR_PROC_REPORT_DIR=./reports p0_audit_max_dirs=5 ./scripts/p0_b_recovery_audit.sh
```

## 四、P0-07 灰度写入阶段阻塞（待执行）

- [ ] 修复 `table ... not found in storage mapping`。
- [ ] 整合 read/write/restore 演练链路。
- [ ] 输出字段包含：`stage/schema/table/error_code/log_path/duration_ms`。

建议命令：

```bash
./scripts/p0_e_backup_snapshot.sh list
```

## 五、P0-08/P0-09 可观测性（待执行）

- [ ] 慢查询日志链路（配置/落盘/字段）
- [ ] 监控指标与告警（QPS、错误率、连接、活跃事务、P50/P95/P99、redo/undo/锁等待）

建议命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log|Test.*Stats|Test.*Metrics|Test.*Alert|Test.*Monitor' -count=1
```

## 六、P1 阶段（P0 完成后）

- [ ] 子查询（IN / EXISTS / 标量 / 相关子查询）
- [ ] 窗口函数（ROWS/RANGE）
- [ ] HAVING、DISTINCT、LIMIT OFFSET

## 七、验收更新记录

- 每完成一项，请在该文件中将对应条目标记为完成，并补齐当次测试命令与失败定位。
