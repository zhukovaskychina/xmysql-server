# XMySQL Server 修改清单（待改项，2026-06-11）

> 目标：回答“项目还差什么”，并给出下一步可执行的改动清单。  
> 扫描口径：`server/innodb/engine | manager | storage` 下 `*.go`。  
> 命令：`rg -n "TODO|FIXME|not implemented|unimplemented|placeholder|暂未实现|暂时|待实现|stub|简化实现" server/innodb/{engine,manager,storage} --glob '*.go'`.

## 一、当前整体状态

- `engine` 目录关键标记：`90`
- `manager` 目录关键标记：`114`
- `storage` 目录关键标记：`82`
- 合计：`286`
- 说明：这些数字包含了生产阻塞项、测试桩、测试占位以及一些历史简化实现；不等于全部功能完成度。  

## 二、P0（先改）

### P0-03 执行器错误可见性与关键失败路径（未闭环）

- [ ] 继续补齐 `server/innodb/engine/executor.go`：在剩余入口分支里统一返回 `ExecutionError`，并补对应的失败断言（目前仍有大量 `fmt.Errorf` 文本错误）。
- [ ] 继续补齐 `server/innodb/engine/dml_operators.go`：
  - `findDuplicateRecord` 当前返回 `findDuplicateRecord not fully implemented`。
  - `insertRow/updateRecord/getTableRows` 等是简化实现，未覆盖唯一索引冲突边界与 Undo/索引一致性。
  - `replace/delete` 路径仍有简化实现和占位行为。
- [ ] 继续补齐 `server/innodb/engine/storage_integrated_dml_helper.go`：
  - `findUpdateRows` 与 `findDeleteRows` 空条件返回行为不可作为最终行为。
- [ ] 继续补齐 `server/innodb/engine/storage_integrated_index_helper.go` 的 TODO：
  - 索引重建、优化、完整性检查、长度/类型验证仍为占位/简化。
- [ ] 继续补齐 `server/innodb/engine/storage_integrated_checkpoint.go`：
  - 写操作阻塞与解阻塞逻辑为 TODO。
- [ ] 补齐 `server/innodb/engine/index_transaction_adapter.go`：
  - `ReleaseLock` 仅 `ReleaseLocks`，且注释明确说明单锁释放缺失。
  - `lockManager == nil` 直接返回成功，可能掩盖并发问题。
- [ ] 为上述分支补齐回归测试：新增失败路径测试并校验 `ExecutionErrorCode`。

### P0-06 崩溃恢复演练闭环（未闭环）

- [ ] 标准化脚本入口：`scripts/crash_recovery_drill.sh`、`scripts/crash_recovery_process_drill.sh`。
- [ ] 输出固定格式审计：`scripts/p0_b_recovery_audit.sh` + `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`。
- [ ] 输出 3 轮可复放报告（redo、undo、半提交）并绑定 commit/时间戳。

### P0-07 灰度写入阶段阻塞修复（未闭环）

- [ ] 继续推进 `stage1 insert` 演练阻塞点：存储映射找不到表时可恢复的处理链。
- [ ] 明确恢复动作与回退动作的最小可复用步骤，并补入演练时间线。

### P0-08 慢查询日志（未闭环）

- [ ] 落地慢查询日志路径和字段规范：module、schema、table、sql、rows、cost、trace、duration_ms。
- [ ] 提供一条可复用慢查询样例输出。

### P0-09 指标与告警（未闭环）

- [ ] 导出最小指标：QPS、错误率、连接数、活跃事务、P50/P95/P99、redo/undo/锁等待。
- [ ] 补告警规则与告警触发演练记录。

## 三、P1（建议下一阶段）

- [ ] 子查询、窗口函数、HAVING / DISTINCT / LIMIT OFFSET 的完整实现与回归测试。
- [ ] 优化器与代价估算：`plan/selectivity_estimator.go`、`plan/cost_estimator.go`。
- [ ] 索引一致性与 rebuild 路径（`manager/enhanced_btree_*`、`storage_integrated_index_helper.go`）。
- [ ] 关键页面序列化/反序列化与 MVCC 页字段实现。

## 四、执行顺序（建议）

1. 先关 P0-03（按文件顺序）：`unified_executor.go` → `dml_operators.go` → `executor.go` → `index_transaction_adapter.go`。
2. 同步启动 P0-06、P0-07 的演练闭环，先拿到可复放证据。
3. 再并行推进 P0-08/P0-09。
4. P0 全绿后再进入 P1。

## 五、验收命令（可复用）

```bash
go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML|Slow|Stats|Metrics)' -count=1
```

```bash
CR_PROC_REPORT_DIR=./reports p0_audit_max_dirs=5 ./scripts/p0_b_recovery_audit.sh
```
