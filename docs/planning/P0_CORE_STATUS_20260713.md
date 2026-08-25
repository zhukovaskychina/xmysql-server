# P0 核心实现状态快照（2026-07-13）

本文用于覆盖 2026-06-11 历史扫描中的过期判断。旧文档仍保留原始扫描背景，但判断当前 P0 核心实现状态时，以本文和可复验命令为准。

## 当前结论

- P0 核心正确性路径已完成一轮最小闭环：DML 写入、SHOW 元数据、checkpoint 写阻塞、buffer pinned-page 驱逐、EnhancedBTree rebuild/drop、engine 包拆分验证。
- 当前可复验命令：

```bash
scripts/verify_p0_core.sh
```

- 上述命令覆盖 P0 core 的定向回归；需要做全量发布判断时，仍要继续执行崩溃恢复、灰度、慢查询、指标告警等生产验收任务。

## 已关闭的历史 P0 core 项

| 历史项 | 当前状态 | 证据路径 |
| --- | --- | --- |
| DML 算子未接真实存储写接口 | 已关闭核心路径 | `server/innodb/engine/dml_operators.go`、`server/innodb/engine/storage_adapter.go` |
| StorageIntegratedDML 行格式、UPDATE 覆盖写、DELETE 页内一致性 | 已关闭核心路径 | `server/innodb/engine/storage_integrated_dml_helper.go`、`server/innodb/engine/storage_integrated_dml_executor.go` |
| SHOW SQL 未接真实元数据 | 已关闭 executor 入口核心路径 | `server/innodb/engine/executor.go`、`server/innodb/engine/show_executor.go` |
| checkpoint 写阻塞语义 | 已关闭核心路径 | `server/innodb/engine/storage_integrated_checkpoint.go` 及 P0 验证脚本 |
| legacy BufferPool pinned-page 驱逐 | 已关闭核心路径 | `server/innodb/buffer_pool/buffer_page.go`、`server/innodb/buffer_pool/buffer_lru.go`、`server/innodb/buffer_pool/buffer_lru_optimized.go` |
| EnhancedBTree rebuild/drop 最小可用路径 | 已关闭核心路径 | `server/innodb/manager/enhanced_btree_manager.go` |
| engine 包测试长时间无输出 | 已建立可重复 P0 core 命令 | `scripts/verify_p0_core.sh`、`scripts/verify_p0_engine_suites.sh`、`docs/superpowers/plans/P0_ENGINE_TEST_SPLIT_MANIFEST.md` |

## 仍未关闭的广义生产 P0

这些不是本轮 P0 core 的剩余项，而是生产上线维度仍需要继续验收的 P0：

1. P0-03 残余：`ExecutionError` 全链路统一、`index_transaction_adapter.go` 锁释放边界、`storage_integrated_index_helper.go` 复合索引/一致性检查仍需继续收敛。
2. P0-06：崩溃恢复演练闭环，包含 redo / undo / half-commit 固定场景、多轮复放和报告归档。
3. P0-07：灰度写入与回滚链路，特别是 storage mapping、read -> write -> restore 脚本和失败日志字段。
4. P0-08：慢查询日志配置、执行打点、样例复放与字段验收。
5. P0-09：基础指标、延迟分位、redo/undo/锁等待指标、告警规则与演练证据。

## 2026-07-13 后续完善记录

- P0-03：已补 `TransactionAdapter` 在 `lockManager == nil` 时返回结构化错误，不再静默成功；已补复合索引键构建，单列保持原语义，多列按索引列顺序返回完整键。
- P0-06：`crash_recovery_process_drill.sh` 已生成每轮每场景 `before.json / after.json / diff.json` 证据；`p0_b_recovery_audit.sh` 已强制检查 redo / undo / half_commit 每轮证据文件和 marker；新增 `scripts/p0_b_recovery_audit_selftest.sh`。
- P0-07：`p0_e_backup_snapshot.sh` 的 canary 阶段日志补齐表头和固定字段；新增 `selftest` 模式与 `scripts/p0_e_canary_selftest.sh`，不依赖真实服务即可验证 read / write / restore 编排与日志契约。
- P0-08：慢查询日志 JSON 字段补齐 `schema / table / error_msg`，保留 `sql / duration_ms / rows_affected / conn_id / txn_id / error_code / status / stage`。
- P0-09：新增 `manager.OperationalMetrics`，输出 QPS、错误率、连接数、活跃事务、P50/P95/P99、redo/undo、锁等待，并支持错误率、P99、活跃事务阈值告警与恢复验证。

当前实现验证：

```bash
scripts/verify_p0_core.sh
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test(TransactionAdapterWithoutLockManager|TransactionAdapterLocking|StorageIntegratedDMLExecutorBuildIndexKey|StorageIntegratedDMLExecutorHandleIndexError|SlowQueryLogger)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'TestOperationalMetrics' -count=1
bash scripts/p0_b_recovery_audit_selftest.sh
bash scripts/p0_e_canary_selftest.sh
```

仍需真实环境执行的验收：

```bash
CR_PROC_ROUNDS=3 scripts/crash_recovery_process_drill.sh
CR_PROC_REPORT_DIR=./reports B_AUDIT_MAX_DIRS=1 scripts/p0_b_recovery_audit.sh
P0E_MODE=canary P0E_DSN=<dsn> scripts/p0_e_backup_snapshot.sh
```

## 说明

- EnhancedBTree 的 rebuild/drop 当前是“基于已加载索引记录的最小可用实现”，不是从表全量扫描重建；后续若补齐表扫描接口，应升级为真实全量重建。
- SHOW 当前优先走 `InfoSchemaManager` 真实元数据路径，旧 data-dir fallback 保留为兼容路径。
- 历史文档中出现的 `findDuplicateRecord not fully implemented`、`SHOW 返回暂未接入真实元数据`、`清空整页模拟删除` 等描述已不代表当前 P0 core 状态。
