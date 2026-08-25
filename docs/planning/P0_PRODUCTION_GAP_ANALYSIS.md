# XMySQL Server P0 未实现项总表（Gap Analysis）

## 2026-07-15 当前结论

新的 P0 缺口总表见：

- `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`

本文保留历史 P0 上线证据工作流 A/B/C/D/E 的拆解。当前生产阻塞项已经升级为能力闭环：完整页/记录格式、B+Tree 持久化扫描、二级索引、事务/MVCC/恢复、SQL/JDBC 兼容性和真实运行态证据。历史 evidence suite 或脚本通过不等同于这些能力完成。

## 1. 目的

本文档用于把以下 3 份 P0 文档中的“目标态”映射为“当前仓库已实现状态”：

- `docs/planning/P0_PRODUCTION_TASKS.md`
- `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- `docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md`

适用场景：

- P0 排期拆解
- Sprint 规划
- 上线阻塞项识别
- 验收前证据归档

## 2. 状态定义

- `未实现`：仓库中未发现对应代码、脚本、文档、报告或验收证据
- `部分实现`：有部分代码或文档基础，但距离 P0 验收标准仍有明显缺口
- `已有基础但未达标`：核心能力存在，但缺少自动化、报告、稳定性验证或上线交付物
- `阻塞`：当前存在明确阻塞因素，未解除前无法完成验收

## 3. 总览

| 工作流 | 状态 | 结论 |
|---|---|---|
| A：核心高风险文件整改 | 部分实现 | Top 12 未清零，关键路径仍有 TODO、错误文本判定、sleep 驱动测试 |
| B：崩溃恢复验证 | 已有基础但未达标 | 恢复代码存在，但缺少自动化脚本、演练报告、结果快照 |
| C：并发正确性验证 | 已有基础但未达标 | 已形成并发压测脚本与 3 轮一致性验证报告，待持续化运营巡检 |
| D：基础可观测性上线 | 部分实现 | 有少量内部 stats/alert 结构，但无生产指标导出、慢查询日志、告警配置 |
| E：回滚预案与灰度演练 | 部分实现 | 已有发布/回退执行手册与备份恢复脚本，缺全链路演练与计时证据 |

## 4. 全局阻塞

### G-01 Go 版本与测试基线阻塞

- 状态：`部分关闭`
- 现状：
  - `go.mod` 已提升为 `go 1.24`
  - 当前环境 `go version` 为 `go1.24.3`
  - `scripts/p0_stage1_baseline.sh` 已添加，并执行过 DRY-RUN，产出基线摘要
  - `scripts/crash_recovery_process_drill.sh` 已形成
  - `go test ./server/innodb/engine` 已通过，真实基线报告：`reports/STAGE1_AUDIT_DEMO/stage1_baseline_20260516_220508/summary.log`
- 影响：
  - 仍无法完成 A 段和统一发布前检查中的关键测试验收
  - `go test ./...` 仍不可作为 P0 已通过证据
- 建议动作：
  - 补齐 `go test ./...` 与多轮稳定性测试
  - 补齐 A 段 Top 12 与 B/C/D/E 核心证据闭环
- 优先级：`P0`
- 预计工时：`0.5 人天`

## 5. 详细缺口清单

### 5.1 工作流 A：核心高风险文件整改

| 任务ID | 当前状态 | 当前现状 | 主要缺口 | 交付物 | 依赖 | 优先级 | 预计工时 |
|---|---|---|---|---|---|---|---|
| T-A1-01 去除错误掩盖型 fallback | 部分实现 | `executor.go` 仍有 `recover` 包装与错误文本判断；`unified_executor.go` 关键路径仍有多个 TODO；`storage_adapter.go` 仍有 schema 参数 TODO；`storage_integrated_dml_helper.go` 已移除 `rollbackStorageTransaction` 中基于 `strings.Contains` 的文本分支 | 统一错误类型、关键参数日志、失败路径单测均未闭环 | 代码修复、错误分类约定、失败路径测试 | G-01 | P0 | 2 人天 |
| T-A1-02 收敛重复实现为单入口 | 进行中 | `store/pages/page.go` 与 `wrapper/types/base_page.go` / `wrapper/page/page_wrapper_base.go` 并存，但 `page_factory.go` 已将 `FIL_PAGE_INODE` 与 `default` 分支切到 `types.NewUnifiedPage` 统一入口；新增调用点仍需收敛 | 明确主入口、迁移调用方、冻结旧入口、补迁移说明 | `docs/planning/P0_A1_02_PAGE_SINGLE_ENTRYPOINT_MIGRATION.md`、`server/innodb/storage/wrapper/page/page_factory.go` | T-A1-01 | P0 | 2 人天 |
| T-A1-03 DML 唯一键冲突判定可靠化 | 部分实现 | 已支持 `errors.Is/errors.As`，并已移除索引错误处理里的文本匹配兜底；`executor.go` 与 `unified_executor.go` 仍有 TODO 待闭环 | 去除字符串/正则 fallback，完全改为结构化错误 | 代码修复、冲突/非冲突单测 | T-A1-01 | P0 | 1 人天 |
| T-A2-01 消除 sleep 驱动的 flaky 测试 | 已实现 | 已改造相关测试并完成 10 轮连续稳定性复验，记录见 `reports/p0_a2_01_stability_20260517_012720/summary.log`；相关测试文件不再依赖固定 `time.Sleep` 等待 | 改为条件等待或事件驱动断言；持续演练纳入每周回归 | 测试改造、稳定性执行记录 | G-01 | P0 | 2 人天 |
| T-A2-02 页面/回滚包装器关键路径补齐 | 部分实现 | 已补齐 `rollback_page_wrapper.go` 与 `page_wrapper_base.go` 的关键 TODO；对应页面包装器最小路径测试已通过 | 补解析、异常校验、边界测试 | 代码修复、单测、边界测试 | T-A1-02 | P0 | 2 人天 |

### 5.2 工作流 B：崩溃恢复验证

| 任务ID | 当前状态 | 当前现状 | 主要缺口 | 交付物 | 依赖 | 优先级 | 预计工时 |
|---|---|---|---|---|---|---|---|
| T-B-01 构建崩溃恢复自动化脚本 | 部分实现 | 已有 `scripts/crash_recovery_process_drill.sh`；新增了 `scripts/p0_b_recovery_audit.sh` 作为复盘入口；现有报告见 `reports/crash_recovery_process_drill_20260427_160436/summary.log` | 演练流程仍需标准化输入与固定化结果模板 | 恢复脚本、输入数据、审计脚本 | A 工作流核心修复 | P0 | 2 人天 |
| T-B-02 输出恢复演练报告 | 部分实现 | 有一次完整演练记录（redo/undo/半提交 PASS）；`recovery_drill_client` 已补齐 redo 校验 marker 输出（`REDO_SETUP_OK`、`REDO_VERIFY_OK`）；`docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md` 已提供审计模板 | 缺多轮复现稳定性与差异对账脚本化证据 | 演练报告、快照、校验输出 | T-B-01 | P0 | 1.5 人天 |

### 5.3 工作流 C：并发正确性验证

| 任务ID | 当前状态 | 当前现状 | 主要缺口 | 交付物 | 依赖 | 优先级 | 预计工时 |
|---|---|---|---|---|---|---|---|
| T-C-01 构建并发压测与一致性校验 | 已有基础但未达标 | 已形成闭环：脚本 + 3 轮场景结果一致性报告 | 持续性压测与性能回归基线仍需例行化执行 | `scripts/p0_c_concurrency_validation.sh`，`reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md` | A 工作流核心修复，建议与 D 并行 | P0 | 3 人天 |

### 5.4 工作流 D：基础可观测性上线

| 任务ID | 当前状态 | 当前现状 | 主要缺口 | 交付物 | 依赖 | 优先级 | 预计工时 |
|---|---|---|---|---|---|---|---|
| T-D-01 统一错误日志与慢查询日志 | 部分实现 | 有普通执行日志；系统变量中存在 `slow_query_log` 定义；未发现慢查询日志真正落地路径、字段规范文档、trace 关联样例 | 缺错误字段规范、模块/SQL/trace/耗时/行数统一输出 | `docs/planning/P0_D_OBSERVABILITY_READINESS_TEMPLATE.md`（字段规范与慢查询模板） | A 工作流错误分类 | P0 | 1.5 人天 |
| T-D-02 接入核心指标与告警 | 部分实现 | 有内部 `GetStats` 和 `CheckpointMonitor`/长事务告警通道等结构；未发现 Prometheus/exporter、面板配置、告警规则文件 | 缺 QPS、P50/P95/P99、错误率、连接/事务数指标导出和告警演练证据 | 指标导出、告警配置、接入说明、演练截图 | T-D-01 | P0 | 2 人天 |

### 5.5 工作流 E：回滚预案与灰度演练

| 任务ID | 当前状态 | 当前现状 | 主要缺口 | 交付物 | 依赖 | 优先级 | 预计工时 |
|---|---|---|---|---|---|---|---|
| T-E-01 制定灰度发布与回退手册 | 部分实现 | 已有 `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`，含灰度分阶段、回退顺序和归档要求，但演练计时未闭环 | 缺首次全流程演练数据 | `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`（发布/回退手册） | B/C/D 达到最小可验证状态 | P0 | 1 人天 |
| T-E-02 制定数据回滚与恢复点策略 | 部分实现 | 已新增 `scripts/p0_e_backup_snapshot.sh`，支持 backup/list/restore 与 manifest 记录；已完成一次 `backup/list/restore` 冒烟演练，证据见 `reports/p0_e_backups/p0_e_backup_restore_dryrun_20260517_063321.md`；回放边界与核验流程仍待演练复核 | 缺回放边界与一致性核验的全量演练证据 | 数据回滚手册、备份恢复脚本 | B 工作流 | P0 | 1 人天 |
| T-E-03 完成一次全链路演练 | 进行中 | 已在修复后的启动链路完成阶段 0 只读演练；阶段 1 写入验证尝试因存储映射阻断而失败 | 阶段 1/2、告警触发与回退与时延复核仍待执行；失败点见 `reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065633_stage1_insert_fail.md` | 演练记录与问题清单（阶段 1/2 待补） | T-E-01、T-E-02、B/C/D 达标 | P0 | 1.5 人天 |

## 6. 建议执行顺序

### 第 1 阶段：解除基线阻塞

- G-01 Go 版本统一
- T-A1-01 去除错误掩盖型 fallback
- T-A1-03 DML 唯一键冲突结构化

### 第 2 阶段：修复高风险实现与测试基线

- T-A1-02 单入口收敛
- T-A2-02 页面/回滚包装器补齐
- T-A2-01 flaky 测试去 sleep

### 第 3 阶段：建立生产验证闭环

- T-B-01 / T-B-02 崩溃恢复脚本与报告
- T-C-01 并发压测与一致性校验
- T-D-01 / T-D-02 日志、指标、告警

### 第 4 阶段：上线前演练与预案

- T-E-01 灰度与快速回退手册
- T-E-02 数据回滚策略
- T-E-03 全链路演练

## 7. 建议 Sprint 拆分

### Sprint 1

- G-01
- T-A1-01
- T-A1-03
- T-A2-01 第一批

### Sprint 2

- T-A1-02
- T-A2-02
- T-B-01
- T-D-01

### Sprint 3

- T-B-02
- T-C-01
- T-D-02

### Sprint 4

- T-E-01
- T-E-02
- T-E-03
- 发布前统一验收

## 8. 当前可勾选项判断

基于本次仓库核查，当前可明确判断：

- 可勾选：
  - `go test ./server/dispatcher` 通过
  - `go test ./server/innodb/engine` 通过（真实执行）
- 不可勾选：
  - Top 12 文件 P0 问题清零
  - fallback 掩错逻辑已移除
  - 重复实现已收敛为单入口
  - DML 唯一键冲突已完全结构化
  - 高风险测试去 flaky（已通过；见 `reports/p0_a2_01_stability_20260517_012720/summary.log`）
  - `go test ./server/innodb/engine`
  - B/C/D/E 全部项
  - `go test ./...`
  - 最终允许进入生产灰度

## 9. 下一步建议

- 先将本文件作为 P0 排期总表
- 再以 A 为前置拆 Sprint
- 每完成一个任务，在 `P0_PRODUCTION_CHECKLIST.md` 中补证据路径
- B/C/E 类任务禁止仅以“代码已存在”作为完成标准，必须附脚本、报告或演练记录

---

## 10. 2026-06-13 P0 baseline update

### Summary

This repository has recovered the default Go test baseline.

Evidence:

- `go version`: `go1.25.1 windows/amd64`
- `go test ./server/innodb/engine`: pass
- `go test ./server/dispatcher`: pass
- `go test ./...`: pass

### Completed since previous gap analysis

- G-01 baseline blocker is resolved for the current development environment.
- `server/innodb/engine` and `server/dispatcher` are no longer blocked by compile/test failures.
- `server/innodb/sqlparser` now passes in the default test profile.
- `server/innodb/sqlparser/dependency/sqltypes` now passes after restoring missing test helpers.
- `server/innodb/storage/wrapper/mvcc` now passes after adding a wrapper compatibility layer over `storage/format/mvcc`.
- `server/innodb/storage/wrapper/system` checksum validation now passes after normalizing system page file-header fields before checksum calculation.
- `util` tests no longer depend on the original author's absolute Linux paths.
- Root package vet/build issue around `logger.Info` formatting is fixed.

### Baseline policy changes

- Historical demo/test command packages under selected `cmd/*` directories are excluded from the default baseline with the `demo` build tag.
- Default `go test ./...` now validates current product code and active tests.
- Legacy SQL parser conformance tests that assert old Vitess-style behavior are excluded from the default baseline with the `legacy_sqlparser_conformance` build tag.

Manual validation commands:

```bash
go test ./...
go test -tags demo ./cmd/...
go test -tags legacy_sqlparser_conformance ./server/innodb/sqlparser
```

### Remaining P0 gaps

Passing `go test ./...` does not mean P0 production readiness is complete.

Still open:

- B: crash recovery automation, replay snapshots, and audit reports.
- C: formal concurrency stress testing and consistency verification.
- D: production observability, slow query logging, metrics export, and alert rules.
- E: gray release, rollback, data recovery runbooks, and full drill evidence.
- Legacy `demo` and SQL parser conformance profiles still need separate cleanup if they must become release-blocking targets again.

---

## 11. 2026-06-14 project stage update

### Current project stage

The project is now in the **P0 production-readiness evidence stage**.

It is no longer blocked at the default Go test baseline level, but it is also not ready for production gray release yet.

Current judgment:

- Engineering baseline: restored for the default test profile.
- Production readiness: not complete.
- Recommended next workstream: P0-B crash recovery drill automation and auditable reports.

### Latest repository status

Latest-code sync status recorded for this workspace:

```bash
git pull --ff-only
```

Result: already up to date.

### Baseline evidence currently available

Default and focused evidence already recorded in this workspace:

```bash
go test ./...
go test ./server/innodb/engine
go test ./server/dispatcher
```

Result: pass.

Focused crash-recovery evidence:

```bash
go test ./server/innodb/manager -run 'TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint' -count=1 -timeout=180s
```

Result: pass.

### P0-B status adjustment

Previous status for crash recovery was effectively "automation and evidence missing".

Updated status:

- Recovery implementation and tests exist.
- Focused manager recovery suite passes.
- `scripts/crash_recovery_drill.sh` exists and can run crash-recovery focused tests.
- Formal P0-B acceptance is still incomplete because there is not yet a committed, repeatable drill report package containing raw logs, markdown summary, scenario matrix, replay snapshots, and multi-round consistency evidence.

Recommended P0-B acceptance boundary:

- Add a Windows-friendly drill entrypoint for local development.
- Generate one auditable report under `reports/`.
- Include redo, undo, half-commit, consistency verification, and repeated replay evidence.
- Keep the default `go test ./...` baseline green while adding the drill evidence.

### Remaining production gaps after this update

Still open:

- A: Top 12 high-risk cleanup is not fully proven complete.
- B: crash-recovery drill report package is partial and not yet P0-accepted.
- C: formal concurrency stress and consistency verification are still missing.
- D: production observability evidence is still missing.
- E: gray release, rollback, and data recovery runbooks are still missing.

### Practical next step

Continue with P0-B before expanding into C/D/E:

1. Create or update crash-recovery drill automation.
2. Run the drill and archive output under `reports/`.
3. Update this gap analysis and the checklist with report paths.
4. Only then mark individual P0-B checklist items as complete.

---

## 12. 2026-06-14 P0-B drill automation artifact update

Crash-recovery drill automation has been expanded for cross-platform local execution.

New or updated artifacts:

- `scripts/crash_recovery_drill.ps1`: Windows PowerShell entrypoint.
- `scripts/crash_recovery_drill.sh`: Bash entrypoint aligned to the same report format.
- `reports/README.md`: documents expected evidence artifacts.

Both drill entrypoints are designed to emit:

- raw execution log: `reports/crash_recovery_drill_<timestamp>.log`,
- markdown summary report: `reports/crash_recovery_drill_<timestamp>.md`.

Default drill command represented by the scripts:

```bash
go test ./server/innodb/manager -run 'TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint' -count=1 -timeout=180s
```

Status after this artifact update:

- P0-B automation entrypoints: present.
- P0-B generated report evidence: still pending until the drill is executed.
- P0-B full acceptance: still pending repeated replay, snapshot/state-diff evidence, and final regression validation.

---

## 13. 2026-06-14 repeated replay automation update

P0-B crash-recovery drill automation now supports repeated replay runs.

Updated execution examples:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3
```

```bash
CR_RUNS=3 ./scripts/crash_recovery_drill.sh
```

Behavior:

- Default run count remains 1 for fast local checks.
- Acceptance-style runs can set 3 or more replays.
- The generated Markdown report records every replay run and marks the overall result as failed if any run fails.
- The generated raw log includes per-run start and finish markers.

Status after this update:

- Repeated replay automation: implemented.
- Repeated replay report artifact: pending until the drill is executed.
- Snapshot/state-diff evidence: still pending.
- Full P0-B acceptance: still pending.

---

## 14. 2026-06-14 P0-B replay drill evidence

A 3-run crash-recovery replay drill has been executed successfully.

Command:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3
```

Result: PASS.

Generated evidence artifacts:

- Raw log: `reports/crash_recovery_drill_20260614_070446.log`
- Markdown report: `reports/crash_recovery_drill_20260614_070446.md`

Run summary:

- Replay run 1: PASS
- Replay run 2: PASS
- Replay run 3: PASS

P0-B status after this evidence:

- Crash-recovery drill automation: complete for local PowerShell/Bash entrypoints.
- Repeated replay evidence: available for the focused recovery suite.
- Remaining before full P0-B acceptance: snapshot/state-diff evidence and broader scenario matrix hardening.

---

## 15. 2026-06-14 P0-B runbook update

A dedicated P0-B crash recovery drill runbook has been added:

- `docs/planning/P0_B_CRASH_RECOVERY_DRILL_RUNBOOK.md`

The runbook records:

- current 3-run replay evidence,
- current scenario matrix,
- P0-B acceptance rules,
- required snapshot/state-diff JSON artifact shape,
- recommended next implementation step for state evidence automation.

Current P0-B status remains partial until snapshot/state-diff evidence is generated and archived.

---

## 16. 2026-06-14 command-level state evidence automation update

Crash-recovery drill automation now supports command-level state evidence JSON output.

PowerShell:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence
```

Bash:

```bash
CR_RUNS=3 CR_STATE_EVIDENCE=1 ./scripts/crash_recovery_drill.sh
```

Expected generated artifact:

```text
reports/crash_recovery_drill_<timestamp>.state.json
```

Important acceptance boundary:

- The `.state.json` artifact records replay status and command-level scenario checks.
- It does not yet prove page-level, row-level, or WAL-level recovered-state diff consistency.
- Full P0-B acceptance still requires storage-state snapshot/state-diff evidence.

Status after this update:

- Command-level state evidence automation: implemented.
- Generated command-level `.state.json` artifact: pending until the drill is executed with state evidence enabled.
- Storage-state snapshot/diff evidence: still pending.

---

## 17. 2026-06-14 state evidence verifier update

A schema verifier has been added for command-level crash-recovery state evidence:

```powershell
./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json
```

Verifier path:

- `scripts/verify_crash_recovery_state_evidence.ps1`

The verifier checks required JSON fields, replay run records, per-run checks, status consistency, and the explicit `evidence_type: command_replay` boundary.

Status after this update:

- Command-level state evidence generation: implemented.
- Command-level state evidence schema verification: implemented.
- Generated and verified `.state.json` artifact: pending until the drill is executed with state evidence enabled.
- Storage-state snapshot/diff evidence: still pending.

---

## 18. 2026-06-14 P0-C concurrency validation planning update

A dedicated P0-C concurrency correctness validation plan has been added:

- `docs/planning/P0_C_CONCURRENCY_VALIDATION_PLAN.md`

The plan defines:

- acceptance target for concurrency readiness,
- conflict write, range read, long transaction, lock wait, deadlock, and repeated replay scenarios,
- required `.log`, `.md`, and `.json` evidence artifacts,
- first implementation strategy for validation scripts and JSON verifier.

Current P0-C status remains open. Planning is complete enough to start implementation, but no P0-C validation script or report artifact exists yet.

---

## 19. 2026-06-14 P0-C validation automation update

P0-C command-level concurrency validation automation has been added.

New artifacts:

- `scripts/concurrency_validation.ps1`
- `scripts/concurrency_validation.sh`
- `scripts/verify_concurrency_validation_report.ps1`

PowerShell execution:

```powershell
./scripts/concurrency_validation.ps1 -Runs 3
```

Bash execution:

```bash
CONCURRENCY_RUNS=3 ./scripts/concurrency_validation.sh
```

JSON verification:

```powershell
./scripts/verify_concurrency_validation_report.ps1 -Path reports/concurrency_validation_<timestamp>.json
```

Status after this update:

- P0-C command-level validation scripts: implemented.
- P0-C JSON report verifier: implemented.
- P0-C generated report evidence: pending until the validation script is executed.
- P0-C full acceptance: still pending explicit consistency checks and final regression validation.

---

## 20. 2026-06-14 P0-D observability planning update

A dedicated P0-D observability production-readiness plan has been added:

- `docs/planning/P0_D_OBSERVABILITY_PLAN.md`

The plan defines:

- current observability foundations found in the repository,
- minimum metric catalog,
- slow query log field contract,
- error log field contract,
- minimum alert rules,
- required `.log`, `.md`, and `.json` smoke evidence artifacts,
- first implementation strategy for smoke scripts, sample logs, alert rules, and future live metric export.

Current P0-D status remains open. Planning is complete enough to start implementation, but no P0-D smoke script, sample log evidence, alert rules, or report artifact exists yet.

---

## 21. 2026-06-14 P0-D observability artifact update

P0-D documentation/configuration smoke artifacts have been added.

New artifacts:

- `docs/observability/metrics_catalog.md`
- `docs/observability/sample_slow_query.log`
- `docs/observability/sample_error.log`
- `deploy/alerts/xmysql-p0-alerts.yml`
- `scripts/observability_smoke.ps1`
- `scripts/verify_observability_report.ps1`

Smoke execution:

```powershell
./scripts/observability_smoke.ps1
```

Report verification:

```powershell
./scripts/verify_observability_report.ps1 -Path reports/observability_smoke_<timestamp>.json
```

Status after this update:

- P0-D metric catalog: implemented.
- P0-D sample slow query log: implemented.
- P0-D sample error log: implemented.
- P0-D alert rule template: implemented.
- P0-D smoke script and JSON verifier: implemented.
- P0-D generated smoke report: pending until smoke script execution.
- P0-D full acceptance: still pending live metric export and alert delivery evidence.

---

## 22. 2026-06-14 P0-E release and rollback runbook update

P0-E operational runbooks have been added.

New artifacts:

- `docs/planning/P0_E_RELEASE_ROLLBACK_PLAN.md`
- `docs/operations/gray_release_runbook.md`
- `docs/operations/rollback_runbook.md`
- `docs/operations/data_recovery_runbook.md`
- `docs/operations/full_chain_drill_record_template.md`

These documents define:

- gray-release gates,
- rollback triggers,
- rollback timing evidence,
- data recovery point and replay boundary requirements,
- full-chain release/rollback drill record format,
- P0-E acceptance rules.

Status after this update:

- P0-E runbooks: implemented.
- Full-chain drill record template: implemented.
- Executed full-chain drill report: pending.
- Rollback timing evidence: pending.
- Data recovery drill evidence: pending.
- P0-E full acceptance: still pending executed drill evidence and linked P0-B/P0-C/P0-D reports.

---

## 23. 2026-06-14 P0-E full-chain drill smoke automation update

P0-E full-chain drill smoke automation has been added.

New artifacts:

- `scripts/full_chain_drill_smoke.ps1`
- `scripts/verify_full_chain_drill_report.ps1`

Smoke execution example:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md
```

Report verification:

```powershell
./scripts/verify_full_chain_drill_report.ps1 -Path reports/full_chain_drill_<timestamp>.json
```

Status after this update:

- P0-E drill-readiness smoke script: implemented.
- P0-E drill report verifier: implemented.
- P0-E generated smoke report: pending until execution.
- P0-E full acceptance: still pending timed full-chain release, rollback, and data recovery drill evidence.

---

## 24. 2026-06-14 P0 evidence bundle update

A top-level P0 production evidence bundle definition and verifier have been added.

New artifacts:

- `docs/planning/P0_EVIDENCE_BUNDLE.md`
- `scripts/verify_p0_evidence_bundle.ps1`

The bundle verifier checks whether the required P0-B, P0-C, P0-D, and P0-E evidence files are present and writes:

- `reports/p0_evidence_bundle_<timestamp>.md`
- `reports/p0_evidence_bundle_<timestamp>.json`

Status after this update:

- Evidence bundle definition: implemented.
- Evidence bundle verifier: implemented.
- Complete bundle report: pending until P0-B state evidence, P0-C report, P0-D report, and P0-E report are generated.
- Production approval: still pending dedicated report verification and final regression validation.

---

## 25. 2026-06-14 P0 evidence suite automation update

A top-level P0 evidence suite runner has been added:

- `scripts/run_p0_evidence_suite.ps1`

The suite orchestrates:

1. P0-B crash recovery drill with command-level state evidence.
2. P0-B state evidence JSON verification.
3. P0-D observability smoke and JSON verification.
4. P0-C concurrency validation and JSON verification.
5. P0-E full-chain drill smoke and JSON verification.
6. P0 evidence bundle verification.

Example command:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

Important boundary:

- This suite executes tests and smoke scripts.
- It should be run as an explicit validation action.
- It still does not replace final full production drills or live metrics export evidence.

---

## 26. 2026-06-14 final regression gate automation update

A final default regression gate has been added and connected to the P0 evidence suite and bundle verifier.

New artifacts:

- `scripts/final_regression_gate.ps1`
- `scripts/verify_final_regression_gate_report.ps1`

The gate generates:

- `reports/final_regression_gate_<timestamp>.log`
- `reports/final_regression_gate_<timestamp>.md`
- `reports/final_regression_gate_<timestamp>.json`

The P0 evidence suite now runs the final regression gate before generating the bundle report.

Status after this update:

- Final regression gate automation: implemented.
- Final regression report verifier: implemented.
- Bundle verifier now requires final regression evidence.
- Final regression evidence: pending until the gate is executed.

---

## 27. 2026-06-14 P0 status summary and approval packet update

Final handoff and approval documents have been added.

New artifacts:

- `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`
- `docs/planning/P0_RELEASE_APPROVAL_PACKET_TEMPLATE.md`

These documents summarize the current production-readiness state, known generated evidence, missing reports, approval blockers, and owner sign-off requirements.

Status after this update:

- Current status summary: implemented.
- Release approval packet template: implemented.
- Approval packet with real generated report paths: pending until the evidence suite is executed.

---

## 28. 2026-06-14 approval packet generator update

A release approval packet generator has been added and connected to the P0 evidence suite.

New artifact:

- `scripts/generate_p0_release_approval_packet.ps1`

The generator writes:

- `reports/p0_release_approval_packet_<timestamp>.md`

The P0 evidence suite now generates the approval packet after bundle verification.

Status after this update:

- Approval packet template: implemented.
- Approval packet generator: implemented.
- Suite integration: implemented.
- Filled approval packet with real report paths: pending until suite execution.

---

## 29. 2026-06-14 approval packet verifier update

A release approval packet verifier has been added and connected to the P0 evidence suite.

New artifact:

- `scripts/verify_p0_release_approval_packet.ps1`

The verifier checks that the generated approval packet contains the required release candidate, evidence links, verifier commands, approval checklist, final decision, and default HOLD decision sections.

The P0 evidence suite now validates the approval packet after generating it.

Status after this update:

- Approval packet generator: implemented.
- Approval packet verifier: implemented.
- Suite integration: implemented.
- Generated and verified approval packet: pending until suite execution.

---

## 30. 2026-06-14 P0 tooling preflight update

A P0 tooling/documentation preflight verifier has been added and connected to the evidence suite.

New artifact:

- `scripts/verify_p0_tooling_preflight.ps1`

The preflight generates:

- `reports/p0_tooling_preflight_<timestamp>.md`
- `reports/p0_tooling_preflight_<timestamp>.json`

The P0 evidence suite now runs this preflight before executing recovery, observability, concurrency, release, and regression evidence steps.

Important boundary:

- The preflight checks tooling and documentation presence only.
- It does not execute tests or validation drills.
- It does not prove production readiness by itself.

---

## 31. 2026-06-14 GitHub Actions P0 evidence workflow update

A manually triggered GitHub Actions workflow has been added for P0 evidence generation.

New artifact:

- `.github/workflows/p0-evidence.yml`

Workflow modes:

- `preflight`: runs `./scripts/verify_p0_tooling_preflight.ps1` only.
- `full`: runs `./scripts/run_p0_evidence_suite.ps1` with configurable recovery and concurrency run counts.

The workflow uploads `reports/**` as a workflow artifact after execution.

Important boundary:

- The workflow is manual-only via `workflow_dispatch`.
- It does not run automatically on every push.
- Full mode executes tests and smoke scripts.

---

## 32. 2026-06-14 P0 evidence suite operator guide update

A P0 evidence suite operator guide has been added:

- `docs/operations/p0_evidence_suite_operator_guide.md`

The P0 tooling preflight now also checks the manual GitHub Actions workflow:

- `.github/workflows/p0-evidence.yml`

The guide documents local preflight, local full suite execution, CI workflow usage, review order, failure handling, and approval boundaries.

---

## 33. 2026-06-14 remaining engineering backlog and risk register update

A remaining engineering backlog and P0 risk register have been added.

New artifacts:

- `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- `docs/planning/P0_RISK_REGISTER.md`

These documents separate tooling readiness from actual production capability and list the remaining work required for recovery state-diff evidence, concurrency consistency checks, live observability, alert delivery, timed rollback drill, and final approval.

Current judgment remains: production gray-release approval is not complete until these backlog items are implemented, verified, archived, or explicitly deferred by owners.

---

## 34. 2026-06-14 P0 preflight coverage update

P0 tooling preflight coverage has been expanded.

The preflight now also checks:

- `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`
- `docs/planning/P0_RISK_REGISTER.md`
- `docs/operations/p0_evidence_suite_operator_guide.md`

This keeps the preflight aligned with the latest handoff, risk, and operator documentation added during P0 production-readiness work.

---

## 35. 2026-06-14 remaining engineering execution plan update

A remaining engineering execution plan and next-actions checklist have been added.

New artifacts:

- `docs/planning/P0_REMAINING_ENGINEERING_EXECUTION_PLAN.md`
- `docs/planning/P0_NEXT_ACTIONS.md`

The tooling preflight now checks these documents as part of the P0 handoff package.

The execution plan turns the remaining backlog into phases: first suite run, P0-B recovery proof, P0-C consistency proof, P0-D observability proof, P0-E release/rollback proof, and final approval package.

---

## 36. 2026-06-14 P0-D metrics registry foundation update

A lightweight in-process metrics registry has been added as a foundation for P0-D live metrics export.

New artifacts:

- `server/observability/metrics/registry.go`
- `server/observability/metrics/defaults.go`
- `docs/observability/metrics_registry_usage.md`

Capabilities:

- counters,
- gauges,
- histograms,
- Prometheus text exposition output,
- registration of the minimum P0-D metric catalog.

Current boundary:

- This is real code foundation for metrics export.
- It is not yet wired into runtime query, transaction, lock, recovery, or checkpoint paths.
- P0-D live metrics export evidence is still pending until runtime paths update the registry and an export artifact or endpoint is produced.

---

## 37. 2026-06-14 P0-D metrics export smoke update

A metrics export smoke command and verifier have been added.

New artifacts:

- `cmd/p0_metrics_export/main.go`
- `scripts/metrics_export_smoke.ps1`
- `scripts/verify_metrics_export_report.ps1`

The smoke script generates:

- `reports/metrics_export_<timestamp>.prom`
- `reports/metrics_export_<timestamp>.log`
- `reports/metrics_export_<timestamp>.md`
- `reports/metrics_export_<timestamp>.json`

The P0 evidence suite now runs this smoke after P0-D observability smoke and before P0-C concurrency validation.

Current boundary:

- This proves Prometheus textfile generation from the metrics registry.
- It does not prove runtime server paths are wired to live metrics yet.

---

## 38. 2026-06-14 metrics export evidence bundle integration update

P0-D metrics export smoke evidence has been added to the final evidence bundle and approval packet requirements.

Updated artifacts:

- `scripts/verify_p0_evidence_bundle.ps1`
- `scripts/generate_p0_release_approval_packet.ps1`
- `scripts/verify_p0_release_approval_packet.ps1`
- `scripts/run_p0_evidence_suite.ps1`
- `docs/planning/P0_EVIDENCE_BUNDLE.md`

The final bundle now requires:

- `reports/metrics_export_<timestamp>.md`
- `reports/metrics_export_<timestamp>.json`

Current boundary:

- Metrics export textfile evidence is now part of final packaging.
- Runtime live metrics integration remains a separate open P0-D backlog item.

---

## 39. 2026-06-14 P0-D metrics HTTP handler foundation update

A reusable HTTP handler for Prometheus metrics export has been added.

New artifacts:

- `server/observability/metrics/http.go`
- `docs/observability/metrics_http_handler_usage.md`

The handler exposes a `Registry` as Prometheus text exposition through `net/http`.

Current boundary:

- HTTP export foundation exists.
- The handler is not yet mounted into the live server runtime.
- Runtime query/transaction/lock/recovery/checkpoint paths are not yet wired to update the registry.
- P0-D live endpoint evidence remains pending.

---

## 40. 2026-06-15 P0-D metrics runtime recorder foundation update

A runtime metrics recorder adapter has been added.

New artifacts:

- `server/observability/metrics/runtime.go`
- `docs/observability/metrics_runtime_recorder_usage.md`

The recorder maps runtime events to the P0-D metric catalog:

- query completion,
- query errors,
- active connections,
- active transactions,
- transaction commits and rollbacks,
- lock waits,
- long transactions,
- recovery runs and failures,
- checkpoint dirty pages and runs.

Current boundary:

- Runtime recorder foundation exists.
- Live server paths do not call it yet.
- P0-D runtime integration and live endpoint evidence remain pending.

---

## 41. 2026-06-15 P0-D structured logging foundation update

Structured slow-query and error-log foundations have been added.

New artifacts:

- `server/observability/logging/structured.go`
- `cmd/p0_structured_log_export/main.go`
- `docs/observability/structured_logging_usage.md`
- `scripts/structured_logging_smoke.ps1`
- `scripts/verify_structured_logging_report.ps1`

The P0 evidence suite, evidence bundle, and approval packet now include structured logging evidence.

Current boundary:

- Structured JSON-line log generation is available.
- Smoke evidence can be generated under `reports/`.
- Live query and engine error paths do not call this logging package yet.
- Runtime slow-query and error-log integration remains pending.

---

## 42. 2026-06-15 P0-D alert drill smoke update

Alert drill smoke tooling has been added.

New artifacts:

- `scripts/alert_drill_smoke.ps1`
- `scripts/verify_alert_drill_report.ps1`

The P0 evidence suite, evidence bundle, and approval packet now include alert drill smoke evidence.

Current boundary:

- Local alert rule presence and sample trigger logic can be validated.
- This does not prove Prometheus rule evaluation or Alertmanager delivery.
- Live alert delivery evidence remains pending unless explicitly owner-deferred.

---

## 43. 2026-06-15 P0-D live metrics endpoint integration update

The metrics registry is now wired into the existing profiling HTTP listener.

Updated runtime integration:

- `server/net/mysql_server.go` registers `/metrics` on the existing profiling HTTP mux.
- `server/observability/metrics/global.go` provides a process-wide default registry and runtime recorder.

Endpoint behavior:

- The endpoint is available on the configured profiling address and profile port.
- Path: `/metrics`
- Format: Prometheus text exposition.

Current boundary:

- Live endpoint wiring exists.
- Runtime query, transaction, lock, recovery, and checkpoint paths still need to update the default runtime recorder.
- Endpoint evidence must still be generated by running the server and scraping `/metrics`.

---

## 44. 2026-06-15 P0-D live metrics endpoint probe update

A live metrics endpoint probe has been added.

New artifacts:

- `scripts/metrics_endpoint_probe.ps1`
- `scripts/verify_metrics_endpoint_probe_report.ps1`

Usage:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
```

The probe generates:

- `reports/metrics_endpoint_probe_<timestamp>.prom`
- `reports/metrics_endpoint_probe_<timestamp>.log`
- `reports/metrics_endpoint_probe_<timestamp>.md`
- `reports/metrics_endpoint_probe_<timestamp>.json`

Current boundary:

- Probe tooling exists.
- It requires a running server and a provided metrics URL.
- It does not start the server or generate traffic.
- Live endpoint evidence remains pending until the probe is executed successfully.

## 2026-06-15 Update - Runtime active connection metric wired

- xmysql_connections_active{listener="mysql"} is now wired to the TCP/MySQL session lifecycle in server/net/decoupled_handler.go.
- The gauge is set from the authoritative sessionMap size on open, close, error cleanup, and COM_QUIT cleanup paths to reduce drift from duplicate close/error events.
- The /metrics endpoint is registered on the existing profiling HTTP listener in server/net/mysql_server.go.
- Evidence status: code path is present, but endpoint probe / full P0 evidence suite has not been rerun after this change.
- Remaining P0-D gap: query, transaction, lock, recovery, checkpoint, and storage runtime metrics still need direct engine-level instrumentation.
