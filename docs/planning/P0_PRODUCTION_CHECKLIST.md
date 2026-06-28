# XMySQL Server P0 上线验收清单（Checklist）

## 0. 使用说明

- 勾选原则：必须有“证据链接或报告路径”
- 通过标准：P0 全部项必须为 `[x]`
- 对应计划：
  - [P0_PRODUCTION_DEPLOYMENT_PLAN.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md)
  - [P0_PRODUCTION_TASKS.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_TASKS.md)

### 当前状态（2026-03-21 仓库核查）

- 本清单当前不应勾选为通过态
- 当前可明确判断：
  - `go test ./server/dispatcher` 可通过
  - `go test ./server/innodb/engine` 当前不可通过
  - B/C/D/E 工作流未形成验收证据闭环
- 详细缺口总表见：
  - `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`

## 1. 核心高风险文件整改（A）

当前判断：`整体未通过`

- [ ] Top 12 文件 P0 问题清零（附问题关闭清单）
- [ ] 关键路径 fallback 掩错逻辑已移除（附代码变更链接）
- [ ] 重复实现已收敛为单入口（附迁移说明）
- [ ] DML 唯一键冲突判定已结构化（附测试用例）
- [ ] 高风险测试去 flaky（附 10 轮稳定运行记录）
- [ ] `go test ./server/innodb/engine` 通过
- [ ] `go test ./server/dispatcher` 通过

说明：

- `go test ./server/dispatcher` 当前可作为已通过候选项
- `go test ./server/innodb/engine` 当前被 Go 版本和构建错误阻塞
- Top 12 中仍存在 TODO、错误文本判定、sleep 驱动测试和包装器未实现逻辑

## 2. 崩溃恢复验证（B）

当前判断：`整体未通过`

- [ ] 已实现异常退出恢复脚本（附脚本路径）
- [ ] redo 场景恢复通过（附报告）
- [ ] undo 场景恢复通过（附报告）
- [ ] 半提交事务恢复通过（附报告）
- [ ] 恢复结果一致性校验通过（附校验输出）
- [ ] 同场景重复回放结果一致（附多轮记录）

说明：

- 当前有恢复代码基础和相关测试
- 但无 `scripts/` 恢复脚本、正式演练报告、结果快照和多轮回放记录

## 3. 并发正确性验证（C）

当前判断：`整体未通过`

- [ ] 已建立并发压测脚本（附脚本路径）
- [ ] 冲突写场景通过（附报告）
- [ ] 范围读场景通过（附报告）
- [ ] 长事务场景通过（附报告）
- [ ] 死锁/锁等待行为可控（附统计）
- [ ] 无明显脏读/丢写/重复写（附一致性校验）

说明：

- 当前仅有零散并发相关测试和 demo
- 未发现正式压测脚本、一致性校验脚本和并发验证报告

## 4. 基础可观测性上线（D）

当前判断：`整体未通过`

- [ ] 错误日志字段规范已落地（附字段文档）
- [ ] 慢查询日志可用（附样例日志）
- [ ] QPS 指标可观测（附面板或输出）
- [ ] 延迟分位 P50/P95/P99 可观测（附面板或输出）
- [ ] 错误率可观测（附面板或输出）
- [ ] 活跃连接/事务数可观测（附面板或输出）
- [ ] 告警规则已配置并可触发（附演练截图）

说明：

- 仓库中存在少量内部 stats/alert 结构
- 未发现可用于生产验收的 metrics exporter、面板、告警配置和慢查询样例证据

## 5. 回滚预案与灰度演练（E）

当前判断：`整体未通过`

- [ ] 灰度发布手册完成（附文档路径）
- [ ] 快速回退手册完成（附文档路径）
- [ ] 数据回滚手册完成（附文档路径）
- [ ] 全链路演练完成（附演练记录）
- [ ] 演练复盘完成并闭环问题（附复盘链接）
- [ ] 可在预期窗口内完成回退（附计时记录）

说明：

- `docs/planning/` 下当前未发现灰度发布、快速回退、数据回滚手册
- 未发现全链路演练记录、复盘与计时证据

## 6. 统一发布前检查

当前判断：`整体未通过`

- [ ] `go test ./...` 通过
- [ ] IDE 诊断为 0
- [ ] 关键配置已版本化与备份
- [ ] 上线评审包已归档

说明：

- `go test ./...` 当前不可勾选
- Go 版本与 `go.mod` 不一致，需先恢复基线
- 上线评审包依赖 B/C/D/E 的交付物，目前不具备归档条件

## 7. 审批信息

- 技术负责人：`[ ] 同意上线`
- 测试负责人：`[ ] 同意上线`
- 运维负责人：`[ ] 同意上线`
- 业务负责人：`[ ] 同意上线`

最终结论：`[ ] 允许进入生产灰度`

当前判断：

- `不可进入生产灰度`

---

## 8. 2026-06-13 baseline evidence update

### Current baseline result

The default repository test baseline is now green.

Evidence command:

```bash
go test ./...
```

Result: pass.

Additional focused evidence:

```bash
go test ./server/innodb/engine
go test ./server/dispatcher
go test ./server/innodb/sqlparser
go test ./server/innodb/storage/wrapper/mvcc
go test ./server/innodb/storage/wrapper/system
go test ./util
```

All commands above pass in the current workspace.

### Checklist interpretation update

The following checklist items now have baseline evidence and can be treated as completed for the default Go test profile:

- `go test ./server/innodb/engine` passes.
- `go test ./server/dispatcher` passes.
- `go test ./...` passes.

The following are intentionally not counted as production-ready completion yet:

- Historical demo/test commands are excluded from default tests by the `demo` build tag.
- Legacy SQL parser conformance tests are excluded from default tests by the `legacy_sqlparser_conformance` build tag.
- B/C/D/E production evidence is still missing: recovery drill reports, concurrency stress reports, observability evidence, gray release and rollback drill records.

### Manual profiles

Use these commands when those excluded profiles need explicit validation:

```bash
go test -tags demo ./cmd/...
go test -tags legacy_sqlparser_conformance ./server/innodb/sqlparser
```

### Final production judgment

Current status: default engineering baseline restored, but P0 production gray-release approval is still not complete.

---

## 9. 2026-06-14 checklist update

### Project phase

Current phase: **P0 production-readiness evidence collection**.

The project should not be treated as production-ready yet. The default engineering baseline is green, but P0 acceptance still requires operational evidence, drill reports, and release/rollback runbooks.

### Items that now have evidence

The following items have current evidence in this workspace:

- [x] Latest code pulled with `git pull --ff-only`; repository was already up to date.
- [x] Default repository baseline passes with `go test ./...`.
- [x] `go test ./server/innodb/engine` passes.
- [x] `go test ./server/dispatcher` passes.
- [x] Focused crash-recovery manager suite passes:

```bash
go test ./server/innodb/manager -run 'TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint' -count=1 -timeout=180s
```

### P0-B crash recovery checklist delta

Crash recovery is upgraded from "no usable evidence" to "partial evidence available".

- [x] Crash-recovery code and focused manager tests exist.
- [x] A shell drill entrypoint exists at `scripts/crash_recovery_drill.sh`.
- [x] Focused redo/undo/recovery-related manager tests pass in the current workspace.
- [ ] Windows-friendly drill entrypoint is committed.
- [ ] Drill output is archived under `reports/`.
- [ ] Markdown drill report is archived under `reports/`.
- [ ] Repeated replay evidence is archived.
- [ ] Snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

### Current production approval judgment

Current status: **not approved for production gray release**.

Reason:

- P0-B is only partially evidenced.
- P0-C concurrency stress evidence is missing.
- P0-D observability evidence is missing.
- P0-E gray release, rollback, and recovery runbooks are missing.

### Next checklist action

Next action should update P0-B deliverables:

1. Generate a crash-recovery drill report.
2. Store raw logs and markdown summary under `reports/`.
3. Link the report path back into this checklist.
4. Re-run `go test ./...` after any code changes that affect the baseline.

---

## 10. 2026-06-14 P0-B drill automation update

Crash-recovery drill automation artifacts have been added or aligned.

- [x] Windows-friendly drill entrypoint exists at `scripts/crash_recovery_drill.ps1`.
- [x] Bash drill entrypoint exists at `scripts/crash_recovery_drill.sh`.
- [x] Report directory purpose is documented at `reports/README.md`.
- [ ] Drill output is archived under `reports/`.
- [ ] Markdown drill report is archived under `reports/`.
- [ ] Repeated replay evidence is archived.
- [ ] Snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

Run command for Windows local evidence generation:

```powershell
./scripts/crash_recovery_drill.ps1
```

Run command for Bash-compatible environments:

```bash
./scripts/crash_recovery_drill.sh
```

Current judgment: P0-B has executable drill automation, but still needs generated reports and repeated evidence before acceptance.

---

## 11. 2026-06-14 repeated replay checklist update

Repeated replay support has been added to the crash-recovery drill scripts.

- [x] PowerShell drill supports repeated replay with `-Runs`.
- [x] Bash drill supports repeated replay with `CR_RUNS`.
- [ ] Multi-run replay report is archived under `reports/`.
- [ ] Snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

Recommended acceptance command for Windows:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3
```

Recommended acceptance command for Bash-compatible environments:

```bash
CR_RUNS=3 ./scripts/crash_recovery_drill.sh
```

The generated Markdown report should be linked here after execution.

---

## 12. 2026-06-14 P0-B replay drill evidence

A 3-run replay drill has been executed and archived.

Command:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3
```

Result: PASS.

Evidence artifacts:

- [x] Multi-run replay raw log archived: `reports/crash_recovery_drill_20260614_070446.log`
- [x] Multi-run replay Markdown report archived: `reports/crash_recovery_drill_20260614_070446.md`
- [x] Replay run 1 passed.
- [x] Replay run 2 passed.
- [x] Replay run 3 passed.
- [ ] Snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

Current judgment: P0-B has repeatable drill automation and one successful 3-run replay report, but full acceptance still requires snapshot/state-diff evidence and broader scenario matrix hardening.

---

## 13. 2026-06-14 P0-B runbook update

A dedicated P0-B crash recovery drill runbook now exists:

- [x] Runbook added: `docs/planning/P0_B_CRASH_RECOVERY_DRILL_RUNBOOK.md`
- [x] Current 3-run replay report is referenced by the runbook.
- [x] Snapshot/state-diff JSON requirement is specified.
- [ ] Snapshot or state-diff JSON artifact is archived.
- [ ] Half-commit or interrupted-commit evidence is archived.
- [ ] P0-B can be marked fully accepted.

Next implementation target: add state evidence output to the crash-recovery drill automation.

---

## 14. 2026-06-14 command-level state evidence automation update

Crash-recovery drill scripts now support command-level state evidence JSON output.

- [x] PowerShell drill supports `-StateEvidence`.
- [x] Bash drill supports `CR_STATE_EVIDENCE=1`.
- [ ] Command-level `.state.json` artifact is archived under `reports/`.
- [ ] Storage-state snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

Command to generate the next evidence artifact:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence
```

The generated `.state.json` should be linked here after execution.

---

## 15. 2026-06-14 state evidence verifier checklist update

A PowerShell schema verifier now exists for command-level state evidence JSON.

- [x] State evidence verifier added: `scripts/verify_crash_recovery_state_evidence.ps1`.
- [ ] Command-level `.state.json` artifact is generated.
- [ ] Command-level `.state.json` artifact passes schema verification.
- [ ] Storage-state snapshot or state-diff evidence is archived.
- [ ] P0-B can be marked fully accepted.

Verification command after generating state evidence:

```powershell
./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json
```

---

## 16. 2026-06-14 P0-C concurrency validation planning update

A dedicated P0-C concurrency correctness validation plan now exists:

- [x] P0-C validation plan added: `docs/planning/P0_C_CONCURRENCY_VALIDATION_PLAN.md`.
- [x] Scenario matrix defined for conflict writes, range reads, long transactions, lock behavior, deadlock behavior, and repeated replay.
- [x] Required report artifact shape defined.
- [ ] Concurrency validation script exists.
- [ ] Concurrency validation JSON verifier exists.
- [ ] Multi-run concurrency report is archived under `reports/`.
- [ ] Consistency checks are implemented and passing.
- [ ] P0-C can be marked fully accepted.

Current judgment: P0-C is planned, not implemented.

---

## 17. 2026-06-14 P0-C validation automation update

P0-C command-level validation automation now exists.

- [x] PowerShell concurrency validation script added: `scripts/concurrency_validation.ps1`.
- [x] Bash concurrency validation script added: `scripts/concurrency_validation.sh`.
- [x] JSON report verifier added: `scripts/verify_concurrency_validation_report.ps1`.
- [ ] Multi-run concurrency report is archived under `reports/`.
- [ ] Multi-run concurrency JSON report passes schema verification.
- [ ] Explicit consistency checks are implemented and passing.
- [ ] P0-C can be marked fully accepted.

Recommended report generation command:

```powershell
./scripts/concurrency_validation.ps1 -Runs 3
```

Recommended report verification command:

```powershell
./scripts/verify_concurrency_validation_report.ps1 -Path reports/concurrency_validation_<timestamp>.json
```

---

## 18. 2026-06-14 P0-D observability planning update

A dedicated P0-D observability production-readiness plan now exists:

- [x] P0-D observability plan added: `docs/planning/P0_D_OBSERVABILITY_PLAN.md`.
- [x] Minimum metric catalog defined.
- [x] Slow query log field contract defined.
- [x] Error log field contract defined.
- [x] Minimum alert rule set defined.
- [ ] Observability smoke script exists.
- [ ] Observability smoke report verifier exists.
- [ ] Sample slow query log evidence exists.
- [ ] Sample error log evidence exists.
- [ ] Alert rule templates exist.
- [ ] Observability smoke report is archived under `reports/`.
- [ ] P0-D can be marked fully accepted.

Current judgment: P0-D is planned, not implemented.

---

## 19. 2026-06-14 P0-D observability artifact update

P0-D documentation/configuration smoke artifacts now exist.

- [x] Metric catalog added: `docs/observability/metrics_catalog.md`.
- [x] Sample slow query log added: `docs/observability/sample_slow_query.log`.
- [x] Sample error log added: `docs/observability/sample_error.log`.
- [x] Alert rules added: `deploy/alerts/xmysql-p0-alerts.yml`.
- [x] Observability smoke script added: `scripts/observability_smoke.ps1`.
- [x] Observability report verifier added: `scripts/verify_observability_report.ps1`.
- [ ] Observability smoke report is archived under `reports/`.
- [ ] Observability smoke JSON passes schema verification.
- [ ] Live metrics export evidence exists.
- [ ] Alert delivery drill evidence exists.
- [ ] P0-D can be marked fully accepted.

Recommended smoke command:

```powershell
./scripts/observability_smoke.ps1
```

Recommended verification command:

```powershell
./scripts/verify_observability_report.ps1 -Path reports/observability_smoke_<timestamp>.json
```

---

## 20. 2026-06-14 P0-E release and rollback runbook update

P0-E operational runbooks now exist.

- [x] P0-E release/rollback plan added: `docs/planning/P0_E_RELEASE_ROLLBACK_PLAN.md`.
- [x] Gray release runbook added: `docs/operations/gray_release_runbook.md`.
- [x] Fast rollback runbook added: `docs/operations/rollback_runbook.md`.
- [x] Data recovery runbook added: `docs/operations/data_recovery_runbook.md`.
- [x] Full-chain drill template added: `docs/operations/full_chain_drill_record_template.md`.
- [ ] Full-chain drill report is archived under `reports/`.
- [ ] Rollback timing evidence is archived.
- [ ] Data recovery point and replay boundary evidence are archived.
- [ ] P0-B/P0-C/P0-D evidence is linked from a completed drill record.
- [ ] P0-E can be marked fully accepted.

Current judgment: P0-E runbooks are present, but no executed full-chain drill evidence exists yet.

---

## 21. 2026-06-14 P0-E full-chain drill smoke automation update

P0-E full-chain drill smoke automation now exists.

- [x] Full-chain drill smoke script added: `scripts/full_chain_drill_smoke.ps1`.
- [x] Full-chain drill JSON verifier added: `scripts/verify_full_chain_drill_report.ps1`.
- [ ] Full-chain drill smoke report is archived under `reports/`.
- [ ] Full-chain drill smoke JSON passes schema verification.
- [ ] Timed release/rollback drill evidence is archived.
- [ ] Data recovery point and replay boundary evidence are archived.
- [ ] P0-E can be marked fully accepted.

Recommended smoke command after P0-C and P0-D reports exist:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_20260614_070446.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md
```

Recommended verification command:

```powershell
./scripts/verify_full_chain_drill_report.ps1 -Path reports/full_chain_drill_<timestamp>.json
```

---

## 22. 2026-06-14 P0 evidence bundle update

A top-level P0 evidence bundle now exists.

- [x] Evidence bundle definition added: `docs/planning/P0_EVIDENCE_BUNDLE.md`.
- [x] Evidence bundle verifier added: `scripts/verify_p0_evidence_bundle.ps1`.
- [ ] P0 evidence bundle report is archived under `reports/`.
- [ ] P0 evidence bundle JSON passes presence checks.
- [ ] Every linked report passes its dedicated verifier.
- [ ] Fresh final regression gate passes.
- [ ] Production gray-release approval can be considered.

Current judgment: bundle tooling exists, but the complete bundle cannot pass until remaining P0-B/C/D/E reports are generated and linked.

---

## 23. 2026-06-14 P0 evidence suite automation update

A top-level P0 evidence suite runner now exists.

- [x] P0 evidence suite runner added: `scripts/run_p0_evidence_suite.ps1`.
- [ ] P0 evidence suite has been executed successfully.
- [ ] P0 evidence bundle report from the suite is archived.
- [ ] Fresh final regression gate passes.
- [ ] Production gray-release approval can be considered.

Suite command:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

The suite runs P0-B/P0-C/P0-D/P0-E evidence scripts and verifiers, then generates the top-level evidence bundle.

---

## 24. 2026-06-14 final regression gate automation update

A final default regression gate now exists.

- [x] Final regression gate script added: `scripts/final_regression_gate.ps1`.
- [x] Final regression gate verifier added: `scripts/verify_final_regression_gate_report.ps1`.
- [x] P0 evidence suite includes final regression gate execution.
- [x] P0 evidence bundle requires final regression report evidence.
- [ ] Final regression gate report is archived under `reports/`.
- [ ] Final regression gate JSON passes schema verification.
- [ ] Production gray-release approval can be considered.

Final regression command:

```powershell
./scripts/final_regression_gate.ps1
```

Verifier command:

```powershell
./scripts/verify_final_regression_gate_report.ps1 -Path reports/final_regression_gate_<timestamp>.json
```

---

## 25. 2026-06-14 P0 status summary and approval packet update

Final handoff and approval documents now exist.

- [x] Current status summary added: `docs/planning/P0_CURRENT_STATUS_SUMMARY.md`.
- [x] Release approval packet template added: `docs/planning/P0_RELEASE_APPROVAL_PACKET_TEMPLATE.md`.
- [ ] Release approval packet is filled with actual generated report paths.
- [ ] Owner sign-off is complete.
- [ ] Production gray-release approval can be considered.

Current judgment: approval packet structure exists, but final approval is still blocked until the evidence suite is executed and report paths are filled in.

---

## 26. 2026-06-14 approval packet generator update

A release approval packet generator now exists.

- [x] Approval packet generator added: `scripts/generate_p0_release_approval_packet.ps1`.
- [x] P0 evidence suite generates approval packet after bundle verification.
- [ ] Generated approval packet is archived under `reports/`.
- [ ] Generated approval packet has all evidence marked present.
- [ ] Owner sign-off is complete.
- [ ] Production gray-release approval can be considered.

Generator command after reports exist:

```powershell
./scripts/generate_p0_release_approval_packet.ps1 <report path arguments>
```

Preferred path: run the full suite and let it generate the approval packet automatically.

---

## 27. 2026-06-14 approval packet verifier update

A release approval packet verifier now exists.

- [x] Approval packet verifier added: `scripts/verify_p0_release_approval_packet.ps1`.
- [x] P0 evidence suite validates approval packet after generation.
- [ ] Generated approval packet passes verifier.
- [ ] Owner sign-off is complete.
- [ ] Production gray-release approval can be considered.

Verifier command:

```powershell
./scripts/verify_p0_release_approval_packet.ps1 -Path reports/p0_release_approval_packet_<timestamp>.md
```

---

## 28. 2026-06-14 P0 tooling preflight update

A tooling/documentation preflight verifier now exists.

- [x] P0 tooling preflight script added: `scripts/verify_p0_tooling_preflight.ps1`.
- [x] P0 evidence suite runs tooling preflight first.
- [ ] P0 tooling preflight report is archived under `reports/`.
- [ ] P0 evidence suite completes successfully.
- [ ] Production gray-release approval can be considered.

Preflight command:

```powershell
./scripts/verify_p0_tooling_preflight.ps1
```

This command checks file presence only. It does not run tests or drills.

---

## 29. 2026-06-14 GitHub Actions P0 evidence workflow update

A manual GitHub Actions workflow now exists for P0 evidence generation.

- [x] P0 evidence workflow added: `.github/workflows/p0-evidence.yml`.
- [x] Workflow supports `preflight` mode.
- [x] Workflow supports `full` mode.
- [x] Workflow uploads `reports/**` artifacts.
- [ ] CI preflight run has completed successfully.
- [ ] CI full evidence run has completed successfully.
- [ ] Production gray-release approval can be considered.

Use GitHub Actions manual dispatch and choose either `preflight` or `full`.

---

## 30. 2026-06-14 P0 evidence suite operator guide update

A P0 evidence suite operator guide now exists.

- [x] Operator guide added: `docs/operations/p0_evidence_suite_operator_guide.md`.
- [x] P0 tooling preflight checks `.github/workflows/p0-evidence.yml`.
- [ ] Local preflight report is archived.
- [ ] Local or CI full evidence suite report set is archived.
- [ ] Production gray-release approval can be considered.

Use the operator guide before running the local or CI evidence suite.

---

## 31. 2026-06-14 remaining engineering backlog and risk register update

Remaining engineering backlog and risk register now exist.

- [x] Remaining engineering backlog added: `docs/planning/P0_REMAINING_ENGINEERING_BACKLOG.md`.
- [x] Risk register added: `docs/planning/P0_RISK_REGISTER.md`.
- [ ] P0-B storage-state diff evidence is complete.
- [ ] P0-C explicit consistency checks are complete.
- [ ] P0-D live metrics/export evidence is complete.
- [ ] P0-D alert delivery drill evidence is complete.
- [ ] P0-E timed release/rollback drill evidence is complete.
- [ ] Risks are closed or explicitly accepted by owners.
- [ ] Production gray-release approval can be considered.

Current judgment: tooling and documentation are strong, but several production capability proofs remain open.

---

## 32. 2026-06-14 P0 preflight coverage update

P0 tooling preflight now covers the latest handoff and risk documents.

- [x] Preflight checks remaining engineering backlog.
- [x] Preflight checks risk register.
- [x] Preflight checks evidence suite operator guide.
- [ ] Local or CI preflight report is archived.
- [ ] Local or CI full evidence suite report set is archived.
- [ ] Production gray-release approval can be considered.

---

## 33. 2026-06-14 remaining engineering execution plan update

Execution planning for the remaining P0 backlog now exists.

- [x] Remaining engineering execution plan added: `docs/planning/P0_REMAINING_ENGINEERING_EXECUTION_PLAN.md`.
- [x] Next-actions checklist added: `docs/planning/P0_NEXT_ACTIONS.md`.
- [x] Preflight checks both files.
- [ ] First complete evidence suite run is archived.
- [ ] First failing workstream is fixed or explicitly tracked.
- [ ] Production gray-release approval can be considered.

Current best next command remains:

```powershell
./scripts/run_p0_evidence_suite.ps1 -RecoveryRuns 3 -ConcurrencyRuns 1
```

---

## 34. 2026-06-14 P0-D metrics registry foundation update

A lightweight metrics registry has been added for P0-D live metrics export.

- [x] Metrics registry foundation added: `server/observability/metrics/registry.go`.
- [x] P0-D metric catalog registration added: `server/observability/metrics/defaults.go`.
- [x] Metrics registry usage doc added: `docs/observability/metrics_registry_usage.md`.
- [ ] Runtime query/transaction/lock/recovery/checkpoint paths update the registry.
- [ ] Production-consumable metrics endpoint or export artifact exists.
- [ ] Live metrics export evidence is archived.
- [ ] P0-D can be marked fully accepted.

Current judgment: P0-D export work now has code foundation, but runtime integration and live evidence remain open.

---

## 35. 2026-06-14 P0-D metrics export smoke update

Metrics export smoke tooling now exists.

- [x] Metrics export command added: `cmd/p0_metrics_export/main.go`.
- [x] Metrics export smoke script added: `scripts/metrics_export_smoke.ps1`.
- [x] Metrics export report verifier added: `scripts/verify_metrics_export_report.ps1`.
- [x] P0 evidence suite runs metrics export smoke after observability smoke.
- [ ] Metrics export smoke report is archived under `reports/`.
- [ ] Metrics export JSON passes schema verification.
- [ ] Runtime server paths update live metrics.
- [ ] P0-D can be marked fully accepted.

Smoke command:

```powershell
./scripts/metrics_export_smoke.ps1
```

Verifier command:

```powershell
./scripts/verify_metrics_export_report.ps1 -Path reports/metrics_export_<timestamp>.json
```

---

## 36. 2026-06-14 metrics export evidence bundle integration update

P0-D metrics export evidence is now required by the final evidence bundle and approval packet.

- [x] Evidence bundle requires metrics export Markdown report.
- [x] Evidence bundle requires metrics export JSON report.
- [x] Approval packet includes metrics export evidence rows.
- [x] Approval packet verifier checks metrics export evidence rows.
- [ ] Metrics export report is generated and archived.
- [ ] Metrics export JSON passes verifier.
- [ ] Runtime live metrics integration is complete.
- [ ] P0-D can be marked fully accepted.

---

## 37. 2026-06-14 P0-D metrics HTTP handler foundation update

A reusable Prometheus metrics HTTP handler now exists.

- [x] Metrics HTTP handler added: `server/observability/metrics/http.go`.
- [x] Metrics HTTP handler usage doc added: `docs/observability/metrics_http_handler_usage.md`.
- [x] P0 preflight checks metrics registry and HTTP handler usage docs.
- [ ] Handler is mounted into live server runtime.
- [ ] Runtime paths update registry values.
- [ ] Live metrics endpoint evidence is archived.
- [ ] P0-D can be marked fully accepted.

---

## 38. 2026-06-15 P0-D metrics runtime recorder foundation update

A reusable runtime metrics recorder now exists.

- [x] Runtime metrics recorder added: `server/observability/metrics/runtime.go`.
- [x] Runtime recorder usage doc added: `docs/observability/metrics_runtime_recorder_usage.md`.
- [x] P0 preflight checks runtime recorder usage docs.
- [ ] Query execution paths call runtime recorder.
- [ ] Transaction paths call runtime recorder.
- [ ] Lock/recovery/checkpoint paths call runtime recorder.
- [ ] Live metrics endpoint/export evidence is archived.
- [ ] P0-D can be marked fully accepted.

---

## 39. 2026-06-15 P0-D structured logging foundation update

Structured slow-query and error-log foundations now exist.

- [x] Structured logging package added: `server/observability/logging/structured.go`.
- [x] Structured log export command added: `cmd/p0_structured_log_export/main.go`.
- [x] Structured logging usage doc added: `docs/observability/structured_logging_usage.md`.
- [x] Structured logging smoke script added: `scripts/structured_logging_smoke.ps1`.
- [x] Structured logging report verifier added: `scripts/verify_structured_logging_report.ps1`.
- [x] P0 evidence suite includes structured logging smoke.
- [x] Evidence bundle and approval packet require structured logging reports.
- [ ] Runtime query paths emit structured slow-query logs.
- [ ] Runtime engine/error paths emit structured error logs.
- [ ] P0-D can be marked fully accepted.

---

## 40. 2026-06-15 P0-D alert drill smoke update

Alert drill smoke tooling now exists.

- [x] Alert drill smoke script added: `scripts/alert_drill_smoke.ps1`.
- [x] Alert drill report verifier added: `scripts/verify_alert_drill_report.ps1`.
- [x] P0 evidence suite includes alert drill smoke.
- [x] Evidence bundle and approval packet require alert drill reports.
- [ ] Alert drill smoke report is archived under `reports/`.
- [ ] Alert drill JSON passes verifier.
- [ ] Live Alertmanager delivery evidence exists or is explicitly owner-deferred.
- [ ] P0-D can be marked fully accepted.

---

## 41. 2026-06-15 P0-D live metrics endpoint integration update

The metrics endpoint is now wired into the existing profiling HTTP listener.

- [x] Default metrics registry added: `server/observability/metrics/global.go`.
- [x] `/metrics` handler registered in `server/net/mysql_server.go`.
- [x] Endpoint uses existing profiling listener and does not add a new port.
- [ ] Runtime query/transaction/lock/recovery/checkpoint paths update the default recorder.
- [ ] Running server endpoint scrape evidence is archived.
- [ ] P0-D can be marked fully accepted.

Current judgment: live endpoint foundation is integrated, but live metric values and scrape evidence are still pending.

---

## 42. 2026-06-15 P0-D live metrics endpoint probe update

Live metrics endpoint probe tooling now exists.

- [x] Metrics endpoint probe added: `scripts/metrics_endpoint_probe.ps1`.
- [x] Metrics endpoint probe verifier added: `scripts/verify_metrics_endpoint_probe_report.ps1`.
- [x] P0 preflight checks endpoint probe tooling.
- [ ] Running server `/metrics` probe report is archived under `reports/`.
- [ ] Probe JSON passes verifier.
- [ ] Runtime paths update metrics with live values.
- [ ] P0-D can be marked fully accepted.

Probe command after starting the server:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
```

## 2026-06-15 Checklist delta - P0-D active connection metric

- [x] Register /metrics on the profiling HTTP listener.
- [x] Wire xmysql_connections_active{listener="mysql"} to MySQL session lifecycle.
- [ ] Run metrics endpoint probe against a live server after the active connection wiring.
- [ ] Run full P0 evidence suite after the latest observability changes.
- [ ] Add direct runtime instrumentation for query / transaction / lock / recovery / checkpoint metrics.

## 2026-06-15 Evidence suite run - PASS

The P0 evidence suite was executed after the latest observability updates.

Result:
- Overall P0 evidence suite: PASS
- Final default regression gate: PASS
- P0 release approval packet: READY_FOR_REVIEW
- Missing evidence count reported by approval packet: 0

Generated evidence:
- P0-B crash recovery report: eports/crash_recovery_drill_20260615_001527.md
- P0-B state evidence: eports/crash_recovery_drill_20260615_001527.state.json
- P0-C concurrency report: eports/concurrency_validation_20260615_001537.md
- P0-D observability smoke: eports/observability_smoke_20260615_001536.md
- P0-D metrics export: eports/metrics_export_20260615_001536.md
- P0-D structured logging: eports/structured_logging_20260615_001536.md
- P0-D alert drill: eports/alert_drill_20260615_001537.md
- P0-E full-chain drill: eports/full_chain_drill_20260615_001550.md
- Final regression gate: eports/final_regression_gate_20260615_001551.md
- Evidence bundle: eports/p0_evidence_bundle_20260615_001559.md
- Release approval packet: eports/p0_release_approval_packet_20260615_001559.md

Tooling note:
- The first suite run exposed a PowerShell singleton-array counting issue in the P0-C concurrency tooling.
- Fixed scripts: scripts/concurrency_validation.ps1 and scripts/verify_concurrency_validation_report.ps1 now wrap possibly singleton collections with @(...) before using .Count.

Remaining live verification gap:
- Start a real server with profiling enabled and run the metrics endpoint probe to observe /metrics and active connection changes from live MySQL sessions.

## 2026-06-19 P0-B recovery state evidence contract update

The crash recovery drill state evidence has been upgraded from plain command replay checks to a structured test-backed recovery state contract.

Updated tooling:
- `scripts/crash_recovery_drill.ps1`
- `scripts/verify_crash_recovery_state_evidence.ps1`

New evidence fields:
- root `verification_level = test_backed_recovery_state_contract`
- root `scenario_matrix` covering transaction lifecycle, redo replay boundary, undo rollback recovery, interrupted-commit recovery, and savepoint partial rollback
- per-run `state_evidence` with WAL replay boundary and interrupted-commit recovery checks
- explicit `row_level_state_diff` and `page_level_state_diff` entries marked `NOT_VERIFIED` until standalone storage snapshot artifacts are implemented

Impact:
- Advances `P0-B-STATE-01` by making recovered-state expectations explicit and verifier-enforced.
- Advances `P0-B-HALF-01` by documenting interrupted/half-commit recovery as a test-backed recovery-phase contract.
- Does not fully close P0-B because standalone page/row/WAL snapshot diff evidence is still required.

Next evidence command:

```powershell
./scripts/crash_recovery_drill.ps1 -Runs 3 -StateEvidence
./scripts/verify_crash_recovery_state_evidence.ps1 -Path reports/crash_recovery_drill_<timestamp>.state.json
```

## 2026-06-20 Delivery readiness hard gate update

A delivery readiness audit has been added to prevent evidence presence from being mistaken for production deliverability.

New tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`

Suite integration:
- `scripts/verify_p0_tooling_preflight.ps1` now checks the delivery audit tooling.
- `scripts/run_p0_evidence_suite.ps1` now runs the delivery readiness audit after release approval packet validation.

Audit behavior:
- `READY` requires all delivery checks to pass.
- `NOT_READY` is expected while the approval packet remains on `HOLD`, owner sign-off items are unchecked, risk owners are `TBD`, or P0 risks remain `Open`.
- This gate is intentionally stricter than the release approval packet's `READY_FOR_REVIEW` status. `READY_FOR_REVIEW` means evidence files are present; `READY` means the project is actually deliverable according to risk and approval gates.

Operational note:
- A full evidence suite may now fail at the final delivery-readiness step even after all evidence-generation steps pass. Treat that as a correct delivery blocker, not as a tooling failure.

Example commands:

```powershell
./scripts/delivery_readiness_audit.ps1 -ApprovalPacket reports/p0_release_approval_packet_<timestamp>.md
./scripts/verify_delivery_readiness_audit.ps1 -Path reports/delivery_readiness_audit_<timestamp>.json
```

## 2026-06-20 Release packet delivery-boundary update

The release approval packet generator now includes an explicit delivery-readiness boundary.

Updated tooling:
- `scripts/generate_p0_release_approval_packet.ps1`
- `scripts/verify_p0_release_approval_packet.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior change:
- `READY_FOR_REVIEW` means required evidence artifacts are present for review.
- It does not mean the project is deliverable.
- The packet now includes `Delivery status: NOT_ASSESSED_BY_PACKET` and points reviewers to `delivery_readiness_audit.ps1` for the actual deliverability gate.
- The approval packet verifier now requires the delivery-readiness boundary section, audit command, and audit verifier command.

Delivery rule:
- Treat release approval packet validation as a review-material check.
- Treat delivery readiness audit `READY` as the delivery gate.

## 2026-06-20 Delivery audit failure evidence preservation

`run_p0_evidence_suite.ps1` now treats the delivery readiness audit specially:

- The audit step is allowed to return non-zero long enough to write its Markdown/JSON reports.
- The suite then runs `verify_delivery_readiness_audit.ps1` against the generated JSON.
- After schema verification, the suite reads the audit status and fails explicitly unless the status is `READY`.

This preserves `NOT_READY` audit evidence instead of stopping before the delivery blocker report can be schema-verified.

## 2026-06-20 Post-packet delivery gate row update

The release approval packet now renders the delivery readiness audit row as `POST_PACKET_GATE` when the audit JSON has not been generated yet.

Why:
- The approval packet is generated before `delivery_readiness_audit.ps1` runs.
- Showing the audit JSON as `MISSING` inside the packet made the evidence table conflict with `Missing evidence count: 0`.
- `POST_PACKET_GATE` makes the ordering explicit: the packet is review material, and the delivery audit is the follow-up gate.

Interpretation:
- `PRESENT` means an evidence artifact exists before packet generation.
- `POST_PACKET_GATE` means the artifact is expected to be generated after packet validation.
- `READY_FOR_REVIEW` still does not mean deliverable; delivery requires a later audit status of `READY`.

## 2026-06-21 Delivery audit blocker action update

The delivery readiness audit now emits actionable delivery blockers.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`

New report fields:
- `blockers`: failed delivery checks with severity, actual state, and next action
- `next_actions`: de-duplicated remediation actions for delivery review

Verifier behavior:
- `NOT_READY` reports must include at least one blocker.
- `NOT_READY` reports must include at least one next action.

Operational interpretation:
- A `NOT_READY` audit is now an actionable delivery remediation list, not just a failed gate.
- Reviewers should work through `next_actions` before rerunning the delivery audit.

## 2026-06-21 Delivery remediation packet update

A delivery remediation packet generator has been added for turning delivery audit blockers into owner-trackable work items.

New tooling:
- `scripts/delivery_remediation_packet.ps1`
- `scripts/verify_delivery_remediation_packet.ps1`

Preflight integration:
- `scripts/verify_p0_tooling_preflight.ps1` now checks both remediation packet scripts.

Usage:

```powershell
./scripts/delivery_remediation_packet.ps1 -AuditJson reports/delivery_readiness_audit_<timestamp>.json
./scripts/verify_delivery_remediation_packet.ps1 -Path reports/delivery_remediation_packet_<timestamp>.json
```

Interpretation:
- The delivery readiness audit remains the source of truth for `READY` / `NOT_READY`.
- The remediation packet converts `NOT_READY` blockers into `DELIVERY-*` work items with owner and status fields.
- This packet is intended for handoff and tracking after a failed delivery gate.

## 2026-06-21 Full suite remediation packet integration

`run_p0_evidence_suite.ps1` now generates a delivery remediation packet automatically after the delivery readiness audit.

Final delivery sequence:
1. Run `delivery_readiness_audit.ps1` and preserve the audit JSON even when it returns `NOT_READY`.
2. Verify the delivery audit JSON schema.
3. Generate `delivery_remediation_packet_<timestamp>.md/json` from the audit blockers.
4. Verify the remediation packet JSON schema.
5. Fail the suite if the delivery audit status is not `READY`, pointing to both the audit JSON and remediation JSON.

Impact:
- A `NOT_READY` full-suite run now produces both the blocker evidence and an owner-trackable remediation packet before failing.
- Operators no longer need to manually run the remediation packet generator after a failed delivery gate.

## 2026-06-21 Full suite report freshness guard

`run_p0_evidence_suite.ps1` now records a suite start timestamp and only resolves generated reports whose `LastWriteTime` is at or after that start boundary.

Why this matters:
- The suite previously selected the latest matching report by filename pattern only.
- If a step failed before writing a new report, a stale historical report could be selected accidentally.
- Delivery evidence must come from the current suite run, not a previous run.

Behavior:
- Missing current-run artifacts now fail with an error that includes the suite start timestamp.
- Historical reports remain archived but are not reused by the active suite run.

## 2026-06-21 P0 evidence suite summary manifest update

`run_p0_evidence_suite.ps1` now writes a current-run suite summary before the final delivery readiness status is enforced.

Generated artifacts:
- `p0_evidence_suite_<timestamp>.summary.md`
- `p0_evidence_suite_<timestamp>.summary.json`

Purpose:
- Index all evidence artifacts generated by the current suite run.
- Preserve a single handoff index even when the final delivery readiness audit returns `NOT_READY`.
- Point reviewers to the delivery audit JSON, remediation packet JSON, approval packet, evidence bundle, and all P0 workstream reports from the same run.

Boundary:
- The summary is an artifact index, not a replacement for dedicated verifiers.
- Delivery still requires `delivery_readiness_audit.status = READY`.

## 2026-06-21 P0 evidence suite summary verifier update

A dedicated suite summary verifier has been added.

New tooling:
- `scripts/verify_p0_evidence_suite_summary.ps1`

Suite/preflight integration:
- `scripts/run_p0_evidence_suite.ps1` validates the summary JSON before enforcing the final delivery readiness status.
- `scripts/verify_p0_tooling_preflight.ps1` checks that the summary verifier exists.

Verifier behavior:
- Checks `evidence_type = p0_evidence_suite_summary`.
- Checks `delivery_status` is `READY` or `NOT_READY`.
- Checks every listed artifact has a non-empty path.
- Checks every listed artifact path exists.

Boundary:
- This verifier proves the suite summary is structurally valid and points to existing current-run artifacts.
- It does not replace the dedicated P0-B/P0-C/P0-D/P0-E/final/delivery verifiers.

## 2026-06-21 Suite summary human-readable artifact index update

The P0 evidence suite summary now includes human-readable delivery artifacts in addition to JSON evidence paths.

Updated summary entries:
- Delivery readiness Markdown report
- Delivery readiness JSON report
- Delivery remediation Markdown report
- Delivery remediation JSON report
- Suite summary Markdown report
- Suite summary JSON report

Impact:
- Reviewers can start from the suite summary and jump directly to readable delivery blockers and remediation work items.
- `verify_p0_evidence_suite_summary.ps1` checks these paths because they are now listed in the summary artifact array.

## 2026-06-21 Suite summary artifact kind metadata update

The P0 evidence suite summary artifact list now includes a `kind` field for every artifact.

Supported kinds:
- `markdown`
- `json`
- `log`
- `approval`
- `audit-markdown`
- `audit-json`
- `remediation-markdown`
- `remediation-json`
- `summary-markdown`
- `summary-json`

Verifier behavior:
- `verify_p0_evidence_suite_summary.ps1` now requires `artifact.kind`.
- Unsupported artifact kind values fail summary verification.

Purpose:
- Human reviewers can quickly identify readable reports vs machine-verifiable JSON.
- Automation can route artifacts by type without parsing filenames.

## 2026-06-21 Suite summary checksum update

The P0 evidence suite summary now records checksum metadata for generated artifacts.

New artifact fields:
- `checksum_mode`
- `sha256`

Behavior:
- Regular artifacts use `checksum_mode = sha256` and include a lowercase SHA256 digest.
- Summary artifacts use `checksum_mode = self-referential` because a summary cannot stably contain its own content hash.
- `verify_p0_evidence_suite_summary.ps1` recomputes SHA256 for regular artifacts and fails on mismatch.

Purpose:
- Detect accidental or manual artifact modification after suite summary generation.
- Make the suite summary usable as a lightweight integrity manifest for delivery review.

## 2026-06-21 Suite summary artifact freshness update

The P0 evidence suite summary now records `last_write_time` for every indexed artifact.

Verifier behavior:
- `verify_p0_evidence_suite_summary.ps1` parses `suite_started_at` from the summary.
- Every artifact must include `last_write_time`.
- Every artifact `last_write_time` must be at or after the suite start boundary.
- The verifier also checks the actual file `LastWriteTime` to reject stale historical artifacts.

Purpose:
- Prevent current-run summaries from silently pointing at artifacts generated by older suite runs.
- Strengthen the summary as a delivery manifest covering path existence, checksum integrity, and freshness.

## 2026-06-21 Suite summary self-reference timestamp fix

The suite summary now handles self-referential summary artifacts explicitly.

Behavior:
- Regular artifacts record real file `LastWriteTime` and SHA256.
- Summary artifacts use `checksum_mode = self-referential` and record generation-time `last_write_time`, because the summary files do not exist until the summary is written.
- `verify_p0_evidence_suite_summary.ps1` still checks that the actual summary files exist and were written after suite start.
- For regular artifacts, the verifier also compares recorded `last_write_time` to the actual file time within a small tolerance.

Purpose:
- Avoid false failures on summary self-reference.
- Keep strict freshness/integrity checks for all non-summary artifacts.

## 2026-06-21 P0-C concurrency assertion and gap reporting update

P0-C concurrency validation reports now include structured scenario assertions and explicit consistency gaps.

Updated tooling:
- `scripts/concurrency_validation.ps1`
- `scripts/verify_concurrency_validation_report.ps1`

New report fields:
- root `consistency_summary`
- per-scenario `scenario_assertions`
- per-scenario `consistency_gaps`

Behavior:
- `scenario_assertions` record test-backed concurrency expectations for lock behavior, MVCC visibility, long transactions, conflict/recovery concurrency, wrapper concurrency, and storage MVCC deadlock/read-view behavior.
- `consistency_gaps` explicitly mark missing final-state, row-level anomaly, lock-wait metric, deadlock victim, and storage snapshot-diff evidence as `NOT_VERIFIED`.
- The verifier requires these fields and rejects consistency gaps that pretend to be verified before dedicated checks exist.

Impact:
- Advances `P0-C-CONSISTENCY-01`, `P0-C-RANGE-01`, and `P0-C-LOCK-01` by making coverage and gaps machine-readable.
- Does not fully close P0-C because Go-level final-state checkers, row/range anomaly probes, and lock/deadlock metric summaries are still required for full acceptance.

## 2026-06-21 P0-C delivery acceptance gate update

The delivery readiness audit now checks P0-C concurrency acceptance status directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-C concurrency JSON to the delivery readiness audit.
- The audit checks that the P0-C report includes `consistency_summary`.
- Delivery requires `acceptance_status = ACCEPTED`, `required_gap_count = 0`, and `not_verified_gaps = 0`.
- Current `PARTIAL` P0-C reports become explicit delivery blockers with a remediation action to implement final-state checkers, row/range anomaly probes, and lock/deadlock metric summaries.

Impact:
- P0-C gaps are now enforced by the final delivery gate instead of existing only as planning notes.

## 2026-06-21 P0-B delivery acceptance gate update

The delivery readiness audit now checks P0-B recovery state acceptance directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-B state evidence JSON to the delivery readiness audit.
- The audit checks that P0-B state evidence exists.
- The audit checks that row/page state-diff checks are verified and no longer `NOT_VERIFIED`.
- Current P0-B state evidence with row/page diff marked `NOT_VERIFIED` becomes an explicit delivery blocker.

Delivery requirement:
- Implement standalone row/page/WAL state-diff evidence for crash recovery.
- Re-run crash recovery with `-StateEvidence`.
- Verify the generated state JSON.
- Delivery remains `NOT_READY` until P0-B recovery state acceptance passes.

## 2026-06-21 P0-D delivery acceptance gate update

The delivery readiness audit now checks P0-D observability evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-D observability, metrics export, structured logging, and alert drill JSON reports to the delivery readiness audit.
- The audit requires these reports to exist and have `status = PASS`.
- The audit also requires a live metrics endpoint probe JSON with `status = PASS`.
- Because the full suite does not start a live server or generate a metrics endpoint probe by default, missing live endpoint evidence remains an explicit delivery blocker.

Delivery requirement:
- Start a server with profiling enabled.
- Run `scripts/metrics_endpoint_probe.ps1` against `/metrics`.
- Archive and verify a PASS endpoint probe JSON.
- Delivery remains `NOT_READY` until P0-D live metrics endpoint evidence exists.

## 2026-06-21 P0-E delivery acceptance gate update

The delivery readiness audit now checks P0-E release/rollback evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0-E full-chain JSON report to the delivery readiness audit.
- The audit requires the P0-E full-chain smoke report to exist and have `status = PASS`.
- The audit also requires `timed_rollback_evidence` with `status = PASS`, rollback duration, and recovery point evidence.
- Current smoke-only P0-E evidence remains an explicit delivery blocker until a timed release/rollback/data-recovery drill is archived.

Delivery requirement:
- Execute a timed release/rollback/data-recovery drill.
- Record rollback duration.
- Record recovery point and replay boundary evidence.
- Delivery remains `NOT_READY` until P0-E timed rollback evidence is present and passing.

## 2026-06-21 Final regression delivery gate update

The delivery readiness audit now checks final regression evidence directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run final regression JSON report to the delivery readiness audit.
- The audit requires `final_regression_gate_<timestamp>.json` to exist and have `status = PASS`.
- A present final regression evidence row in the release approval packet is not sufficient by itself; the delivery gate reads and checks the JSON status.

Delivery requirement:
- Run the final regression gate as part of the current evidence suite.
- Verify the generated final regression JSON.
- Delivery remains `NOT_READY` if the final regression gate is missing or not PASS.

## 2026-06-21 Evidence bundle delivery gate update

The delivery readiness audit now checks the P0 evidence bundle directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`
- `scripts/verify_delivery_readiness_audit.ps1`
- `scripts/run_p0_evidence_suite.ps1`

Behavior:
- The full suite passes the current-run P0 evidence bundle JSON to the delivery readiness audit.
- The audit requires `p0_evidence_bundle_<timestamp>.json` to exist and have `status = PASS`.
- A present bundle path in the release approval packet is not sufficient by itself; the final delivery gate reads and checks the bundle JSON status.

Delivery requirement:
- Generate the P0 evidence bundle from current-run evidence paths.
- Verify the generated bundle JSON.
- Delivery remains `NOT_READY` if the bundle JSON is missing or not PASS.

## 2026-06-21 Approval packet status delivery gate update

The delivery readiness audit now checks the release approval packet status directly.

Updated tooling:
- `scripts/delivery_readiness_audit.ps1`

Behavior:
- The audit still checks `Missing evidence count: 0`.
- The audit now also requires `Packet status: READY_FOR_REVIEW`.
- An `INCOMPLETE` packet is an explicit delivery blocker even if other approval fields are present.

Delivery requirement:
- Regenerate the release approval packet after all required evidence artifacts are present.
- Delivery remains `NOT_READY` until the packet is ready for review and all later delivery gates pass.

## 2026-06-21 Explicit live endpoint evidence input update

The full P0 evidence suite now accepts an explicit live metrics endpoint evidence path.

Updated tooling:
- `scripts/run_p0_evidence_suite.ps1`
- `scripts/delivery_readiness_audit.ps1`

Behavior:
- `run_p0_evidence_suite.ps1` now supports `-P0DMetricsEndpointJson <path>`.
- The suite passes that path into `delivery_readiness_audit.ps1`.
- During full-suite execution, the delivery audit runs with `-DisableReportFallback`, so it will not silently pick historical latest reports.
- If `-P0DMetricsEndpointJson` is omitted, the live metrics endpoint gate remains a clear delivery blocker.

Recommended flow:

```powershell
./scripts/metrics_endpoint_probe.ps1 -MetricsUrl http://127.0.0.1:<profile-port>/metrics
./scripts/verify_metrics_endpoint_probe_report.ps1 -Path reports/metrics_endpoint_probe_<timestamp>.json
./scripts/run_p0_evidence_suite.ps1 -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json
```

Boundary:
- Standalone delivery audit still supports latest-report fallback for manual review convenience.
- Full-suite delivery review disables fallback to avoid accidentally reusing stale historical evidence.

## 2026-06-21 P0-E timed rollback evidence parameter update

The full-chain drill report can now carry timed rollback evidence from a real drill.

Updated tooling:
- `scripts/full_chain_drill_smoke.ps1`
- `scripts/verify_full_chain_drill_report.ps1`
- `scripts/delivery_readiness_audit.ps1`

New optional parameters:

```powershell
./scripts/full_chain_drill_smoke.ps1 `
  -P0BReport reports/crash_recovery_drill_<timestamp>.md `
  -P0CReport reports/concurrency_validation_<timestamp>.md `
  -P0DReport reports/observability_smoke_<timestamp>.md `
  -RollbackWindowSeconds 300 `
  -TimedRollbackStatus PASS `
  -RollbackDurationSeconds 120 `
  -RecoveryPoint "backup-or-lsn-reference" `
  -ReplayBoundary "last-replayed-log-or-txn-boundary"
```

Behavior:
- Without timed parameters, reports include `timed_rollback_evidence.status = NOT_PROVIDED`.
- With timed parameters, reports include rollback duration, rollback window, `within_window`, recovery point, and replay boundary.
- The full-chain verifier validates the optional structure.
- The delivery readiness audit requires timed rollback evidence to be `PASS`, within window, and include both recovery point and replay boundary.

Boundary:
- The script still does not execute a real release or rollback by itself.
- Operators must supply timed evidence from an actual drill for P0-E delivery acceptance.

## 2026-06-21 Full suite P0-E timed rollback parameter pass-through

The full P0 evidence suite now accepts P0-E timed rollback evidence parameters and passes them to `full_chain_drill_smoke.ps1`.

Updated tooling:
- `scripts/run_p0_evidence_suite.ps1`

New full-suite parameters:

```powershell
./scripts/run_p0_evidence_suite.ps1 `
  -P0DMetricsEndpointJson reports/metrics_endpoint_probe_<timestamp>.json `
  -TimedRollbackStatus PASS `
  -RollbackDurationSeconds 120 `
  -RecoveryPoint "backup-or-lsn-reference" `
  -ReplayBoundary "last-replayed-log-or-txn-boundary"
```

Behavior:
- If timed rollback parameters are omitted, the generated P0-E report remains smoke-only with `timed_rollback_evidence.status = NOT_PROVIDED`.
- If timed rollback parameters are supplied from a real drill, the P0-E report carries rollback duration, recovery point, replay boundary, and window status into the delivery audit.
- Delivery remains `NOT_READY` unless the P0-E timed rollback evidence is `PASS`, within window, and includes recovery point plus replay boundary.

## Accepted delivery deferrals

Delivery readiness now supports explicit accepted deferrals for residual P0 gaps. A failed audit check can only become `DEFERRED` when an accepted deferrals JSON file names the exact check and includes `owner`, `decision: ACCEPTED`, `expires_at`, and `rationale`. Deferred checks remain visible in the audit output and are not counted as `PASS`; they represent time-boxed owner acceptance, not technical completion.

Template: `docs/planning/P0_ACCEPTED_DEFERRALS_TEMPLATE.json`
Suite parameter: `-AcceptedDeferralsJson <path>`


## P0-B state diff artifact path

`crash_recovery_drill.ps1` now accepts external state-diff artifacts with `-RowStateDiffJson`, `-PageStateDiffJson`, and `-WalReplayDiffJson`. Row/page recovery checks stay `NOT_VERIFIED` when no artifact is supplied, become `FAIL` when an artifact is missing or malformed, and become `PASS` only when the command replay passes and the supplied artifact has `status: PASS`.

Template: `docs/planning/P0_STATE_DIFF_ARTIFACT_TEMPLATE.json`
Suite parameters: `-P0BRowStateDiffJson <path> -P0BPageStateDiffJson <path> -P0BWalReplayDiffJson <path>`


## Generating concrete P0-B state diff artifacts

Use `scripts/generate_p0b_state_diff_artifact.ps1` to convert expected/actual JSON snapshots into a delivery-grade state diff artifact. The generator performs semantic JSON comparison with stable key ordering and path-level mismatch counts.

Example row diff:

```powershell
./scripts/generate_p0b_state_diff_artifact.ps1 `
  -Scope row_state_diff `
  -ExpectedSnapshotJson reports/expected_rows.json `
  -ActualSnapshotJson reports/recovered_rows.json `
  -Scenario redo_undo_crash_recovery
```

Example verifier:

```powershell
./scripts/verify_p0b_state_diff_artifact.ps1 -Path reports/p0b_state_diff_row_state_diff_<timestamp>.json
```

Feed passing artifacts into the recovery drill:

```powershell
./scripts/crash_recovery_drill.ps1 `
  -Runs 3 `
  -StateEvidence `
  -RowStateDiffJson reports/p0b_state_diff_row_state_diff_<timestamp>.json `
  -PageStateDiffJson reports/p0b_state_diff_page_state_diff_<timestamp>.json `
  -WalReplayDiffJson reports/p0b_state_diff_wal_replay_diff_<timestamp>.json
```

P0-B delivery acceptance still requires the generated recovery state evidence to show row/page checks as `PASS`; missing artifacts remain `NOT_VERIFIED`, and malformed or failing artifacts remain `FAIL`.


## Focused P0-B snapshot evidence generator

`server/innodb/manager/p0b_state_snapshot_export_test.go` provides an environment-controlled snapshot exporter for focused recovery evidence. It only writes snapshots when `P0B_STATE_SNAPSHOT_DIR` is set. The wrapper script runs that exporter, builds row/page/WAL diff artifacts, and verifies each artifact:

```powershell
./scripts/generate_p0b_state_snapshot_evidence.ps1 -ReportDir reports
```

The generated report includes:

- `row_state_diff_json`
- `page_state_diff_json`
- `wal_replay_diff_json`

Use those paths as inputs to the crash recovery drill:

```powershell
./scripts/crash_recovery_drill.ps1 `
  -Runs 3 `
  -StateEvidence `
  -RowStateDiffJson <row_state_diff_json> `
  -PageStateDiffJson <page_state_diff_json> `
  -WalReplayDiffJson <wal_replay_diff_json>
```

Scope boundary: this is focused mock-buffer recovery evidence for deterministic redo replay and idempotent LSN behavior. It improves the P0-B evidence chain, but it does not replace a full disk-format crash/restart drill.


## Focused P0-C consistency evidence

`server/innodb/manager/p0c_consistency_evidence_export_test.go` provides an environment-controlled focused consistency exporter. It writes evidence only when `P0C_CONSISTENCY_EVIDENCE_DIR` is set. The wrapper script runs that exporter and validates the resulting evidence:

```powershell
./scripts/generate_p0c_consistency_evidence.ps1 -ReportDir reports
```

The generated evidence covers the delivery-gated P0-C consistency gaps:

- `lock_wait_metrics_summary`
- `full_isolation_matrix`
- `long_transaction_runtime_metrics`
- `explicit_final_state_diff`
- `wrapper_state_snapshot_diff`
- `deadlock_victim_report`

Feed the evidence into concurrency validation:

```powershell
./scripts/concurrency_validation.ps1 -P0CConsistencyEvidenceJson reports/p0c_consistency_evidence_<timestamp>.json
```

The full P0 evidence suite can generate and pass this evidence automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0CConsistencyEvidence
```

Scope boundary: this is focused in-process consistency evidence. It improves the P0-C gate from `NOT_VERIFIED` gaps to explicit evidence-backed checks, but it does not replace a full external multi-client SQL workload or history-linearizability checker.


## Focused P0-E timed rollback evidence

`generate_p0e_timed_rollback_evidence.ps1` creates a validated timed rollback evidence contract with rollback duration, rollback window, recovery point, replay boundary, and step-level evidence:

```powershell
./scripts/generate_p0e_timed_rollback_evidence.ps1 -ReportDir reports -RollbackWindowSeconds 300
./scripts/verify_p0e_timed_rollback_evidence.ps1 -Path reports/p0e_timed_rollback_evidence_<timestamp>.json
```

The full P0 evidence suite can generate and pass this evidence automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0ETimedRollbackEvidence
```

When a timed rollback evidence JSON is supplied or generated, the suite maps it into the full-chain drill fields: `TimedRollbackStatus`, `RollbackDurationSeconds`, `RecoveryPoint`, and `ReplayBoundary`.

Scope boundary: this is focused rollback evidence for the delivery contract. It proves the rollback evidence fields and timing window are present and internally consistent, but it does not execute an external deployment platform rollback, production traffic shift, or real data restore by itself.


## Focused P0-D live metrics endpoint evidence

`cmd/p0_metrics_endpoint` exposes the P0 metric catalog over HTTP for focused endpoint probing. The wrapper script starts the endpoint, waits for `/healthz`, runs the existing `/metrics` probe, verifies the probe report, and stops the endpoint:

```powershell
./scripts/generate_p0d_metrics_endpoint_evidence.ps1 -ReportDir reports
```

The delivery audit consumes the generated `metrics_endpoint_probe_<timestamp>.json` as `-P0DMetricsEndpointJson`.

The full P0 evidence suite can generate this endpoint probe automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0DMetricsEndpointEvidence
```

Scope boundary: this focused endpoint proves `/metrics` reachability and required P0 metric-name presence for a local metrics endpoint. It does not prove the full XMySQL server process updates every metric under production traffic.


## P0 delivery candidate runner

`run_p0_delivery_candidate.ps1` is the single-command delivery-candidate entry point. It invokes the full P0 evidence suite with focused evidence generation enabled for P0-B, P0-C, P0-D, and P0-E by default:

```powershell
./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports
```

Default focused evidence enabled by this runner:

- P0-B focused state snapshot evidence
- P0-C focused consistency evidence
- P0-D focused metrics endpoint evidence
- P0-E focused timed rollback evidence

Optional switches can disable individual focused evidence generators when external production-grade artifacts are supplied instead:

```powershell
./scripts/run_p0_delivery_candidate.ps1 `
  -DisableFocusedP0BStateSnapshotEvidence `
  -DisableFocusedP0CConsistencyEvidence `
  -DisableFocusedP0DMetricsEndpointEvidence `
  -DisableFocusedP0ETimedRollbackEvidence
```

Scope boundary: this runner does not relax any delivery readiness gates. It only reduces operator error by collecting the focused evidence defaults into one command. A candidate is deliverable only if the underlying suite, verifiers, approval packet, evidence bundle, and delivery readiness audit all pass.

## 2026-06-21 focused delivery candidate checklist update

The P0 delivery candidate runner is now the preferred single-command path for generating reviewable evidence:

```powershell
./scripts/run_p0_delivery_candidate.ps1 -ReportDir reports
```

The candidate runner enables focused P0-B/C/D/E evidence by default and routes it into:

- P0 evidence bundle
- P0 release approval packet
- P0 delivery readiness audit
- P0 delivery remediation packet
- P0 evidence suite summary

Checklist interpretation:

- A `PASS` candidate run is the strongest local evidence that this repository is ready for delivery review.
- A `FAIL` candidate run is still useful because it should emit delivery readiness and remediation artifacts that identify exact owner-trackable blockers.
- Production approval still requires owner sign-off, accepted or closed risks, and no unresolved P0 delivery blockers.
