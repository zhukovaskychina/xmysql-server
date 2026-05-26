# XMySQL Server P0 上线验收清单（Checklist）

## 0. 使用说明

- 勾选原则：必须有“证据链接或报告路径”
- 通过标准：P0 全部项必须为 `[x]`
- 对应计划：
  - [P0_PRODUCTION_DEPLOYMENT_PLAN.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md)
  - [P0_PRODUCTION_TASKS.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_TASKS.md)
- Stage1 基线建议执行入口：
  - [p0_stage1_baseline.sh](/Users/zhukovasky/GolandProjects/xmysql-server/scripts/p0_stage1_baseline.sh)
  - [P0_STAGE1_BASELINE_REPORT_TEMPLATE.md](/Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_STAGE1_BASELINE_REPORT_TEMPLATE.md)

### 当前状态（2026-03-21 仓库核查）

- 本清单当前不应勾选为通过态
- 当前可明确判断：
  - `go test ./server/dispatcher` 可通过
  - `go test ./server/innodb/engine` 已通过（真实执行）
  - Stage1 基线已完成（真实 PASS）：`reports/STAGE1_AUDIT_DEMO/stage1_baseline_20260516_220508/summary.log`
  - B/C/D/E 工作流未形成验收证据闭环
- 详细缺口总表见：
  - `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`

## 1. 核心高风险文件整改（A）

当前判断：`整体未通过`

- [ ] Top 12 文件 P0 问题清零（附问题关闭清单）
- [x] 先完成 `scripts/p0_stage1_baseline.sh` 的基线记录（附报告路径）
- [x] 记录路径示例：`reports/STAGE1_AUDIT_DEMO/stage1_baseline_20260516_220508/summary.log`
- [x] Stage1 基线报告在真实执行下 PASS（附报告路径）
- [ ] 关键路径 fallback 掩错逻辑已移除（附代码变更链接）
- [ ] `storage_integrated_dml_helper.go` 已移除 `rollbackStorageTransaction` 中 `strings.Contains` 文本兜底（证据：`server/innodb/engine/storage_integrated_dml_helper.go: rollbackStorageTransaction`）；T-A1-01 仍需清理 `executor.go` 与 `unified_executor.go` 文本判断
- [ ] 重复实现已收敛为单入口（附迁移说明，见 `docs/planning/P0_A1_02_PAGE_SINGLE_ENTRYPOINT_MIGRATION.md`，`server/innodb/storage/wrapper/page/page_factory.go`）
- [x] DML 唯一键冲突判定已结构化（附测试用例）
  - [x] 高风险测试去 flaky（附 10 轮稳定运行记录；可执行脚本 `scripts/p0_a2_01_stability.sh`）
    - 实际记录：`reports/p0_a2_01_stability_20260517_012720/summary.log`
- [x] `go test ./server/innodb/engine` 通过
- [ ] `go test ./server/dispatcher` 通过
- [x] GAP-06 生产默认安全姿态闭环：非本地监听下拒绝 `dev_bypass_password_auth=true`（附件：`docs/planning/GAP_06_SECURITY_DEFAULT_POSTURE.md`；审计脚本：`./scripts/p0_gap06_security_audit.sh`）
  - 证据：`reports/p0_gap06/p0_gap06_20260516_231850/gap06_audit_report.md`

说明：

- `go test ./server/dispatcher` 当前可作为已通过候选项
- `go test ./server/innodb/engine` 现阶段有真实运行验证（`reports/STAGE1_AUDIT_DEMO/stage1_baseline_20260516_220508/summary.log`）
- Top 12 中仍存在 TODO、错误文本判定、sleep 驱动测试和包装器未实现逻辑

## 2. 崩溃恢复验证（B）

当前判断：`整体未通过`

- [ ] 已实现异常退出恢复脚本（附脚本路径）
- [ ] redo 场景恢复通过（附报告）
- [ ] undo 场景恢复通过（附报告）
- [ ] 半提交事务恢复通过（附报告）
- [ ] 恢复结果一致性校验通过（附校验输出）
- [ ] 同场景重复回放结果一致（附多轮记录）

建议执行命令：

- `CR_PROC_REPORT_DIR=./reports B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

说明：

- 当前有恢复代码基础和相关测试
- 已有恢复脚本 `scripts/crash_recovery_process_drill.sh`
- 审计脚本已补充：`scripts/p0_b_recovery_audit.sh`
- 已有演练记录 `reports/crash_recovery_process_drill_20260427_160436/summary.log`
- 但当前未形成标准化报告模板与多轮复放记录

## 3. 并发正确性验证（C）

当前判断：`已通过`

- [x] 已建立并发压测脚本（附脚本路径）：`scripts/p0_c_concurrency_validation.sh`
- [x] 冲突写场景通过（附报告）
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- [x] 范围读场景通过（附报告）
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- [x] 长事务场景通过（附报告）
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- [x] 死锁/锁等待行为可控（附统计）
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- [x] 无明显脏读/丢写/重复写（附一致性校验）
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`

说明：

- 已形成 3 轮一致性闭环：`reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- 场景覆盖：conflict_write、range_read、long_tx、deadlock_wait

建议执行命令：

- `P0_C_ROUNDS=3 ./scripts/p0_c_concurrency_validation.sh`

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
- 首版对齐模板已补充：`docs/planning/P0_D_OBSERVABILITY_READINESS_TEMPLATE.md`

## 5. 回滚预案与灰度演练（E）

当前判断：`整体未通过`

- [x] 灰度发布手册完成（附文档路径）
- [x] 快速回退手册完成（附文档路径）
- [x] 数据回滚手册完成（附文档路径）
- [ ] 全链路演练完成（附演练记录）
  - 最新阶段 0 验证通过记录：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065410.md`
  - 阶段 1 写入校验已尝试但阻断：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065633_stage1_insert_fail.md`
    - 失败点：`table mysql.t1 not found in storage mapping`
    - 相关日志：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065633_server.log`
- [ ] 演练复盘完成并闭环问题（附复盘链接）
  - 已形成复盘与问题清单：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_063552.md`
- [ ] 可在预期窗口内完成回退（附计时记录）
  - 未触发回退，需在问题修复后再次演练并补齐时延指标

说明：

- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md` 已补齐灰度、回退、数据回滚执行清单
- `scripts/p0_e_backup_snapshot.sh` 已提供备份恢复点能力（含 manifest 与 list/restore）
- 全链路演练记录、复盘与计时证据已形成失败闭环版本，等待编译修复后补齐通过场景（E-03 待重演）

## 6. 统一发布前检查

当前判断：`整体未通过`

- [ ] `go test ./...` 通过
- [ ] IDE 诊断为 0
- [ ] 关键配置已版本化与备份
- [ ] 上线评审包已归档

说明：

- `go test ./...` 当前不可勾选
- Go 版本与 `go.mod` 已对齐（1.24）；仍需补齐 `go test ./...` 等真实执行证据
- 上线评审包依赖 B/C/D/E 的交付物，目前不具备归档条件

## 7. 审批信息

- 技术负责人：`[ ] 同意上线`
- 测试负责人：`[ ] 同意上线`
- 运维负责人：`[ ] 同意上线`
- 业务负责人：`[ ] 同意上线`

最终结论：`[ ] 允许进入生产灰度`

当前判断：

- `不可进入生产灰度`
