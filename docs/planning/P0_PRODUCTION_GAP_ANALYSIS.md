# XMySQL Server P0 未实现项总表（Gap Analysis）

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
