# XMySQL Server P0 上线任务分解（Tasks）

## 1. 使用说明

- 本文档是 [P0_PRODUCTION_DEPLOYMENT_PLAN.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md) 的执行拆解版
- 每项任务都包含：目标、涉及路径、完成定义、验证方式
- 建议执行顺序：A1 -> A2 -> B -> C -> D -> E

## 2. A1：核心高风险文件整改（实现侧）

### 当前状态（2026-05-17 仓库核查）

- 整体状态：`部分实现`
- 说明：
  - Top 12 文件未清零
  - 关键路径仍存在 `TODO`、错误文本判定、旧/新实现并存
  - `go test ./server/innodb/engine` 已通过（含基线脚本可复现）

### T-A1-01 去除错误掩盖型 fallback

- 目标：关键路径失败时返回明确错误，不再 silently fallback
- 涉及路径：
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
- 完成定义：
  - 错误路径有统一错误码或错误类型
  - 日志可定位模块、SQL、关键参数
- 验证：
  - 增加失败路径单测
  - 运行 `go test ./server/innodb/engine`
- 当前状态：
  - `部分实现`
  - `executor.go` 仍有 `recover` 包装和错误文本判定
  - `unified_executor.go` 关键查询路径仍有多个 TODO
  - `storage_adapter.go` 仍有 schema 相关 TODO
  - 验收缺口：错误类型统一、日志字段统一、失败路径单测未闭环

### T-A1-02 收敛重复实现为单入口

- 目标：同类功能仅保留一条生产路径
- 涉及路径：
  - `server/innodb/storage/store/pages/page.go`
  - `server/innodb/storage/wrapper/types/base_page.go`
  - `server/protocol/error_helper.go`
- 完成定义：
  - 重复代码迁移或删除
  - 调用方全部切换到统一入口
- 验证：
  - 重复 API 无新增调用点
  - 相关包测试通过
- 当前状态：
  - `进行中`
  - `page_factory.go` 已将 `FIL_PAGE_INODE` 与 `default` 分支统一到 `types.NewUnifiedPage`
  - 仍保留兼容入口（`wrapper/page/page_wrapper_base.go`、`wrapper/types/base_page.go`、`store/pages/page.go`）用于历史路径
  - 迁移说明补录：`docs/planning/P0_A1_02_PAGE_SINGLE_ENTRYPOINT_MIGRATION.md`

### T-A1-03 DML 唯一键冲突判定可靠化

- 目标：去除字符串 contains 式错误识别
- 涉及路径：
  - `server/innodb/engine/dml_operators.go`
- 完成定义：
  - 基于结构化错误判定冲突类型
  - 兼容已有语义不回归
- 验证：
  - 冲突/非冲突测试均通过
  - DML 回归通过
- 当前状态：
  - `部分实现`
  - 已支持 `errors.Is/errors.As` 与 SQL 错误码判断
  - 已清理索引错误路径中的文本兜底判定（`strings.Contains("duplicate key", ...)`）
  - 存在遗留场景仍需确认：若 `executor.go`/`unified_executor.go` 的 TODO 与兜底路径未闭环，仍需复核

## 3. A2：核心高风险文件整改（测试侧）

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`未完成`
- 说明：
  - 目标测试文件仍有多处固定 `time.Sleep`
  - 页面包装器关键路径仍存在未实现逻辑

### T-A2-01 消除 sleep 驱动的 flaky 测试

- 目标：改为条件等待或事件驱动断言
- 涉及路径：
  - `server/innodb/manager/btree_cache_limit_test.go`
  - `server/innodb/manager/space_expansion_concurrent_test.go`
  - `server/innodb/buffer_pool/prefetch_test.go`
- 完成定义：
  - 不依赖固定 sleep 时长判定
  - 多轮执行结果稳定
- 验证：
  - 重复运行同一测试不少于 10 次
  - 无随机失败
- 当前状态：
  - `已实现`
  - 已确认目标测试文件内不再直接依赖 `time.Sleep` 的固定等待（改用条件等待/事件驱动断言）
  - 已完成 `10` 轮稳定性连续通过记录
    - `reports/p0_a2_01_stability_20260517_012720/summary.log`
  - 可复用验证脚本：`scripts/p0_a2_01_stability.sh`

### T-A2-02 页面/回滚包装器关键路径补齐

- 目标：补齐最小可用解析与校验路径
- 涉及路径：
  - `server/innodb/storage/wrapper/page/page_wrapper_base.go`
  - `server/innodb/storage/wrapper/page/rollback_page_wrapper.go`
- 完成定义：
  - 核心解析路径可用
  - 异常输入明确报错
- 验证：
  - 对应单测与边界测试通过
- 当前状态：
  - `部分完成`
  - `rollback_page_wrapper.go` 与 `page_wrapper_base.go` 的关键 TODO 已补齐
  - 关键异常输入统一返回 `ErrInvalidRollbackData` 或 `ErrInvalidPageSize` 之后按包装器语义封装
  - 已补齐最小生产路径回归测试：
    - `server/innodb/storage/wrapper/page/page_wrapper_base_test.go`
    - `go test ./server/innodb/storage/wrapper/page -run "TestBasePageWrapperPinAndStats|TestRollbackPageWrapper" -count=1 -v`
  - 验收现状：`go test ./server/innodb/storage/wrapper/page -count=1` 通过

## 4. B：崩溃恢复验证

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`已有基础但未达标`
- 说明：
  - 已有崩溃恢复代码和测试基础
  - 缺少 P0 定义要求的自动化脚本、报告模板、演练记录和多轮回放证据

### T-B-01 构建崩溃恢复自动化脚本

- 目标：自动执行“异常退出 -> 重启恢复 -> 一致性校验”
- 涉及路径：
  - `scripts/`（新增恢复脚本）
  - `docs/reports/`（新增恢复报告模板）
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 完成定义：
  - 覆盖 redo/undo/半提交事务场景
  - 每个场景可重复执行
- 验证：
  - 脚本全场景通过
  - 校验结果一致
- 当前状态：
  - `部分实现`
  - 已有 `scripts/crash_recovery_process_drill.sh`
  - 已补 `scripts/crash_recovery_process_drill.sh` 与 `scripts/p0_b_recovery_audit.sh` 的参数化/复放检查能力
  - 已有演练报告：`reports/crash_recovery_process_drill_20260427_160436/summary.log`
  - 验收缺口：仍需演练固定脚本的 2+ 轮稳定结果与审计清单逐项对齐

### T-B-02 输出恢复演练报告

- 目标：形成可审计的演练证据
- 涉及路径：
  - `docs/reports/`
- 完成定义：
  - 包含输入、步骤、恢复结果、差异校验
- 验证：
  - 评审通过并可复现
- 当前状态：
  - `未实现`
  - 已新增 `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`，包含可复核输出字段
  - 验收缺口：仍缺多轮一致性归档与可复放的输入参数模板（下一步由 B 脚本输出覆盖）

## 5. C：并发正确性验证

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`已实现`
- 说明：
  - 已建立正式并发压测闭环，3 轮报告全部通过
  - 报告：`reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`

### T-C-01 构建并发压测与一致性校验

- 目标：验证高并发读写正确性
- 涉及路径：
  - `tests/` 或 `cmd/`（并发测试工具）
  - `docs/reports/`（并发验证报告）
- 完成定义：
  - 覆盖冲突写、范围读、长事务
  - 支持死锁、锁等待、吞吐统计
- 验证：
  - 压测后数据一致性校验通过
  - 无明显脏读/丢写
- 当前状态：
  - `已实现`
  - 验收：`scripts/p0_c_concurrency_validation.sh` + 3 轮一致性报告

## 6. D：基础可观测性上线

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`部分实现`
- 说明：
  - 仓库中存在少量内部 stats/alert 结构
  - 但未形成可用于生产验收的日志、指标导出、告警配置和演练证据

### T-D-01 统一错误日志与慢查询日志

- 目标：关键行为可追踪可定位
- 涉及路径：
  - `server/` 日志相关模块
- 完成定义：
  - 错误日志字段标准化
  - 慢查询日志完整输出 SQL、耗时、行数、trace
- 验证：
  - 样例请求可在日志完整追踪
- 当前状态：
  - `部分实现`
  - 已有普通执行日志与 `slow_query_log` 系统变量定义
  - 验收缺口：未发现错误字段规范文档、慢查询样例日志、trace 关联输出

### T-D-02 接入核心指标与告警

- 目标：可监控 QPS/延迟/错误率并告警
- 涉及路径：
  - `server/` 指标导出模块
  - `deploy/` 或 `docs/` 告警配置
- 完成定义：
  - QPS、P95/P99、错误率、连接/事务数可观测
  - 告警规则可触发并可恢复
- 验证：
  - 人工注入故障，告警触发符合预期
- 当前状态：
  - `部分实现`
  - 已有内部 `GetStats` / `CheckpointMonitor` / 长事务告警通道等基础结构
  - 验收缺口：未发现 Prometheus/exporter、面板输出、告警规则和演练截图

## 7. E：回滚预案与灰度演练

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`部分实现`
- 说明：
  - 已补齐灰度/回退手册与数据回滚脚本，待首次全链路演练与计时复盘

### T-E-01 制定灰度发布与回退手册

- 目标：故障可快速止损
- 涉及路径：
  - `docs/planning/`
- 完成定义：
  - 灰度阶段步骤明确
  - 版本回退、流量切回、配置回滚可执行
- 验证：
  - 预演可在限定窗口内完成
- 当前状态：
  - `部分实现`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md` 已落地可执行步骤（含阈值与归档路径）

### T-E-02 制定数据回滚与恢复点策略

- 目标：异常情况下数据可恢复
- 涉及路径：
  - `docs/planning/`
  - `scripts/`（备份恢复脚本）
- 完成定义：
  - 恢复点、回放边界、校验步骤明确
- 验证：
  - 演练后数据校验通过
- 当前状态：
  - `部分实现`
  - 已新增 `scripts/p0_e_backup_snapshot.sh`（支持 backup/list/restore 与 manifest）
  - 已完成一次 `backup/list/restore` 冒烟演练，证据见 `reports/p0_e_backups/p0_e_backup_restore_dryrun_20260517_063321.md`

### T-E-03 完成一次全链路演练

- 目标：验证从灰度到回退的全链路闭环
- 涉及路径：
  - `docs/planning/P0_E_CANARY_REHEARSAL_20260517.md`
  - `reports/`
- 完成定义：
  - 演练记录、问题清单、改进行动项完整
- 验证：
  - 评审通过，准入生产灰度
- 当前状态：
  - `进行中`
  - 已形成一次真实尝试并补齐 `T-E-03` 复盘骨架：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_063552.md`
  - 已完成阶段 0 验证：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065410.md`
  - 缺口：阶段 1 写入验证在 `INSERT` 阶段失败（`table mysql.t1 not found in storage mapping`），告警触发与回退计时仍待执行

## 8. 统一验证命令（建议基线）

- `go test ./server/dispatcher ./server/innodb/engine`
- `go test ./...`
- 关键并发/恢复脚本按报告模板执行并归档
