# XMySQL Server Production Gap List

> **最后更新**: 2026-04-16
> **结论**: 当前仓库 **不具备进入生产灰度的条件**
> **定位**: 本文档是“生产就绪差距”的执行清单，面向单机部署与灰度上线，不覆盖复制/分布式等长期目标
> **配套执行表**: [PRODUCTION_GAP_EXECUTION_TABLE.md](./PRODUCTION_GAP_EXECUTION_TABLE.md)

---

## 1. 目的

本文档用于把“距离生产级还有多远”整理成一份可执行的 Gap List，方便用于：

- 生产灰度前的阻塞项识别
- Sprint 排期与责任分配
- 验收前证据归档
- 对齐现有 P0 / 路线图文档

本清单以以下材料为主要依据：

- `docs/planning/P0_PRODUCTION_TASKS.md`
- `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`
- `docs/未实现功能梳理.md`
- 2026-04-16 对仓库代码、脚本、CI、测试基线的实查结果

---

## 2. 状态定义

- `P0 必补`: 不完成则不能进入生产灰度
- `P1 增强`: 不阻塞最小灰度，但达不到“真正生产级”的预期
- `P2 长期`: 长期能力建设项，通常对应更高兼容性、高可用或性能工程

---

## 3. 当前判断摘要

### 当前正向信号

- `server/net -> protocol -> dispatcher -> engine` 正式启动链路已形成
- 引擎启动已包含 crash recovery 与 checkpoint manager
- 仓库中已有部分恢复、事务、锁、B+ 树、执行器能力
- `go test ./server/dispatcher` 可通过
- 已出现恢复演练脚本雏形：`scripts/crash_recovery_drill.sh`

### 当前阻塞信号

- 全局测试基线未恢复，`go test` 非全绿
- `server/innodb/engine`、`server/innodb/sqlparser`、`server/auth` 等存在实测失败
- 生产所需的恢复演练、并发验证、监控告警、灰度回退证据未形成闭环
- 默认安全姿态仍偏开发联调而非生产默认

---

## 4. P0 必补 Gap List

### GAP-01 发布基线不稳定

- 修复正式发布包集的编译/测试基线
- 当前信号：
  - `go test ./server/innodb/engine` 构建失败
  - `go test` 过程中可见 `sqlparser`、`auth`、`root package` 等多处失败或告警
  - 当前仓库尚不具备“每次改动都有统一回归门槛”的条件
- 退出标准：
  - 至少正式发布包集测试全绿
  - 最优标准为 `go test ./...` 全绿
  - CI 使用的 Go 版本、依赖与本地基线一致
- 证据入口：
  - `docs/planning/P0_PRODUCTION_CHECKLIST.md`
  - `.github/workflows/go.yml`
  - `go.mod`

### GAP-02 崩溃恢复缺少真实演练闭环

- 将恢复验证从“单测/脚本聚合”提升到“真实异常退出 -> 重启 -> 校验”的闭环
- 当前信号：
  - 已有 `CrashRecovery` 相关实现与测试
  - 已有 `scripts/crash_recovery_drill.sh`
  - `crash_recovery_drill.sh` 已升级为按 `redo / undo / 半提交（故障注入）` 分组执行，并生成分组日志与汇总报告（`reports/crash_recovery_drill_*/`）
  - 新增 `scripts/crash_recovery_process_drill.sh`（真实进程 `start -> crash -> restart -> verify` 演练框架）；最新样例显示 `undo/half_commit` 组通过但 `verify redo` 仍失败，说明恢复闭环已具备执行骨架但尚未达标
  - 但当前仍以测试驱动演练为主，尚未形成完整的 `kill/restart/recover/data-check` 真实进程闭环
- 退出标准：
  - redo、undo、半提交事务至少三类场景可自动演练
  - 每类场景均有恢复前输入、恢复后校验、差异输出和多轮记录
  - 能形成可审计报告
- 证据入口：
  - `docs/planning/P0_PRODUCTION_TASKS.md`
  - `docs/transaction-reports/CRASH_RECOVERY_IMPLEMENTATION_SUMMARY.md`
  - `scripts/crash_recovery_drill.sh`

### GAP-03 并发正确性证据不足

- 建立正式的并发压测与一致性校验流程
- 当前信号：
  - 仓库中存在锁、MVCC、Gap / Next-Key Lock 等能力与零散测试
  - 但缺冲突写、范围读、长事务的正式压测脚本与结果报告
  - 缺锁等待/死锁统计与压测后数据一致性校验
- 退出标准：
  - 有可重复执行的并发测试工具或脚本
  - 至少覆盖冲突写、范围读、长事务三类场景
  - 产出一致性校验报告，无明显脏读/丢写/重复写
- 证据入口：
  - `docs/planning/P0_PRODUCTION_CHECKLIST.md`
  - `docs/mvcc-reports/`
  - `docs/transaction-reports/`

### GAP-04 可观测性未形成生产系统

- 完成日志、指标、告警的最小生产闭环
- 当前信号：
  - 已有少量内部 stats / alert 结构
  - `slow_query_log` 仅见系统变量定义，未见完整落地证据
  - 未见 Prometheus/exporter、面板、告警规则与告警演练证据
- 退出标准：
  - 错误日志字段规范清晰且可追踪模块/SQL/耗时/错误
  - 慢查询日志可实际输出
  - 至少可观测 QPS、P50/P95/P99、错误率、连接数、事务数
  - 告警规则可触发并有演练记录
- 证据入口：
  - `docs/planning/P0_PRODUCTION_TASKS.md`
  - `docs/planning/P0_PRODUCTION_CHECKLIST.md`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/innodb/manager/transaction_manager.go`

### GAP-05 灰度发布与回退体系缺失

- 补齐灰度、快速回退、数据回滚与全链路演练
- 当前信号：
  - 已落地灰度/回退手册，且有备份恢复脚本与一次 dryrun 证据
  - 仍缺完整全链路演练记录和回退计时证据
- 退出标准：
  - 有灰度发布手册
  - 有快速回退手册
  - 有数据回滚/恢复点策略
  - 有一次完整演练与复盘
- 证据入口：
  - `docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md`
  - `docs/planning/P0_PRODUCTION_CHECKLIST.md`

### GAP-06 默认安全姿态不适合生产

- 将默认配置从“开发联调友好”切换为“生产默认安全”
- 当前信号：
  - `DevBypassPasswordAuth` 默认值已改为 `false`
  - 已通过启动期校验阻断非本地监听 + `dev_bypass_password_auth=true` 场景
  - 对应审计证据已归档：`reports/p0_gap06/p0_gap06_20260516_231850/gap06_audit_report.md`
- 退出标准：
  - 生产配置默认关闭免密
  - 认证失败路径和权限校验经回归验证
  - 文档中明确区分开发默认与生产默认
- 证据入口：
  - `server/conf/config.go`
  - `conf/default.ini`
  - `docs/planning/GAP_06_SECURITY_DEFAULT_POSTURE.md`

### GAP-07 核心高风险实现未完全收口

- 清理高风险实现中的 fallback 掩错、重复入口、flaky 测试和未完包装器逻辑
- 当前信号：
  - 现有 P0 文档仍将其列为工作流 A 的主要缺口
  - 说明当前正确性风险更多来自“关键路径还不够干净”，而不是单纯功能缺失
- 退出标准：
  - 高风险 Top 文件问题清零
  - 关键路径只有单入口
  - 失败路径返回结构化错误
  - flaky 测试去除固定 sleep 依赖
- 证据入口：
  - `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`
  - `docs/planning/P0_PRODUCTION_TASKS.md`

---

## 5. P1 生产增强 Gap List

### GAP-08 认证与 TLS 仍偏开发态

- 完整收口认证插件、TLS 配置、证书管理和默认策略
- 当前信号：
  - `caching_sha2_password` 仍为简化实现
  - TLS 构建器存在，但距离生产级策略和证书治理还有距离
- 退出标准：
  - 主流认证插件行为明确
  - TLS 默认策略清晰
  - 证书管理、文档和验证流程完整

### GAP-09 备份与恢复体系缺失

- 建立可验证的备份/恢复能力
- 当前信号：
  - 仓库中未见成熟的备份恢复手册与周期性恢复演练
- 退出标准：
  - 至少有可执行的逻辑备份/恢复流程
  - 更优标准为定期恢复演练

### GAP-10 SQL 正确性与兼容性仍有缺口

- 明确支持矩阵并补齐关键 SQL 能力
- 当前信号：
  - 子查询、CTE、窗口函数、部分 DDL/DML 仍未完整实现
- 退出标准：
  - 有明确“支持/不支持”矩阵
  - JDBC / 兼容性关键路径有回归测试

### GAP-11 优化器仍偏启发式

- 完善统计信息、连接顺序、代价估算等优化器核心能力
- 当前信号：
  - 当前更接近规则型 + 局部启发式
  - 复杂查询的计划稳定性仍待验证
- 退出标准：
  - 统计信息维护可用
  - 复杂查询计划质量稳定
  - 有查询性能基线与回归样例

---

## 6. P2 长期能力 Gap List

### GAP-12 高可用能力缺失

- 建设复制、binlog、GTID、故障切换等高可用基础
- 当前信号：
  - 当前定位仍偏单机内核
  - 复制/binlog/GTID 相关能力未形成完整产品能力
- 退出标准：
  - 如果目标超出单机灰度，需要完整复制与切换方案

### GAP-13 交付工程化不足

- 建立更严格的 CI/CD、版本矩阵、发布工件和环境一致性约束
- 当前信号：
  - 现有 CI 较轻
  - workflow 中 Go 版本与 `go.mod` 基线存在漂移
- 退出标准：
  - CI/CD、发布工件、版本矩阵、回归门槛固定

---

## 7. 最短上线路径

如果目标不是“做完整生产数据库”，而是“先进入有限业务灰度”，建议优先只做以下顺序：

1. `GAP-01` 修复测试和发布基线
2. `GAP-07` 收口高风险实现
3. `GAP-02` 建立真实恢复演练
4. `GAP-03` 建立并发一致性验证
5. `GAP-04` 接通日志、指标、告警
6. `GAP-05` 完成灰度与回退手册并演练
7. `GAP-06` 收紧默认安全配置

---

## 8. 进入生产灰度前的最小核对清单

- 正式发布包集测试全绿
- 恢复演练至少覆盖 redo / undo / 半提交事务
- 并发压测和一致性校验完成
- 错误日志、慢查询日志、核心指标、告警可用
- 灰度发布、快速回退、数据回滚文档齐备
- 至少完成一次全链路演练并形成复盘
- 生产默认配置关闭免密或其他开发快捷路径

---

## 9. 关联文档

- `docs/planning/P0_PRODUCTION_TASKS.md`
- `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- `docs/planning/P0_PRODUCTION_GAP_ANALYSIS.md`
- `docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md`
- `docs/未实现功能梳理.md`
