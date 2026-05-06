# XMySQL Server P0 上线任务分解（Tasks）

## 1. 使用说明

- 本文档是 [P0_PRODUCTION_DEPLOYMENT_PLAN.md](file:///Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md) 的执行拆解版
- 每项任务都包含：目标、涉及路径、完成定义、验证方式
- 建议执行顺序：A1 -> A2 -> B -> C -> D -> E

## 2. A1：核心高风险文件整改（实现侧）

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`部分实现`
- 说明：
  - Top 12 文件未清零
  - 关键路径仍存在 `TODO`、错误文本判定、旧/新实现并存
  - `go test ./server/innodb/engine` 当前未满足验收基线

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
  - `部分实现`
  - `store/pages/page.go` 旧接口仅标记 deprecated，未真正移除
  - `wrapper/types/base_page.go` 与 `wrapper/page/page_wrapper_base.go` 并存
  - 验收缺口：主入口未冻结，调用方未全部切换，缺迁移说明

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
  - 仍保留错误消息正则匹配 fallback
  - 验收缺口：尚未完全达到“纯结构化错误判定”

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
  - `未实现`
  - 3 个目标文件仍存在多处 `time.Sleep`
  - 验收缺口：未改为条件等待，未见 10 轮稳定性记录

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
  - `未实现`
  - `rollback_page_wrapper.go` 仍有 rollback page 解析 TODO
  - `page_wrapper_base.go` 仍缺状态、pin count、stats、read、flush 等关键逻辑
  - 验收缺口：核心解析路径和异常输入校验未闭环

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
- 完成定义：
  - 覆盖 redo/undo/半提交事务场景
  - 每个场景可重复执行
- 验证：
  - 脚本全场景通过
  - 校验结果一致
- 当前状态：
  - `未实现`
  - 仓库当前无 `scripts/` 目录
  - 验收缺口：未发现 redo/undo/半提交事务的自动化恢复脚本与模板

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
  - 当前存在实现总结文档，但未发现正式的 P0 恢复演练报告、结果快照和多轮记录

## 5. C：并发正确性验证

### 当前状态（2026-03-21 仓库核查）

- 整体状态：`未实现`
- 说明：
  - 当前只有零散并发测试与 demo
  - 缺少并发压测脚本、一致性校验脚本、验证报告

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
  - `未实现`
  - 验收缺口：未发现覆盖冲突写、范围读、长事务的正式脚本和报告

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

- 整体状态：`未实现`
- 说明：
  - `docs/planning/` 中尚无灰度发布手册、快速回退手册、数据回滚手册和演练记录

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
  - `未实现`

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
  - `未实现`
  - 仓库当前无备份恢复脚本目录

### T-E-03 完成一次全链路演练
- 目标：验证从灰度到回退的全链路闭环
- 涉及路径：
  - `docs/reports/`
- 完成定义：
  - 演练记录、问题清单、改进行动项完整
- 验证：
  - 评审通过，准入生产灰度
- 当前状态：
  - `未实现`
  - 未发现演练记录、复盘和计时证据

## 8. 统一验证命令（建议基线）

- `go test ./server/dispatcher ./server/innodb/engine`
- `go test ./...`
- 关键并发/恢复脚本按报告模板执行并归档
