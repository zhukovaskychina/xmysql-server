# XMySQL Server 修改清单（执行版，2026-06-10）

> 目标：告诉你“还要改什么”，并给出每项下一步的落地动作。  
> 数据口径：基于当前仓库快照（`server/**/*.go` + `docs` 中的 P0/P1 任务文档）。

## 一、当前基线

- 近期已完成：
  - Go 工具链与发布包门禁：`scripts/p0_release_package_tests.sh` 已落地。
  - DML 回滚失败不会被直接吞掉，关键失败会返回错误（`server/innodb/engine/dml_operators.go`）。
  - `storage_adapter` 依赖/空值检查已补齐一部分。
- 按 `rg "TODO|FIXME|not implemented|未实现|未完成|stub" server --glob "*.go"` 统计：
  - 命中行数：`190`
  - 涉及文件：`83`
- 当前可进入“下一步改造”的重点：先补齐 P0，再补 P1。

## 二、P0：必须先改

### P0-03 关键执行器错误可见性（未闭环）

- 关键风险：失败路径有时只是返回错误文本，缺少统一上下文，定位耗时高。
- 涉及路径：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`（请复查 `rollbackStorageTransaction` 附近）
- 需要动作：
  - 统一失败上下文（模块、SQL、schema、table、txnID、错误码）。
  - 去掉文本判定兜底，改结构化错误判断。
  - 为存储失败、提交失败、回滚失败、唯一键冲突补测试（断言错误类别）。
- 验收命令：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML|Index|Storage|Transaction)' -count=1`

### P0-06 崩溃恢复与一致性证据（未闭环）

- 关键风险：现有恢复逻辑中仍有“分析/redo/undo 未完成”路径，演练闭环还不稳定。
- 涉及路径：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
- 需要动作：
  - 统一 3 类场景：redo、undo、半提交。
  - 3 轮以上复放，且每轮可比对恢复前后快照。
  - 演练报告输出恢复输入、输出、差异、耗时和回放指令。

### P0-07 灰度写入阶段阻塞修复（未闭环）

- 关键风险：阶段1 写入存在历史阻断：`table mysql.t1 not found in storage mapping`。
- 涉及路径：
  - `scripts/p0_e_backup_snapshot.sh`
  - `scripts/p0_e_backup_restore` 或相关恢复入口
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 需要动作：
  - 让阶段0/1/2 串成一条可复现链路。
  - 补齐失败时输出可定位的日志路径和阶段号。
  - 补齐一次全链路通过演练并归档。

### P0-08 慢查询日志（未闭环）

- 关键风险：`slow_query_log` 相关开关和字段未形成完整采集。
- 涉及路径：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 需要动作：
  - 输出标准字段：SQL、耗时、影响行数、连接 ID、事务ID、错误码/错误类型。
  - 支持阈值配置和关闭/开启策略。

### P0-09 指标与告警（未闭环）

- 关键风险：尚未有可直接接入生产告警链路的指标导出和触发演练。
- 涉及路径：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 需要动作：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99。
  - 按 checkpoint/redo/undo/锁等待补齐核心告警事件。
  - 做至少一条告警触发与恢复演练。

## 三、P1：下一阶段继续推进

- 子查询与复杂 SQL 能力（执行/计划完整度）
  - `server/innodb/engine/subquery_executor*.go`
  - `server/innodb/engine/cte_executor.go`
  - `server/innodb/plan/physical_plan.go`
- 窗口函数与窗口帧
  - `server/innodb/engine/window_function_executor.go`
- 代价估算与统计信息真实化
  - `server/innodb/plan/cost_estimator.go`
  - `server/innodb/plan/statistics_collector_helpers.go`
- 索引/页面高风险 TODO 仍较多（不阻塞本轮，但建议列入下一阶段）
  - `server/innodb/storage/wrapper/page/page_impl.go`
  - `server/innodb/storage/wrapper/page/*.go`
  - `server/innodb/storage/wrapper/mvcc/mvcc_page.go`

## 四、按优先级的执行顺序（建议）

1. 先把 `P0-03` 收口：错误路径改完并补齐失败类测试。
2. 同步补齐 `P0-06`：3类恢复场景 + 3轮复放证据。
3. 再打通 `P0-07` 的灰度阶段1（重点修掉存储映射阻断）。
4. 跟进 `P0-08`、`P0-09`，形成“能看见、能报警”的最小可观测闭环。
5. 把前 10 个 TODO 热点文件做计划化处理（见下文“待清理文件 Top10”）。

## 五、TODO 热点（快速入口）

以下文件是扫描里出现较多 TODO/FIXME 的高频点，适合后续清单化排期：

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/parallel.go`（8）
3. `server/innodb/engine/index_reading_test.go`（8）
4. `server/innodb/plan/physical_plan.go`（6）
5. `server/innodb/metadata/convert.go`（6）
6. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
7. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
8. `server/innodb/plan/statistics_collector_helpers.go`（5）
9. `server/innodb/manager/page.go`（5）
10. `server/innodb/manager/crash_recovery.go`（5）

> 这里的数字来自 `rg -n` 行命中数汇总（仅作清理优先级参考，不等于功能优先级）。

## 六、产出要求（每项至少）

- 代码改动
- 一个对应测试（失败路径优先）
- 一条可复现命令或脚本
- 一份结果路径（日志/报告路径）说明

