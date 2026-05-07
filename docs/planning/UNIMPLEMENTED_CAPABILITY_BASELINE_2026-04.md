# XMySQL 未实现能力基线盘点（2026-04）

## 1. 结论摘要

- 当前项目已具备单机内核主链路，但距离“可安全生产灰度”仍有明显差距。
- 能力缺口主要集中在三类：查询正确性边界、生产可运维闭环、默认安全与兼容性收口。
- 对外建议优先路径：先补 P0 正确性与安全，再补 P1 稳定性与可观测，最后推进 P2 功能增强。

## 2. 证据基线与权重

主要依据以下文档与代码证据（同类冲突时以较新文档和代码为准）：

- [README.md](/Users/zhukovasky/GolandProjects/xmysql-server/README.md)
- [docs/未实现功能梳理.md](/Users/zhukovasky/GolandProjects/xmysql-server/docs/未实现功能梳理.md)
- [docs/planning/PRODUCTION_GAP_LIST.md](/Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/PRODUCTION_GAP_LIST.md)
- [docs/analysis/MISSING_FEATURES_LIST.md](/Users/zhukovasky/GolandProjects/xmysql-server/docs/analysis/MISSING_FEATURES_LIST.md)（**2025-10-29** 逐项对比长文；文首已加 **2026-04** 与当前代码差异说明，**勿单独以各条「当前状态」为准**）
- [server/innodb/engine/executor.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/executor.go)
- [server/innodb/engine/show_executor.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/show_executor.go)
- [server/innodb/engine/unified_executor.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/unified_executor.go)
- [server/innodb/sqlparser/ast.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/sqlparser/ast.go)
- [server/innodb/plan/parallel.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/parallel.go)
- [server/innodb/storage/store/mvcc/trx_sys.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/storage/store/mvcc/trx_sys.go)
- [server/innodb/storage/wrapper/page/compression_manager.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/storage/wrapper/page/compression_manager.go)
- [server/conf/config.go](/Users/zhukovasky/GolandProjects/xmysql-server/server/conf/config.go)

## 3. 能力现状矩阵（已实现 / 部分实现 / 未实现）

### 3.1 分域矩阵


| 能力域        | 状态      | 关键证据                                                                                | 用户影响            |
| ---------- | ------- | ----------------------------------------------------------------------------------- | --------------- |
| SQL 解析与语义  | 部分实现    | `docs/未实现功能梳理.md`（存储过程/触发器/分区/视图未实现）；`sqlparser/ast.go` 存在 `panic("unimplemented")` | 特定 SQL 直接失败或崩溃  |
| 查询执行器      | 部分实现    | `EXEC-001/008/009/011/012` 未实现或部分；`unified_executor.go` 有 WHERE 过滤 TODO             | 查询结果正确性与功能完备性不足 |
| SHOW/元数据查询 | 部分实现    | `executor.go` 中 `SHOW statements not implemented yet`；`show_executor.go` 以硬编码数据返回   | 元数据查询结果不可信      |
| 查询优化器      | 部分实现    | 文档标注子查询优化/CBO Join 顺序/统计更新缺失；`plan/parallel.go` 多处 TODO                             | 复杂查询计划不稳定、性能波动  |
| 索引能力       | 部分实现    | 二级索引为“部分完成”；唯一索引/FULLTEXT/在线 DDL 未实现                                                | 索引功能与约束不完整      |
| 事务与 MVCC   | 部分实现    | Savepoint 已实现；`trx_sys.go` 标注 `GlobalTrxSys` 未实现；Undo/Purge 仍有收口项                   | 高并发与恢复边界风险      |
| 持久化与恢复     | 部分实现    | 有 CrashRecovery/Redo 测试积累；`PRODUCTION_GAP_LIST` 指出真实演练闭环缺失                          | 生产故障恢复可审计性不足    |
| 协议兼容       | 部分实现    | `COM_STMT_`* 基础路径已具备；批量、多结果集、binlog dump、完整认证插件未实现                                  | JDBC/客户端高级能力受限  |
| 安全与认证      | 部分实现    | `DevBypassPasswordAuth: true`（默认开发态）；TLS/认证仍在 P1 收口                                 | 生产默认安全姿态不足      |
| 可观测与运维     | 未实现或弱实现 | `PRODUCTION_GAP_LIST` 的监控、慢查询日志、告警闭环缺失                                              | 线上故障定位与风险控制不足   |


### 3.2 代码直接证据（节选）

- SHOW 主路径仍存在未实现分支：`server/innodb/engine/executor.go`
- SHOW 数据源仍为 TODO + 硬编码：`server/innodb/engine/show_executor.go`
- 解析器存在 `panic("unimplemented")`：`server/innodb/sqlparser/ast.go`
- 并行计划关键算子多处 TODO：`server/innodb/plan/parallel.go`
- MVCC 全局事务结构标注未实现：`server/innodb/storage/store/mvcc/trx_sys.go`
- 压缩能力存在直接未实现错误：`server/innodb/storage/wrapper/page/compression_manager.go`
- 默认免密为 true：`server/conf/config.go`

## 4. P0 / P1 / P2 缺口排序（按生产影响）

### 4.1 P0（不完成不能安全灰度）

1. 发布与测试基线稳定（GAP-01）
2. 崩溃恢复真实演练闭环（GAP-02）
3. 并发正确性证据闭环（GAP-03）
4. SHOW 真值化与元数据可信输出（正确性）
5. 解析器 `panic("unimplemented")` 降级为可控错误（稳定性）
6. DML 条件过滤算子收口（正确性）
7. 生产默认安全配置切换（GAP-06）

### 4.2 P1（不阻塞最小灰度，但达不到生产级）

1. 可观测性最小闭环（日志、指标、慢查询、告警）
2. 灰度发布与回退手册 + 演练（GAP-05）
3. 认证/TLS 完整收口（GAP-08）
4. 备份与恢复流程标准化（GAP-09）
5. 优化器统计与 Join 顺序能力增强（GAP-11）

### 4.3 P2（长期能力增强）

1. 子查询优化、窗口函数、CTE 深化
2. Index Merge/直方图/计划缓存等优化器增强
3. 压缩/加密/碎片整理/表空间增强
4. 复制、binlog、GTID、高可用体系

## 5. Top10 缺口修复路线（含验证标准）


| 排名  | 缺口                | 优先级 | 主要责任域     | 完成标准（DoD）                               | 建议验证                   |
| --- | ----------------- | --- | --------- | --------------------------------------- | ---------------------- |
| 1   | 发布包测试基线不稳定        | P0  | 工程/内核     | 发布包集测试全绿，CI 基线一致                        | `go test` 包集 + CI 记录   |
| 2   | 崩溃恢复缺少真实演练        | P0  | 事务/恢复     | kill/restart/recover/data-check 三类场景可重复 | 自动脚本+恢复报告              |
| 3   | 并发正确性证据不足         | P0  | 事务/锁/MVCC | 冲突写/范围读/长事务通过并产出一致性报告                   | 压测脚本+校验报告              |
| 4   | SHOW 返回非真实元数据     | P0  | 执行器/元数据   | SHOW 从真实 schema 读取并回归通过                 | SQL 回归 + JDBC 元数据测试    |
| 5   | DML WHERE 过滤路径未收口 | P0  | 执行器       | UPDATE/DELETE 条件路径完整且全量回归               | DML 功能与回归测试            |
| 6   | 解析器未实现分支会 panic   | P0  | SQL 解析    | 未支持语法统一返回结构化错误码，不可崩溃                    | Fuzz + 非法 SQL 回归       |
| 7   | 默认免密配置不适合生产       | P0  | 配置/认证     | 生产默认禁用免密，文档区分 dev/prod                  | 配置回归 + 认证回归            |
| 8   | 可观测性缺少闭环          | P1  | 运维/内核     | 慢查询、核心指标、告警链路可用                         | 观测演练与告警演练              |
| 9   | 灰度与回退体系缺失         | P1  | 发布/运维     | 灰度、回退、数据恢复手册齐备并演练                       | 演练记录与复盘                |
| 10  | 优化器统计/CBO 能力不足    | P1  | 优化器       | 统计信息与 Join 计划稳定性达标                      | Benchmark + EXPLAIN 对比 |


## 6. 上线前最小可行能力（Go/No-Go）

### Go 条件（全部满足）

- 发布包测试基线达标（至少正式发布包集全绿，最好 `go test ./...` 全绿）
- 恢复演练覆盖 redo / undo / 半提交事务，并有可审计报告
- 并发场景（冲突写/范围读/长事务）完成一致性校验
- SHOW、核心 DML 条件语义通过回归测试
- 生产默认安全配置生效（免密关闭、认证失败路径通过）
- 慢查询日志、核心指标与告警链路可用
- 灰度发布与快速回退至少完成一次全链路演练

### No-Go 条件（任一触发即禁止灰度）

- 出现解析器 panic 或关键 SQL 路径崩溃
- 恢复演练无法稳定复现或校验结果不一致
- 并发一致性出现脏读、丢写、重复写且无闭环修复
- 默认配置仍为开发态安全策略
- 缺少灰度回退手册或无演练证据

## 7. 推荐执行节奏（两阶段）

### 阶段 A（2~4 周，先“可安全灰度”）

- 收口 Top1~Top7（全部 P0）
- 每周固化一次基线回归与证据归档

### 阶段 B（4~8 周，补“生产级稳定”）

- 推进 Top8~Top10（P1）
- 建立持续性能与运维评审机制

