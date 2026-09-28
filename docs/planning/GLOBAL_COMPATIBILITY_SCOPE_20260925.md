# xmysql-server 全局 MySQL 兼容范围梳理

更新时间：2026-09-28

本文把已确认的兼容目标统一纳入一个全局任务边界。状态以源码、专项测试和
`reports/compatibility/scope-matrix-current-continuation1544.json` 为准；本轮最新增量证据见
`reports/compatibility/p2-show-binary-log-status-current-continuation1545.txt`；仅有表名、
列名或单元测试通过，不代表已经达到 MySQL 完整语义。

## 1. 范围决策

| 优先级 | 范围 | 当前状态 | 结论 |
|---|---|---|---|
| P0 | MySQL 启动、协议、核心 CRUD、集群复制/故障切换、Connector/J | implemented | 已达到当前 P0 gate，Connector/J 与原有 P0 同级 |
| P1-A | INFORMATION_SCHEMA 全量表/列形状、过滤、权限可见性、运行时数据 | partial | 纳入全局任务，继续补齐，不以“可查询”作为完成条件 |
| P1-A | PERFORMANCE_SCHEMA 全量表/列形状、instrument/consumer、事件生命周期、运行时统计 | partial | 纳入全局任务，继续补齐；组件不存在时必须有明确兼容语义 |
| P2 | XA、原生 binlog、GTID、relay/applied state、崩溃恢复、提升/切换 | partial | 纳入全局任务，继续收敛持久化提交边界和恢复互操作 |
| P3 | 非 Connector/J 客户端完整矩阵：mysql CLI、Go、Python、Node.js 及集群端点场景 | implemented | 纳入全局任务；单端点、集群源端和晋升端四类客户端用例均已有 PASS 证据 |
| P4 | 官方 MySQL fixture 的 XA/binlog/GTID/复制/崩溃恢复互操作 | partial | 已验证隔离的 xmysql -> 官方 MySQL XA 应用；全量历史追平、反向 XA、崩溃恢复等仍需验收 |
| deferred | FULLTEXT | deferred | 按当前决定暂不进入本轮实现 |
| out of scope | MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎，以及非 InnoDB 专用 REPAIR/引擎转换 | out_of_scope | 明确不纳入本项目兼容目标 |

## 2. 已完成的 P0

- 服务可启动并执行真实查询/事务/DDL/DML 基础路径。
- 集群复制、故障切换和核心客户端连接 gate 已有专项证据。
- Connector/J 1.3.9 连接及 JDBC 测试作为 P0 gate，与启动和集群同级，不后置到 P3。

P0 的“implemented”只表示本项目定义的 gate 已通过；后续 P1/P2/P3/P4 仍可能暴露
更细的 MySQL 语义差异。

## 2.1 本轮范围确认

以下四项正式纳入同一个全局任务，不再拆成“以后再看”的独立遗留项：

- 完整 `INFORMATION_SCHEMA`：逐表列形状、字段精度、过滤、权限/角色可见性和有权威来源的运行时值；
- 完整 `PERFORMANCE_SCHEMA`：逐表列形状、instrument/consumer、事件生命周期、统计维度和组件语义；
- 非 Connector/J 客户端矩阵：`mysql` CLI、Go、PyMySQL、Node.js/mysql2，以及集群端点切换场景；
- XA 与原生 binlog/GTID/复制/崩溃恢复互操作：包括本地 xmysql-to-xmysql 收口和官方 MySQL 双向 fixture。

本轮明确不纳入：`FULLTEXT` 继续 deferred；MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎、
非 InnoDB 专用 `REPAIR TABLE` 和引擎转换继续 out of scope。

完成判定仍按“源码真实行为 + 针对可观察语义的回归 + 受影响包/全仓验证 + 外部项的当前
fixture 证据”执行；注册表有表名、返回空集或单次本地测试通过，都不能单独把全局条目改为
implemented。

## 2.2 本次范围梳理结论（2026-09-28）

1. 完整 `INFORMATION_SCHEMA` / `PERFORMANCE_SCHEMA` 继续纳入全局 P1。常用表、主要字段
   形状、部分运行时路径和权限切片已实现，但全量表/字段精度、组件生命周期、运行时统计
   以及全部权限/角色语义仍为 `partial`。
2. 非 Connector/J 客户端矩阵继续纳入全局 P3，当前状态为 `implemented`。已有证据覆盖
   Docker MySQL 8.4.11 CLI、Go mysql driver、PyMySQL、Node mysql2，并覆盖源端和晋升端；
   这表示矩阵 gate 已通过，不表示客户端范围从全局任务中删除。
3. XA 原生互操作继续纳入全局 P2/P4。本地 XA 状态机、native binlog 基线和官方 MySQL
   作为 source 的读取已有证据；官方双向 XA/binlog/GTID、重复投递、崩溃窗口、恢复后
   promotion/failover 仍未形成完整闭环，因此聚合项保持 `partial`。
4. 非 InnoDB 引擎不纳入本全局任务，状态为 `out_of_scope`。MyISAM、ARCHIVE、CSV、非
   InnoDB 专用 `REPAIR TABLE` 和引擎转换均不作为剩余开发项。

本次结论不把 Docker daemon、受保护密码或官方 fixture 的当前不可复跑条件误报为通过；
已有 PASS 证据与当前环境复跑条件分开记录。FULLTEXT 继续保持 `deferred`，不阻塞当前
启动、集群和 Connector/J 目标。

## 2.3 本轮增量（Continuation 1545）

- 补齐 MySQL 8.4 `SHOW BINARY LOG STATUS`，复用已有 native source 的文件位点和
  `Executed_Gtid_Set`，并保留 `SHOW MASTER STATUS`/`SHOW SOURCE STATUS` 兼容别名。
- 定向回归通过；这只是原生 binlog 状态 SQL 的一个局部收口，不改变 P2 聚合项的
  `partial` 状态。统一 storage/WAL/native-binlog/GTID 提交协议、官方双向 XA/binlog
  fixture、崩溃恢复和 promotion/failover 仍需继续完成。

## 3. P1：完整 INFORMATION_SCHEMA / PERFORMANCE_SCHEMA

### 3.1 INFORMATION_SCHEMA

以 MySQL 8.0 官方表参考为基线，必须逐表核对：

- 表是否存在、`SHOW COLUMNS`/`DESCRIBE` 的列名、顺序、类型、长度、可空性和默认值；
- `TABLES`、`COLUMNS`、`STATISTICS`、约束、分区、视图、触发器、存储过程/函数等
  元数据的真实投影；
- 角色与权限表的当前用户可见性、grantor provenance、角色继承和默认角色；
- InnoDB 诊断、buffer pool、metrics、tablespace、transaction/lock 视图的运行时数据；
- 对暂不具备对应运行时组件的表，给出可解释的空集、只读投影或明确错误，禁止伪造
  Group Replication、NDB、thread pool 等不存在的运行时状态。

当前已实现的是常用核心元数据、部分 InnoDB 诊断/metrics、角色授权表和 grantor
来源；本轮补齐了 NDB 专用连接映射表的注册和精确字段 metadata，但仍未完成的是
官方全表覆盖、所有字段精度、权限/连接生命周期和组件级运行时语义。

### 3.2 PERFORMANCE_SCHEMA

按官方族群逐项完成：

- setup：actors、consumers、instruments、objects、threads；
- instance：file、socket、mutex、rwlock、condition；
- wait/stage/statement/transaction history 与 summary；
- connection、connection attributes、user variables；
- replication、lock、system/status variables；
- instrument/consumer 的 ENABLED/TIMED、线程/全局 gate、TRUNCATE 隔离、历史代际；
- 事件的真实来源、时间/次数/字节统计、错误维度和可重置生命周期。

当前已实现的是语句/阶段/事务、等待/锁、线程/socket/file/memory 的主要运行时路径，
以及多类 summary/history/truncate 和部分 instrument 控制；仍为 partial 的原因是
完整表注册、字段精度、组件专属表和全部运行时维度尚未逐表完成。

## 4. P2：XA 与原生复制持久化边界

实现顺序固定为：

1. 统一 storage commit、WAL/redo flush、native binlog flush、GTID/applied marker、
   relay event 和 replica source position 的提交顺序及恢复判定；
2. 覆盖 XA PREPARE、COMMIT、ROLLBACK、重复 terminal event、失败重试和重启恢复；
3. 覆盖 position-based 与 GTID-based dump/apply、过滤、空事件推进、relay 重放和
   promotion/failover；
4. 用官方 MySQL fixture 验证双方互相作为 source/replica，以及 XA 与 binlog/GTID
   的交叉场景。

当前已经补齐多项本地故障窗口，包括 native relay、replication commit marker、
source position、HTTP pull position、prepared-XA 重试和恢复测试；仍未完成的是单一
物理提交协议，以及与官方 MySQL 的真实互操作证明。

官方 MySQL 参考明确要求 GTID execution history、binlog 同步和 XA prepare/terminal
状态在异常重启场景下保持一致，因此不能仅以“重启后本地数据存在”作为完成标准：

- [INFORMATION_SCHEMA Table Reference](https://dev.mysql.com/doc/refman/8.0/en/information-schema-table-reference.html)
- [Performance Schema Table Descriptions](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-table-descriptions.html)
- [GTID Format and Storage](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
- [Binary Logging Options and Variables](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html)
- [GTID Life Cycle](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-lifecycle.html)

## 5. P3：完整非 Connector/J 客户端矩阵

矩阵必须覆盖每个客户端的单节点和集群端点，并区分“能连上”和“行为兼容”：

- mysql CLI：握手、认证、TLS、命令行结果/错误码、事务、prepared statement、
  `SHOW`/`INFORMATION_SCHEMA`/`PERFORMANCE_SCHEMA`、binlog/replication 管理命令；
- Go MySQL driver：连接池、参数绑定、事务、multi-result、断线重连、读写切换；
- PyMySQL：同上，并覆盖 charset、cursor、异常和 server-side prepared 行为；
- Node.js mysql2：Promise/callback、prepared statement、流式结果、池和 failover；
- 集群端点：主节点切换、旧连接失败语义、新连接路由和重试边界。

当前 Go/Python/Node 以及 Docker 提供的 MySQL CLI 已有通过证据；但连接池、旧连接
失效、重试/TLS、管理命令和更广版本组合仍未完成，因此完整矩阵和集群端点的所有边界
仍是 partial。

## 6. 当前计数与验收门槛

当前矩阵：26 条记录，其中 24 条在范围内、2 条明确 out of scope；状态为：

- implemented：14
- partial：8
- unverified：0
- pending_external：0
- deferred：1
- out_of_scope：2

后续只有同时满足以下条件，才能把对应条目从 partial 改为 implemented：

1. 源码路径具备真实行为，不是固定空行或仅注册表名；
2. 有针对 MySQL 可观察语义的红/绿回归测试；
3. 有受影响包测试和必要的全仓串行回归；
4. 对外部互操作项，有当前 fixture、版本、命令、退出码和结果文件；
5. 矩阵中的 evidence path、状态和文档同步更新。

当前环境未发现 `mysqld.exe`/`mysql.exe`，Docker CLI 存在但 Linux daemon 不可用，
因此 P4 不是代码完成度结论，而是等待外部 fixture 的明确阻塞项。

## Continuation 1439

补齐 native XA 复制的官方生命周期事件：xmysql 现在在 XA PREPARE 的同一 GTID
组内发出 `XA START` 和 `XA END` QUERY_EVENT，再发出既有的
`XA_PREPARE_EVENT`；XID body 编码保持与官方 MySQL 实现一致。使用全新 MySQL
8.4.11 官方副本、从当前 binlog 位置隔离旧 relay 状态后，xmysql -> 官方副本的
XA 提交行已验证可见，`Replica_IO_Running=Yes`、`Replica_SQL_Running=Yes`，错误码为
0。证据见
`reports/compatibility/p2-p4-xa-lifecycle-official-replica-current-continuation1439.txt`。

该切片只关闭“孤立 XA 事务的 xmysql -> 官方 MySQL 应用”这一窄场景；旧历史 binlog
全量追平、官方 MySQL -> xmysql XA、崩溃后表映射恢复、GTID/applied marker 原子性、
提升/切换和重复投递仍保持 P2/P4 partial。P1 全量运行时语义、P3 更广泛客户端边界、
FULLTEXT 和非 InnoDB 范围边界不变。

## Continuation 1440

修复重启后的表存储映射恢复缺口：执行器持久化的是 `.frm` JSON 定义，但 schema
重启枚举此前只扫描 `.json`，导致表定义存在而 `SyncFromInfoSchema` 无法发现该表。
现在 `.json` 与 `.frm` 均会被加载，并对同名双格式去重。红测先复现 `.frm` 表恢复为
空，再在修复后通过；普通表、跨 schema 重命名和分区表重启测试均通过。证据见
`reports/compatibility/p2-table-mapping-restart-current-continuation1440.txt`。

该切片只关闭表定义发现这一恢复缺口；统一 storage/WAL/native-binlog/GTID/relay/
applied-marker 原子提交、全部 crash window、promotion 和官方双向 gate 仍为 P2/P4
partial。P1、P3、FULLTEXT 和非 InnoDB 范围边界不变。

## Continuation 1441

复跑了受影响的本地门禁：`TestInformationSchema|TestPerformanceSchema` 全部通过
（74.560 秒），复制包中 `TestReplica|TestNative|TestRuntime|TestSource` 全部通过
（6.419 秒）。本轮同时修正了两个落后的 XA native 事件序列断言，使测试契约包含
官方生命周期要求的 `XA START`/`XA END` QUERY_EVENT 及独立 terminal GTID 组；生产
writer 未为旧断言回退。证据见
`reports/compatibility/p2-local-gate-current-continuation1441.txt`。

这只确认本地 P1/P2 回归没有因最近的 XA 和表映射改动而回退，不把局部绿灯升级为
全局完成。P1 仍缺完整 I_S/P_S 运行时、权限、生命周期和组件语义；P2/P4 仍缺
完整 GTID/applied-marker 崩溃恢复、重复投递、promotion 和官方 MySQL 双向 fixture；
P3 仍缺完整非 Connector/J 客户端边界矩阵。`FULLTEXT` 继续 deferred；非 InnoDB
引擎、非 InnoDB 专用 `REPAIR TABLE` 和引擎转换继续 out of scope。

## Continuation 1370

重新执行了 P1/P2 受影响的 INFORMATION_SCHEMA、PERFORMANCE_SCHEMA、复制提交屏障、
XA/native-binlog 重试、promotion 和 auto-failover 定向测试，均通过。当前仍未把这些
结果误升级为全局完成：本机没有 `mysql.exe`/`mysqld`/`mysqlsh`，Docker Linux daemon
不可用，且保护认证凭据未提供，因此 P3 CLI/保护认证和 P4 官方 MySQL fixture 继续保持
`unverified`/`pending_external`。详细证据见
`reports/compatibility/local-compatibility-gate-current-continuation1370.txt`。

## Continuation 1371

补齐 MySQL NDB 专用的 `INFORMATION_SCHEMA.ndb_transid_mysql_connection_map`：
注册官方小写表/列名，`mysql_connection_id`、`node_id`、`ndb_transid` 分别返回
`BIGINT UNSIGNED`、`INT UNSIGNED`、`BIGINT UNSIGNED` 的非空 metadata。由于当前
xmysql 没有 NDB Cluster 运行时，查询返回空集而不伪造事务行。红测先确认原表不存在，
实现后专项测试和完整 INFORMATION_SCHEMA 回归均通过。证据见
`reports/compatibility/p1-ndb-connection-map-registry-current-continuation1371.txt`。

该切片只关闭 NDB 表注册/metadata 缺口；P1 聚合项仍为 partial，FULLTEXT 和非 InnoDB
边界不变。

## Continuation 1372

修正线程池三张表的官方列形状：`tp_thread_group_state` 23 列、
`tp_thread_group_stats` 22 列、`tp_thread_state` 6 列，同时同步
`INFORMATION_SCHEMA` 与 `PERFORMANCE_SCHEMA` 注册表。xmysql 没有 MySQL Enterprise
Thread Pool 运行时，因此只返回可发现的官方形状和空结果，不伪造线程池行。专项回归
验证两个 schema 的列顺序和空结果均通过。证据见
`reports/compatibility/p1-thread-pool-table-shapes-current-continuation1372.txt`；I_S/P_S
全族回归和 engine 串行全量回归也已通过。

该切片只关闭线程池注册表形状缺口；组件运行时和完整字段 metadata 仍使 P1 聚合项保持
partial，FULLTEXT 和非 InnoDB 边界不变。

## Continuation 1333

P2 新增可持久化的 `PURGE BINARY LOGS TO` 路径。source/runtime 会校验目标
binlog、删除目标之前的 native 文件、同步裁剪 GTID 物理位置索引，并保存保留文件
边界；重启或 native 文件重建时不会把已 purge 的文件从内部 logical stream 重新生成。
SQL 管理入口已接入 source-role/fencing 检查。证据见
`reports/compatibility/p2-purge-binary-logs-current-continuation1333.txt`。

该切片只关闭按文件名 purge 的本地管理缺口；统一 storage/WAL/native-binlog/GTID/
relay/applied-state 物理提交协议、`PURGE ... BEFORE` 时间语义以及官方 MySQL
互操作仍保持 P2/P4 未完成。P1 完整 I_S/P_S、P3 全量非 Connector-J 矩阵、FULLTEXT
和非 InnoDB 范围边界不变。

## Continuation 1334

P2 新增按时间戳执行的 `PURGE BINARY LOGS BEFORE` 及兼容别名
`PURGE MASTER LOGS BEFORE`。实现按逻辑事件时间选择跨越 cutoff 的首个物理文件，
复用 durable retention boundary、GTID 物理位置索引裁剪和 native rebuild 保护；
source/runtime 继续执行 source-role/fencing 校验。证据见
`reports/compatibility/p2-purge-binary-logs-before-current-continuation1334.txt`。

该切片仍不改变 P2 partial 状态：统一 storage/WAL/native-binlog/GTID/relay/
applied-state 提交协议和官方 MySQL fixture 互操作仍未完成；P1/P3、FULLTEXT 和
非 InnoDB 边界不变。

## Continuation 1335

P2 补齐崩溃窗口中的本地源端发布恢复：当存储已提交、但 binlog/GTID 发布失败时，
重启恢复会从普通客户端事务的 durable commit journal 读取稳定事务键和行镜像，
重新调用源端发布钩子，成功后写入 committed marker 并清理活动日志。`replication-`
前缀的副本回放日志不参与该流程，避免把副本事务反向发布到源端。证据见
`reports/compatibility/p2-replication-commit-recovery-current-continuation1335.txt`。

该切片仍不改变 P2 partial 状态：统一 storage/WAL/native-binlog/GTID/relay/
applied-state 提交协议、提升/切换的完整语义和官方 MySQL fixture 互操作仍未完成；
P1/P3、FULLTEXT 和非 InnoDB 边界不变。

## Continuation 1336

按 MySQL 8.4 官方 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表参考完成一次逐项
注册表审计：核心普通表、InnoDB 表和已有专用运行时投影已纳入；NDB、Clone、
Keyring、Firewall、Thread Pool、Scheduler、Group Replication 等组件表保留原生
可发现形状，但在组件不存在时不伪造运行时行。证据见
`reports/compatibility/p1-official-schema-registry-audit-current-continuation1336.txt`。

该审计不把“表名可发现”升级为 implemented。P1 仍需完成逐表字段精度、权限可见性
和实际生命周期/运行时语义；FULLTEXT 和非 InnoDB 范围边界不变。

## Continuation 1337

P2 的提交恢复日志现在同时保存稳定事务键、行镜像和原始 SQL statements。重启后
源端发布重试不再丢失 statement-based binlog 所需的 SQL；旧 row-only 日志继续兼容，
副本 `replication-*` 日志仍不会反向发布。证据见
`reports/compatibility/p2-replication-commit-statements-recovery-current-continuation1337.txt`。

该切片仍不改变 P2 partial 状态：统一 storage/WAL/native-binlog/GTID/relay/
applied-state 的物理原子提交协议和官方 MySQL fixture 互操作仍未完成。

## Continuation 1343

补齐两阶段 XA 的独立阶段 GTID。source 为 XA PREPARE 保留第一阶段 GTID，
为 XA COMMIT/ROLLBACK 分配第二个终结 GTID；逻辑事件同时保留 `GTID` 与
`TerminalGTID`，native 输出在 XA 终结查询前写入终结阶段 `GTID_EVENT`。
native decoder、replica、relay import、重启恢复和重复提交路径均保留两阶段身份。
证据见 `reports/compatibility/p2-xa-separate-phase-gtids-current-continuation1343.txt`。

本切片关闭本地两阶段 XA 的 GTID/event-shape 缺口，但不等于官方 MySQL server
fixture 互操作；P4 fixture 仍为 pending_external，P2 更大的统一
storage/WAL/native-binlog/GTID/relay/applied-state 原子提交协议仍为 partial。
I_S/P_S 全量字段、权限和运行时语义以及非 Connector/J 全量客户端矩阵继续纳入
全局任务；FULLTEXT 延后，非 InnoDB 继续明确为范围外。

## Continuation 1341

P1 补齐 Performance Schema `setup_*` 表写入的会话权限路径：通过 SQL 执行的
`UPDATE performance_schema.setup_*` 现在按当前账户检查对应表的 `UPDATE` 权限，
无授权账户被拒绝，授予表级 `UPDATE` 后才允许改变运行时配置。红测先证明无授权
更新成功，修复后定向权限测试和完整 Performance Schema 测试通过，证据见
`reports/compatibility/p1-performance-schema-setup-update-privilege-current-continuation1341.txt`。

该切片只关闭 setup 表写权限缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 的
逐表字段、组件、生命周期和运行时语义仍保持 P1 partial。

## Continuation 1342

补齐 `XA COMMIT ... ONE PHASE` 的原生 binlog 边界：source 现在写入带
`one_phase=1` 的 `XA_PREPARE_EVENT`，而不是普通 `XID_EVENT`；native decoder、replica
回放和 promotion relay history 都保留该标记，并在副本上立即提交、不进入 prepared-XA
集合。证据见
`reports/compatibility/p2-xa-one-phase-native-binlog-current-continuation1342.txt`。

该切片只关闭 XA 一阶段的本地 native event 缺口；完整 storage/WAL/native-binlog/GTID/
relay/applied-state 原子提交协议以及官方 MySQL fixture 互操作仍保持 P2/P4 未完成。

## Continuation 1338

P2 的稳定事务键提交 API 现在返回首次提交实际追加的 `BEGIN`、`ROW`、`COMMIT`
事件；重复 key 仍保持幂等并返回空事件，不会追加第二笔事务。证据见
`reports/compatibility/p2-keyed-commit-event-return-current-continuation1338.txt`。

该切片只补齐 keyed commit 的物理结果可观察性，不改变 P2 partial 状态：统一
storage/WAL/native-binlog/GTID/relay/applied-state 的物理原子提交协议和官方
MySQL fixture 互操作仍未完成。

## Continuation 1340

P2 补齐副本晋升时的 relay 历史继承：晋升现在会把副本已持久化的已提交 relay
事务导入新 Source 的 logical binlog，并重新生成 promoted server-id 对应的 native
binlog 帧，同时保留上游 GTID、事务键和后续新写入顺序。红测先观察到晋升后
`Dump()` 为空，修复后定向 promotion/native 回归和 replication 全包通过，证据见
`reports/compatibility/p2-promotion-relay-history-current-continuation1340.txt`。

该切片关闭本地 promotion history 缺口，但不改变 P2 partial 状态：storage/WAL/
native-binlog/GTID/relay/applied-state 的统一物理提交协议以及官方 MySQL XA/binlog/
复制/崩溃恢复 fixture 互操作仍未完成。

## Continuation 1339

P2 补齐 statement-only 自动提交发布的 durable recovery：DDL 等没有 row-image
的语句现在会在调用 source publisher 前写入并同步稳定事务 key 与原始 SQL；发布
失败后恢复可重试一次并清理 journal，重复恢复无副作用。证据见
`reports/compatibility/p2-autocommit-statement-recovery-current-continuation1339.txt`。

该切片仍不改变 P2 partial 状态：统一 storage/WAL/native-binlog/GTID/relay/
applied-state 的物理原子提交协议和官方 MySQL fixture 互操作仍未完成。
## Continuation 1344

P2 补齐两阶段 XA 回滚终态的重试幂等：当终结事件已经持久化、但 prepared-XA
内存状态已经丢失或 source 已重载时，按 durable transaction key 和已执行终结
GTID 识别重复回滚；native writer 同时对 XA COMMIT/ROLLBACK 终结 GTID 去重。
红测转绿、复制包、引擎 XA/恢复/提升测试和全仓串行回归均通过。证据见
`reports/compatibility/p2-xa-rollback-retry-idempotency-current-continuation1344.txt`。

该切片只关闭本地 XA rollback terminal retry 缺口；P2 的统一
storage/WAL/native-binlog/GTID/relay/applied-state 原子提交协议及官方 MySQL
fixture 互操作仍未完成。完整 I_S/P_S 语义和非 Connector/J 客户端矩阵继续纳入
全局任务；FULLTEXT 延后，非 InnoDB 继续为范围外。
## Continuation 1345

P1 收口 Performance Schema 同步对象表的精确 metadata：`cond_instances`、
`mutex_instances`、`rwlock_instances` 的 `NAME`、对象地址、持有线程和读锁计数
现在按 MySQL 8.4 的长度、NULL 和 UNSIGNED 语义返回。红测先复现通用
`VARCHAR(255) NULL` fallback，随后 I_S/P_S 回归和全仓串行回归通过。证据见
`reports/compatibility/p1-performance-schema-sync-instance-metadata-current-continuation1345.txt`。

该切片只关闭一个 metadata 精度子缺口；完整 I_S/P_S 的逐表字段、运行时实例行、
锁生命周期、权限可见性和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和非
InnoDB 的范围边界不变。
## Continuation 1346

P1 继续收口 Performance Schema instance metadata：`file_instances` 的文件名、
事件名和打开计数，以及 `socket_instances` 的事件名、对象地址、线程、句柄、IP、
端口和状态，现在按 MySQL 8.4 的精确类型、长度、NULL 和 UNSIGNED 语义返回。
红测、I_S/P_S 回归和全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-file-socket-instance-metadata-current-continuation1346.txt`。

该切片仍只关闭 metadata 精度子缺口；文件/连接实例的真实生命周期、运行时行、
权限可见性和完整 P_S 语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的
范围边界不变。

## Continuation 1347

P1 继续收口 Performance Schema 计时器 metadata：`performance_timers` 的
`TIMER_NAME`、频率、分辨率和开销列现在使用 MySQL 8.4 的 ENUM、BIGINT、可空性
和平台不支持计时器返回 NULL 的定义，不再使用通用 BIGINT UNSIGNED fallback。
红测、I_S/P_S 受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-performance-timers-metadata-current-continuation1347.txt`。

该切片仍只关闭一个 metadata 精度子缺口；完整 P_S 表覆盖、计时器/事件生命周期、
权限可见性和组件运行时语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的
范围边界不变。

## Continuation 1348

P1 继续收口 Performance Schema 连接属性 metadata：`session_connect_attrs` 和
`session_account_connect_attrs` 的 `PROCESSLIST_ID`、属性名/值及序号现在按
MySQL 8.4 的精确长度、类型、字符集和可空性返回。红测、I_S/P_S 受影响回归及
全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-connection-attributes-metadata-current-continuation1348.txt`。

该切片仍只关闭两张连接属性表的 metadata 精度；真实连接属性采集/生命周期、
权限可见性、完整 P_S 表覆盖和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和
非 InnoDB 的范围边界不变。

## Continuation 1355

P1 补齐 Performance Schema `host_cache` 的 MySQL 8.4 metadata：IP/主机字段长度
与字符集、验证状态 ENUM、错误计数的 signed BIGINT 语义，以及首见/末见和错误首见/
末见时间列的可空性现在按官方定义返回。定向测试、I_S/P_S 受影响回归及全仓串行
回归均通过。证据见
`reports/compatibility/p1-performance-schema-host-cache-metadata-current-continuation1355.txt`。

该切片仍只关闭 `host_cache` metadata 精度；完整 P_S 表/列覆盖、组件运行时语义、
权限可见性和生命周期仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的范围边界
不变。

## Continuation 1356

P1 继续补齐 Performance Schema `table_handles` 与
`prepared_statements_instances` 的 MySQL 8.4 metadata：对象列长度/可空性、prepared
statement 的执行引擎枚举、新增 CPU/内存/secondary 统计列及官方列集合已按顺序注册；
现有 inventory 没有权威 secondary-engine、CPU、内存来源的字段继续显式为零。定向测试、
I_S/P_S 受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-table-handle-prepared-metadata-current-continuation1356.txt`。

该切片仍只关闭两张 P_S 表的 metadata 精度；完整 P_S 表/列覆盖、组件运行时语义、
权限可见性和生命周期仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的范围边界
不变。

## Continuation 1357

P1 接通 SQL 文本协议 prepared statement：`PREPARE ... FROM`、`EXECUTE ... USING`
与 `DEALLOCATE PREPARE` 现在拥有会话级命名语句、参数绑定、执行统计和 reset/change-user
生命周期；`performance_schema.prepared_statements_instances` 同时投影二进制协议与
SQL 文本协议 inventory。定向测试、I_S/P_S 受影响回归和网络生命周期回归均通过。证据见
`reports/compatibility/p1-performance-schema-sql-prepared-statements-current-continuation1357.txt`。

该切片关闭的是 prepared statement 的 SQL 文本协议运行时缺口；完整 I_S/P_S 表/列和
组件运行时语义、权限可见性仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的范围
边界不变。

## Continuation 1358

P2 完成本地物理提交顺序的一项关键收口：普通客户端事务和复制事务现在都在真实
`TransactionManager.Commit` 成功后立即写入并同步 durable commit marker；普通客户端
的本地 native binlog/GTID publisher 也在同一 barrier 内完成，再进入脏页刷盘。barrier
或刷盘失败不会把已提交存储误判为可回滚状态。新增顺序测试、引擎/网络受影响回归及
全仓串行回归均通过。证据见
`reports/compatibility/p2-storage-commit-barrier-current-continuation1358.txt`。

验证过程中全量引擎回归捕获并修复了 XA 重复 native transaction：XA 会话现在跳过普通
client publisher，仅由 XA terminal publisher 发布；修正后的完整引擎和全仓串行回归均通过。

该切片不等于官方 MySQL 互操作完成：native binlog/GTID/XA 外部 fixture、真实
promotion 拓扑、完整 P1 I_S/P_S 运行时/权限/组件语义及 P3 全客户端矩阵仍在全局任务中；
FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1360

P2 继续统一本地复制提交协议：复制回放的 applied GTID/committed marker 现在在真实
存储提交成功后、脏页刷盘前写入，与 durable `replication_commit` journal record
处于同一 barrier；外层 replay COMMIT 只保留幂等安全网。marker/刷盘失败回归、引擎
全量回归和全仓串行回归均通过。证据见
`reports/compatibility/p2-applied-gtid-commit-barrier-current-continuation1360.txt`。

这仍是 xmysql-to-xmysql 的本地提交协议收口，不等于官方 MySQL XA/binlog/GTID
互操作或外部 promotion fixture 完成；P1/P3/P4 的剩余边界不变。

## Continuation 1349

P1 继续收口 Performance Schema 线程变量/状态表 metadata：
`user_variables_by_thread` 现在使用 `BIGINT UNSIGNED`、`VARCHAR(64)` 和
`LONGBLOB` 的 MySQL 8.4 定义；`variables_by_thread` 与 `status_by_thread`
使用 `BIGINT UNSIGNED`、`VARCHAR(64)` 和 `VARCHAR(1024)` 的精确定义。
定向测试、I_S/P_S 受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-thread-variable-metadata-current-continuation1349.txt`。

该切片仍只关闭三张线程变量/状态表的 metadata 精度；运行时行生命周期、
权限可见性、完整 P_S 表覆盖和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT
和非 InnoDB 的范围边界不变。

## Continuation 1354

P1 修复 INFORMATION_SCHEMA 授权视图的动态管理员可见性：账号或已启用角色的
`GlobalGrants` 现在并入 `*.*` 有效权限集合，因此具备 `CREATE USER` 与
`SYSTEM_USER` 的会话可以看到其它账号的授权行，同时普通会话仍保持仅能查看自身
或有明确授权的行。定向权限/角色回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-information-schema-dynamic-system-user-visibility-current-continuation1354.txt`。

该切片仍只关闭动态 `SYSTEM_USER` 管理员例外；完整角色继承、partial revoke、
对象级可见性和全部 I_S/P_S 权限语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和
非 InnoDB 的范围边界不变。

## Continuation 1350

P1 继续收口 Performance Schema 内存统计 metadata：全局、线程、账号、主机、
用户五张 `memory_summary_*_by_event_name` 表现在按 MySQL 8.4 精确返回
`EVENT_NAME`、`USER/HOST/THREAD_ID` 维度以及 unsigned/signed BIGINT 统计列的
类型、长度和可空性。定向测试、I_S/P_S 受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-memory-summary-metadata-current-continuation1350.txt`。

该切片仍只关闭五张内存统计表的 metadata 精度；运行时聚合、权限可见性、完整
P_S 表覆盖和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的范围
边界不变。

## Continuation 1351

P1 继续收口 Performance Schema 状态维度 metadata：`status_by_account`、
`status_by_host`、`status_by_user` 现在按 MySQL 8.4 精确返回用户/主机维度以及
变量名、变量值的类型、长度和可空性。定向测试、I_S/P_S 受影响回归及全仓串行
回归均通过。证据见
`reports/compatibility/p1-performance-schema-status-dimension-metadata-current-continuation1351.txt`。

该切片仍只关闭三张状态维度表的 metadata 精度；运行时状态聚合、权限可见性、
完整 P_S 表覆盖和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB
的范围边界不变。

## Continuation 1352

P1 继续收口 Performance Schema 变量视图 metadata 和 registry：
`session/global_status`、`session/global_variables`、`persisted_variables` 与
`variables_info` 现在按 MySQL 8.4 返回变量名/值、持久化时间与身份字段；同时
补齐此前遗漏的 `variables_info.MIN_VALUE/MAX_VALUE` 两列。定向测试、I_S/P_S
受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-variable-metadata-current-continuation1352.txt`。

该切片仍只关闭变量视图的 metadata/registry 精度；运行时变量持久化、完整
P_S 表覆盖和权限可见性仍为 P1 partial。P2/P3/P4、FULLTEXT 和非 InnoDB 的
范围边界不变。

## Continuation 1353

P1 继续收口 Performance Schema I/O 汇总表：`table_io_waits_summary_*`、
`file_summary_*` 和 `socket_summary_*` 现在按 MySQL 8.4 返回官方列顺序、
字段长度、可空性及 signed/unsigned 数值语义；文件汇总的 `COUNT_STAR`、
`SUM_NUMBER_OF_BYTES_READ/WRITE` 也已接入专用执行器，IBD 页读写会累计字节数。
定向测试、I_S/P_S 受影响回归及全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-io-summary-metadata-current-continuation1353.txt`。

该切片仍只关闭一组 I/O 汇总表的 metadata/runtime 投影缺口；完整 P_S 表覆盖、
全部运行时生命周期、权限可见性和组件语义仍为 P1 partial。P2/P3/P4、FULLTEXT
和非 InnoDB 的范围边界不变。

## Continuation 1373

P1 继续收口 Performance Schema 运行时生命周期：语句汇总的全局、线程、账号、
主机、用户五个维度现在使用独立聚合源；对
`events_statements_summary_by_thread_by_event_name`、
`events_statements_summary_by_account_by_event_name`、
`events_statements_summary_by_host_by_event_name` 和
`events_statements_summary_by_user_by_event_name` 执行 `TRUNCATE` 时只清空
对应维度，不能误清空全局或其它维度。定向隔离测试、Performance Schema 回归、
metrics 回归和 engine 全量回归均通过。证据见
`reports/compatibility/p1-performance-schema-summary-dimension-truncate-current-continuation1373.txt`。

该切片只关闭语句汇总这一组维度的运行时/生命周期缺口；完整 I_S/P_S 表覆盖、
其它汇总族的维度独立性、权限和组件生命周期仍为 P1 partial。P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 fixture
仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1377

刷新 P3 非 Connector/J 客户端边界诊断：Go、Python/PyMySQL、Node.js/mysql2
运行时可用；`mysql.exe` 不存在；集群端点的真实 source/replica/promotion
场景因 `XMYSQL_CLIENT_PASSWORD` 未设置而跳过。两个诊断脚本均按设计返回非零，
因为完整矩阵不能把缺失环境当作 PASS。证据见
`reports/compatibility/p3-client-boundary-current-continuation1377.txt`。

因此 P3 客户端矩阵继续保持 partial，MySQL CLI 保持 unverified，cluster endpoint
继续保持 partial；未写入凭据。

## Continuation 1378

尝试执行可用客户端的实际功能矩阵时，脚本在启动隔离服务前因缺少
`XMYSQL_CLIENT_PASSWORD` 终止；没有执行任何客户端 case，也没有把运行时可用误判
为协议 PASS。证据见
`reports/compatibility/p3-client-functional-boundary-current-continuation1378.txt`。

## Continuation 1376

P1 继续收口 Performance Schema waits 汇总生命周期：线程、账号、主机、用户
四个维度现在对已完成 wait 记录使用独立 reset 指纹；对
`events_waits_summary_by_thread_by_event_name`、
`events_waits_summary_by_account_by_event_name`、
`events_waits_summary_by_host_by_event_name` 和
`events_waits_summary_by_user_by_event_name` 执行 `TRUNCATE` 时只清空对应
维度，global 汇总和 live wait 不受影响。定向隔离测试、wait 回归和 engine
全量回归均通过。证据见
`reports/compatibility/p1-performance-schema-wait-summary-dimension-truncate-current-continuation1376.txt`。

该切片只关闭 waits 汇总维度 reset 隔离；完整 I_S/P_S 表覆盖、所有组件/权限
语义、P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和
P4 官方 fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1379

P1 继续收口 INFORMATION_SCHEMA InnoDB 元数据：`INNODB_TABLESPACES` 现在按
MySQL 8.4 原生列顺序暴露，`SPACE`、`FLAG`、页大小、文件大小、版本和状态等
字段的 INT/BIGINT、UNSIGNED、长度和可空性已从通用推断改为显式定义；持久化行值
和扩展字段保持不变。定向 metadata 回归、I_S 权限/角色回归、engine 全量回归和
全仓串行回归均通过。证据见
`reports/compatibility/p1-innodb-tablespaces-metadata-current-continuation1379.txt`。

该切片只关闭 `INNODB_TABLESPACES` 的列顺序和字段 metadata 缺口；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1383

P1 修正 `INFORMATION_SCHEMA.INNODB_TABLESPACES` 的官方列形状：移除当前实现
追加的 `FLAGS`、`SDI_*`、`SPACE_FLAGS*` 项目字段，注册表、运行时投影和 metadata
现在只暴露 MySQL 8.4 官方 15 列，持久化空间标志统一通过官方 `FLAG` 暴露。旧
实现先由 `SELECT *` 回归确认返回 22 列，再完成生产收紧；I_S 定向、engine 全量
和全仓串行回归均通过。证据见
`reports/compatibility/p1-innodb-tablespaces-shape-current-continuation1383.txt`。

该切片只关闭一个 InnoDB 字典表的官方列形状偏差；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1382

P1 修正 `INFORMATION_SCHEMA.INNODB_VIRTUAL` 的官方表形状：移除当前实现额外
暴露的非 MySQL `M_COLS`，注册表、运行时行投影和字段 metadata 现在只暴露
`TABLE_ID`、`POS`、`BASE_POS`。旧实现先由回归测试确认会返回四列，再完成生产
收紧；I_S 定向、engine 全量和全仓串行回归均通过。证据见
`reports/compatibility/p1-innodb-virtual-shape-current-continuation1382.txt`。

该切片只关闭一个 InnoDB 字典表的官方列形状偏差；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1381

P1 继续核对 INFORMATION_SCHEMA InnoDB 字典表的字段语义：`INNODB_INDEXES`
和 `INNODB_COLUMNS` 的列顺序、类型、长度、UNSIGNED 和可空性已按 MySQL 8.4
原生定义加入精确回归。`INNODB_INDEXES.NAME` 保持 `VARCHAR(193)`，
`INNODB_COLUMNS.POS` 保持 `BIGINT UNSIGNED`，`DEFAULT_VALUE` 保持可空 BLOB。
定向 metadata、engine 全量和全仓串行回归均通过。证据见
`reports/compatibility/p1-innodb-dictionary-metadata-current-continuation1381.txt`。

该切片只锁定两个已实现字典表的 metadata 契约；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1375

P1 继续收口 Performance Schema transaction 汇总生命周期：线程、账号、主机、
用户四个维度现在使用带独立 reset cutoff 的事件聚合；对
`events_transactions_summary_by_thread_by_event_name`、
`events_transactions_summary_by_account_by_event_name`、
`events_transactions_summary_by_host_by_event_name` 和
`events_transactions_summary_by_user_by_event_name` 执行 `TRUNCATE` 时只清空
对应维度，不能误清空全局汇总。定向隔离测试、transaction/summary 回归和
engine 全量回归均通过。证据见
`reports/compatibility/p1-performance-schema-transaction-summary-dimension-truncate-current-continuation1375.txt`。

该切片只关闭 transaction 汇总维度 reset 隔离；完整 I_S/P_S 表覆盖、其它汇总
族的维度独立性、wait 汇总、权限和组件生命周期仍为 P1 partial。P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 fixture
仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1374

P1 继续收口 Performance Schema stage 汇总生命周期：线程、账号、主机、用户
四个维度现在使用独立的 `stage/sql/execute` 聚合源；对
`events_stages_summary_by_thread_by_event_name`、
`events_stages_summary_by_account_by_event_name`、
`events_stages_summary_by_host_by_event_name` 和
`events_stages_summary_by_user_by_event_name` 执行 `TRUNCATE` 时只清空对应
维度，不能误清空全局或其它维度。定向隔离测试、stage/summary 回归、metrics
回归和 engine 全量回归均通过。证据见
`reports/compatibility/p1-performance-schema-stage-summary-dimension-truncate-current-continuation1374.txt`。

该切片只关闭 stage 汇总这一组维度的运行时/生命周期缺口；完整 I_S/P_S 表
覆盖、其它汇总族的维度独立性、权限和组件生命周期仍为 P1 partial。P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 fixture
仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1384

P1 补齐 InnoDB 字典与监控视图的 PROCESS 权限边界：
`INNODB_TABLESPACES`、`INNODB_DATAFILES`、`INNODB_TABLES`、
`INNODB_SESSION_TEMP_TABLESPACES`、`INNODB_CACHED_INDEXES`、
`INNODB_FIELDS`、`INNODB_COLUMNS`、`INNODB_INDEXES`、`INNODB_FOREIGN`、
`INNODB_FOREIGN_COLS`、`INNODB_TABLESTATS`、`INNODB_TEMP_TABLE_INFO`、
`INNODB_VIRTUAL` 现在在无 PROCESS 时返回 MySQL 兼容的拒绝，有权限时继续
进入原有运行时投影；同时锁定 `INNODB_TABLESPACES_BRIEF`、`INNODB_DATAFILES`
和 `INNODB_SESSION_TEMP_TABLESPACES` 的精确 metadata。
旧实现先由新增回归确认 `INNODB_TABLESPACES` 可被无 PROCESS 查询，再完成生产
拦截；I_S 专项、engine 全量和全仓串行回归均通过。证据见
`reports/compatibility/p1-innodb-process-privilege-current-continuation1384.txt`。

该切片只关闭四个 InnoDB 视图的权限边界和三个辅助表的 metadata 回归；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1385

P1 收口 Performance Schema 复制过滤表的官方字段注册和 metadata：
`replication_applier_filters` 现在暴露 `CHANNEL_NAME`、`FILTER_NAME`、
`FILTER_RULE`、`CONFIGURED_BY`、`ACTIVE_SINCE`、`COUNTER` 六列，
`replication_applier_global_filters` 暴露四列；类型、长度、ENUM 值、
时间精度、UNSIGNED 和可空性按 MySQL 8.4 原生表定义锁定。旧实现先由红测
确认 `CHANNEL_NAME` 落入 `VARCHAR(255)` 兜底、且 `CONFIGURED_BY` 未注册，
随后补齐注册表和 metadata 映射。P_S 定向、engine 全量和全仓串行回归均通过。
证据见 `reports/compatibility/p1-performance-schema-replication-filters-metadata-current-continuation1385.txt`。

该切片只关闭两个复制过滤表的字段/metadata 缺口；项目尚未接入原生复制过滤器
运行时来源，因此不把空行投影视为过滤器生命周期已完成。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1386

P1 继续收口 Performance Schema 异步复制故障转移表的官方字段和 metadata：
`replication_asynchronous_connection_failover` 现在使用
`CHANNEL_NAME`、`HOST`、`PORT`、`NETWORK_NAMESPACE`、`WEIGHT`、
`MANAGED_NAME` 六列，managed 表使用 `CHANNEL_NAME`、`MANAGED_NAME`、
`MANAGED_TYPE`、`CONFIGURATION` 四列；CHAR/JSON、长度、ASCII 字符集、
UNSIGNED、可空性和列顺序按 MySQL 8.4 原生定义锁定。旧实现先由红测确认
`CHANNEL_NAME` 落入 `VARCHAR(255)` 兜底且注册表列集合错误，随后修正注册表和
metadata 映射。P_S 定向、engine 全量和全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-async-failover-metadata-current-continuation1386.txt`。

该切片只关闭两个异步故障转移表的字段/metadata 缺口；项目尚未接入异步故障转移
源列表持久化、Group Replication 管理组和晋升切换运行时，因此不把成形的空表视为
故障转移生命周期已完成。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/
运行时/权限覆盖仍为 P1 partial。P2 native binlog/GTID/XA/崩溃恢复/晋升、P3
非 Connector/J 客户端矩阵和 P4 官方 MySQL fixture 仍在全局任务中；FULLTEXT
延后，非 InnoDB 仍为范围外。

## Continuation 1387

P1 继续收口 Performance Schema Group Replication 元数据：
`replication_group_member_stats` 从旧的非官方子集扩展为 MySQL 8.4 的完整
13 列，补齐 `VIEW_ID`、事务队列/冲突/验证/远端/本地统计以及两个文本集合列；
CHAR 长度、BIGINT UNSIGNED、LONGTEXT/TEXT 和可空性按原生定义锁定。旧实现先由
红测确认首列落入 `VARCHAR(255)` 兜底，随后修正注册表和 metadata 映射。P_S
定向回归通过；engine 和全仓回归需在本切片后重新执行并记录。证据见
`reports/compatibility/p1-performance-schema-group-member-stats-metadata-current-continuation1387.txt`。

该切片只关闭 `replication_group_member_stats` 的字段/metadata 缺口；项目仍未接入
Group Replication 成员统计、通信、成员、动作和晋升运行时。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为 P1 partial。
P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方
MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1388

P1 继续收口 Group Replication 相关 Performance Schema 表：
`replication_group_communication_information` 改为官方六列，
`replication_group_configuration_version` 改为 `NAME, VERSION`，
`replication_group_member_actions` 补齐 `TYPE`、`PRIORITY`、
`ERROR_HANDLING`，并锁定 `replication_group_members` 的 CHAR/ASCII、
可空 `MEMBER_PORT` 等精确 metadata。新增运行时边界测试证明没有 Group
Replication 组件时五张表保持官方列顺序但返回空集，不伪造成员、动作或统计行。
定向、engine 全量和全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-group-replication-metadata-current-continuation1388.txt`。

该切片只关闭四张 Group Replication 表的字段、metadata 和无组件边界；Group
Replication 成员发现、共识通信、member actions、统计和 promotion 运行时仍未
实现。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖仍为
P1 partial。P2 native binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵
和 P4 官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1389

P1 继续收口已有 Performance Schema 运行时表的精确 metadata：
`setup_loggers` 现在使用官方 `NAME VARCHAR(128)`、五值小写 `LEVEL ENUM` 和
`DESCRIPTION VARCHAR(1023)`；`tls_channel_status` 现在使用
`CHANNEL VARCHAR(128)`、`PROPERTY VARCHAR(128)`、`VALUE VARCHAR(2048)`，三列均
非空。同步修正 `COLUMN_TYPE` 格式化逻辑，只规范化 ENUM/SET 类型关键字并保留
字面值大小写，避免把官方枚举成员错误转成大写。红测确认旧实现分别落入
`VARCHAR(255)` 兜底，随后定向、engine 和全仓串行回归均通过。证据见
`reports/compatibility/p1-performance-schema-setup-loggers-tls-metadata-current-continuation1389.txt`。

该切片只关闭两个 Performance Schema 表的字段/metadata 缺口，并不表示完整
P1 的表、字段、运行时、组件和权限语义已经完成。P2 native binlog/GTID/XA/
崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 MySQL fixture 仍在
全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1390

P1 修正 `performance_schema.clone_status` 的官方表形状：从旧的七列扩展为
MySQL 8.4 的 12 列，补齐 `ERROR_NO`、`ERROR_MESSAGE`、`BINLOG_FILE`、
`BINLOG_POSITION` 和 `GTID_EXECUTED`，并同步锁定 INT、CHAR(16)、TIMESTAMP(3)、
VARCHAR(512)、BIGINT、LONGTEXT 及可空性。新增边界回归确认没有 Clone 插件时
仍返回官方列顺序和空集，不伪造 Clone 生命周期。定向回归和全仓串行回归均
通过。证据见 `reports/compatibility/p1-performance-schema-clone-status-shape-current-continuation1390.txt`。

该切片只关闭 Clone 状态表的列形状和 metadata 缺口；Clone 执行、持久化状态、
binlog/GTID 坐标发布和重启恢复仍未实现。完整 INFORMATION_SCHEMA/
PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P2 native binlog/GTID/XA/
崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 MySQL fixture 仍在
全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1391

P1 修正 `performance_schema.clone_progress` 的官方表形状：将非原生的
`THREAD_ID` 改为 `THREADS`，并锁定 `STAGE CHAR(32)`、`STATE CHAR(16)`、
`TIMESTAMP(6)` 时间列、INT 计数列和 BIGINT 字节列的可空性。新增边界回归
确认没有 Clone 插件时仍返回官方列顺序和空集；定向回归和全仓串行回归均通过。
证据见 `reports/compatibility/p1-performance-schema-clone-progress-shape-current-continuation1391.txt`。

该切片只关闭 Clone 进度表的列名、字段类型和 metadata 缺口；Clone 执行、
进度持久化、binlog/GTID 坐标发布和重启恢复仍未实现。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 MySQL
fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1392

P1 继续收口组件/函数类 Performance Schema 表的精确 metadata：
`user_defined_functions` 现在按 MySQL 8.4.11 源码使用五列
`VARCHAR(64)`、`VARCHAR(20)`、`VARCHAR(20)`、`VARCHAR(1024)`、`BIGINT`；
`keyring_component_status` 使用 `VARCHAR(256)`/`VARCHAR(1024)` 非空列；
`keyring_keys` 使用 `VARCHAR(255)`、`VARCHAR(255)`、`VARCHAR(255)`，后两列可空。
红测确认旧实现落入通用 `VARCHAR(255)` 兜底，随后定向和全仓串行回归均通过。
证据见 `reports/compatibility/p1-performance-schema-udf-keyring-metadata-current-continuation1392.txt`。

该切片只关闭三个表的字段/metadata 缺口；UDF 注册与生命周期、Keyring 服务和
组件运行时仍未接入，空结果不代表这些外部组件能力已完成。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector/J 客户端矩阵和 P4 官方 MySQL
fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1393

P1 继续收口 Performance Schema 锁表：`metadata_locks` 按 MySQL 8.4.11
源码补齐缺失的 `COLUMN_NAME`，并锁定 11 列的顺序、VARCHAR 长度、BIGINT
UNSIGNED、可空性和 I_S.COLUMNS 精度；运行时 `SELECT *` 也同步暴露该列。
现有 DDL 协调器只产生表级锁，因此列名保持空值，不伪造列级 MDL。定向
元数据/运行时回归通过；证据见
`reports/compatibility/p1-performance-schema-metadata-locks-shape-current-continuation1393.txt`。

该切片只关闭 `metadata_locks` 的表形状和 metadata 缺口；列级 MDL、完整
原生状态转换以及 source/owner 细节仍未完成。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P2 native
binlog/GTID/XA/崩溃恢复/晋升、P3 非 Connector-J 客户端矩阵和 P4 官方
MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1394

P1 对已注册的 Performance Schema 做全表 metadata 审计，补齐标准
Community 表的 `events_statements_summary_by_program`、digest 分位数、
object/table-lock summary 和现有 processlist `TIME_MS` 扩展的显式映射。
新增注册表级审计红测，确保标准表不再静默落入通用兜底；Firewall、NDB、
Thread Pool 和 MySQL 8.0+ 已移除的 `setup_timers` 单独列为组件/历史边界。
定向回归通过；证据见
`reports/compatibility/p1-performance-schema-standard-metadata-audit-current-continuation1394.txt`。

该切片只收口标准表的 metadata 覆盖，不等于所有 P_S 运行时统计、组件
生命周期或权限语义已完成。完整 I_S/P_S、P2 native binlog/GTID/XA/崩溃
恢复/晋升、P3 非 Connector-J 客户端和 P4 官方 fixture 仍在全局任务中；
FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1395

P2 收口普通客户端 DML 的一个 native transaction 缺口：`REPLACE` 现在记录
删除前镜像和插入后镜像，`INSERT ... ON DUPLICATE KEY UPDATE` 现在记录非冲突
插入及冲突更新的 before/after 镜像，source 会为这两类已提交操作生成 GTID
和 row event。同步将单语句 autocommit 的 INSERT/REPLACE/UPDATE/DELETE 包装为
一条客户端事务边界，使 DML journal 在物理 storage commit 前落盘，native
binlog/GTID 发布仍发生在 storage commit 后，发布失败时保留可重试身份。证据见
`reports/compatibility/p2-autocommit-dml-row-images-current-continuation1395.txt`。

该切片只关闭普通 DML row-image 和 autocommit 提交顺序的本地缺口；P2 仍未完成
统一 storage/WAL/native-binlog/GTID/relay/applied-state 物理提交协议、完整崩溃
恢复/晋升互操作和官方 MySQL fixture。完整 I_S/P_S、P3 非 Connector-J 矩阵、
FULLTEXT 和非 InnoDB 范围边界不变。

## Continuation 1396

P2 补齐本地物理提交恢复边界：`TransactionManager.Commit` 现在把
`LOG_TYPE_TXN_COMMIT` 与事务 redo 写入同一条 WAL 并在返回前刷盘；事务 journal
同时保留逻辑 transaction ID、物理 storage transaction ID 和可重放 statement。
孤儿 journal 恢复前先检查物理 commit marker，已提交的 storage transaction 不再被
误回滚，并可按稳定 transaction identity 重试 native publication。新增的
`TestCommittedStorageWALPreventsOrphanJournalRollback` 以及 publisher 失败、重启、
幂等回归均通过。

本轮还修正嵌入式/直接构造 `Cfg` 的 redo/undo 路径：优先使用 typed
`InnodbRedoLogDir`/`InnodbUndoLogDir`，未配置时落到当前 `DataDir`，不再让多个
engine 共享进程级临时 WAL。该问题曾导致恢复读取其他实例的重复低 LSN page redo。
定向、完整 engine、manager/replication 和全仓串行回归均通过；证据见
`reports/compatibility/p2-storage-wal-commit-marker-and-redo-isolation-current-continuation1396.txt`。

该切片关闭的是本地 storage/WAL 与 transaction journal 的提交判定及实例隔离，
不等于完成与 MySQL 原生 binlog、复制、崩溃恢复、晋升的完整互操作。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P3 非
Connector/J 客户端矩阵和 P4 官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，
非 InnoDB 仍为范围外。

## Continuation 1397

P2 修正 redo 进程重启后的 LSN 连续性：`NewRedoLogManager` 启动时扫描已有
`redo.log` 的完整记录，以最大持久化 LSN 初始化分配器，避免重启后重新从低位
LSN 分配并破坏 recovery 的日志顺序。新增重启续接红测先确认旧实现会出现
`first=2, second=2`，修复后通过；manager 和完整 engine 回归均通过。证据见
`reports/compatibility/p2-redo-lsn-restart-continuity-current-continuation1397.txt`。

该切片只关闭本地 redo LSN 持久化连续性；与 MySQL 原生 binlog、复制、崩溃恢复、
晋升的完整互操作、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 运行时/权限、
P3 非 Connector/J 客户端矩阵和 P4 官方 fixture 仍在全局任务中；FULLTEXT 延后，
非 InnoDB 仍为范围外。

## Continuation 1398

P1 收口一批高价值 INFORMATION_SCHEMA 字段元数据契约：`INNODB_TRX` 25 列、旧版
`INNODB_LOCKS/INNODB_LOCK_WAITS`、`ROUTINES`、`PARAMETERS`、`PARTITIONS`、
`FILES`、`TABLESPACES`、变量表和连接控制表现在不再静默使用通用字段推导；同时
将 ROUTINES/PARAMETERS 的 `DATA_TYPE`、例程时间列和标识列对齐 MySQL 8.4。
新增关键表显式契约审计，定向、完整 engine 和全仓串行回归均通过。证据见
`reports/compatibility/p1-information-schema-metadata-contracts-current-continuation1398.txt`。

本轮仍只关闭字段 metadata 精度/可空性/类型契约，不等于完整 I_S/P_S 运行时统计、
权限/组件生命周期已完成。Thread Pool 插件专用表已在下一轮补齐精确的 MySQL 8.4
字段契约；全量审计当前剩余的是 Enterprise Firewall 的组件专用字段/运行时边界。
P2 native binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和
P4 官方 MySQL fixture 仍在全局任务中。FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1399

P1 收口 Thread Pool 的 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 元数据契约：
`tp_thread_group_state`、`tp_thread_group_stats` 和 `tp_thread_state` 在两个 schema
中现在都有明确的 MySQL 8.4 列顺序、类型、长度、数值精度、可空性和 `COLUMN_TYPE`，
而没有 Thread Pool 运行时时仍保持可发现但空结果。新增官方字段契约红绿测试、
registry-wide P_S 显式 metadata 审计，定向、完整 engine 和全仓串行回归均通过。
证据见 `reports/compatibility/p1-thread-pool-metadata-contracts-current-continuation1399.txt`。

本轮不伪造 Enterprise Firewall 的精确 8.4 插件字段或运行时：公开 Community 源码/文档
不足以证明其插件内部 metadata 定义，当前仅保留表可见性边界；Firewall 的精确字段、
插件生命周期、规则持久化和权限语义仍是 P1 未完成项。完整 I_S/P_S 运行时/权限、
P2 native binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和 P4
官方 MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1400

P1 修正 `PERFORMANCE_SCHEMA.tp_connections` 的公开 MySQL 8.4 列形状：注册表
从旧的 12 列 process-list-like 兼容形状改为官方定义的 19 列及其顺序，新增
红绿测试；无 Enterprise Thread Pool runtime 时仍保持可发现但返回空结果。证据见
`reports/compatibility/p1-thread-pool-connections-shape-current-continuation1400.txt`。

本轮只关闭列名/顺序契约，不宣称 Enterprise 插件的精确字段类型、长度、精度、
可空性、生命周期和运行时统计已完成。公开 Community 源码/文档不足以证明这些
内部插件 metadata，需要 Enterprise fixture 或具备该插件的受控构建环境。完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖、P2 native
binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和 P4 官方
MySQL fixture 仍在全局任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1401

P1 补齐 `PERFORMANCE_SCHEMA.accounts`、`hosts`、`users` 的连接汇总表截断语义：
现在 `TRUNCATE TABLE` 会保留当前认证连接、将 `TOTAL_CONNECTIONS` 重置为当前连接数，
并移除没有当前连接的历史账号；三张表通过同一个 live session snapshot 处理。证据见
`reports/compatibility/p1-performance-schema-connection-truncate-current-continuation1401.txt`。

本轮只关闭连接汇总表的 reset 语义，不等于所有依赖 account/host/user 维度的 summary
表、Enterprise 组件表或完整 P_S 运行时/权限覆盖已完成。P2 native binlog/GTID/XA/
崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和 P4 官方 MySQL fixture 仍在全局
任务中；FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1402

P1 扩展连接汇总表的 `TRUNCATE` 影响范围：`accounts` 同步重置 account/thread
维度的 error、statement、stage、transaction、wait 汇总；`hosts` 额外重置 host
维度；`users` 额外重置 user 维度。这样与 MySQL 8.4 对连接表隐式截断依赖汇总表的
关系保持一致。证据见
`reports/compatibility/p1-performance-schema-connection-dependent-summary-reset-current-continuation1402.txt`。

本轮仍未关闭 memory/component 汇总、Enterprise Firewall 精确语义、完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖；P2 native
binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和 P4 官方
MySQL fixture 仍在全局任务中。FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1403

P1 收口 memory summary 的独立维度：现在 global、thread、account、host、user
分别维护汇总源，显式 `TRUNCATE` 各 derived memory table 只重置对应维度；连接表
截断按 MySQL 8.4 关系重置对应 account/host/user 维度并同步 thread 维度，global
memory summary 截断重置全部维度。证据见
`reports/compatibility/p1-performance-schema-memory-summary-dimension-reset-current-continuation1403.txt`。

本轮仍未关闭 Enterprise/component memory 来源、Enterprise Firewall 精确语义、
完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/权限覆盖；P2 native
binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J 客户端矩阵和 P4 官方
MySQL fixture 仍在全局任务中。FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1404

本轮重新确认全局边界：完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表、字段、
运行时、组件和权限语义纳入全局任务；完整非 Connector/J 客户端矩阵纳入全局
任务但放在 P3；XA 与 MySQL 原生 binlog、复制、崩溃恢复和晋升互操作纳入 P2/P4。
非 InnoDB 引擎、专用修复和引擎转换明确不纳入；FULLTEXT 继续 deferred。

对 Enterprise Firewall 做了官方文档核对。MySQL 8.4 公开文档确认
`MYSQL_FIREWALL_USERS(USERHOST, MODE)` 和
`MYSQL_FIREWALL_WHITELIST(USERHOST, RULE)` 的列名及运行含义，并说明它们只在
相应 Enterprise Firewall 插件启用时提供且已 deprecated；但当前没有 Enterprise
fixture/插件构建，公开页面也不足以证明完整 `INFORMATION_SCHEMA.COLUMNS` 类型、
长度、可空性、生命周期和权限契约。因此这四列仍保持 P1 `partial`，不伪造字段
类型实现。证据见
`reports/compatibility/p1-global-scope-audit-current-continuation1404.txt`。
`reports/compatibility/p1-global-scope-audit-current-continuation1404.txt`。

## Continuation 1405

P1 补齐部分撤销（partial revoke）下的信息架构对象可见性：当账号只有
`*.*` 全局权限、但对指定 schema 存在部分撤销时，`SCHEMATA`、`TABLES`、
`COLUMNS` 和 `STATISTICS` 不再泄露被限制对象；当随后授予显式 schema/table/
column 权限时，对应对象重新可见。该行为与 MySQL 8.4 的“限制全局权限、
允许显式对象授权恢复有限访问”规则一致。

红绿测试、相关权限/角色回归、完整 engine 回归和全仓串行回归均通过，证据见
`reports/compatibility/p1-information-schema-partial-revoke-object-visibility-current-continuation1405.txt`。

本轮只关闭已覆盖的 I_S 对象可见性切片；完整 I_S/P_S 表、字段、运行时、组件
和权限语义仍为 `partial`。Enterprise Firewall 精确插件契约仍需要 Enterprise
fixture/build；P2 native binlog/GTID/XA/崩溃恢复/晋升互操作、P3 非 Connector/J
客户端矩阵、P4 官方 MySQL fixture 仍未完成。FULLTEXT 继续 deferred，非 InnoDB
引擎、修复和转换继续明确为范围外。

## Continuation 1406

P1 补齐 `ROLE_ADMIN` 会话下的角色可授予性：`APPLICABLE_ROLES` 和
`ADMINISTRABLE_ROLE_AUTHORIZATIONS` 现在在当前 session 持有动态
`ROLE_ADMIN` 时，将适用角色报告为 `IS_GRANTABLE=YES`，即使持久化角色边
本身没有 `WITH ADMIN OPTION`。该语义对应 MySQL 对 `ROLE_ADMIN` 的定义。

红绿测试、相关角色/权限回归和完整 engine 回归均通过，证据见
`reports/compatibility/p1-information-schema-role-admin-grantability-current-continuation1406.txt`。

本轮只关闭 session 级 ROLE_ADMIN 可授予性投影；完整 I_S/P_S 表、字段、运行时、
组件和权限语义仍为 `partial`。P2 native binlog/GTID/XA/崩溃恢复/晋升互操作、
P3 非 Connector/J 客户端矩阵、P4 官方 MySQL fixture 仍未完成。FULLTEXT 继续
deferred，非 InnoDB 引擎、修复和转换继续明确为范围外。

## Continuation 1407

继续补齐 `ROLE_ADMIN` 的权限来源：当 `ROLE_ADMIN` 只存在于持久化
`mysql.global_grants`，而 session 对象没有显式 `dynamic_privileges` 参数时，
`APPLICABLE_ROLES` 和 `ADMINISTRABLE_ROLE_AUTHORIZATIONS` 也会通过有效授权
集合识别为 `IS_GRANTABLE=YES`。这同时覆盖 active/inherited role 提供的动态权限。

红绿测试、相关角色/权限回归、完整 engine 回归和全仓串行回归均通过。证据见
`reports/compatibility/p1-information-schema-role-admin-persisted-grant-current-continuation1407.txt`。

完整 I_S/P_S 语义、P2 原生复制互操作、P3 非 Connector/J 客户端矩阵和 P4
官方 fixture 仍未完成；FULLTEXT 继续 deferred，非 InnoDB 继续范围外。

## Continuation 1408

补充角色授权边界回归：父角色以 `WITH ADMIN OPTION` 授予用户时，
`APPLICABLE_ROLES` 和 `ADMINISTRABLE_ROLE_AUTHORIZATIONS` 只将直接授予的父
角色标记为 `IS_GRANTABLE=YES`，通过角色继承可见的子角色保持 `NO`。这避免
把角色图中的权限聚合误当成子角色本身的 ADMIN OPTION，造成角色授权越权。
证据见
`reports/compatibility/p1-information-schema-nested-role-admin-boundary-current-continuation1408.txt`。

该切片是负向安全回归，不改变全局范围判断：完整 I_S/P_S 表、字段、运行时、
组件和权限语义仍为 `partial`；P2 原生 binlog/GTID/XA/崩溃恢复/晋升互操作、
P3 非 Connector/J 客户端矩阵和 P4 官方 fixture 仍未完成。FULLTEXT 继续
`deferred`，非 InnoDB 引擎、修复和转换继续范围外。

## Continuation 1409

重新执行 P3 客户端环境诊断：MySQL CLI 仍不存在；Go、PyMySQL 和 Node/mysql2
运行时可用，但全量认证矩阵和集群 endpoint 认证场景因未设置受保护的
`XMYSQL_CLIENT_PASSWORD` 而跳过。该结果只刷新环境证据，不把可用性误记为
功能通过。证据见
`reports/compatibility/p3-client-environment-current-continuation1409.txt`。

## Continuation 1410

补齐 P2 原生复制的 tagged GTID 基础互操作：`COM_BINLOG_DUMP_GTID` 现在
保留旧 Binary v0，同时解析 MySQL 8.4 的 tagged GTID Binary v1/v2，包括
TSID/tag、重复 UUID、varint 和压缩区间边界；当逻辑身份为规范
`uuid:tag` 时，native 输出发出事件类型 42。标签在内部按 `uuid:tag`
保留，避免把不同 tag 的事务合并到同一 GTID 空间。

新增测试先在旧实现上复现 v1/v2 请求被误判为 v0、最终报 interval truncated，
再验证两种编码均能得到正确的半开区间。`server/net` 全包回归通过，证据见
`reports/compatibility/p2-native-gtid-tagged-dump-set-current-continuation1410.txt`。

本轮只关闭 tagged GTID 请求解码和基础事件输出切片；仍未声称可产生全部官方
MySQL 8.4 tagged GTID 事务或通过外部 mysqld source/replica fixture。P2 native
binlog/GTID/XA/崩溃恢复/晋升互操作继续为 `partial`，官方 MySQL fixture 继续
为 P4 `pending_external`；完整 I_S/P_S 和非 Connector/J 客户端矩阵仍在全局任务中。

## Continuation 1411

补齐 native 文件头的 tagged previous-GTID 边界：无标签集合继续使用 Binary v0；
包含 tag 的集合改用 Binary v1，支持同一 UUID 下的有标签和无标签 TSID，并在
decoder 中恢复为独立的 `uuid:tag` 区间。超长 tag 也改为正确拒绝，符合 MySQL
8.4 的 tag 约束。

新增回归和完整 `server/replication`、`server/net` 回归通过，证据见
`reports/compatibility/p2-native-previous-gtid-tagged-set-current-continuation1411.txt`。

本轮仍未声称支持 Binary v2 previous-set 或通过外部 mysqld fixture；P2 原生
binlog/GTID/XA/崩溃恢复/晋升互操作继续为 `partial`，P4 官方 fixture 继续为
`pending_external`。

## Continuation 1412

补齐 SQL 层 tagged GTID 函数：`GTID_SUBSET()` 与 `GTID_SUBTRACT()` 现在识别
`uuid:tag:interval`，支持同一 tag 的多个区间以及同一 UUID 的多个 tag，并在
运算中保持 tag 隔离。此前 SQL 投影使用独立旧解析器，会把 tag 当成数字区间而
失败；plan 层红测、engine SQL 投影回归均已通过。

证据见
`reports/compatibility/p2-tagged-gtid-sql-functions-current-continuation1412.txt`。
这只关闭本地 SQL 函数切片，官方 source/replica、XA、崩溃恢复和 fixture 互操作
仍属于 P2/P4 未完成范围。

## Continuation 1413

修复复制层 GTID 文本集合解析：现在支持同一 UUID 下的无标签区间、多个 tag
区间组以及同一 tag 的多个区间，并按 `uuid:tag` 保持身份隔离。旧实现只能
识别第一个 tag。

证据见
`reports/compatibility/p2-tagged-gtid-text-parser-current-continuation1413.txt`。
这只关闭本地 GTID 文本解析切片，P2 官方 source/replica、XA、崩溃恢复和晋升
互操作仍未完成。

## Continuation 1414

补齐 native `PREVIOUS_GTIDS_EVENT` 的 Binary v2 decoder：支持 v2 header、48 位
TSID 数量、tag 表、TSID code 的 UUID 复用、serialization varlen 整数和 delta
区间边界（含首边界为 1 的优化），并将结果恢复为 `uuid:tag` 区间。官方 GTID
库当前仍只由 encoder 选择 v0/v1，因此本轮只扩展输入 decoder，不改变 writer
格式选择。

红测先确认旧实现返回 `unsupported tagged PREVIOUS_GTIDS_EVENT format 2`；单测、
`server/replication` 和 `server/net` 包级回归均已通过。证据见
`reports/compatibility/p2-native-previous-gtid-binary-v2-current-continuation1414.txt`。

这只关闭本地 Binary v2 previous-set 解码切片；官方 mysqld source/replica、XA、
崩溃恢复和晋升互操作仍未完成。

## Continuation 1415

收紧复制层 GTID 文本输入：拒绝序号 `0` 和反向区间，避免非法输入被
`AddRange` 静默丢弃后伪装成空集合。红测、tagged/multi-tag 回归及大区间保持
通过，证据见
`reports/compatibility/p2-gtid-text-invalid-boundary-current-continuation1415.txt`。

这只关闭本地 GTID 文本校验切片，官方 source/replica、XA、binlog、崩溃恢复和
晋升互操作仍未完成。

## Continuation 1416

完成一次官方 MySQL 8.4 与 xmysql 的非 Connector/J 客户端基线对照。修正了
共享客户端夹具中的多结果列别名，使其不再触发 MySQL 8.4 的保留窗口函数语法；
PyMySQL 和 Node mysql2 运行器也显式选择 `mysql` 数据库，使缺表错误场景与 Go
运行器一致。Go、PyMySQL、Node mysql2 在官方 MySQL 8.4 和 xmysql 两侧均通过
当前矩阵；本机没有 `mysql.exe`，所以 CLI 仍为环境跳过，不能宣称完整客户端矩阵
已完成。

证据见
`reports/compatibility/p3-client-baseline-official-vs-xmysql-current-continuation1416.txt`、
`reports/compatibility/p4-official-mysql84-client-matrix-rerun2/client-matrix-20260925-175836.json`
和
`reports/compatibility/xmysql-client-matrix-current-continuation1416/client-matrix-20260925-175849.json`。

该切片只关闭三种可用客户端的基础功能对照；mysql CLI、集群 endpoint 故障切换、
更多驱动版本/错误/字符集边界以及完整 I_S/P_S、XA/native binlog/崩溃恢复互操作
仍在全局任务中。FULLTEXT 延后，非 InnoDB 仍为范围外。

## Continuation 1417

用临时官方 `mysql:8.4` fixture 对 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 做了
表名级差分。官方 I_S 的 78 张表中，本地显式注册表只缺 6 张 InnoDB FULLTEXT
专用表：`INNODB_FT_BEING_DELETED`、`INNODB_FT_CONFIG`、
`INNODB_FT_DEFAULT_STOPWORD`、`INNODB_FT_DELETED`、`INNODB_FT_INDEX_CACHE`、
`INNODB_FT_INDEX_TABLE`；这与当前 FULLTEXT deferred 边界一致。官方 P_S 的 114
张表名均已出现在本地注册表中。

证据见 `reports/compatibility/p1-official-table-registry-diff-current-continuation1417.txt`。
这只是表名注册差分，不代表字段精度、运行时统计、组件生命周期或权限语义已完成，
所以 P1 I_S/P_S 仍保持 partial。

## Continuation 1418

接入临时官方 MySQL 8.4.11 fixture，完成一条真实 native binlog 输入链路：
新增 MySQL replication protocol source，保留原始事件帧，处理
`FORMAT_DESCRIPTION_EVENT` 前后的 checksum 边界，按 Query Event 的
`status_variables_length` 定位数据库字段，并支持官方 XA Query 中的
`X'hex'` XID 表达式。

官方 fixture 侧的普通提交事务和 `XA START/END/PREPARE/COMMIT` 事务均已通过：
`TestMySQLBinlogSourceReadsOfficialTransaction`、
`TestMySQLBinlogSourceReadsOfficialXATransaction` 和
`TestNativeXAQueryDecodesMySQLHexXIDLiterals`。证据见
`reports/compatibility/p4-official-mysql84-native-binlog-xa-current-continuation1418.txt`。

本轮只证明“官方 MySQL 8.4 -> xmysql”的 native 事件读取和 XA 帧解码，
不等于 xmysql -> 官方副本应用、GTID 重启、崩溃恢复/晋升或完整双向 XA/binlog
互操作已完成。因此 P2 native binlog/GTID/XA 仍为 partial，P4 官方 XA/binlog
互操作仍保留 pending_external；完整 I_S/P_S 表/字段/运行时/组件/权限语义和
完整非 Connector/J 客户端矩阵继续纳入全局任务。FULLTEXT 延后，非 InnoDB
引擎、修复和转换仍为范围外。

## Continuation 1424

P4 官方副本探针进一步暴露了一个更底层的 P0 协议问题：服务端在 MySQL
capability 协商前启用了 Getty raw flate，导致官方 MySQL 8.4 客户端把压缩流当成
MySQL packet header，停在 initial communication 阶段。现在 `CompressNone` 保持
原始 TCP reader/writer，服务端不再对所有新连接自动套用 Getty 压缩，默认
`compress_encoding` 改为关闭，并新增握手包头不被包装的回归。

`server/net`、`server/conf` 定向测试通过；同一官方副本探针在修复后已推进到
authentication 阶段，但本轮没有完成干净的 source-user provisioning，因此不能
把它写成官方 row/XA/GTID 复制通过。证据见
`reports/compatibility/p0-mysql-transport-compression-current-continuation1424.txt`。

这关闭了本地 MySQL transport handshake 缺陷；官方 mysqld 作为副本消费 xmysql、
双向 XA/binlog、崩溃恢复/晋升继续为 P4 external gate。完整 I_S/P_S 运行时/组件/
权限语义和完整非 Connector/J 客户端矩阵继续纳入全局任务；FULLTEXT 延后，非
InnoDB 引擎、修复和转换仍为范围外。

## Continuation 1425

本轮完成了官方 MySQL 8.4.11 副本对 xmysql 原生 binlog 的真实消费验证。
修复内容包括：`COM_BINLOG_DUMP` 首个响应包序号从 1 开始；Format Description
Event 按官方 8.4.11 的 UNKNOWN_EVENT 槽位、post-header 长度和 CRC32 描述符
编码；复制握手支持逗号分隔的 `SET @变量` 列表和 `@@GLOBAL.xxx`；裸
`VARCHAR` row metadata 不再使用当前值长度。

官方副本探针现已达到 `Replica_IO_Running=Yes`、`Replica_SQL_Running=Yes`，并
成功应用新建 `VARCHAR(64)` 表及后续插入，源位点追平到 1752。全量串行回归
`go test -p 1 ./... -count=1 -timeout 45m` 通过，engine 303.499s、net
8.523s、replication 6.298s。证据见
`reports/compatibility/p2-p4-official-replica-dump-current-continuation1425.txt`。

边界仍需明确：P2 仍是 `partial`，尚未完成官方副本对 XA 事件的完整应用、GTID
崩溃续传、晋升和重复投递验证；P4 仍保留为完整官方 XA/binlog 双向互操作门槛。
完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 的字段、运行时、组件和权限语义，
以及完整非 Connector/J 客户端矩阵继续纳入全局任务。FULLTEXT 延后；非 InnoDB
引擎、`REPAIR TABLE` 和引擎转换按已确认范围外处理。

## Continuation 1426

按官方 MySQL 8.4.11 fixture 对现有 Performance Schema 虚拟表做了第二轮
字段级元数据差分，补齐了可在当前范围内落地的 P1 元数据契约：ENUM/SET 的
`CHARACTER_MAXIMUM_LENGTH` 与 `utf8mb4` 字符集、TEXT 系列的官方最大长度，
以及 `NESTING_EVENT_TYPE`、`DIGEST`、`GTID`、等待事件数值/字符串字段等
具体类型和长度。规范化逻辑只作用于 `performance_schema` 虚拟列路径，避免
改变其他 INFORMATION_SCHEMA 元数据行为。

定向测试和完整 `TestInformationSchema|TestPerformanceSchema` 回归均通过；
证据见
`reports/compatibility/p1-performance-schema-official-metadata-current-continuation1426.txt`。

这只关闭了 P1 的一组字段元数据差异，I_S/P_S 的完整表行为、运行时统计、
组件语义、锁/等待细节和权限可见性仍保持 `partial`；P2 官方 XA/GTID/崩溃
恢复/晋升及双向互操作、P3 全量非 Connector/J 客户端矩阵继续纳入全局任务。
FULLTEXT 延后；非 InnoDB 引擎、`REPAIR TABLE` 和引擎转换仍为范围外。

## Continuation 1419

把已验证的 `mysql://` native binlog source 接入复制 runtime：replica 现在
可以从 MySQL COM_BINLOG_DUMP 读取物理帧，按持久化的文件/位置恢复连接，
通过 `ApplyNativeAtSource` 将 native relay、prepared-XA、executed GTID 和
source position 一起持久化。跨 pull batch 的未完成事务会先保存 relay，
收到 XID 后只应用一次；官方 MySQL EOF 返回的 synthetic ROTATE/
FORMAT_DESCRIPTION 前导帧不会触发错误重连循环；状态接口不会泄露 mysql
密码。

本地 split-transaction runtime 测试和官方 MySQL 8.4.11 runtime 端到端测试
均通过，普通事务与 XA source 测试也通过。证据见
`reports/compatibility/p2-native-runtime-source-apply-current-continuation1419.txt`。

本轮仍只证明“官方 MySQL -> xmysql runtime/apply”；xmysql -> 官方副本应用、
GTID auto-position、持续跨文件轮转、外部 mysqld 崩溃恢复/晋升和完整双向
XA/binlog 互操作仍未完成。P2/P4 继续保持 partial/pending_external；完整
I_S/P_S 语义、完整非 Connector/J 客户端矩阵继续纳入全局任务。FULLTEXT
延后，非 InnoDB 引擎、修复和转换仍为范围外。

## Continuation 1420

补齐 native MySQL runtime 的 GTID auto-position 重连切片：runtime 现在从
持久化 executed GTID 生成 MySQL interval set，并在事务已完整提交后使用
`COM_BINLOG_DUMP_GTID` 重连。若上一次 pull 在事务中间结束，则暂时回到文件/位置
继续完成 relay，避免 GTID dump 重播事务前缀。另修正 native decoder 对官方 MySQL
自提交 DDL `GTID_EVENT + QUERY_EVENT`（没有 `XID_EVENT`）的边界识别。

官方 MySQL 8.4.11 runtime 测试已验证首个事务提交后重新用 GTID 定位并接收第二个
事务；decoder 单测、replication/net/InnoDB 相关回归和全仓串行回归均通过。证据见
`reports/compatibility/p2-native-gtid-auto-position-current-continuation1420.txt`。

本轮仍不宣称完整 GTID 崩溃重启、多文件持续轮转、外部 mysqld 晋升、xmysql ->
官方副本应用或双向 XA/binlog 互操作完成，因此 P2/P4 仍为 partial/pending_external。
完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表/字段/运行时/组件/权限语义和完整
非 Connector/J 客户端矩阵继续纳入全局任务；FULLTEXT 延后，非 InnoDB 引擎、修复和
转换仍为范围外。

## Continuation 1421

按官方 MySQL 8.4.11 fixture 做了字段级差分，补齐了两个可在当前开源范围内
落地的 Performance Schema 形状缺口：`events_waits_current/history/history_long`
新增 `OBJECT_INSTANCE_BEGIN`、`NESTING_EVENT_ID`、`NESTING_EVENT_TYPE`，并将
`table_lock_waits_summary_by_table` 从旧的 23 列扩展到官方 68 列。新增列已同时
接入 dedicated `SELECT *` 投影和 `INFORMATION_SCHEMA.COLUMNS` 类型/无符号/可空
元数据；旧 `COUNT_MISC` 仅保留显式查询别名，不再污染 MySQL 8.4 的发现形状。

当前锁管理器只能区分 shared/read 与 exclusive/write，因此映射到官方的
`*_NORMAL` 桶，其他优先级、external、concurrent-insert 等细分桶暂返回零，
等待运行时的细粒度语义仍未完成。engine 全量回归通过。证据见
`reports/compatibility/p1-performance-schema-field-shapes-current-continuation1421.txt`。

本轮只关闭 P1 的一个字段契约切片；完整 I_S/P_S 表/字段/运行时/组件/权限语义、
完整非 Connector-J 客户端矩阵、xmysql -> 官方副本应用、GTID 崩溃重启/晋升和
双向 XA/binlog 互操作仍在全局任务中。FULLTEXT 延后，非 InnoDB 引擎、修复和转换
仍为范围外。

## Continuation 1422

用官方 MySQL 8.4.11 fixture 验证并修复了 native runtime 的持续轮转/重启边界：
第一事务提交后执行 `FLUSH BINARY LOGS`，第二事务写入新文件，停止并重建 xmysql
replica，再写入第三事务，三者均成功应用。根因是重连后的 MySQL dump 可能只发送
ROW_EVENT 而不重复 TABLE_MAP；当前实现现在单独持久化 TABLE_MAP 字典，并在 native
解码失败时从当前文件位置 4 重放已执行 GTID，以重建字典而不重复提交。

证据见 `reports/compatibility/p2-native-rotation-restart-current-continuation1422.txt`。
本轮关闭了 P2 的“持续跨文件轮转 + replica 重启”切片，但外部 mysqld 晋升、
xmysql -> 官方副本应用和完整双向 XA/binlog 互操作仍未完成；P2/P4 继续保持
partial/pending_external。完整 I_S/P_S 语义和完整非 Connector-J 客户端矩阵继续
纳入全局任务；FULLTEXT 延后，非 InnoDB 引擎、修复和转换仍为范围外。

## Continuation 1427

按官方 MySQL 8.4.11 fixture 复现并修复了 native runtime 的 TABLE_MAP 重放边界：
较新的缓存映射不会被较旧 relay 映射覆盖；强制文件重放会清空持久化 TABLE_MAP
缓存，并跳过持久化 source position 之前的历史 ROWS_EVENT/事务。

官方 MySQL 8.4.11 普通 native 事务、XA native 事务、runtime 连续拉取、binlog
轮转和 replica restart 四项集成测试全部通过；完整串行回归也通过。证据见
`reports/compatibility/p2-native-table-map-replay-current-continuation1427.txt`。

本轮关闭 P2 的 native TABLE_MAP 重放/轮转切片；官方 XA 双向互操作、外部 mysqld
崩溃恢复/晋升、重复投递和完整双向复制仍未完成。完整 I_S/P_S 语义与非
Connector-J 客户端矩阵继续纳入全局任务；FULLTEXT 延后，非 InnoDB 引擎、修复
和转换仍为范围外。

## Continuation 1428

修复了 `performance_schema.setup_objects` 更新条件只匹配
`OBJECT_TYPE` 的缺口。现在 `OBJECT_TYPE`、`OBJECT_SCHEMA`、`OBJECT_NAME`
三个已提供的等值或 `LIKE` 条件会共同匹配配置行；例如
`TABLE/app/orders` 不会错误更新官方默认的 `TABLE/%/%` 行。定向测试和
完整 `TestInformationSchema|TestPerformanceSchema` engine 回归均通过，
证据见
`reports/compatibility/p1-performance-schema-setup-object-predicate-current-continuation1428.txt`。

这只关闭了 P1 setup_objects 条件匹配的一项错误；自定义行的 INSERT/DELETE、
对象模式优先级，以及其余 I_S/P_S 运行时、组件和权限语义仍未完成。P2 官方
XA/binlog/GTID/崩溃恢复/晋升双向互操作和 P3 全量非 Connector/J 客户端矩阵
继续保留；FULLTEXT 延后，非 InnoDB 引擎、修复和转换仍为范围外。

## Continuation 1429

继续完成了 `performance_schema.setup_objects` 的本地生命周期：支持自定义
行 INSERT/DELETE、官方 20 行初始配置、精确对象/精确 schema 通配/全局通配
的优先级、`TRUNCATE TABLE`、只允许更新 `ENABLED`/`TIMED`，并补充
INSERT/DELETE 权限检查。TABLE 规则现在会影响 table I/O 和 table-lock
summary 的可见性及计时投影。定向、权限和完整 `TestPerformanceSchema`
回归均通过，证据见
`reports/compatibility/p1-performance-schema-setup-object-lifecycle-current-continuation1429.txt`。

P1 的其余 I_S/P_S 运行时、组件、锁/等待细分和权限语义仍未完全完成；P2
 native 复制提交边界、P3 全量非 Connector/J 客户端矩阵和 P4 官方 MySQL
 双向 XA/binlog/GTID/崩溃恢复/晋升验收继续保留。FULLTEXT 延后，非 InnoDB
 引擎、修复和转换仍为范围外。

## Continuation 1430

扩展了共享的非 Connector/J 客户端矩阵：增加 metadata 列形状和大小写容忍、
AUTO_INCREMENT 的 insert id/affected rows、SAVEPOINT 部分回滚、会话用户变量
状态四类场景，Go、PyMySQL、Node/mysql2 三个可用客户端现在均通过共同的 13
项本地矩阵。证据见
`reports/compatibility/p3-client-matrix-current-continuation1430.txt`。

当前机器没有 `mysql.exe`，因此 mysql CLI 仍为 `SKIPPED_ENVIRONMENT`；这轮不把
三种客户端的本地通过误判为完整客户端生态兼容。TLS/认证变体、连接池/负载均衡、
其他语言驱动以及 P2/P4 官方 MySQL 双向 XA/binlog/GTID/崩溃恢复/晋升验收继续
保留。FULLTEXT 延后，非 InnoDB 引擎、修复和转换仍为范围外。

## Continuation 1423

在 native binlog 源端审计中发现 Format Description Event 的 8.4 事件表仍是
旧的 40 项布局，而且 post-header 长度把 `UNKNOWN_EVENT` 当成了数组项，最后也
没有显式写入 CRC32 checksum algorithm descriptor。这样会使官方副本在 QUERY、
TABLE_MAP、row、GTID 或 tagged GTID 事件上按错误边界解析。

现在 writer 和 MySQL 协议 dump 两条生成路径统一输出官方 MySQL 8.4.11 的
Format Description Event 表，保留 UNKNOWN_EVENT 槽位并按官方 event type 索引
写入 post-header 长度，同时在尾部写入 CRC32 descriptor。`server/replication` 和
`server/net` 全量包回归通过，证据见
`reports/compatibility/p2-native-format-description-current-continuation1423.txt`。

这关闭了本地 native FDE 协议声明缺陷，但仍不等于官方 mysqld 已成功作为副本
应用 xmysql 的 row/XA/tagged-GTID 流；xmysql -> 官方副本、双向 XA/binlog 和
外部崩溃恢复/晋升继续保留为 P4 external gate。完整 I_S/P_S 运行时/组件/权限
语义和完整非 Connector/J 客户端矩阵继续纳入全局任务；FULLTEXT 延后，非 InnoDB
引擎、修复和转换仍为范围外。

## Continuation 1431

使用临时官方 `mysql:8.4.11` fixture 新鲜复跑了普通事务、XA 事务、native
runtime 拉取以及 binlog 轮转/replica restart 四项 source-to-xmysql 集成测试，
全部通过（1.192 秒）。证据见
`reports/compatibility/p2-official-mysql-source-current-continuation1431.txt`。

这只关闭了官方 MySQL 作为 source 的读取/解码/运行时门禁；xmysql 作为 source
向官方 MySQL 应用 XA、GTID 崩溃续传、晋升、重复投递和完整双向 XA/binlog 仍未
完成。P1/P3 其余语义继续推进；FULLTEXT 延后，非 InnoDB 引擎、修复和转换仍为
范围外。

## Continuation 1435

用临时官方 MySQL 8.4.11 fixture 对 Performance Schema 共享表做了新鲜的字段级
差分，比较列名、类型、可空性、索引键、默认值、extra、字符集和排序规则。官方
114 张表与本地共同的 114 张表全部匹配，官方缺失 0 张，字段级差异 0 张；本地
额外的 16 张是 Clone、Firewall、NDB、组件、Thread Pool 或旧版 `setup_timers`
表，不是官方共享表缺失。完整 `TestInformationSchema|TestPerformanceSchema`
engine 回归通过（73.726 秒）。证据见
`reports/compatibility/p1-official-performance-schema-metadata-current-continuation1435.txt`。

本轮关闭的是 P1 的官方 P_S 共享元数据契约，不是完整 P1：Information Schema
全表/字段/运行时值差分、P_S 组件和权限运行时语义仍需继续；xmysql -> 官方
副本的 XA/binlog、GTID 崩溃续传、晋升、重复投递及完整双向互操作，以及全量
非 Connector/J 客户端矩阵仍未完成。FULLTEXT 延后，非 InnoDB 引擎、修复和
转换仍为范围外。

## Continuation 1442

复制包全量回归通过（7.218 秒）。引擎聚焦门禁首次发现一条落后的
`XA COMMIT ... ONE PHASE` 测试断言：生产 writer 已按官方生命周期发出 8 个 native
事件（包含 `XA START`/`XA END` QUERY_EVENT 与 type-38 `XA_PREPARE`），测试仍期待 6 个。
更新断言后，XA one-phase、`USER_ATTRIBUTES` MySQL 8.4 可见性和 partial-revoke 元数据
聚焦门禁通过（3.229 秒）。另一次引擎全量尝试因 Windows Go runtime
`VirtualAlloc errno=1455` 内存不足中止，不能计为全量 PASS。

该切片修正测试契约并确认局部实现没有回退，不改变全局状态：P1 完整
`INFORMATION_SCHEMA`/`PERFORMANCE_SCHEMA` 运行时、权限、生命周期和组件语义仍为
partial；P2/P4 的统一提交边界、GTID/applied-marker 崩溃恢复、提升/重复投递及官方
双向 fixture 仍为 partial；P3 完整非 Connector/J 矩阵仍为 partial。`FULLTEXT` 继续
deferred；非 InnoDB 引擎、非 InnoDB 专用 `REPAIR TABLE` 和引擎转换继续 out of scope。

## Continuation 1443

补齐原生 MySQL 复制的 GTID 自动定位启动/重启边界：`mysql://` source 现在支持
`gtid_set` 参数；启用 GTID auto-position 时可以不提供 `binlog_file`，并会在打开
stream 前校验 GTID 集。副本重启时从持久化的 `Executed` 集恢复合法 MySQL UUID 的
区间，优先使用 `COM_BINLOG_DUMP_GTID`，不再依赖陈旧的文件/位置对。初次 GTID
stream 尚未获知物理文件名时，位置和 GTID 仍可一起持久化，待后续事件提供文件名。
同时修正 native source 状态接口，完全移除 MySQL source URL 的 userinfo，避免在状态
接口暴露用户名。临时官方 MySQL 8.4.11 Docker fixture 上，普通事务、XA、运行时连续
拉取以及跨 binlog 轮转/重启测试均通过；定向测试和整个 `server/replication` 包均通过，证据见
`reports/compatibility/p2-native-gtid-auto-position-current-continuation1443.txt`。

该切片只关闭 GTID 自动定位的配置与本地重启恢复缺口；P2/P4 的统一
storage/WAL/native-binlog/GTID/applied-marker 崩溃协议、完整提升/切换、xmysql 到官方
MySQL 的反向 fixture 和完整双向 fixture 仍保持 partial。P1/P3、`FULLTEXT` 和非
InnoDB 范围边界不变。

## Continuation 1444

新鲜的 xmysql -> 官方 MySQL 8.4.11 反向 fixture 已完成普通事务、XA 事务和源端
重启后的继续复制：官方副本最终得到且仅得到 `1 ordinary`、`2 xa`、
`3 after-restart-fixed` 三行；`Replica_IO_Running=Yes`、`Replica_SQL_Running=Yes`、
`Auto_Position=1`，I/O/SQL 错误号均为 0，拉取与执行 GTID 集一致。证据见
`reports/compatibility/p2-p4-xmysql-to-official-reverse-restart-current-continuation1444.txt`。

该 fixture 暴露并修复了一个真实的重启缺口：持久化表定义存在于磁盘，但重启后没有
恢复到内存 table-storage mapping，导致重启后的首个写入报表不存在。新增
`TestEngineReloadsPersistedDictionaryTablesIntoStorageMappingAfterRestart` 红绿测试；
修复后定向引擎测试（2.039 秒）和完整 `server/replication` 回归（9.156 秒）均通过。

本轮只关闭反向普通/XA/优雅重启续传切片；crash-kill 恢复、统一 storage/WAL/native-binlog
崩溃协议、完整提升/切换、重复投递抑制和完整双向官方互操作仍为 P2/P4 partial。
P1/P3、`FULLTEXT` 和非 InnoDB 范围边界不变。

## Continuation 1445

运行三轮真实进程级 crash-recovery drill：服务进程在未提交事务保持期间被强制停止，
随后重启并校验已提交数据、未提交数据回滚、prepared-statement redo 数据以及 DDL/index
元数据。三轮均通过；证据见
`reports/compatibility/p2-external-crash-recovery-current-continuation1445.txt` 和
`reports/compatibility/external-crash-current-continuation1445-r3/external-crash-20260927-064228.json`。

同时移除了 crash drill 脚本及客户端默认 DSN 中的硬编码测试密码，开发旁路认证改用空密码。
本轮关闭的是外部 storage crash-recovery 证据切片，不代表统一 storage/WAL/native-binlog/
GTID/applied-marker 提交协议、所有复制故障窗口、完整提升/切换、重复投递抑制或官方
MySQL 双向互操作已经完成；这些仍为 P2/P4 partial。

## Continuation 1446

官方 MySQL 8.4.11 反向 fixture 新增 crash/reconnect 验证：xmysql 源端在官方副本
连接期间被强制终止，随后使用同一数据目录启动并让副本重新连接。普通事务和 XA 事务
均保留，重连后的新事务也成功复制；官方副本最终只有 `1 before-crash`、
`2 xa-before-crash`、`3 after-crash-restart` 三行，复制线程为 `Yes/Yes`，I/O/SQL
错误号均为 0。证据见
`reports/compatibility/p4-official-mysql-reverse-crash-reconnect-current-continuation1446.txt`。

该切片关闭 xmysql -> 官方 MySQL 的普通/XA crash/reconnect 证据；所有 storage/WAL/
native-binlog/GTID/applied-marker 故障窗口、重复投递边界、完整提升/切换和完整双向
官方互操作仍保持 P2/P4 partial。

## Continuation 1447

将外部 crash-recovery matrix 中已有但未被调用的 `race_commit` 故障模式接入真实
三轮 drill：服务在提交中途被强制停止，重启后验证该主键最多一行，同时确认已提交数据、
未提交回滚、redo 及 DDL/index 元数据仍正确。三轮均通过，证据见
`reports/compatibility/p2-external-crash-race-current-continuation1447.txt`。

本轮增强 storage-side interrupted-commit 证据；native binlog/GTID/applied-marker
的全部 crash 位置、重复投递重试、完整提升/切换及官方 MySQL 双向互操作仍为 P2/P4
partial。

## Continuation 1448

当前工作树的完整 `TestInformationSchema|TestPerformanceSchema` 聚焦回归通过（74.517 秒）。
同轮 P3 客户端矩阵在认证参数保护检查处停止：没有提供受保护的
`XMYSQL_CLIENT_PASSWORD`，因此没有客户端真正执行，本轮状态保持 `unverified`，不计为
PASS。证据见 `reports/compatibility/p1-p3-current-gate-continuation1448.txt`。

## Continuation 1449

使用进程内受保护认证参数运行当前 P3 基线：mysql CLI（Docker）、Go、PyMySQL、Node.js/mysql2
的单节点矩阵全部通过；集群端点矩阵中 source 与 promoted replica 两端的四个客户端全部
通过，且 source catch-up、source stop、replica promotion 和切换后客户端行为均通过。证据见
`reports/compatibility/p3-client-cluster-current-continuation1449.txt`。

这只关闭当前认证客户端/促销端点基线；旧连接失效、重试边界、连接池读写路由、TLS 和完整
CLI 管理命令矩阵仍保持 P3 partial。

## Continuation 1450

在当前工作树上重新尝试完整 `server/innodb/engine` 回归：`go test` driver 和绕过
driver 输出缓冲的独立测试二进制都在执行中触发 Windows `VirtualAlloc errno=1455`，后者
发生在 `TestPerformanceSchemaStatementDigestUsesObservedLatencyQuantiles`。因此完整 engine
仍不能计为 PASS；当前 I_S/P_S 聚焦套件 74.517 秒通过，证据及边界见
`reports/compatibility/full-engine-regression-resource-gate-current-continuation1450.txt`。

## Continuation 1451

本轮修复并验证引擎关闭生命周期：`XMySQLEngine.Close()` 现在会停止并等待
Insert Buffer 后台合并线程，同时尝试关闭其拥有的字典、索引、信息架构、加密、
压缩和存储管理器；`IndexManager.Close()` 修复了持锁刷新导致的死锁；字典管理器
对未初始化根页包装器的嵌入式实例执行安全清理。引擎生命周期定向回归（2.626 秒）、
manager 生命周期回归（0.810 秒）、当前 I_S/P_S 聚焦回归（73.915 秒）和完整
`server/replication` 回归（6.659 秒）均通过，证据见
`reports/compatibility/engine-lifecycle-current-continuation1451.txt`。

完整 `server/innodb/engine` 回归再次在当前 Windows 主机触发
`VirtualAlloc errno=1455`，仍不能计为全量 PASS；P1 全量运行时/权限/组件语义、
P2/P4 统一崩溃协议与提升/双向互操作、P3 完整边界矩阵继续保持 partial。
FULLTEXT 继续 deferred，非 InnoDB 引擎/修复/转换继续 out of scope。

## Continuation 1452

修复旧版 `BufferPool` 的关闭生命周期：关闭时清空 LRU、释放预分配的页帧，
并由 `StorageManager.Close()` 在优化版 BufferPool 刷新/停止后调用。新增的
BufferPool/StorageManager 定向回归通过；随后当前工作树上的完整
`server/innodb/engine` 串行回归通过（274.586 秒）。前一轮在同一主机触发的
Windows Go runtime `VirtualAlloc errno=1455` 已不再复现，证据见
`reports/compatibility/full-engine-regression-current-continuation1452.txt`。

本轮关闭的是引擎资源释放和全引擎回归门禁，不改变功能范围分类：P1 的完整
Information Schema/Performance Schema 运行时、权限和组件语义，P2/P4 的原生复制
崩溃窗口、提升/切换、重复投递和完整官方双向互操作，P3 的完整客户端边界矩阵仍为
partial；FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB 专用修复和引擎转换继续
out of scope。

## Continuation 1453

同步 P3 矩阵状态：当前认证 Docker fixture 已证明 mysql CLI、Go、PyMySQL、Node.js/mysql2
在单节点和 source/promoted-replica 端点均通过，因此单项 `mysql-cli` 从 `unverified`
更正为 `implemented`；完整非 Connector/J 聚合项仍为 `partial`，因为连接池路由、旧
连接失效/重试、TLS、管理命令和更广客户端版本边界还未关闭。证据见
`reports/compatibility/p3-scope-status-current-continuation1453.txt`。

## Continuation 1454

旧版 BufferPool 生命周期修复后的全仓 Go 串行回归 `go test -p 1 ./... -count=1
-timeout 90m` 通过，退出码为 0；engine、manager、net、replication 和 metrics
包均在同一轮通过。证据见
`reports/compatibility/full-go-regression-current-continuation1454.txt`。

## Continuation 1455

重新审计官方 MySQL 8.4.11 Performance Schema 元数据差异：114 张官方表全部存在，
114 张共享表的字段、类型、可空性、键、默认值、额外属性、字符集和排序规则均无差异；
本轮将 scope matrix 中的 P_S `complete-table-registry` 从旧的 `partial` 更正为
`implemented`。这只代表官方共享表注册和元数据契约已完成，不代表 P_S 的运行时值、
权限、组件和生命周期语义全部完成。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1455.json`；原始差异证据见
`reports/compatibility/p1-official-performance-schema-metadata-current-continuation1435.txt`。

当前在范围内仍有 8 项 partial：I_S 完整注册（仅剩 FULLTEXT 相关官方表，按约定 deferred）、
I_S 权限/角色可见性、P3 完整非 Connector/J 客户端边界、集群端点客户端场景、P2 原生
XA/binlog、native binlog/GTID、崩溃恢复/提升，以及 P4 官方 MySQL XA/binlog 双向互操作。
FULLTEXT 继续 deferred；非 InnoDB 引擎、非 InnoDB 专用修复和引擎转换继续 out of scope。

## Continuation 1456

P_S 回归 `go test -p 1 ./server/innodb/engine -run 'TestPerformanceSchema' -count=1
-timeout 20m` 通过，耗时 26.959 秒。证据见
`reports/compatibility/p1-performance-schema-registry-current-continuation1456.txt`。
该回归支持 P_S 注册/运行时切片的当前结论，但不关闭更广泛的组件、权限、生命周期，
也不改变 P2/P3/P4 外部门禁状态。

## Continuation 1460

补齐 `performance_schema.error_log` 的 DDL 语义：MySQL 8.4 明确禁止对该表执行
`TRUNCATE TABLE`，xmysql 现在在通用物理表路径之前返回明确的“不允许”错误；错误日志
仍是独立的内存环形缓冲区，摘要表的截断行为不受影响。红测、P_S 相关测试和完整
engine 回归均通过。证据见
`reports/compatibility/p1-performance-schema-error-log-truncate-current-continuation1460.txt`。

## Continuation 1461

将 error_log DDL 语义证据同步到 scope matrix；总体状态仍为 15 项 implemented、8 项
partial、1 项 deferred、2 项 out-of-scope，P_S 元数据完成不等同于 P_S 全部运行时/组件
语义完成。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1461.json`。

## Continuation 1457

修复虚拟 `mysql.*` grant table 的权限分发缺口：元数据处理在普通 SELECT 权限检查之前
直接返回，导致非 root 会话可以无 SELECT 权限读取 `mysql.user` 等授权表。现在对
`user`、`db`、`tables_priv`、`columns_priv`、`procs_priv`、`proxies_priv`、
`global_grants`、`role_edges`、`default_roles` 统一执行表级 SELECT 检查。红测先复现
越权读取，随后定向测试、相关权限/角色测试及完整 engine 回归均通过。证据见
`reports/compatibility/p1-mysql-grant-table-privilege-dispatch-current-continuation1457.txt`。

该切片只关闭 grant-table 虚拟分发权限边界；完整 I_S/P_S 运行时/组件/权限语义、P2/P3/P4
外部互操作仍未全部完成。

## Continuation 1458

grant-table 权限分发修复后的全仓 Go 串行回归通过：`go test -p 1 ./... -count=1
-timeout 90m`。engine 238.342 秒、manager 7.527 秒、net 8.138 秒、replication
5.386 秒、metrics 0.542 秒，所有报告包均通过。证据见
`reports/compatibility/full-go-regression-current-continuation1458.txt`。

## Continuation 1459

将 1457/1458 的权限分发修复和全仓回归证据同步到矩阵；当前统计仍为 15 项
implemented、8 项 partial、1 项 deferred、2 项 out-of-scope，权限/角色聚合项继续
保持 partial。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1459.json`。

## Continuation 1462

补齐 `performance_schema.threads` 的 SELECT/UPDATE 权限边界：普通会话读取该表
必须拥有表级 `SELECT`，修改 `INSTRUMENTED`/`HISTORY` 必须拥有表级 `UPDATE`；
无会话的内部投影和显式授权会话保持可用。红测先复现无权限也能读取/修改，
修复后线程定向测试、P_S 相关回归及完整 engine 回归均通过。证据见
`reports/compatibility/p1-performance-schema-threads-update-privilege-current-continuation1462.txt`。

该切片只关闭一个 P_S 运行时权限边界；完整 I_S/P_S 运行时、组件生命周期、
P2/P3/P4 外部互操作仍保持未完成。FULLTEXT 继续 deferred；非 InnoDB 引擎、
非 InnoDB 修复和引擎转换继续 out of scope。

## Continuation 1463

将线程更新权限切片同步到 scope matrix；当前统计仍为 15 项 implemented、8 项
partial、1 项 deferred、2 项 out-of-scope。P_S 的官方共享表与元数据已完成，
但完整运行时、组件生命周期和权限聚合仍保持 partial。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1463.json`。

## Continuation 1464

修正 1462 证据范围：本轮同时关闭 `performance_schema.threads` 的 SELECT 和
UPDATE 权限校验，完整 engine 回归为 238.314 秒；矩阵统计不变，仍不把 P_S
全部运行时/组件/生命周期语义误报为完成。

## Continuation 1465

重新生成权限切片后的矩阵，统计仍为 15 项 implemented、8 项 partial、1 项
deferred、2 项 out-of-scope。`performance_schema.threads` 的 SELECT/UPDATE
权限证据已纳入 P_S 条目；其余全局 P1/P2/P3/P4 边界保持原状态。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1465.json`。

## Continuation 1466

线程 SELECT/UPDATE 权限修复后的全仓 Go 串行回归通过：`go test -p 1 ./... -count=1
-timeout 90m`，engine 239.029 秒、manager 7.609 秒、net 8.169 秒、replication
5.531 秒、metrics 0.465 秒，其余包全部通过。证据见
`reports/compatibility/full-go-regression-current-continuation1466.txt`。

## Continuation 1467

将全仓回归证据同步到矩阵；统计仍为 15 项 implemented、8 项 partial、1 项
deferred、2 项 out-of-scope。全仓测试通过不改变 P1 完整运行时/组件语义以及
P2/P3/P4 外部门禁的 partial 状态。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1467.json`。

## Continuation 1468

补齐 P_S 统一 SELECT 权限入口：持久化普通账户读取直接映射的 P_S 表前必须拥有
表级 `SELECT`；`performance_schema.processlist` 保持独立的 PROCESS 可见性规则，
内部、root、无会话和未持久化合成会话不受误伤。P_S 全族和完整 engine 回归通过。
证据见
`reports/compatibility/p1-performance-schema-select-privilege-current-continuation1468.txt`。

## Continuation 1469

统一 P_S SELECT 权限后的全仓 Go 串行回归通过：engine 239.237 秒、manager 7.731
秒、net 8.088 秒、replication 5.476 秒、metrics 0.439 秒，其余包全部通过。证据见
`reports/compatibility/full-go-regression-current-continuation1469.txt`。

## Continuation 1470

将统一 P_S SELECT 权限和全仓回归证据同步到矩阵；统计仍为 15 项 implemented、8
项 partial、1 项 deferred、2 项 out-of-scope。P_S 表级权限切片已补齐，但 P1
完整运行时、组件生命周期和复杂多表权限语义仍未全部完成。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1470.json`。

## Continuation 1471

补齐 `INFORMATION_SCHEMA.ROUTINES/PARAMETERS` 的按 routine 权限可见性：
DEFINER、全局 `SELECT`、`SHOW_ROUTINE` 可查看定义；覆盖对象的 `CREATE ROUTINE`、
`ALTER ROUTINE`、`EXECUTE` 可查看元数据行但 `ROUTINE_DEFINITION` 返回 `NULL`；
无权限用户隐藏对应行。`SHOW PROCEDURE/FUNCTION STATUS` 和 `SHOW CREATE` 的既有
`SHOW_ROUTINE` 边界保持不变。证据见
`reports/compatibility/p1-information-schema-routines-privilege-current-continuation1471.txt`。

## Continuation 1472

routine 元数据权限修复后的全仓 Go 串行回归通过：`go test -p 1 ./... -count=1
-timeout 90m`；engine 239.726 秒、manager 7.630 秒、net 8.225 秒、replication
5.307 秒、metrics 0.463 秒，测试进程确认退出。证据见
`reports/compatibility/full-go-regression-current-continuation1472.txt`。

## Continuation 1473

将 routine 权限和全仓回归证据同步到矩阵；当前统计为 16 项 implemented、7 项
partial、1 项 deferred、2 项 out-of-scope。P1 的完整 I_S/P_S 运行时、组件生命周期
和所有权限语义仍未全部完成；P2/P3/P4 外部门禁保持 partial。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1473.json`。

## Continuation 1474

补齐 `INFORMATION_SCHEMA.VIEW_TABLE_USAGE` / `VIEW_ROUTINE_USAGE` 的权限语义：
usage 表不再错误复用 `VIEWS` 的 `SHOW VIEW` 门槛，而是要求账户分别对 view、
引用的表/视图或存储函数拥有某项有效权限；无源对象权限的 usage 行被过滤。
`VIEWS` 本身仍保持 `SHOW VIEW` 语义。证据见
`reports/compatibility/p1-information-schema-view-usage-privilege-current-continuation1474.txt`。

## Continuation 1475

view usage 权限修复后的全仓 Go 串行回归通过：`go test -p 1 ./... -count=1
-timeout 90m`；engine 240.253 秒、manager 7.117 秒、net 8.134 秒、replication
5.391 秒、metrics 0.451 秒，测试进程确认退出。证据见
`reports/compatibility/full-go-regression-current-continuation1475.txt`。

## Continuation 1476

将 view usage 权限和全仓回归证据同步到矩阵；当前统计仍为 16 项 implemented、7 项
partial、1 项 deferred、2 项 out-of-scope。P1 的完整 I_S/P_S 运行时、组件生命周期
和全部权限语义仍未全部完成；P2/P3/P4 外部门禁保持 partial。当前矩阵见
`reports/compatibility/scope-matrix-current-continuation1476.json`。

## Continuation 1477

补做一次不带凭据的客户端环境审计：Go、PyMySQL、Node mysql2 运行器可用；本机没有
`mysql.exe`，Docker CLI fallback 未启用。客户端真实认证矩阵因保护性密码环境变量
`XMYSQL_CLIENT_PASSWORD` 未设置而未启动，因此该结果只能标记为环境未完成，不能当作
兼容性失败或通过。集群端点客户端探针同样确认 Go/PyMySQL/Node 可用，但需要保护性
认证环境才能执行真实 source/promoted endpoint 场景。证据见
`reports/compatibility/client-matrix-diagnostic-current-continuation1477/client-matrix-20260927-105556.json`
和
`reports/compatibility/client-cluster-endpoint-diagnostic-current-continuation1477/client-cluster-endpoint-20260927-105631.json`。

本轮不改变 P3 partial 状态，也不把未设置凭据归因于服务端兼容性；FULLTEXT 继续
deferred，非 InnoDB 引擎、非 InnoDB 修复和引擎转换继续 out of scope。

## Continuation 1478

补齐 `INFORMATION_SCHEMA.SCHEMATA` 对空数据库的发现语义：数据库目录存在但尚无
表或 `.frm` 元数据时，拥有全局 `SELECT` 的用户现在可以看到该 schema；无匹配权限
仍被 `schemaMetadataVisibleToSession` 过滤，partial revoke 规则保持有效。红测先复现
旧实现返回空行，修复后定向测试、全部 `TestInformationSchema` 和全仓 Go 串行回归均
通过。证据见
`reports/compatibility/p1-information-schema-schemata-empty-database-current-continuation1478.txt`。

## Continuation 1479

将本轮 P1 修复、客户端环境诊断和全仓回归证据重新生成到统一范围矩阵；分类仍为
16 项 implemented、7 项 partial、1 项 deferred、2 项 out-of-scope。矩阵见
`reports/compatibility/scope-matrix-current-continuation1478.json`。

## Continuation 1480

修复 `INFORMATION_SCHEMA.ST_GEOMETRY_COLUMNS` 的对象可见性：无表级权限的账户不再
看到其他用户表的空间列；授予目标表 `SELECT` 后可看到对应 geometry 列。相关定向
权限测试、全部 `TestInformationSchema` 和全仓 Go 串行回归均通过。证据见
`reports/compatibility/p1-information-schema-st-geometry-visibility-current-continuation1480.txt`。

## Continuation 1481

将本轮空间列权限修复和全仓回归证据同步到范围矩阵；分类仍为 16 项 implemented、
7 项 partial、1 项 deferred、2 项 out-of-scope。矩阵见
`reports/compatibility/scope-matrix-current-continuation1480.json`。

## Continuation 1482

修复 `INFORMATION_SCHEMA.COLUMN_STATISTICS` 的表级权限可见性：无权限账户不再看到
其他用户表的 ANALYZE histogram，授予目标表 `SELECT` 后恢复对应统计行。专项测试、
全部 `TestInformationSchema` 与全仓 Go 串行回归均通过。证据见
`reports/compatibility/p1-information-schema-column-statistics-visibility-current-continuation1482.txt`。

## Continuation 1483

将 histogram 权限修复和最新全仓回归证据同步到统一范围矩阵；分类仍为 16 项
implemented、7 项 partial、1 项 deferred、2 项 out-of-scope。矩阵见
`reports/compatibility/scope-matrix-current-continuation1482.json`。

## Continuation 1484

修复 `INFORMATION_SCHEMA.PARTITIONS` 的表级权限可见性：无权限账户不再看到其他
用户表的分区/表统计行，授予目标表 `SELECT` 后恢复可见。I_S 整族测试和全仓 Go
串行回归均通过。证据见
`reports/compatibility/p1-information-schema-partitions-visibility-current-continuation1484.txt`。

## Continuation 1485

将分区元数据权限修复和最新全仓回归证据同步到统一范围矩阵；分类仍为 16 项
implemented、7 项 partial、1 项 deferred、2 项 out-of-scope。矩阵见
`reports/compatibility/scope-matrix-current-continuation1484.json`。

## Continuation 1486

按官方 MySQL 8.4 语义补齐 `INFORMATION_SCHEMA.FILES` 的 `PROCESS` 权限门禁：
普通账号查询该表现在返回权限错误，具备 `PROCESS` 的会话继续进入文件/表空间
元数据投影。专项测试、全部 `TestInformationSchema` 与全仓 Go 串行回归均通过；
证据见 `reports/compatibility/p1-information-schema-files-process-privilege-current-continuation1486.txt`。

本条只关闭一个明确的 P1 权限语义缺口，I_S/P_S 的运行时、组件、生命周期和剩余
权限语义仍保持 partial；P2/P3/P4 外部兼容门禁仍未完成。FULLTEXT 继续 deferred，
非 InnoDB 引擎、非 InnoDB 修复和引擎转换继续 out of scope。

## Continuation 1487

按官方 MySQL 8.4 语义补齐 `INFORMATION_SCHEMA.FILES` 的 NULL/空字符串字段投影，
并修复新表创建时的一页 `.ibd` 占位文件与物理 extent 未扩展问题；否则默认根页会被
登记但无法读取。专项 FILES 测试、辅助表回归、全部 `TestInformationSchema`、权限/角色
回归、完整 engine 包和后续全仓串行包结果均通过。证据见
`reports/compatibility/p1-information-schema-files-and-tablespace-pages-current-continuation1487.txt`。

本轮客户端矩阵因未设置受保护的 `XMYSQL_CLIENT_PASSWORD` 在启动前停止，不能记作
客户端通过或失败；P3 继续 partial。I_S/P_S 的完整权限/运行时/组件生命周期、P2/P4
官方 MySQL 双向 XA/binlog、崩溃恢复/提升矩阵仍保持 partial；FULLTEXT 继续 deferred，
非 InnoDB 引擎、非 InnoDB 修复和引擎转换继续 out of scope。

## Continuation 1490

本轮重新执行了复制、原生 binlog/GTID、XA、崩溃恢复/提升以及 I_S/P_S 的本地回归：
`go test -p 1 ./server/replication ./server/net ./server/innodb/engine -run
'Test.*(XA|Native|Binlog|GTID|Recovery|Promotion|Replica|InformationSchema|PerformanceSchema|Privilege|Role)'
-count=1 -timeout 60m` 通过，三个包分别耗时 5.100s、3.127s、154.292s。
这证明本地实现没有回退，但不替代官方 MySQL 双向 fixture。

客户端环境审计确认 Go、PyMySQL、Node/mysql2 可用；本机没有 `mysql.exe`，Docker CLI
fallback 也不可用。由于没有提供受保护的 `XMYSQL_CLIENT_PASSWORD`，真实认证矩阵和
source/promoted endpoint 矩阵未启动，分类为 `SKIPPED_ENVIRONMENT`，不能记为失败或
通过。现有官方 fixture 已覆盖官方 MySQL 8.4 到 xmysql 的普通/XA 输入，以及 xmysql
到官方 MySQL 的普通/XA、重启和崩溃重连；完整双向 GTID 重启、所有崩溃窗口、提升/故障
切换和重复投递矩阵仍保持 partial。证据见
`reports/compatibility/global-compatibility-status-current-continuation1490.txt`。

## Continuation 1491

本轮修复了原生 MySQL 拉取的三个边界：持久化 GTID 集驱动的过期文件位置恢复、旧
ROTATE_EVENT 不得回退 source position，以及官方 MySQL `MYSQL_TYPE_STRING` 紧凑行镜像
的解码。相关本地 replication 回归通过。官方 MySQL 8.4.11 的普通事务单次读取和 XA
读取通过，轮转/重启场景在本轮 fixture 中通过；但运行时连续拉取第二笔普通事务仍会
在 GTID 仅返回元数据/轮转前导时在文件位置与 GTID 重试之间循环，因此 P2/P4 原生
互操作仍保持 partial。证据见
`reports/compatibility/global-compatibility-status-current-continuation1491.txt`。

## Continuation 1492

修正 GTID 模式状态计算时机后，官方 MySQL 8.4.11 源端完整矩阵通过：普通事务、独立
GTID 拉取、XA、运行时连续事务、binlog 轮转与重启共 5 类测试全部通过，replication
包耗时 76.493s。本轮关闭 P2 原生 MySQL 源端 binlog/GTID/XA/轮转重启缺口；xmysql 到
官方 MySQL 的双向 GTID/XA、崩溃窗口、提升/故障切换、重复投递矩阵仍保持 partial。
证据见 `reports/compatibility/global-compatibility-status-current-continuation1492.txt`。

## Continuation 1493

真实认证客户端矩阵通过：mysql CLI、Go mysql driver、PyMySQL、Node mysql2 在 source
端点全部通过；随后 replica 追平、提升，并在 promoted endpoint 上再次由四种客户端
全部通过。P3 非 Connector/J 客户端与集群端点矩阵因此关闭为 implemented。剩余范围
收敛为 P1 全量 I_S/P_S 运行时/权限/组件语义，以及 P2/P4 xmysql 到官方 MySQL 的
反向 GTID/XA、崩溃窗口、提升和重复投递矩阵。证据见
`reports/compatibility/global-compatibility-status-current-continuation1493.txt`。

## Continuation 1494

补齐原生 MySQL 复制源身份的运行时投影：`MySQLBinlogSource` 通过独立元数据连接读取
`@@GLOBAL.server_uuid`，Runtime 状态保存并脱敏暴露该值，
`performance_schema.replication_connection_status.SOURCE_UUID` 在有权威值时返回真实
UUID、无值时保持 NULL。首次 fixture 验证发现并修复了 `[]byte` 结果被格式化为字节数组
文本的问题。官方 MySQL 8.4.11、native runtime 和 P_S 专项回归均通过；证据见
`reports/compatibility/p1-source-uuid-performance-schema-current-continuation1494.txt`。

本轮只关闭 SOURCE_UUID 投影切片；完整 I_S/P_S 运行时、组件生命周期和权限语义仍为
partial，P2/P4 官方双向复制、提升和完整崩溃窗口矩阵仍需继续完成。

## Continuation 1495

将同一权威复制运行时的 `Source_UUID` 继续投影到 `SHOW REPLICA STATUS` /
`SHOW SLAVE STATUS` 的当前兼容结果，相关 engine 回归通过。本轮只关闭已实现的
`Source_UUID` 字段切片；完整 MySQL `SHOW REPLICA STATUS` 列集合、顺序、channel/error
元数据和全部生命周期/权限语义仍未声称完成。证据见
`reports/compatibility/p1-show-replica-status-source-uuid-current-continuation1495.txt`。

## Continuation 1496

对客户端矩阵的范围做了校正：mysql CLI、Go mysql driver、PyMySQL、Node mysql2
以及它们在 promoted endpoint 上的场景仍是已验证子集；但“完整非 Connector/J
客户端兼容矩阵”还包含更多驱动、ORM、认证/TLS、协议边界和版本组合，因此聚合项
恢复为 `partial`，不能用四个客户端的通过结果代表全生态完成。证据见
`reports/compatibility/p3-client-scope-boundary-current-continuation1496.txt`。

## Continuation 1497

补齐 native replica 源身份的持久化生命周期：权威 `@@GLOBAL.server_uuid` 现在写入
独立的 `replication/source_identity.json`，重启构造 Runtime 时提前恢复，且测试确认
不会把带密码的 source URL 写入该文件；变更 source 时会清除旧身份。replication、P_S
和 `SHOW REPLICA STATUS` 定向回归及 replication 包全量回归均通过。证据见
`reports/compatibility/p1-source-identity-persistence-current-continuation1497.txt`。

## Continuation 1498

在当前代码上重新启动官方 MySQL 8.4.11 fixture，普通事务、`SOURCE_UUID`、GTID、XA、
运行时连续拉取、轮转/重启 6 个 native source 测试全部通过，耗时 `76.517s`。这确认
源端读取路径没有因身份持久化回归；xmysql → 官方 MySQL 的提升/故障切换、全崩溃窗口、
重复投递和完整双向 XA/binlog 仍保持 partial。证据见
`reports/compatibility/p2-p4-official-source-regression-current-continuation1498.txt`。

## Continuation 1499

补齐提升后的本地角色持久化：`Runtime.Promote()` 现在将 promoted source 写入
`replication/role.json`，重启时即使仍传入原来的 `RoleReplica` 配置，也会恢复为
`RoleSource` 并继续提供 source。replication 全量回归及 SHOW REPLICA/P_S 定向回归
均通过。证据见
`reports/compatibility/p2-promotion-role-persistence-current-continuation1499.txt`。

本轮只关闭本地提升角色的持久化恢复；官方 MySQL 反向提升、全部崩溃窗口、重复投递、
多节点故障仲裁和完整双向 GTID/XA/binlog 互操作仍保持 partial。

## Continuation 1500

修复 `RESET REPLICA ALL` 的 native 源身份生命周期：重置现在同时清除旧的
`replication/source_identity.json`，避免后续绑定新源时错误暴露旧的 `SOURCE_UUID`。
focused reset 回归和 replication 全量回归均通过。证据见
`reports/compatibility/p1-replica-reset-clears-source-identity-current-continuation1500.txt`。

本轮只关闭本地 reset 的身份清理切片；完整 MySQL RESET REPLICA 选项/元数据以及
I_S/P_S 全量权限和生命周期语义仍保持 partial。

## Continuation 1501

当前工作树通过新鲜的串行全仓回归：`go test -p 1 ./... -count=1 -timeout 45m`。
同时通过 cluster smoke（单源、双副本、DML/DDL、回滚隔离、副本提升）和三次重复的
crash-recovery 本地门禁。证据见
`reports/compatibility/p2-p0-local-gates-current-continuation1501.txt`。

这些门禁只确认本地回归、集群复制/提升 smoke 和可重复的 manager recovery 切片；官方
MySQL 反向提升/故障切换、全部崩溃窗口、所有重试路径下的重复投递，以及完整双向
XA/binlog 互操作仍保持 partial。P1 的完整 I_S/P_S 运行时、组件生命周期和权限语义，
以及完整非 Connector/J 客户端生态矩阵同样仍保持 partial。FULLTEXT 继续 deferred；
非 InnoDB 引擎、非 InnoDB REPAIR/引擎转换继续 out of scope。

## Continuation 1502

对照新鲜 MySQL 8.4.11 fixture 的 `SHOW REPLICA STATUS` 结果，补齐了
`SHOW REPLICA STATUS` / `SHOW SLAVE STATUS` 的 60 列官方顺序和当前运行时可获得的
源文件、位置、GTID、错误、运行状态投影。xmysql 未维护的 relay worker、过滤、TLS、
source account 等字段保持 NULL/空值，不伪造组件状态。engine 定向回归、P_S 复制定向
回归和 replication 全包均通过。证据见
`reports/compatibility/p1-show-replica-status-column-contract-current-continuation1502.txt`。

本轮只关闭 SHOW REPLICA STATUS 的列契约和 source-position 观察切片；P1 其它运行时、
组件生命周期、权限语义，P2/P4 双向复制故障窗口和 P3 完整非 Connector/J 客户端矩阵
仍保持 partial。

## Continuation 1503

对照 MySQL 8.4.11 的 `performance_schema.replication_connection_configuration`，补齐
native `mysql://` source 的主机、端口、用户、`AUTO_POSITION` 和 `SSL_ALLOWED` 投影。
source 用户只在 SQL/P_S 运行时对象中使用，不进入 HTTP status JSON；HTTP xmysql source
的既有完整 endpoint 投影保持不变。定向 P_S 回归通过。证据见
`reports/compatibility/p1-performance-schema-native-connection-fields-current-continuation1503.txt`。

本轮只关闭 native connection configuration 的字段投影切片；P1 其它组件生命周期、
重试/TLS/压缩配置、权限和 worker/filter 运行时语义仍保持 partial。

## Continuation 1504

补齐复制源配置的一条真实互操作链路：`CHANGE REPLICATION SOURCE TO` / `CHANGE
MASTER TO` 现在在保留 HTTP xmysql source 行为的同时，可生成 native `mysql://`
source URL，并解析 `SOURCE_USER`、`SOURCE_LOG_FILE`、`SOURCE_LOG_POS` 和
`SOURCE_AUTO_POSITION`。运行中的 replica 必须先停止复制线程，之后 source 变更会
立即重建 native source，并可从持久化配置重载；这使 SQL 管理面能够指向官方 MySQL
或另一台 xmysql 的 COM_BINLOG_DUMP 端点。没有把 source password 写入新的证据或
HTTP status；需要受保护凭据的生产场景仍须走安全配置/环境注入，密码管理和 TLS
复制仍不是本轮关闭项。

新增 focused red/green 测试覆盖 native URL 生成、运行时切换和重启重载；复制包及
复制管理/SHOW/P_S 相关 engine 定向回归通过。证据见
`reports/compatibility/p2-p4-native-source-change-sql-current-continuation1504.txt`。

全局任务边界保持不变：完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 的表覆盖、
字段精度、权限、运行时统计、锁/等待、线程和组件生命周期仍为 P1 partial；完整
非 Connector/J 客户端/驱动/ORM/TLS/认证矩阵仍为 P3 partial；官方 MySQL 与 xmysql
双向 XA/binlog/GTID、崩溃恢复、promotion/failover 和重复投递矩阵仍为 P2/P4
partial。FULLTEXT 继续 deferred；MyISAM/ARCHIVE/CSV 等非 InnoDB 引擎、非 InnoDB
REPAIR TABLE 和引擎转换明确不纳入本项目范围。

## Continuation 1505

修复 native GTID 首次接入的边界：当 replica 尚无本地已执行 GTID 时，保留配置中
明确提供的 `gtid_set` 基线，避免首次拉取退化为“没有 binlog file”；同时补齐官方
标准 `TABLE_MAP` 合成列名（`column_N`）到本地持久化表列顺序的绑定，避免官方
MySQL 的行事件因列名不匹配而把非空主键写成 NULL。新增回归测试并通过
replication 全包、native source-change/列映射定向 engine 测试。

官方 MySQL 8.4.11 反向 promotion fixture 已准备并可执行，但本轮 Docker Desktop
CLI 在容器启动阶段无响应，未将该场景记为通过；P2/P4 反向 XA/binlog/GTID、崩溃
恢复、promotion/failover 和重复投递聚合项继续保持 partial。P1 全量
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 运行时/权限/组件语义与 P3 完整非
Connector/J 客户端矩阵也继续保持 partial；FULLTEXT deferred，非 InnoDB 引擎及
其 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p2-p4-official-mysql-reverse-promotion-current-continuation1505.txt`。

## Continuation 1506

补齐 native `SOURCE_AUTO_POSITION=1` 的首次接入语义：当 replica 没有已持久化的
GTID 集、也没有 file/position 时，允许以空 GTID 集启动 COM_BINLOG_DUMP_GTID，且
`CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1` 不再被错误拒绝。replication
全包和 source-change engine 定向回归通过。官方 MySQL 网络 fixture 仍因 Docker CLI
不可用未验证，因此 P2/P4 的双向 XA/binlog/GTID、崩溃恢复、promotion/failover 和
重复投递聚合项继续保持 partial。证据见
`reports/compatibility/p2-native-empty-gtid-auto-position-current-continuation1506.txt`。

## Continuation 1507

补齐 native MySQL 复制 TLS 配置链路：source URL 支持 `ssl`、服务端证书校验、CA、
客户端证书和 key；`CHANGE REPLICATION SOURCE TO` / `CHANGE MASTER TO` 能生成这些
非敏感配置；COM_BINLOG_DUMP 和 `SOURCE_UUID` 元数据连接都会使用 TLS 配置，且
Performance Schema 的 `SSL_ALLOWED` 反映真实配置。replication/engine 定向回归通过，
全仓串行回归在本 slice 之前也已通过。官方 MySQL TLS 网络 fixture 因 Docker CLI
仍不可用未验证，完整 P2/P4 双向 XA/binlog/GTID、崩溃恢复、promotion/failover 和
重复投递继续保持 partial。证据见
`reports/compatibility/p2-p3-native-replication-tls-current-continuation1507.txt`。

## Continuation 1508

继续补齐 native 复制连接配置的可观察语义：source URL 和 `CHANGE REPLICATION SOURCE`
现在保留连接重试间隔/次数、心跳周期、压缩算法和 zstd 等级，
`performance_schema.replication_connection_configuration` 对这些字段与 TLS 字段
统一投影。相关 source、管理 SQL 和 P_S 回归通过。实际网络重试、心跳发送和压缩协商
仍需官方 MySQL fixture 验证；Docker CLI 当前不可用，完整 P2/P4 双向复制与故障窗口
继续保持 partial。证据见
`reports/compatibility/p1-p2-native-replication-runtime-options-current-continuation1508.txt`。

## Continuation 1509

将 `SOURCE_CONNECT_RETRY` 的实际运行时作用接到 native replica 拉取循环：native
source 发生拉取失败后，下一轮 pull 使用配置的秒级重试间隔；同时保留 go-mysql
同步器已经接入的重试次数、heartbeat 配置。新增 red/green 回归并通过 replication
全包、复制相关 engine 定向回归和完整 engine 回归。go-mysql 依赖内部每次重连之间仍固定等待 1 秒，
因此没有把这部分误标为“可配置完成”；压缩协商和官方 MySQL 网络互操作仍需外部
fixture/协议层支持。证据见
`reports/compatibility/p1-p2-native-replication-retry-runtime-current-continuation1509.txt`。

## Continuation 1510

补齐 native replication 心跳运行时语义：收到官方 `HEARTBEAT_EVENT` 后，runtime
现在记录心跳次数和最后接收时间，推进 source position 但不会把心跳帧交给事务
解码器；`performance_schema.replication_connection_status` 对应投影
`COUNT_RECEIVED_HEARTBEATS` 和 `LAST_HEARTBEAT_TIMESTAMP`。更换或重置 replica
source 时计数器清零。新增 runtime/P_S 回归，replication 全包和复制相关 engine
定向回归通过。官方 MySQL 网络 fixture、压缩协商、完整 P_S 组件生命周期和双向
XA/binlog/GTID 故障矩阵仍未关闭。证据见
`reports/compatibility/p1-performance-schema-replication-heartbeat-current-continuation1510.txt`。

## Continuation 1511

补齐 native replication 错误状态的 P_S 投影：runtime 现在从标准 MySQL 错误文本中
保留错误号和 UTC 发生时间，`replication_connection_status` 以及 coordinator/worker
applier 视图真实投影 `LAST_ERROR_NUMBER`、`LAST_ERROR_MESSAGE` 和
`LAST_ERROR_TIMESTAMP`；更换 source 或 reset replica 时清理这些 channel 状态。
新增 red/green 回归，replication 全包和复制相关 engine 定向回归通过。完整 I_S/P_S
表与权限语义、P3 非 Connector/J 完整客户端矩阵，以及官方 MySQL 双向
XA/binlog/GTID、崩溃恢复、promotion/failover 仍未关闭；FULLTEXT 继续 deferred，
非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-replication-errors-current-continuation1511.txt`。

## Continuation 1512

补齐 `performance_schema.host_cache` 的认证失败生命周期：metrics recorder 现在记录
每个账号首次/最近失败时间，并在 host 级聚合后投影 `FIRST_ERROR_SEEN` 和
`LAST_ERROR_SEEN`；成功认证仍按原语义清理活动失败计数。新增 metrics、host-cache 和
完整 P_S 回归通过。完整 I_S/P_S 表、组件、生命周期和权限语义、P3 非 Connector/J
完整客户端矩阵、以及官方 MySQL 双向 XA/binlog/GTID/崩溃恢复/promotion/failover
仍未关闭；FULLTEXT 继续 deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-host-cache-lifecycle-current-continuation1512.txt`。

## Continuation 1513

补齐 `performance_schema.replication_applier_configuration` 的本地 native applier
配置投影：补充 `PRIVILEGE_CHECKS_USER`、`REQUIRE_ROW_FORMAT`、
`REQUIRE_TABLE_PRIMARY_KEY_CHECK` 和 `ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_*` 默认
语义，并用 runtime status provider 作为数据源。focused applier/P_S 回归通过。完整
复制过滤器、Group Replication 以及官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、
promotion/failover 仍未关闭。证据见
`reports/compatibility/p1-performance-schema-applier-configuration-current-continuation1513.txt`。

## Continuation 1514

补齐 replication 错误分类链路：native source 拉取/连接错误归为 IO 错误，native
行事件应用错误归为 SQL/applier 错误；runtime 保存两类错误的消息、错误号和 UTC
时间。`SHOW REPLICA STATUS` 现在分别投影 `Last_IO_*` 与 `Last_SQL_*`，P_S
connection status 使用 IO 类，coordinator/worker applier status 使用 SQL 类，并
为旧 status provider 保留 generic fallback。replication 全包、复制管理/SHOW/P_S
定向回归以及完整 engine 回归均通过。完整 I_S/P_S 权限与生命周期、非 Connector/J
客户端矩阵、官方 MySQL 双向 XA/binlog/GTID、崩溃恢复和 promotion/failover 仍为
partial；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-p2-replication-error-classification-current-continuation1514.txt`。

## Continuation 1515

补齐本地复制过滤器链路：新增持久化的 `ReplicationFilterConfig`，支持
`REPLICATE_DO_DB`、`REPLICATE_IGNORE_DB`、`REPLICATE_DO_TABLE`、
`REPLICATE_IGNORE_TABLE`、`REPLICATE_WILD_DO_TABLE` 和
`REPLICATE_WILD_IGNORE_TABLE`；`CHANGE REPLICATION FILTER` 要求停止复制线程后
替换配置。过滤发生在 row/statement storage callback 之前，被完全过滤的事务仍推进
executed GTID 和 relay 边界；带 row image 的事务不会在 row 全部被过滤后退回执行
statement image。P_S `replication_applier_filters` 与
`replication_applier_global_filters` 已投影持久化规则。replication 全包、复制管理/P_S
定向回归和完整 engine 回归通过。完整过滤语法、官方 MySQL 双向互操作、崩溃恢复、
promotion/failover、P_S 权限生命周期仍为 partial；FULLTEXT deferred，非 InnoDB
继续 out of scope。证据见
`reports/compatibility/p2-replication-filters-current-continuation1515.txt`。

## Continuation 1516

补齐 `REPLICATE_REWRITE_DB`：新增有序、持久化的数据库重写规则，支持官方
`CHANGE REPLICATION FILTER REPLICATE_REWRITE_DB = ((from_db, to_db), ...)` 语法；
重写在其他数据库/表过滤器之前执行，row table 和 statement 默认数据库都会传递
改写后的目标。native prepared-XA commit replay 也复用相同过滤链，避免 XA 路径绕过
过滤器；P_S 复制过滤器视图同步投影重写规则。replication 全包、复制相关 engine
定向回归和完整 engine 回归通过。channel-specific/Group Replication 限制、完整 SQL
文本改写、官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/failover 仍为
partial；FULLTEXT deferred，非 InnoDB 继续 out of scope。证据见
`reports/compatibility/p2-replication-rewrite-db-current-continuation1516.txt`。

## Continuation 1517

补齐 `SHOW REPLICA STATUS.Replicate_Rewrite_DB` 的可观察语义：从 runtime filter
快照读取重写规则，并按 MySQL 的 `(from_db,to_db),(from_db2,to_db2)` 格式输出；未配置
规则时保留 NULL。新增红绿回归并通过 SHOW/P_S/复制定向测试和完整 engine 回归。
channel-specific SHOW、完整复制权限语义、官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、
promotion/failover 仍为 partial；FULLTEXT deferred，非 InnoDB 继续 out of scope。
证据见 `reports/compatibility/p1-show-replica-rewrite-db-current-continuation1517.txt`。

## Continuation 1518

补齐 `SHOW REPLICA STATUS` 及其 `SHOW SLAVE STATUS` 兼容别名的权限边界：已认证
非 root 会话必须拥有 `REPLICATION CLIENT`，同时保留旧版 `SUPER` 兼容放行；持久化
全局授权、激活角色授权和认证会话全局权限快照均参与判定。无权限时在生成复制状态
行之前返回 access denied；nil/internal session 仍保持既有内部投影行为。新增红绿回归，
相关权限定向回归和完整 engine 回归通过。完整 I_S/P_S 表、生命周期和权限语义、
channel-specific 复制权限、非 Connector/J 完整客户端矩阵、官方 MySQL 双向
XA/binlog/GTID、崩溃恢复、promotion/failover 仍未关闭；FULLTEXT deferred，
非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-show-replica-status-permission-current-continuation1518.txt`。

## Continuation 1519

补齐复制和 binlog 管理语句的权限分发：`SHOW MASTER STATUS`/`SHOW BINARY LOGS`
要求 `REPLICATION CLIENT`，`SHOW BINLOG EVENTS`/`SHOW RELAYLOG EVENTS` 要求
`REPLICATION SLAVE`；`START/STOP REPLICA`、`CHANGE REPLICATION SOURCE` 和
`CHANGE REPLICATION FILTER` 要求 `REPLICATION_SLAVE_ADMIN` 或兼容 `SUPER`；
`RESET REPLICA`、`FLUSH BINARY LOGS`、`RESET MASTER` 要求 `RELOAD`，而
`PURGE BINARY LOGS` 要求 `BINLOG_ADMIN`。持久化账号授权、角色授权和会话静态/动态
权限快照均接入现有授权路径，nil/internal session 行为保持不变。新增红绿回归，
相关定向测试和完整 engine 回归通过。完整 I_S/P_S 表、生命周期和权限语义、
channel-specific 复制、非 Connector/J 完整客户端矩阵、官方双向 XA/binlog/GTID、
崩溃恢复、promotion/failover 仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续
out of scope。证据见
`reports/compatibility/p1-p2-replication-permissions-current-continuation1519.txt`。

## Continuation 1520

补齐 `SHOW REPLICAS`、`SHOW SLAVE HOSTS` 和 `SHOW REPLICA HOSTS` 的复制可见性权限：
已认证非 root 会话必须拥有 `REPLICATION SLAVE`，三种语句共用现有持久化账号、角色
和会话权限解析路径；成功授权后的五列结果形状和 native replica registration 数据源
保持不变。新增红绿回归，相关结果形状测试和完整 engine 回归通过。完整
I_S/P_S 表、生命周期和权限语义、channel-specific 复制、非 Connector/J 完整客户端
矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover 仍未关闭；FULLTEXT
deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-show-replica-hosts-permission-current-continuation1520.txt`。

## Continuation 1521

补齐 `SHOW ENGINE INNODB STATUS` 的诊断权限边界：已认证非 root 会话必须拥有全局
`PROCESS`，授权解析复用持久化账号、激活角色和会话静态/动态权限路径；成功授权后的
三列结果形状和实时 InnoDB 状态投影保持不变。新增权限回归、既有结果形状回归和完整
engine 回归均通过。完整 I_S/P_S 表、生命周期和权限语义、channel-specific 复制、非
Connector/J 完整客户端矩阵、官方双向 XA/binlog/GTID、崩溃恢复、promotion/failover
仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-show-engine-process-permission-current-continuation1521.txt`。

## Continuation 1522

补齐 Performance Schema 多表读取的权限语义：元数据分发器现在扫描查询中所有直接
`performance_schema.<table>` 引用，并逐表要求 `SELECT`，不再只检查第一张表；保留
`performance_schema.processlist` 的 `PROCESS` 可见性规则、反引号表名和内部会话兼容
行为。新增红绿多表权限回归，完整 P_S 测试族和完整 engine 回归通过。完整 I_S/P_S
运行时值、组件生命周期、剩余权限/角色语义、P2/P4 官方复制互操作、P3 客户端矩阵仍
未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-multi-table-select-current-continuation1522.txt`。

## Continuation 1523

补齐 INFORMATION_SCHEMA 多表读取的 `PROCESS` 权限边界：元数据分发器现在扫描查询中
所有直接 `information_schema.<table>` 引用，对 `TABLESPACES`、`FILES` 以及 InnoDB
诊断表逐表执行 `PROCESS` 门禁，不再只依据第一张表；保留 processlist 的行级可见性
规则和 nil/internal session 行为。新增红绿回归，聚焦权限回归与完整 I_S/P_S 测试族
均通过。完整 I_S/P_S 运行时值、组件生命周期、剩余权限/角色语义、P2/P4 官方复制
互操作、P3 客户端矩阵仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。
证据见 `reports/compatibility/p1-information-schema-multi-table-process-current-continuation1523.txt`。

## Continuation 1524

补齐 `mysql.*` 虚拟授权表的多表 `SELECT` 权限边界：查询现在扫描所有直接引用的授权
表，并逐表复用持久化账号、角色和会话权限解析；`mysql.user JOIN mysql.db` 不再因
只检查首表而放行。保留单表 handler 的原有检查及 nil/internal session 行为。新增红绿
回归，授权表、完整 I_S/P_S 测试族均通过。完整 I_S/P_S 运行时值、组件生命周期、
剩余权限/角色语义、P2/P4 官方复制互操作、P3 客户端矩阵仍未关闭；FULLTEXT deferred，
非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-mysql-metadata-multi-table-process-current-continuation1524.txt`。

## Continuation 1525

补齐协议级 `COM_BINLOG_DUMP` 和 `COM_BINLOG_DUMP_GTID` 的复制读取权限：已认证会话
在开始 native binlog dump 前必须拥有 `REPLICATION SLAVE`，兼容保留 `SUPER/ALL` 放行；
未绑定认证会话的内部测试投影行为不变。新增权限红绿回归，完整 `server/net` 与
`server/replication` 回归通过。官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/
failover 仍需官方 fixture 完整验证；I_S/P_S 聚合、P3 客户端矩阵仍未关闭；FULLTEXT
deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p2-binlog-dump-replication-privilege-current-continuation1525.txt`。

## Continuation 1526

补齐 `COM_REGISTER_SLAVE`（legacy replica registration）的协议权限：已认证会话现在
复用 native binlog dump 的 `REPLICATION SLAVE` 门禁，兼容保留 `SUPER/ALL` 放行；无权限
时不会写入 replica registry，授权后原有注册数据和响应保持不变。新增红绿回归，完整
`server/net` 与 `server/replication` 回归通过。官方 MySQL 双向 XA/binlog/GTID、崩溃
恢复、promotion/failover 仍需官方 fixture 完整验证；I_S/P_S 聚合和 P3 客户端矩阵
仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p2-register-replica-replication-privilege-current-continuation1526.txt`。

## Continuation 1527

补齐 Performance Schema 状态汇总表的连接生命周期：
`status_by_account`、`status_by_host` 和 `status_by_user` 现在从 recorder 的按账户、主机、
用户维度的生命周期汇总投影 `Queries` 和 `Com_*` 计数；客户端断开后这些历史统计仍保留，
而 `Threads_connected`/`Threads_running` 继续来自实时会话和活动语句。新增断开身份的红绿
回归，完整 I_S/P_S 测试族通过。全 engine 回归在全包顺序下仍有
`TestPerformanceSchemaSessionRuntimeViewsExposeVariablesAndUserVariables` 失败，但该测试
单独运行通过，因此本轮不把全 engine 记为通过，保留为 suite-order/isolation 待诊断项。
完整 I_S/P_S 权限、组件生命周期、锁/等待/线程语义、P2/P4 官方复制互操作、P3 客户端矩阵
仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-status-summary-lifecycle-current-continuation1527.txt`。

## Continuation 1528

补齐列级 `GRANT ... WITH GRANT OPTION` 的作用域和权限元数据：列级授权现在将
`GRANT OPTION` 保存在列授权上，不再制造空的表级授权范围；`mysql.columns_priv.Column_priv`
隐藏该控制项，`INFORMATION_SCHEMA.COLUMN_PRIVILEGES.IS_GRANTABLE` 单独反映可授权状态。
新增列级/表级权限红绿回归，以及账号、角色、GRANT/REVOKE、动态权限和部分撤销权限回归，
均通过。本轮只关闭一个 P1 权限元数据切片；完整 I_S/P_S 表覆盖、运行时生命周期、剩余
角色权限语义、P2/P4 官方复制互操作和 P3 客户端矩阵仍未关闭；FULLTEXT deferred，非
InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-column-grant-option-scope-current-continuation1528.txt`。

## Continuation 1529

补齐列级 `REVOKE GRANT OPTION FOR`：撤销列级 delegation 时只移除
`GRANT OPTION`，保留原有列权限，并且不改动 partial revoke 状态。新增表级/列级
撤销回归和权限族回归均通过。本轮仍只关闭一个 P1 列级授权生命周期切片；完整
I_S/P_S 表与运行时语义、剩余角色权限、P2/P4 官方复制互操作和 P3 客户端矩阵仍未
关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-column-revoke-grant-option-current-continuation1529.txt`。

## Continuation 1530

修正列级授权的 `SHOW GRANTS` 展示：`GRANT OPTION` 不再作为普通列权限输出，而是按
MySQL 语法放在语句末尾的 `WITH GRANT OPTION`；实际列权限仍保持在 `(column)` 前。
新增展示回归和权限族回归均通过。本轮只关闭一个 P1 列级授权展示切片；完整
I_S/P_S 语义、剩余角色权限、P2/P4 官方复制互操作和 P3 客户端矩阵仍未关闭；
FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-column-show-grants-format-current-continuation1530.txt`。

## Continuation 1531

补齐 Performance Schema `accounts/hosts/users` 的会话内存高水位：连接汇总现在从
recorder 的 account 维度投影 `MAX_SESSION_CONTROLLED_MEMORY` 和
`MAX_SESSION_TOTAL_MEMORY`，并保留断开后的历史身份与 TRUNCATE 重置语义。定向连接
汇总回归和隔离运行回归通过；完整 P_S 全包仍因测试间共享历史身份而失败，单测通过，
因此不宣称全包通过。完整 I_S/P_S 语义、组件权限生命周期、P2/P4 官方复制互操作和
P3 客户端矩阵仍未关闭；FULLTEXT deferred，非 InnoDB 引擎继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-connection-memory-current-continuation1531.txt`。

## Continuation 1532

补齐 INFORMATION_SCHEMA 的 `ALL/ALL PRIVILEGES` 投影：表、库、列、全局权限视图，
以及 `ROLE_TABLE_GRANTS`、`ROLE_COLUMN_GRANTS`、`ROLE_ROUTINE_GRANTS` 现在按 MySQL
规则拆成逐项权限，不再把 `ALL` 或 `GRANT OPTION` 当作普通权限行；同时保留
grant option 和 grantor provenance。专项权限/角色回归、完整 Performance Schema
回归和 engine 全包回归均通过，engine 结果为 `GO_EXIT=0`。另外修复两个测试中
process-wide recorder 历史身份泄漏造成的顺序依赖；生产 recorder 归属未改变。

本轮只关闭一个 P1 权限投影切片和测试隔离缺口；完整 I_S/P_S 运行时、组件生命周期
与权限语义，P2/P4 官方 XA/binlog/GTID/复制互操作及 P3 全量客户端矩阵仍未关闭。
FULLTEXT 继续 deferred，非 InnoDB 引擎、非 InnoDB `REPAIR TABLE` 和引擎转换继续
out of scope。证据见
`reports/compatibility/p1-privilege-all-expansion-and-engine-regression-current-continuation1532.txt`。

## Continuation 1533

补齐 Performance Schema 语句事件和摘要的内存峰值投影：runtime recorder 在语句内存
仪表仍处于 live 状态时捕获 `memory/sql/THD::main_mem_root` 的 current/语句级 high-water
值，并用每线程语句开始基线隔离前一条语句的生命周期峰值；`events_statements_history_long`、
按维度语句摘要和 digest 摘要现在输出真实的 `MAX_CONTROLLED_MEMORY` / `MAX_TOTAL_MEMORY`。新增 metrics、引擎定向回归，完整
I_S/P_S 族和全仓串行 Go 回归均通过，后者 `GO_EXIT=0`。

本轮只关闭语句内存观测切片；CPU、锁时间、临时表和 secondary-engine 统计没有可靠
生产来源，未用估算值伪造。完整 I_S/P_S 运行时/权限语义、P2/P4 官方 XA/binlog/
GTID/复制/崩溃互操作及 P3 全量客户端矩阵仍未关闭。FULLTEXT 继续 deferred，非
InnoDB 引擎、非 InnoDB `REPAIR TABLE` 和引擎转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-memory-current-continuation1533.txt`。

## Continuation 1534

接入 MySQL `CLIENT_COMPRESS` 传输协议：握手声明压缩能力，认证成功后仅在客户端实际
协商该能力时启用；Session 发送层统一生成标准 7 字节压缩包头并压缩普通 MySQL 包，避免
同步/异步两条发送路径二次压缩；接收层支持解压一个压缩帧中的多个普通包并继续派发。
新增握手、认证协商、压缩读写和多包排队回归，定向 `server/net`、`server/protocol` 测试
通过。当前环境没有 `mysql`/`mariadb` CLI，真实 CLI 以及完整 Python/Go/Node 客户端压缩
连接仍需外部客户端矩阵验证，因此 P3 全量客户端兼容性继续保持 partial。

本轮只关闭传输协议实现切片；完整 I_S/P_S 运行时/权限语义、P2/P4 官方 XA/binlog/
GTID/复制/崩溃互操作仍未关闭。FULLTEXT 继续 deferred，非 InnoDB 引擎、非 InnoDB
`REPAIR TABLE` 和引擎转换继续 out of scope。证据见
`reports/compatibility/p3-mysql-transport-compression-current-continuation1534.txt`。

## Continuation 1535

补齐本地复制过滤器 `REPLICATE_REWRITE_DB` 的 statement-based/native replay 边界：除
默认数据库和 row-image 表名外，现在会对 SQL 文本中的 `source_db.table`、反引号标识符
和安全的双引号标识符进行目标库改写，同时跳过字符串字面量、行注释和块注释；未限定的
表名保持不变。新增改写结果和 storage callback 回归，完整 `server/replication`、引擎
复制过滤/P_S 定向回归以及全仓串行 Go 回归均通过，`GO_EXIT=0`。

本轮关闭本地 SQL 文本改写切片；完整 MySQL filter grammar、channel-specific/
Group Replication 限制、官方 MySQL 双向 XA/binlog/GTID、崩溃恢复、promotion/failover
和重复投递仍未关闭。I_S/P_S 完整运行时/组件/权限语义、P3 全量客户端矩阵继续保持
partial；FULLTEXT 继续 deferred，非 InnoDB 引擎及相关 REPAIR/转换继续 out of scope。
证据见 `reports/compatibility/p2-rewrite-qualified-statement-db-current-continuation1535.txt`。

## Continuation 1536

补齐本地复制过滤器的 statement-based 表级过滤：`REPLICATE_DO_TABLE`、
`REPLICATE_IGNORE_TABLE`、`REPLICATE_WILD_DO_TABLE` 和
`REPLICATE_WILD_IGNORE_TABLE` 现在会作用于可可靠识别的 `FROM`、`JOIN`、`UPDATE`
和 `INSERT/REPLACE ... INTO` 表引用；支持默认库、库表限定名和多表语句，并跳过
字符串、注释和无法可靠解析的子查询目标，避免误拦截。完整 MySQL filter grammar、
channel-specific/Group Replication 限制、官方 MySQL 双向 XA/binlog/GTID、崩溃恢复与
promotion/failover 仍未关闭。I_S/P_S 完整运行时/组件/权限语义和 P3 全量客户端矩阵
继续保持 partial；FULLTEXT 继续 deferred，非 InnoDB 引擎及相关 REPAIR/转换继续
out of scope。证据见
`reports/compatibility/p2-statement-table-filters-current-continuation1536.txt`。

## Continuation 1537

复核并修正 P3 客户端聚合状态：同一工作区的 continuation1493 报告已证明 mysql CLI
（Docker MySQL 8.4.11 客户端）、Go mysql driver、PyMySQL、Node mysql2 在源端和晋升
端均通过完整客户端用例，客户端聚合项由 partial 修正为 implemented。当前机器重新执行
需要 Docker daemon 和受保护密码；它们是复跑条件，不改变已有 PASS 证据。剩余未闭环项为
P1 I_S/P_S 完整运行时/组件/权限语义、P2/P4 官方 XA/binlog/GTID/复制互操作和崩溃晋升
矩阵。FULLTEXT 继续 deferred，非 InnoDB 引擎及相关 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/global-compatibility-status-current-continuation1493.txt`。

## Continuation 1538

按用户范围注释重新梳理全局任务边界：完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 和
XA 与原生 binlog/GTID/复制/崩溃恢复/晋升互操作继续作为全局未完成项；非 Connector/J
客户端矩阵仍保留在全局任务中，但基于 continuation1493 的单端点与集群源/晋升端证据将
状态确认为 implemented；FULLTEXT 继续 deferred；非 InnoDB 引擎、专用 REPAIR TABLE 和
引擎转换明确 out_of_scope。主矩阵与本文顶部状态已同步，详见
`reports/compatibility/global-scope-reconciliation-current-continuation1538.txt`。

## Continuation 1539

收口 Performance Schema `setup_actors` 的多规则生命周期：现在按 HOST/USER/ROLE 保存多行
规则，支持新前台线程按最具体规则获取 `INSTRUMENTED/HISTORY`，并支持带权限校验的
`INSERT`、`DELETE` 和 `TRUNCATE TABLE`；既有线程的会话快照不被事后规则修改回写。定向
actor 回归、完整 `TestPerformanceSchema` 和全仓串行 Go 回归均通过，engine 378.292s，
全仓 exit 0。该切片已完成，但完整 P1 I_S/P_S 运行时、组件和权限聚合仍保持 partial。
证据见 `reports/compatibility/p1-performance-schema-setup-actors-lifecycle-current-continuation1539.txt`。

## Continuation 1540

补齐 `performance_schema.setup_threads` 的线程类别注册表切片：现在至少暴露
`thread/performance_schema/setup`、`thread/sql/event_scheduler`、`thread/sql/main` 和
`thread/sql/one_connection`，按 NAME 排序返回 MySQL 的 `PROPERTIES`、`VOLATILITY` 和
`DOCUMENTATION` 形状，并支持按 NAME 更新单个或多个匹配类别。按照 MySQL 语义，
`TRUNCATE TABLE performance_schema.setup_threads` 会被拒绝；实际运行线程仍只由
`performance_schema.threads` 按真实连接和复制线程生成，没有伪造后台实例。

setup_threads 定向回归、完整 Performance Schema 回归和全仓串行 Go 回归均通过，
全仓 exit 0。该切片不等于完整 I_S/P_S 组件、插件和后台线程生命周期完成；P1 聚合仍为
partial。P2/P4 官方 XA/binlog/GTID/复制/崩溃晋升互操作仍为 partial；P3 非 Connector/J
矩阵沿用 continuation1493 的 implemented 证据，但当前 Docker 不可用，未在本轮复跑。
FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-setup-threads-registry-current-continuation1540.txt`。

## Continuation 1541

补齐 `RESET REPLICA`/`RESET SLAVE` 的 `FOR CHANNEL` 语法切片：现在支持可选的
`ALL FOR CHANNEL <channel>` 和 `FOR CHANNEL <channel>` 后缀。单通道 runtime 对空引号
默认 channel 正确委托既有 reset/reset-all 生命周期；非空 channel 明确返回仅支持默认
channel 的错误，不将多通道语义伪装成已实现。相关 admin、权限和 runtime reset 回归均通过。

该切片只关闭 reset 语法和默认 channel 分发，不等于多通道复制、官方 XA/binlog/GTID、
反向/双向复制、崩溃恢复或晋升互操作完成；这些 P2/P4 仍保持 partial。P1 I_S/P_S
完整组件、插件、后台线程生命周期仍保持 partial；P3 非 Connector/J 客户端矩阵沿用
continuation1493 的 implemented 证据。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p2-reset-replica-channel-current-continuation1541.txt`。

## Continuation 1542

补齐 `START/STOP REPLICA` 与旧别名 `START/STOP SLAVE` 的 `FOR CHANNEL` 语法切片：
单通道 runtime 对空引号默认 channel 正确复用现有启动/停止回调；命名 channel 明确
拒绝；原有不支持的线程选项和 `REPLICATION_SLAVE_ADMIN` 权限检查保持不变。定向
admin、复制控制和 reset 回归均通过。

该切片仍只关闭单通道语法分发，不等于多通道复制或官方 XA/binlog/GTID、反向/双向、
崩溃恢复、重复投递和晋升互操作完成；P2/P4 仍为 partial。P1 I_S/P_S 完整组件、
插件、后台线程生命周期和权限聚合仍为 partial；P3 非 Connector/J 矩阵沿用既有
implemented 证据。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续
out of scope。证据见
`reports/compatibility/p2-replication-channel-control-current-continuation1542.txt`。

## Continuation 1543

补齐 `SHOW REPLICA STATUS` 与 `SHOW SLAVE STATUS` 的 `FOR CHANNEL` 语法切片：空引号
默认 channel 复用现有单通道 status provider，并保留 `REPLICATION CLIENT` 权限检查；
命名 channel 明确拒绝。新增状态列契约、运行时投影、默认 channel 和拒绝命名 channel
回归均通过。

该切片不等于多通道状态存储、`NONBLOCKING` 语义或官方 XA/binlog/GTID、反向/双向、
崩溃恢复、重复投递和晋升互操作完成；P2/P4 仍为 partial。P1 I_S/P_S 完整组件、
插件、后台线程生命周期和权限聚合仍为 partial；P3 非 Connector/J 矩阵沿用既有
implemented 证据。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续
out of scope。证据见
`reports/compatibility/p2-show-replica-status-channel-current-continuation1543.txt`。

## Continuation 1544

补齐 `CHANGE REPLICATION SOURCE TO`、`CHANGE MASTER TO` 和 `CHANGE REPLICATION FILTER`
的 `FOR CHANNEL` 语法切片：空引号默认 channel 复用当前单通道 source/filter callback，
命名 channel 明确拒绝；原有 source option 校验、filter 解析、停止副本生命周期和权限
检查保持不变。相关 source/filter/admin/status 回归均通过。

该切片不等于多通道 source/filter 状态、统一存储/WAL/binlog/GTID 提交协议或官方
XA/binlog/GTID、反向/双向、崩溃恢复、重复投递和晋升互操作完成；P2/P4 仍为 partial。
P1 I_S/P_S 完整组件、插件、后台线程生命周期和权限聚合仍为 partial；P3 非 Connector/J
矩阵沿用既有 implemented 证据。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p2-replication-source-filter-channel-current-continuation1544.txt`。

## Continuation 1546

补齐 MySQL 8.4 `RESET BINARY LOGS AND GTIDS` 及可选 `TO n` 文件序号：命令现在经过
executor、runtime、source 到 native binlog writer 的真实链路，清空 logical binlog 和
Executed GTID 状态，按指定序号重建 native binlog，并在重启后保持文件序号和清空状态；
旧 `RESET MASTER` 继续作为默认序号 1 的兼容别名。该命令沿用 source binlog 的 `RELOAD`
权限边界，相关 admin、权限、native 文件和重启持久化回归均通过，受影响包及全仓串行
Go 回归均通过。

该切片只关闭 RESET BINARY LOGS AND GTIDS 的本地语法、权限、native reset 和持久化行为，
不等于统一 storage/WAL/binlog/GTID 提交协议、官方双向 XA/binlog/GTID、崩溃恢复、重复
投递和 promotion/failover 完成；P2/P4 聚合仍为 partial。P1 I_S/P_S 完整组件、运行时、
权限和生命周期语义继续为 partial；P3 非 Connector/J 客户端矩阵沿用既有 implemented
证据。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p2-reset-binary-logs-gtids-current-continuation1546.txt`。

## Continuation 1547

补齐 Performance Schema setup 表的一个真实 DDL 边界：`TRUNCATE TABLE
performance_schema.setup_consumers` 与 `setup_instruments` 现在直接返回 MySQL 兼容的
`Invalid performance_schema usage`，不再错误落入保留数据库校验；已有 `setup_actors`、
`setup_objects` 可截断和 `setup_threads` 禁止截断行为保持不变。红测、完整
`TestPerformanceSchema*` 专项和 engine 全包均通过。

该切片只关闭两张 setup 表的截断边界，不等于完整 P_S 组件/插件生命周期、所有运行时计数、
锁/等待/线程字段精度和权限矩阵完成；P1 聚合仍为 partial。P2/P4 官方 XA/binlog/GTID、
崩溃恢复、重复投递和 promotion/failover 仍为 partial；FULLTEXT 继续 deferred，非 InnoDB
引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-setup-truncate-boundary-current-continuation1547.txt`。

## Continuation 1548

补齐 `performance_schema.setup_consumers` 的 `statements_digest` consumer：现在按 MySQL
8.4 暴露默认 `ENABLED='YES'` 的配置行，并真实控制
`events_statements_summary_by_digest` 的产出；关闭该 consumer 后不再生成 digest 汇总。
红测、完整 P_S 专项和 engine 全包均通过。

该切片只关闭 statements digest consumer 的注册和 gate，不等于所有 consumer/instrument
类别、组件/插件生命周期、运行时计数、锁/等待/线程字段精度和权限矩阵完成；P1 聚合仍为
partial。P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为
partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statements-digest-consumer-current-continuation1548.txt`。

## Continuation 1549

按 MySQL 8.4 官方默认矩阵修正 `performance_schema.setup_consumers`：statement 当前/历史、
transaction 当前/历史、`statements_digest`、global/thread instrumentation 默认开启；
statement history-long、所有 stage/wait consumer 和 transaction history-long 默认关闭。
生产 runtime gate 与该矩阵一致；依赖历史/等待数据的测试改为显式开启对应 consumer。默认矩阵、
完整 P_S、engine 全包和全仓串行 Go 回归均通过。

该切片只关闭 setup_consumers 默认值及已有 gate 的偏差，不等于完整 instrument 类别、组件/插件
生命周期、运行时计数、锁/等待/线程字段精度或权限矩阵完成；P1 仍为 partial。P2/P4 官方
XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为 partial。FULLTEXT 继续 deferred，
非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-consumer-defaults-current-continuation1549.txt`。

## Continuation 1550

补齐 `performance_schema.setup_instruments` 的 `transaction` 行，默认
`ENABLED='YES'`、`TIMED='YES'`，并让事务当前/历史事件遵守该 instrument 的
`ENABLED`/`TIMED` 配置：关闭采集后不再追加新事件，已保留历史仍可读取；关闭计时后
计时列为空。事务 instrument 注册、关闭 gate、完整 P_S、engine 全包和全仓串行 Go
回归均通过。

该切片只关闭 transaction instrument 的注册与本地采集/计时 gate，不等于完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表覆盖、组件/插件生命周期、运行时统计、锁/等待/
线程字段精度或权限语义完成；P1 仍为 partial。P2/P4 官方 XA/binlog/GTID、崩溃恢复、
重复投递和 promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及
专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-transaction-instrument-current-continuation1550.txt`。

## Continuation 1551

补齐 `performance_schema.setup_threads` 对前台连接生命周期的运行时语义：更新
`thread/sql/one_connection` 后，后续首次观测的新连接读取新的 `INSTRUMENTED/HISTORY`
配置，已观测连接保留原快照；`performance_schema.threads` 与语句级 Performance Schema
采集使用同一线程 gate。红测、相关线程测试、完整 P_S、engine 全包和全仓串行 Go 回归均
通过。

该切片只关闭 setup_threads 的前台连接快照与本地 gate 缺口，不等于完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表覆盖、所有线程类别、组件/插件生命周期、运行时
统计、锁/等待/线程字段精度或权限语义完成；P1 仍为 partial。P2/P4 官方 XA/binlog/GTID、
崩溃恢复、重复投递和 promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB
引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-setup-threads-runtime-current-continuation1551.txt`。

## Continuation 1552

补齐 `setup_instruments` 对 `statement/sql/<type>` 的 ENABLED 运行时 gate：关闭
`statement/sql/select` 后，普通 `XMySQLEngine.ExecuteQuery` 和内部 executor 两条执行路径
都不再写入 statement history、statement summary、digest summary 及相关 stage/table-I/O
汇总。红测、完整 P_S、engine 全包和全仓串行 Go 回归均通过。

该切片只关闭 statement instrument 的 ENABLED 记录 gate，不等于完整 instrument 的 TIMED
语义、所有 instrument 家族、组件/插件生命周期、运行时统计、锁/等待/线程字段精度或权限
语义完成；P1 仍为 partial。P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-instrument-gate-current-continuation1552.txt`。

## Continuation 1553

补齐 statement instrument 的 `TIMED` 语义：`statement/sql/<type>` 设置为
`TIMED='NO'` 后，保留语句 count 和业务计数，但 statement summary、digest、stage/table-I/O
相关汇总的 timer 值为 0；executor 与 engine 两条记录路径统一使用 timer-aware recorder。
红测、P_S 专项、metrics 包、engine 全包和全仓串行 Go 回归均通过。

该切片只关闭 statement instrument 的汇总计时 gate，不等于所有 instrument 家族的完整
TIMED/生命周期语义、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 覆盖、组件/插件生命周期、
运行时统计、锁/等待/线程字段精度或权限语义完成；P1 仍为 partial。P2/P4 官方
XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为 partial。FULLTEXT 继续
deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-instrument-timed-current-continuation1553.txt`。

## Continuation 1554

补齐 `stage/sql/execute` instrument 的 ENABLED 生命周期：关闭 stage instrument 期间执行的
语句不再写入 stage summary/history；重新开启后也不会把关闭窗口的事件补回。statement
summary 和业务计数仍独立保留，executor 与 engine 两条记录路径统一使用 stage gate。红测、
相关 stage/statement/setup 测试、完整 P_S、engine 全包和全仓串行 Go 回归均通过。

该切片只关闭 stage/sql/execute 的本地记录 gate，不等于完整 stage/instrument 类别、组件/
插件生命周期、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 覆盖、运行时统计、锁/等待/
线程字段精度或权限语义完成；P1 仍为 partial。P2/P4 官方 XA/binlog/GTID、崩溃恢复、
重复投递和 promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-stage-instrument-lifecycle-current-continuation1554.txt`。

## Continuation 1555

分离 `stage/sql/execute` 的 TIMED 状态与 statement instrument：stage summary 使用独立的
stage timer，`TIMED='NO'` 时保留 count 但计时为 0；事件保存自身的 StageTimed 状态，重新
开启 TIMED 不会给关闭期间采集的事件补回计时。红测、相关测试、完整 P_S、engine 全包和
全仓串行 Go 回归均通过。

该切片只关闭 stage summary/history 的本地计时状态隔离，不等于所有 instrument 家族的
完整 TIMED/生命周期语义、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 覆盖、组件/插件
生命周期、运行时统计、锁/等待/线程字段精度或权限语义完成；P1 仍为 partial。P2/P4
官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为 partial。FULLTEXT
继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-stage-instrument-timed-current-continuation1555.txt`。

## Continuation 1556

补齐支持的 `REPLACE` 语句族 Performance Schema instrument：`setup_instruments`
现在暴露默认 `ENABLED='YES'`、`TIMED='YES'` 的 `statement/sql/replace`，并且该配置
真实控制 REPLACE 进入 `events_statements_summary_global_by_event_name` 的采集；关闭
instrument 后后续 REPLACE 不再增加汇总计数。定向测试、完整 P_S、engine 全包和全仓
串行 Go 回归均通过。

该切片只关闭 REPLACE instrument 的注册和本地 ENABLED gate，不等于所有 statement
instrument 家族、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 运行时/组件/权限语义
完成；P1 仍为 partial。P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-replace-instrument-current-continuation1556.txt`。

## Continuation 1557

修复 statement history 的 `TIMED` 生命周期快照：事件现在保存产生时的 `TIMED` 状态，
历史查询不再读取当前 setup instrument 的 TIMED 值来 retroactive 改写
`TIMER_START/TIMER_END/TIMER_WAIT`。同时修复全仓串行回归暴露的测试 MockSession 属性
map 并发读写问题。statement history 红测、完整 P_S、engine、server/net 和全仓串行
回归均通过。

该切片只关闭 statement history 的计时快照缺口并修复测试并发缺陷；stage history 的
instrument 生命周期快照、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 组件/权限语义、
P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover
仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续
out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-history-timed-snapshot-current-continuation1557.txt`。

## Continuation 1558

修复 stage history 的事件时 `ENABLED/TIMED` 生命周期语义：历史查询现在使用事件捕获时的
`StageTimed` 快照，不再因当前 setup instrument 的变化而隐藏或重算已有事件；关闭 stage
instrument 期间不会在重新启用后回填事件，已捕获的 history 也不会因后续关闭 instrument
而消失。stage history 专项、完整 P_S 和 engine 全包回归均通过。

该切片只关闭 stage history 的生命周期快照缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
表、组件/权限语义，P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-stage-history-snapshot-current-continuation1558.txt`。

## Continuation 1559

修复 statement history 的事件时 `ENABLED` 生命周期语义：历史查询现在依据事件捕获时的
`Instrumented` 快照决定是否展示，不再因当前 `setup_instruments` 变化而回填关闭期间的
事件，或隐藏已经捕获的历史。statement history 专项、完整 P_S 和 engine 全包回归均通过。

该切片只关闭 statement history 的 ENABLED 快照缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
表、字段、组件/权限语义，P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-history-instrument-snapshot-current-continuation1559.txt`。

## Continuation 1560

补齐 `setup_actors` 对 stage instrumentation 的生命周期约束：actor `ENABLED=NO` 时，前台
语句不再写入 `stage/sql/execute` 汇总或 history；actor 的 `HISTORY=NO` 继续控制历史展示，
而不改变已启用 instrumentation 的汇总语义。stage actor 专项、完整 P_S 和 engine 全包回归
均通过。

该切片只关闭 setup_actors 到 stage 的本地生命周期缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
表、字段、组件/权限语义，P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-setup-actors-stage-current-continuation1560.txt`。

## Continuation 1561

补齐 record-lock 与 metadata-lock wait history 的事件时 `ENABLED/TIMED` 快照：历史 wait
现在使用事件完成时保存的 instrument 状态，后续修改 setup instrument 不会重算计时或隐藏
已有历史；current wait 仍使用当前配置。两类 wait 的专项测试、manager 全包、完整 P_S 和
engine 全包回归均通过。

该切片只关闭已完成 lock wait history 的生命周期快照缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
表、字段、组件/权限语义，P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-wait-history-instrument-snapshot-current-continuation1561.txt`。
