# xmysql-server 剩余兼容能力边界

更新时间：2026-10-04

最新矩阵：`reports/compatibility/scope-matrix-current-final.json`

最新验证记录：`reports/compatibility/p2-p4-official-mysql-fresh-interoperability-current-continuation1886.txt`、
`reports/compatibility/p3-client-fault-and-cluster-fresh-current-continuation1892.txt`、
`reports/compatibility/official-mysql-privilege-lifecycle-current/privilege-lifecycle-20261003-204707.json`、
`reports/compatibility/p1-performance-schema-native-lifecycle-current-continuation1900.txt`、
`reports/compatibility/full-go-regression-current-continuation1901.txt`

本次切片记录：官方 MySQL 8.4 权限/角色生命周期对照，以及 P_S 原生生命周期、容量和丢失计数

## 当前统计

- 29 项总计；27 项纳入范围，2 项明确排除。
- 24 项 implemented，2 项 partial，1 项 deferred，2 项 out_of_scope。
- P0（MySQL 启动/核心执行、基础集群、Connector/J）已具备当前发布门禁证据。

## 全局执行顺序

| 优先级 | 全局目标 | 当前边界 |
| --- | --- | --- |
| P0 | MySQL 能启动、核心 CRUD 可用、基础集群/故障切换可用、Connector/J 1.3.9 兼容 | 已实现；Connector/J 与原有 P0 同级，不再后置 |
| P1-A | 完整 INFORMATION_SCHEMA / PERFORMANCE_SCHEMA，包括表/字段覆盖、真实运行时统计、锁/等待/线程生命周期、组件和权限语义 | 2 个组件运行时聚合项仍为 `partial`，继续纳入全局任务 |
| P2 | 本地复制、XA、原生 binlog/GTID、崩溃恢复与 promotion/fencing | 当前矩阵已实现；保留跨版本和更广拓扑扩展空间 |
| P3 | 非 Connector/J 客户端全量兼容矩阵：MySQL CLI、Python、Go、Node.js、连接池/ORM 及故障场景 | 当前核心矩阵、重启、网络故障和集群端点均通过 |
| P4 | 与官方 MySQL 的 XA/binlog/GTID/恢复/提升互操作 | MySQL 8.4 官方 fixture 双向/崩溃/分区矩阵通过 |
| 后置 | Fulltext | `deferred`，当前不阻塞 P0/P1/P2 |
| 明确不做 | MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎，以及非 InnoDB 的 REPAIR/引擎转换 | `out_of_scope`，不纳入本项目全局任务 |

因此，当前剩余工作集中在 P1 的完整 I_S/P_S 运行时、组件和权限语义；P0/P2/P3/P4
已有当前环境下的可复核门禁证据，但不等于所有 MySQL 版本和 Enterprise 组件均覆盖。

Continuation 1883 修正了 `INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS.SRS_ID`
对 `LIKE` 谓词的处理：`srs_id LIKE '4%'` 现在可以命中内置 4326，精确数值过滤和显式
空字符串语义保持不变。专项回归、完整 I_S 族和全仓编译通过。该切片只关闭一个 P1
目录过滤语义缺口，P1/P2/P3/P4 聚合仍按矩阵保持 `partial`。Evidence:
`reports/compatibility/p1-information-schema-srs-id-like-filter-current-continuation1883.txt`。

Continuation 1884 对当前工作区重新执行了完整 P_S、本地 native P2 回归和客户端矩阵入口。
P_S 与本地 P2 通过；客户端矩阵因缺少受保护密码参数提前停止，MySQL CLI/Docker/官方 MySQL
fixture 仍是环境未验证，未将其误记为 PASS。Evidence:
`reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt`。

Continuation 1885 重新执行了本地账户、角色、授权表、权限视图和 partial revoke 回归，
18.135 秒通过。该结果确认当前已实现的 P1 权限/角色切片没有回归，但不关闭完整
`mysql.*` 生命周期、组件运行时或外部官方互操作边界。Evidence:
`reports/compatibility/p1-local-privilege-role-fresh-audit-current-continuation1885.txt`。

Continuation 1886-1892 恢复 Docker 官方 MySQL 8.4 fixture，并通过官方源端崩溃/重启/网络
分区、xmysql 反向 promotion，以及普通/XA 两阶段/XA one-phase 双向复制验证；同时通过
mysql CLI、Go、PyMySQL、Node mysql2 的基础矩阵、重启重连、网络故障和集群端点切换矩阵。
对应聚合项已从 `partial` 提升为 `implemented`，证据见：
`reports/compatibility/p2-p4-official-mysql-fresh-interoperability-current-continuation1886.txt`、
`reports/compatibility/p3-non-connector-client-fresh-matrix-current-continuation1888.txt`、
`reports/compatibility/p3-client-fault-and-cluster-fresh-current-continuation1892.txt`。

Continuation 1894 重新执行完整 I_S/P_S 测试族（192.762 秒）并完成 P1 最终边界审计：
共享官方字段合同无差异，当前剩余仅为未实现的原生组件运行时/分配器和完整权限生命周期
聚合语义；没有新的本地失败回归。Evidence:
`reports/compatibility/p1-final-boundary-audit-current-continuation1894.txt`。

Continuation 1900 完成官方 MySQL 8.4 与 xmysql 的权限/角色生命周期对照：`mysql.user`、
`db`、`tables_priv`、`columns_priv`、`global_grants`、`proxies_priv`、`role_edges`、
`default_roles` 及 INFORMATION_SCHEMA 权限投影的创建、授予、回收、默认角色和删除场景
均通过。期间修正了 `mysql.role_edges` 的官方方向（FROM 为被授予角色，TO 为被授予账户），
并通过角色回归。Evidence:
`reports/compatibility/official-mysql-privilege-lifecycle-current/privilege-lifecycle-20261003-204707.json`、
`server/innodb/engine/account_metadata_compatibility.go`、
`server/innodb/engine/account_compatibility_test.go`。

同一轮补齐并 fresh 验证了 P_S 当前可由 xmysql 原生运行时支撑的生命周期切片：组件表独立
SELECT 权限、setup actors 的 active/default role 生命周期、mutex/rwlock/condition 实例容量
与 instrument 开关、官方容量变量、statement/instrument/thread/telemetry 限制、digest sample
age，以及 statement-stack/table-instance/file-handle 的 lost 计数。Evidence:
`reports/compatibility/p1-performance-schema-native-lifecycle-current-continuation1900.txt`。

Final verification（2026-10-04）重新生成当前矩阵并完成 `go test ./... -run '^$' -count=1`
全仓编译；矩阵更新为 24 项 `implemented`、2 项 `partial`、1 项 `deferred`、2 项
`out_of_scope`。这次验证没有把 P1 原生组件/底层分配器边界误标为完成。

当前 2 个 P1 partial 的精确定义是：

- `information_schema/runtime-component-and-permission-semantics`：普通 I_S/P_S 权限和运行时
  投影已覆盖；尚未实现 Scheduler、Clone、Keyring、Firewall、Thread Pool、NDB、Group
  Replication 等没有 xmysql 原生服务实现的组件运行时。
- `performance_schema/runtime-component-lifecycle-and-permissions`：原生容量、生命周期和
  lost 计数切片已通过；没有 xmysql 原生 allocator 的仪器，无法宣称 MySQL 精确 allocator
  统计。

Continuation 1901 完成修复后的串行全仓回归：`go test -p 1 ./... -count=1 -timeout 45m`
通过，engine 包耗时 558.584 秒，其余 Go 包全部通过；P_S 解析错误计数测试另以独立
recorder 重复运行 20 次通过。Evidence:
`reports/compatibility/full-go-regression-current-continuation1901.txt`。

Continuation 1734 补齐了二进制预处理协议的 P_S 状态计数：`Com_stmt_prepare`、
`Com_stmt_execute`、`Com_stmt_close`、`Com_stmt_reset`、`Com_stmt_send_long_data` 和
`Com_stmt_fetch` 现在由协议处理器记录，并从 `global_status/session_status` 投影。
引擎状态回归（17.920 秒）和预处理协议专项回归（1.859 秒）通过。该切片不等于完整
P_S 组件/权限/字段语义完成。Evidence:
`reports/compatibility/p1-performance-schema-prepared-command-status-current-continuation1734.txt`。

Continuation 1735 继续补齐预处理协议状态的身份维度：COM_STMT_PREPARE/EXECUTE/CLOSE/
RESET/SEND_LONG_DATA/FETCH 在拥有权威在线 session 时，现在同时进入
`status_by_thread` 和 `status_by_account/status_by_user` 的生命周期汇总；没有身份时仍只
保留全局计数，不制造伪造线程。引擎身份维度专项（13.470 秒）和网络预处理回归（1.985
秒）及完整 `server/net` 包（36.452 秒）通过。该切片仍不等于完整 I_S/P_S 运行时、组件、
权限和全局兼容矩阵完成。Evidence:
`reports/compatibility/p1-performance-schema-prepared-command-session-status-current-continuation1735.txt`。

Continuation 1733 继续复用权威事务到 session/thread 映射，修正了 InnoDB record-lock 在
`events_waits_current`、`events_waits_history` 和按线程等待汇总中的 `THREAD_ID`：有在线
session 时返回连接 ID，manager-only 调用继续使用兼容回退。包含 1732 数据锁视图的六项
专项回归通过（26.635 秒），完整 `TestPerformanceSchema` 族也通过（922.654 秒）。完整
P_S 字段精度、组件生命周期、权限语义和 P2/P3/P4 聚合仍未完成。Evidence:
`reports/compatibility/p1-performance-schema-wait-thread-mapping-current-continuation1733.txt`。

Continuation 1732 把在线 `StorageTransactionContext` 与
`performance_schema.data_locks`/`data_lock_waits` 的 `THREAD_ID` 字段接通：有权威
session 注册时返回连接 ID，找不到映射时继续返回 `NULL`，不再用事务 ID 冒充线程 ID。
专项四项回归通过（22.693 秒）。等待事件历史/汇总、完整锁元数据和值解析以及整个
P1/P2/P3/P4 聚合仍未完成。Evidence:
`reports/compatibility/p1-performance-schema-transaction-thread-mapping-current-continuation1732.txt`。

Continuation 1731 修正了 `performance_schema.data_locks` 的 record-lock 身份投影：
`ENGINE_TRANSACTION_ID` 继续返回 LockManager 的真实事务 ID，但没有权威 session/thread
映射时不再把事务 ID 冒充 `THREAD_ID`，而是返回 `NULL`；metadata-lock 仍使用真实 owner
thread。专项 data-lock/wait/metadata-lock 回归和完整 `TestPerformanceSchema` 族
（931.161 秒）通过。该切片只消除一个错误身份别名，不等于完整事务线程映射、锁元数据/值
解析、P1 聚合项或 P2/P3/P4 完成。Evidence:
`reports/compatibility/p1-performance-schema-data-lock-thread-identity-current-continuation1731.txt`。

Continuation 1730 补齐了 `INFORMATION_SCHEMA.INNODB_LOCKS` 的一个真实运行时缺口：
已有 `LockManager` granted/waiting request snapshot 现在直接投影到 `INNODB_LOCKS`，因此
只有已持有记录锁、没有当前 wait edge 时也能查询到锁；资源 ID 的 page/record 部分同时
映射到 `LOCK_PAGE`/`LOCK_REC`，原有 wait graph 回退和过滤保持不变。专项红绿测试及
`TestInformationSchema*`/`TestInformationSchemaPerformanceSchema*` 族（435.486 秒）通过。
该切片不等于完整 InnoDB schema/index/record-value 锁元数据、完整 P1 组件/权限语义或
P2/P3/P4 聚合项完成，P1 仍为 `partial`。Evidence:
`reports/compatibility/p1-innodb-locks-granted-snapshot-current-continuation1730.txt`。

Continuation 1729 补齐了 native binlog 解码器对旧版 MySQL PRE-GA 行事件
`PRE_GWRITE_ROWS_EVENT`/`PRE_GUPDATE_ROWS_EVENT`/`PRE_GDELETE_ROWS_EVENT`
（类型 20/21/22）的兼容：这些事件现在按 v1 行布局解析，不再被当作普通控制事件吞掉，
并正确生成 insert/update/delete 及 before/after 行像。专项测试先红后绿，随后整个
`server/replication` 包（147.705 秒）通过。该切片只关闭 legacy row-frame 解码缺口，
不等于官方 binlog/GTID fixture、XA 互操作、crash-kill、晋升和 fencing 聚合项完成，
P2/P4 仍为 `partial`。Evidence:
`reports/compatibility/p2-native-pre-ga-row-events-current-continuation1729.txt`。

Continuation 1728 补齐了晋升 relay 导入的状态恢复边界：当逻辑/native relay 已经落盘、
但 promoted source 的 `source_state.json` 替换失败时，重启会从已提交 relay 重建 upstream
GTID 和稳定 transaction key；随后重试导入保持单一逻辑 COMMIT，不重复事务。专项 relay
恢复和 native/XA/writer 相关回归通过。该切片只关闭一个 P2 恢复测试缺口，不能替代共识级
fencing、多源调度、分布式 channel 编排、完整官方 XA/binlog/GTID 互操作或 crash-kill
矩阵，P2/P4 聚合项仍为 `partial`。Evidence:
`reports/compatibility/p2-relay-state-persist-recovery-current-continuation1728.txt`。

Continuation 1727 补齐并回归了 P2 native endpoint 的控制面发现链路：source 的
`NativeEndpoint` 通过 `/replication/status` 暴露，replica 的 peer probe 能据此执行
native source repoint；内部凭据仍保留在内存中，持久化和对外状态继续脱敏，旧 source
UUID 在切换后被清除。专项重复 5 次和整个 `server/replication` 包（158.780 秒）通过。
该切片不等于跨节点多源调度、分布式 channel 编排、共识级 fencing 或完整官方拓扑完成，
P2/P4 聚合项仍为 `partial`。Evidence:
`reports/compatibility/p2-native-endpoint-discovery-control-plane-current-continuation1727.txt`。

Continuation 1726 补齐了八个 Performance Schema history-size 全局变量及其动态运行时
语义：statement/stage/wait/transaction 的 short history 默认 10、history_long 默认
10000；`SET GLOBAL` 会立即裁剪现有保留行并更新后续记录，`SELECT @@GLOBAL.<name>` 也能
读取 manager 定义的变量。statement、事务、wait 聚焦回归以及既有 replication system
variable 回归通过；完整 I_S/P_S 生命周期、权限和 per-thread wait 事件模型仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-history-size-variables-current-continuation1726.txt`。

Continuation 1725 修正了 Performance Schema history 的默认保留窗口：statement/stage
的 per-thread history 为 10、history_long 为 10000；transaction history/history_long
同步为 10/10000；metadata wait 的短/长保留窗口同步为 10/10000。metrics 聚焦回归和
`TestPerformanceSchema.*(History|Current|Transaction|Wait)` engine 回归（310.814 秒）
通过。该切片只修正默认容量，不等于动态 history-size 变量、完整 per-thread wait-event
lifecycle 或完整 I_S/P_S 运行时语义已经完成，P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-history-retention-defaults-current-continuation1725.txt`。

Continuation 1724 补齐了四类 Performance Schema per-thread history 的跨会话可见性：
`events_statements_history`、`events_stages_history`、`events_waits_history` 和
`events_transactions_history` 现在从所有实时线程的保留历史投影，查询者会话不再错误地
成为过滤边界；`THREAD_ID` 条件仍可用于显式缩小结果。专项回归、事务/历史回归和更宽的
current/history/transaction 回归（275.825 秒）通过，完整 `TestPerformanceSchema` 族也通过
（843.047 秒，临时目录切到 D:）。该切片只关闭一个 P1 event-history
visibility 缺口，完整 I_S/P_S 生命周期、保留容量、权限和运行时字段精度仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-event-history-all-live-threads-current-continuation1724.txt`。

Continuation 1721 补齐了 `performance_schema.prepared_statements_instances` 的跨会话运行时
可见性：查询现在从实时 process-list 快照汇总所有存活会话的协议和 SQL prepared statement，
并为每行保留正确的 `OWNER_THREAD_ID`；会话关闭或语句释放后不会保留陈旧行。先红后绿的
prepared-statement 聚焦回归（20.719 秒）以及 P_S 会话/预处理语句/processlist 回归
（52.855 秒）通过。该切片只关闭一个 P1 runtime visibility 缺口，完整 I_S/P_S 表、字段精度、
组件生命周期、运行时统计和权限语义仍为 `partial`。Evidence:
`reports/compatibility/p1-performance-schema-prepared-statements-all-live-sessions-current-continuation1721.txt`。

Continuation 1718 补齐了 `performance_schema.threads` server-main fallback 的三个运行时
字段：`PROCESSLIST_DB=mysql`、基于服务启动时间的 `PROCESSLIST_TIME` 以及默认系统资源组
`RESOURCE_GROUP=SYS_default`，并覆盖无 provider 和空 provider 两条路径。专项回归先红后绿，
完整 `TestPerformanceSchema` 族（734.861 秒，临时目录切到 D:）通过。真实 OS 线程号、主
线程内存计数、完整后台线程生命周期、权限和完整 I_S/P_S 仍为 P1 `partial`。Evidence:
`reports/compatibility/p1-performance-schema-main-thread-runtime-fields-current-continuation1718.txt`。

Continuation 1717 补齐了 `performance_schema.threads` 官方 server-main 行
`THREAD_ID=1` 的运行时更新语义：`UPDATE` 现在可修改 `INSTRUMENTED` 和 `HISTORY`，
并由后续查询返回真实值，影响行数也与官方单行更新一致。专项回归先红后绿，完整
`TestPerformanceSchema` 族（741.468 秒，临时目录切到 D:）通过。该切片只补齐主后台
线程的一项 mutation 语义；后台线程完整生命周期、运行时统计、权限和完整 I_S/P_S
仍为 P1 `partial`。Evidence:
`reports/compatibility/p1-performance-schema-main-thread-update-current-continuation1717.txt`。

Continuation 1713 补齐了官方 MySQL 8.4 `setup_instruments` 中四个 InnoDB 文件注册行：
`innodb_tablespace_open_file`、`innodb_temp_file`、`innodb_arch_file` 和
`innodb_clone_file`，默认均为 `ENABLED=YES,TIMED=YES`。该切片只扩展官方注册表，
不宣称已经实现对应的全部 file instances、I/O event lifecycle 或运行时统计；先红后绿
专项门禁和完整 `TestPerformanceSchema` 族（750.467 秒）通过。P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-innodb-file-instrument-registry-current-continuation1713.txt`。

Continuation 1714 补齐了官方 MySQL 8.4 Unix server thread registry 中的六个线程类：
`admin_interface`、`bootstrap`、`compress_gtid_table`、`manager`、`parser_service` 和
`signal_handler`。它们现在与既有四行一起出现在 `setup_threads`，并按 PSI 注册 flags
投影 `singleton/user` 属性；专项回归先红后绿，完整 `TestPerformanceSchema` 族（1023.278
秒，临时目录切到 D:）通过。该切片只补齐 registry，不代表后台线程创建、生命周期和权限
语义全部完成，P1 聚合项仍为 `partial`。Evidence:
`reports/compatibility/p1-performance-schema-thread-instrument-registry-current-continuation1714.txt`。

Continuation 1715 补齐了官方 MySQL 8.4 Windows 条件注册的四个线程类：
`con_named_pipes`、`con_shared_mem`、`con_sockets` 和 `shutdown_restart`。xmysql 现在按
`runtime.GOOS` 仅在 Windows 暴露这些行，默认属性为 `singleton`；当前 Windows 专项门禁
和完整 `TestPerformanceSchema` 族（1139.440 秒，临时目录切到 D:）通过。该切片只关闭
Windows registry 差异，不代表 Windows listener/shutdown 生命周期全部完成，P1 聚合项仍为
`partial`。Evidence:
`reports/compatibility/p1-performance-schema-windows-thread-registry-current-continuation1715.txt`。

Continuation 1716 修正了 `performance_schema.threads` 在没有当前客户端会话时的
server-main fallback：从项目自定义的 `xmysql/FOREGROUND/Sleep` 改为官方 MySQL 8.4
形状 `thread/sql/main/BACKGROUND/PROCESSLIST_ID=NULL/PROCESSLIST_COMMAND=NULL`，
并保留锁等待状态及真实前台会话投影。专项回归先红后绿，完整 `TestPerformanceSchema`
族（711.962 秒，临时目录切到 D:）通过。该切片只关闭无会话 server-main 行形状差异，
后台线程创建/移除、生命周期、运行时统计、权限和完整 I_S/P_S 仍为 P1 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-main-thread-fallback-current-continuation1716.txt`。

Continuation 1700 补齐了 `performance_schema.log_status.STORAGE_ENGINES` 的一个真实运行时
切片：当 InnoDB redo manager 可用时，返回官方 MySQL 8.4 形状
`{"InnoDB":{"LSN":...,"LSN_checkpoint":...}}`，两个值直接来自当前 redo/检查点状态，
并由 `TestPerformanceSchemaReplicationExtendedViewsProjectRuntimeState` 锁定。该切片只
减少 P1 的一个运行时缺口，完整 I_S/P_S 组件、生命周期、权限和逐表运行时语义仍保持
`partial`。Evidence: `reports/compatibility/p1-log-status-storage-engine-current-continuation1700.txt`。

Continuation 1701 修正了 `performance_schema.log_status.REPLICATION` 的 JSON 容器形状：
官方 MySQL 8.4 契约要求它是按 channel 组织的 JSON 数组；在当前 runtime 没有权威本地
relay-log 文件/位点时，现在返回 `[]`，不再返回 `{"channels":[]}` 对象，也不把 source
位点冒充 relay 位点。先红后绿回归和 log_status 字段形状回归通过。该切片只修正容器
契约，完整 relay channel lifecycle、XA/native-binlog/GTID 和官方互操作仍为 `partial`。
Evidence: `reports/compatibility/p1-log-status-replication-json-shape-current-continuation1701.txt`。

Continuation 1702 补齐了 session 变量 `transaction_read_only` 和 `tx_read_only`：默认返回
`OFF`，会话显式设置后返回实时值，两个别名都可从 I_S/P_S session-variable 视图读取。聚焦
回归先红后绿，完整 I_S/P_S 定向族也通过；该切片不改变 P1 聚合项的 `partial` 状态。
Evidence: `reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt`。

Continuation 1703 补齐了 session 变量 `character_set_database` 和 `character_set_server`：实时
会话 context 中存在的值现在可见，缺省使用当前 utf8mb4 基线。聚焦回归先红后绿通过；完整
charset/collation 与系统变量语义仍为 P1 partial。
Evidence: `reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt`。

Continuation 1704 将 session-variable 视图接到现有 `SystemVariablesManager` 清单，并保留
实时 session override；已注册的 `wait_timeout`、`innodb_page_size` 等变量现在可以查询，
`variables_by_thread` 同步受益。聚焦回归和完整 Performance Schema 定向族（737.921 秒）
均通过；MySQL 8.4 全量目录和精确动态语义仍为 P1 `partial`。
Evidence: `reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt`。

Continuation 1705 补齐了 `performance_schema.variables_info` 的全局来源状态：变量定义
初始为 `COMPILED`，通过 `SET GLOBAL` 修改后从现有 `SystemVariablesManager` 真实来源
投影为 `GLOBAL`，`SET PERSIST/PERSIST_ONLY` 仍由持久化元数据覆盖为 `PERSISTED`。新增
回归先复现旧实现错误返回 `COMPILED`，再验证来源切换和既有持久化变量行为；聚焦门禁及
完整 Performance Schema 定向族（705.749 秒）通过。完整 I_S/P_S 逐表运行时、组件生命
周期和权限矩阵仍为 P1 `partial`。
Evidence: `reports/compatibility/p1-variables-info-global-source-current-continuation1705.txt`。

Continuation 1706 修正了 `performance_schema.setup_instruments.FLAGS` 的空值语义：
没有权威 runtime flag 的 instrument 现在返回 SQL `NULL`，不再把可空 SET 列错误投影为
空字符串；`PROPERTIES` 的空 SET 和 `DOCUMENTATION` 的空值保持不变。新增回归先复现
旧实现，再通过定向 setup/variables 门禁和完整 Performance Schema 定向族（60.200 秒）。
该切片不改变 P1 聚合项的 `partial` 状态。
Evidence: `reports/compatibility/p1-performance-schema-setup-instruments-null-flags-current-continuation1706.txt`。

Continuation 1707 修正了无复制 channel 时的 P_S 复制视图语义：standalone/source runtime
不再通过空 `StatusSnapshot` 合成 `replication_applier_status`、
`replication_connection_configuration` 和 `replication_connection_status` 行；只有真实
replica channel 才投影普通复制状态，已配置但没有 source endpoint 的过滤规则仍可在过滤视图
中查询。先红后绿的专项回归和完整 Performance Schema 定向族（60.963 秒）通过。该切片
只关闭一个 runtime 空集边界，不改变 P1 聚合项和 P2/P4 互操作项的 `partial` 状态。
Evidence: `reports/compatibility/p1-performance-schema-replication-empty-runtime-current-continuation1707.txt`。

Continuation 1708 补齐了一个有官方注册来源的 `setup_instruments.PROPERTIES` 值：
`wait/io/socket/sql/client_connection` 现在返回 `user`，对应 MySQL PSI 的
`PSI_FLAG_USER`；没有权威 metadata 的其他 instrument 仍保持空属性、NULL flags、0
volatility 和 NULL documentation，不从名称推断。专项 setup 回归和完整 Performance
Schema 定向族（61.417 秒）通过。该切片只关闭一个 instrument metadata 值，P1 聚合项仍为
`partial`。
Evidence: `reports/compatibility/p1-performance-schema-client-socket-property-current-continuation1708.txt`。

Continuation 1709 继续补齐了官方 PSI 注册可确认的 memory instrument metadata：
`memory/sql/THD::main_mem_root` 现在返回 `PROPERTIES='controlled_by_default'`、
`FLAGS='controlled'`、`VOLATILITY=0` 以及官方 documentation。专项回归验证了 BIGINT
类型和值，完整 Performance Schema 定向族（60.333 秒）通过。该切片只关闭一个
`setup_instruments` metadata 行，P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-memory-instrument-metadata-current-continuation1709.txt`。

Continuation 1710 补齐了官方 `setup_instruments` 注册表中的
`statement/abstract/Query`：现在返回 `PROPERTIES='mutable'`、`FLAGS=NULL`、
`VOLATILITY=0` 和官方 documentation。专项 setup metadata 回归及完整
Performance Schema 定向族（786.386 秒，临时目录切到 D:）通过。该切片只补齐注册/元数据
行，不代表 abstract statement 完整事件生命周期完成，P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-abstract-query-instrument-current-continuation1710.txt`。

Continuation 1711 补齐了官方 statement abstract instrument 注册行：
`statement/abstract/new_packet` 和 `statement/abstract/relay_log`，两者均返回官方
默认 `ENABLED=YES`、`TIMED=YES`。专项 setup 回归及完整 Performance Schema 定向族
（785.895 秒，临时目录切到 D:）通过。该切片只补齐注册/默认状态，不代表 abstract
statement 完整 refinement/lifecycle，P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-abstract-instrument-registry-current-continuation1711.txt`。

Continuation 1712 补齐了 `statement/abstract/new_packet` 和
`statement/abstract/relay_log` 的官方 metadata：`PROPERTIES='mutable'`、`FLAGS=NULL`、
`VOLATILITY=0` 以及官方 documentation。专项 metadata 回归和完整 Performance Schema
定向族（780.342 秒，临时目录切到 D:）通过。该切片不代表完整 statement refinement/
lifecycle，P1 聚合项仍为 `partial`。
Evidence: `reports/compatibility/p1-performance-schema-abstract-instrument-metadata-current-continuation1712.txt`。

## 仍需继续实现或验证

### P1：完整 INFORMATION_SCHEMA / PERFORMANCE_SCHEMA

当前已覆盖常用表、字段形状、InnoDB 诊断、锁/等待、线程/连接、错误汇总、角色和权限
切片。仍需继续处理：

- 全量表和字段精度的逐表官方 8.4 对照；
- 运行时统计、锁/等待/线程生命周期的完整来源；
- 权限、角色、组件和 setup/instrument 生命周期的全部边界；
- Clone、Firewall、Keyring、NDB、Enterprise scheduler 等当前仓库没有对应实现的
  组件，不能用合成行冒充完成。

最后一类需要先引入对应真实 runtime 或明确产品范围，不能仅靠虚拟表注册关闭。

Continuation 1673 修正了 `performance_schema.session_account_connect_attrs` 的账号范围：
现在从实时会话快照中返回当前账号的多个会话属性，并排除其他账号；未认证会话仍只暴露
自身属性。新增回归先复现了旧实现漏掉同账号会话的行为，随后通过定向 P_S 回归和完整
I_S/P_S 定向回归。该切片不改变 P1 聚合项的 partial 状态。
Evidence: `reports/compatibility/p1-performance-schema-session-account-attrs-current-continuation1673.txt`。

Continuation 1674 补齐了 `information_schema.user_privileges` 的默认权限语义：
无显式全局权限的账户现在返回一行 `USAGE`，显式拥有全局权限的账户仍只返回实际
权限。官方 MySQL 8.4.11 Docker 对照、先红后绿的 TDD 回归，以及完整 I_S/P_S 定向
回归均通过。该切片只关闭默认 USAGE 行，P1 聚合项仍为 partial。
Evidence: `reports/compatibility/p1-information-schema-user-privileges-usage-current-continuation1674.txt`。

Continuation 1675 完成了当前定义的非 Connector/J 本地功能矩阵：Docker
`mysql:8.4.11` CLI、Go mysql-driver、PyMySQL 和 Node.js mysql2 均通过连接认证、
DDL/DML、预处理、事务、类型/元数据、保存点、多结果/错误、多会话池和重连等案例。
该切片不改变 P3 聚合项的 partial 状态；更多客户端版本、ORM/连接池、TLS/认证插件、
负协议案例以及集群端点故障切换仍需独立验证。
Evidence: `reports/compatibility/p3-client-matrix-current-continuation1675.txt`。

Continuation 1676 修复了命名 replication channel 没有继承父 Runtime 集群故障配置的缺口：
现在子 channel 会继承 `NativeEndpoint`、`Peers`、`AutoFailover`、`FailureTimeout` 和 `PollInterval`，但 source URL
与 native source 仍保持 channel 隔离。先红后绿回归和完整 replication 包回归均通过。该切片只
收口多源编排的配置继承边界，P2 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-named-channel-failover-config-current-continuation1676.txt`。

Continuation 1677 收口了复制回放的 applied/GTID marker 恢复窗口：当存储提交和 durable
commit record 已经成功、但 marker 写入失败时，恢复不会调用上游 publisher，也不会把已提交
事务当作 orphan 回滚；marker 重试成功后才清理 active journal，失败仍向调用方报告。先红后绿
的 engine 回归、复制/客户端提交恢复定向回归通过。P2/P4 的统一物理提交、crash-kill 全矩阵
和官方 MySQL 互操作仍未完成。
Evidence: `reports/compatibility/p2-replication-marker-recovery-current-continuation1677.txt`。

Continuation 1678 收口了角色激活状态对权限元数据的影响：没有 `active_roles` 参数的合成
metadata session 现在按账户的 default roles 投影；显式空 `active_roles` 仍表示 `SET ROLE NONE`。
因此“已授予但未默认、未激活”的角色不会错误出现在 `ROLE_TABLE_GRANTS` 等角色权限视图中。
先红后绿的 role 回归和完整 I_S/P_S 定向回归通过；P1 总项仍为 partial。
Evidence: `reports/compatibility/p1-role-table-grants-active-role-scope-current-continuation1678.txt`。

Continuation 1679 补齐了 `INFORMATION_SCHEMA.ENABLED_ROLES` 的 default-role 投影：合成
metadata session 缺少 `active_roles` 时，现在从持久化 default roles 加载；显式空 active-role
集合仍保持 `SET ROLE NONE` 语义。角色/P1 定向回归通过；P1 总项仍需完整元数据、组件、权限
和运行时语义验证。
Evidence: `reports/compatibility/p1-role-default-enabled-scope-current-continuation1679.txt`。

Continuation 1680 收口了本地 native promotion 的 committed relay 边界：native 解码返回聚合
commit 时，副本现在同时持久化 BEGIN/ROW 逻辑 relay 事件，提升后的 source 可以重新生成完整
GTID/BEGIN/ROW/XID 事务，而不是只有 XID_EVENT；合法 MySQL UUID 的上游 GTID 身份保持不变。
定向提升回归和完整 replication 包回归通过。P2/P4 的统一物理提交、crash-kill/拓扑矩阵及
官方 MySQL 双向互操作仍为 partial。
Evidence: `reports/compatibility/p2-native-promotion-relay-boundary-current-continuation1680.txt`。

Continuation 1681 收口了 native relay 导入的同进程重试窗口：逻辑 relay 已持久化但 native
追加失败时，后续 `ImportRelayEvents` 重试会比较逻辑流与物理文件，并从 durable logical
stream 重建缺失的 native 帧，而不是把已存在的逻辑事件当成完整成功。focused、replication
全包及 engine replication/XA 相关回归通过；P2/P4 聚合项仍保持 partial。
Evidence: `reports/compatibility/p2-native-relay-retry-rebuild-current-continuation1681.txt`。

Continuation 1682 收口了 native XA PREPARE 的同进程重试窗口：逻辑 `XA_PREPARE` 已持久化
但 native 追加失败时，后续相同 GTID/XID/key 的 prepare 会识别已有逻辑事件、重建 native
流并保持单一 prepare；随后 XA COMMIT 和 native 解码均通过。P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-xa-prepare-retry-rebuild-current-continuation1682.txt`。

Continuation 1683 收口了 native relay 的分批重试窗口：前一批只持久化 logical `BEGIN/ROW`
但 native 追加失败、后一批只补到 `COMMIT` 时，重试现在会比较完整 logical stream 与
native projection，并从 durable logical stream 重建完整事务，避免生成孤立的 native
terminal event。focused、replication 全包及 engine 复制/XA 回归通过；P2/P4 聚合项仍为
partial。Evidence: `reports/compatibility/p2-native-relay-partial-batch-rebuild-current-continuation1683.txt`。

Continuation 1684 收口了 native relay 的 GTID 身份完整性：一致性检查现在覆盖实际输出的
普通 BEGIN、XA 生命周期、终止 GTID 和 tagged GTID 帧，并校验每个物理 GTID 的 SID、tag、
sequence。即使 checksum-valid 的 native GTID 序号被篡改，重试也会从 durable logical stream
重建。focused、replication 全包及 engine 复制/XA 回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-gtid-integrity-rebuild-current-continuation1684.txt`。

Continuation 1685 收口了 native relay 的 payload 完整性：一致性检查现在按 durable logical
event 重生成 deterministic physical body，并与 native 文件中的 TABLE_MAP/ROWS/QUERY/XA
payload 比对；header position 与 checksum 等物理可变字段不参与比较。即使 checksum-valid
的 ROWS_EVENT 内容被篡改，重试也会从 durable logical stream 重建原始 payload。focused、
replication 全包及 engine 复制/XA 回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-payload-integrity-rebuild-current-continuation1685.txt`。

Continuation 1686 收口了 native relay 的 physical-header 完整性：一致性检查现在校验事件
timestamp、server-id、flags，以及物理 `log_pos` 是否等于该帧的 `EndPosition`；文件布局位置
和 checksum 字节仍按物理可变字段处理。即使 checksum-valid 的 ROWS_EVENT server-id 被篡改，
重试也会从 durable logical stream 重建。focused、replication 全包及 engine 复制/XA 回归通过；
P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-header-integrity-rebuild-current-continuation1686.txt`。

Continuation 1687 收口了首文件 native header payload 完整性：FORMAT_DESCRIPTION_EVENT 和
PREVIOUS_GTIDS_EVENT 的 deterministic body、server-id、flags、log_pos 现在参与一致性检查；
创建 timestamp 仍按物理可变字段忽略。即使 checksum-valid 的 PREVIOUS_GTIDS body 被篡改，
重试也会从 durable logical stream 重建。focused、replication 全包及 engine 复制/XA 回归通过；
P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-previous-gtid-integrity-rebuild-current-continuation1687.txt`。

Continuation 1688 收口了 rotation 后的 native header 完整性：一致性检查现在跨 logical
ROTATE 边界追踪 executed GTID，并为每个 retained native 文件生成并校验对应的
FORMAT_DESCRIPTION_EVENT/PREVIOUS_GTIDS_EVENT body、server-id、flags、log_pos。即使旋转文件
的 checksum-valid PREVIOUS_GTIDS body 被篡改，重试也会从 durable logical stream 重建；测试
改为语义帧比较，不把重建时合法的 header timestamp 变化误判为损坏。focused、replication
全包、engine 复制/XA 及全仓串行回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-rotated-header-integrity-current-continuation1688.txt`。

Continuation 1689 收口了 native `ROTATE_EVENT` 本体的一致性：一致性检查现在根据每个
logical ROTATE 边界推导下一个 native 文件名，并比较完整 deterministic rotate body；因此
checksum-valid 的轮转目标文件名篡改也会从 durable logical stream 重建。共享的 rotate body
编码同时用于正常追加和恢复重建。focused、replication 全包及 engine replication/XA 回归
通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-relay-rotate-event-integrity-current-continuation1689.txt`。

Continuation 1690 收口了 native binlog 物理帧的 short-write 检查：native 文件初始化、
`ROTATE_EVENT` 和事务帧写入现在统一检查 `io.Writer.Write` 的完整字节数，短写会返回
`io.ErrShortWrite`，交由现有恢复/重建路径处理，不再静默留下截断帧。focused、replication
全包和 engine replication/XA 回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-binlog-short-write-integrity-current-continuation1690.txt`。

Continuation 1691 收口了 pending native ROTATE 的同进程重试：当 logical ROTATE 已经同步、
但 native `ROTATE_EVENT` 追加失败时，下一次 `Rotate()` 会先从 durable logical stream
重建 native 文件并返回原 ROTATE，不再追加第二个 logical rotation boundary。focused、
rotation/restart/dump、replication 全包和 engine replication/XA 回归通过；P2/P4 聚合项仍为
partial。
Evidence: `reports/compatibility/p2-native-rotate-retry-recovery-current-continuation1691.txt`。

Continuation 1692 收口了 native `binlog.index` 持久化失败后的同进程重试：当 logical
ROTATE、native `ROTATE_EVENT` 和下一个 native 文件已经落盘、但 `binlog.index` 原子替换
失败时，下一次 `Rotate()` 会比较 durable index 与 native 文件投影，修复索引并返回原事件，
不会追加第二个 logical rotation boundary 或第三个 native 文件。focused、replication 全包、
engine replication/XA 及全仓串行回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-rotate-index-retry-recovery-current-continuation1692.txt`。

Continuation 1693 收口了 relay/import 的 durable GTID 索引重试窗口：当 logical relay 和
native binlog 已经完整落盘、但 `binlog.gtid.index` 原子发布失败时，下一次 `ImportEvents()`
会重建并持久化 GTID 索引后再确认幂等成功，不会留下 native 文件可读但 GTID 定位索引为空的
状态。focused、replication 全包、engine replication/XA 及全仓串行回归通过；P2/P4 聚合项仍为
partial。
Evidence: `reports/compatibility/p2-native-gtid-index-import-retry-current-continuation1693.txt`。

Continuation 1694 收口了 promotion/imported relay 的 native dump GTID 身份窗口：晋升节点的
native GTID_EVENT 保留上游 SID、但本机 source UUID 已变化时，dump 现在按 wire SID 将上游
canonical UUID 写入调用方 executed interval set，后续跨文件拉取可以正确过滤已观察事务。
focused、replication 全包、engine replication/XA 及全仓串行回归通过；P2/P4 聚合项仍为
partial。
Evidence: `reports/compatibility/p2-native-dump-imported-gtid-executed-set-current-continuation1694.txt`。

Continuation 1695 收口了 native dump 从事务中间位置恢复时的 GTID 推进窗口：被位置跳过的
partial transaction 不再在看到 `GTID_EVENT` 时提前写入 executed set，只有完整 `XID_EVENT` 或
XA terminal query 到达后才记录 GTID；因此后续完整重拉不会因错误的 executed 标记而丢失事务。
focused、net binlog/GTID、replication、engine 复制/XA 及全仓串行回归通过；P2/P4 聚合项仍为
partial。
Evidence: `reports/compatibility/p2-native-dump-partial-gtid-advancement-current-continuation1695.txt`。

Continuation 1696 补齐了 XA terminal 的中间位置恢复语义：普通事务的 partial position 仍不
推进 executed GTID，但如果位置落在 XA terminal GTID_EVENT 之后、且 terminal query 被输出，
则 terminal GTID 会在该完整终止边界推进，避免已提交 XA 分支在后续续传中重复或丢失。focused、
net binlog/GTID、replication、engine 复制/XA 及全仓串行回归通过；P2/P4 聚合项仍为 partial。
Evidence: `reports/compatibility/p2-native-dump-xa-terminal-mid-resume-current-continuation1696.txt`。

Continuation 1697 收口了复制 storage transaction 的同进程 marker 重试窗口：物理 WAL 已提交、
但 applied/GTID marker 首次写入失败时，重试同一个 committed context 现在只完成 journal/marker
发布，不会再次调用已完成的 `TransactionManager.Commit`。先红后绿的 engine 回归、相关
marker/flush/replay/client-commit 测试、replication、net 和 engine 复制/XA 定向回归均通过；
P2/P4 聚合项仍需统一提交协议和完整 crash-kill/拓扑/官方互操作矩阵。
Evidence: `reports/compatibility/p2-replication-committed-context-marker-retry-current-continuation1697.txt`。

Continuation 1698 收口了 SQL EVENT 与 Performance Schema 程序摘要之间的生命周期竞态：
事件 DML 的结果在提交后先通过内部 observer 发布程序摘要，再继续等待 executeQuery 的
延迟 metrics cleanup；回归同时等待事件行和 `events_statements_summary_by_program` 行，
覆盖事件执行、保留、启停及程序/触发器/存储过程摘要路径。该切片只收窄一个 P1 runtime
publication 窗口，完整 I_S/P_S 表、字段、组件、运行时和权限语义仍为 partial。
Evidence: `reports/compatibility/p1-event-program-summary-publication-current-continuation1698.txt`。

### P2：native 复制、XA、GTID 和崩溃晋升

已通过本地状态机、native binlog、配置驱动 quorum、endpoint 广播、崩溃恢复切片，以及
官方 MySQL 8.4.11 源崩溃重连和反向晋升门禁；最近又补了自节点 peer 拒绝和本地 fencing
状态覆盖保护。仍需继续：

- storage/WAL/native binlog/GTID/applied-marker 的统一物理提交协议；
- 全部 storage、relay、marker 和传输中断交错点的 crash-kill 矩阵；
- 每通道 native 拉取/应用的本地隔离已通过双上游回归；但 native endpoint 发现、跨节点多源调度
  和分布式通道编排仍未闭环。MySQL COM_REGISTER_SLAVE/COM_BINLOG_DUMP 是连接级协议，未引入
  虚构的 channel id；
- 真实进程/容器网络分区、fencing race 和更广泛多节点拓扑；
- 更多官方 MySQL 版本及完整双向 XA/binlog/GTID 矩阵。

Continuation 1652 已把共享原子复制状态文件的 rename 后目录项持久化边界补齐：Unix
目录句柄 `Sync()`；Windows 尝试目录句柄 `FlushFileBuffers()`，对常见文件系统返回的
`ERROR_ACCESS_DENIED` 按平台不支持处理为 best-effort，并有失败传播测试。它只收窄一个
本地目录项崩溃窗口，不改变上述聚合项的 `partial` 状态。Evidence:
`reports/compatibility/p2-replication-directory-fsync-current-continuation1652.txt`。

Continuation 1653 已完成目录持久化改动后的全仓 Go 串行回归：`go test -p 1 ./... -count=1
-timeout 90m` 全部通过，engine、manager、net、replication 及其余包均无失败。该回归只证明
本轮改动没有破坏现有 Go 行为，不改变 P1/P2/P3/P4 的聚合完成条件。Evidence:
`reports/compatibility/full-go-regression-current-continuation1653.txt`。

Continuation 1654 新增 native 复制部分事务批次的传输错误恢复门禁：第一次拉取返回部分帧并
报告 transport reset，重试返回剩余帧；最终行只应用一次，GTID/source position 持久化并前进，
历史 I/O 错误继续可见。Evidence:
`reports/compatibility/p2-native-partial-transport-retry-current-continuation1654.txt`。

这些不是当前已关闭项；官方门禁通过只代表已覆盖场景通过。Continuation 1643 只关闭本地
恢复日志先于物理提交的顺序窗口，不等价于统一物理原子性或分布式拓扑完成。

### P3：非 Connector/J 客户端完整矩阵

mysql CLI（Docker）、Go mysql driver、PyMySQL、Node.js/mysql2 的基础协议、类型、事务、
预处理、连接池、进程重启后的旧连接失效/新连接恢复和集群端点切片已通过。完整聚合仍需：

- 更多客户端版本、字符集/类型、负向协议和网络故障场景；
- ORM、客户端版本和连接池边界；
- 集群端点切换、旧连接失效和不依赖受控免密配置的长期受保护凭据门禁。

Continuation 1655 新增真实进程 TCP fault proxy：xmysql 服务端保持运行，代理在首个查询后
主动关闭 Go mysql-driver、PyMySQL、Node.js/mysql2 的活动连接，三者均观察到旧连接失败并
通过新连接恢复。该切片收口一个网络断连边界，但不改变 P3 聚合项的 `partial` 状态。
Evidence: `reports/compatibility/p3-client-network-fault-reconnect-current-continuation1655.txt`。

Continuation 1656 又用 Docker `mysql:5.7.44` 和 `mysql:8.0.41` 跑通当前定义的 16 个
MySQL CLI 用例；结合既有 `mysql:8.4.11` 证据，CLI 版本切片已覆盖 5.7/8.0/8.4。该测试
使用开发免密夹具，不能替代生产凭据验证，也不改变 P3 聚合项的 `partial` 状态。
Evidence: `reports/compatibility/p3-mysql-cli-version-matrix-current-continuation1656.txt`。

Continuation 1657 新鲜重跑官方 MySQL 8.4.11 反向晋升 fixture：官方 source 的普通事务、两阶段
XA 和一阶段 XA 均进入 xmysql，xmysql 重启后无重复；xmysql 晋升为 source 后，普通/XA/一阶段
XA 又进入官方 target。该子门禁通过，但完整双向 crash-kill、网络分区、更多版本和 GTID/XA
拓扑仍未闭环。Evidence: `reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1657.txt`。

Continuation 1658 新鲜重跑官方 MySQL 8.4.11 source crash/reconnect fixture：xmysql 重启后恢复
崩溃窗口前、窗口中的普通/XA/一阶段 XA 事务，随后在官方 source 重连后继续追平后续普通/XA
事务；GTID 连续且无重复。该子门禁通过，但不改变 P2/P4 聚合项的 `partial` 状态。
Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1658.txt`。

Continuation 1659 新鲜运行官方 MySQL 8.0.41 反向晋升 fixture：官方 source 的普通事务、两阶段
XA 和一阶段 XA 均应用到 xmysql；xmysql 重启后无重复；xmysql 晋升后，三类事务又成功应用到
官方 target。fixture 同时兼容 MySQL 8.0 的 `SHOW MASTER STATUS` 与 xmysql 的
`SHOW BINARY LOG STATUS`。该版本切片通过，但更多版本、完整 crash-kill/网络分区和双向 GTID/XA
拓扑仍未闭环。Evidence:
`reports/compatibility/p2-p4-official-reverse-promotion-mysql80-current-continuation1659.txt`。

Continuation 1660 新鲜运行官方 MySQL 8.0.41 source crash/reconnect fixture：xmysql 在强制终止后
恢复崩溃前及崩溃窗口中的普通、两阶段 XA 和一阶段 XA 事务；官方 source 重启后又继续追平
后续普通/XA 事务，GTID 连续且无重复。该版本 crash/reconnect 子门禁通过，但更多版本、网络
分区/fencing 和完整双向 GTID/XA 拓扑仍未闭环。Evidence:
`reports/compatibility/p2-p4-official-source-crash-reconnect-mysql80-current-continuation1660.txt`。

Continuation 1661 在 8.0.41 版本化状态查询修改后重新运行官方 MySQL 8.4.11 反向晋升 fixture：
普通事务、两阶段 XA、一阶段 XA 双向应用、xmysql 重启去重均通过，确认 `SHOW MASTER STATUS`
回退逻辑没有破坏 8.4.11 路径。该回归只确认 fixture 兼容性，不改变 P2/P4 聚合项的 partial 状态。
Evidence: `reports/compatibility/p2-p4-official-reverse-promotion-script-regression-current-continuation1661.txt`。

Continuation 1662 新增真实进程官方 MySQL 8.4.11 持续网络分区/恢复门禁：官方 source 保持运行，
proxy 在分区期间关闭活动复制连接并拒绝新连接；分区期间 xmysql 不接收新普通/XA 事务；heal
后普通、两阶段 XA、一阶段 XA 全部追平且无重复。该切片收口一个 P2/P4 网络故障边界，但
分布式 fencing/共识、多节点分区晋升和完整版本/拓扑矩阵仍未闭环。Evidence:
`reports/compatibility/p2-p4-official-source-network-partition-current-continuation1662.txt`。

Continuation 1663 回归既有一次性 TCP fault fixture：xmysql 服务端保持运行，Go mysql-driver、
PyMySQL、Node.js/mysql2 均观察到旧连接失败并通过新连接恢复。该回归确认持续分区 proxy 的
扩展没有破坏既有客户端断连路径；它不关闭非 Connector/J 客户端的完整版本、ORM、连接池、
负向协议和集群拓扑矩阵。Evidence:
`reports/compatibility/p3-client-network-fault-reconnect-current-continuation1663.txt`。

Continuation 1664 将同一持续网络分区/恢复门禁扩展到官方 MySQL 8.0.41：分区期间官方 source
保持运行且新复制连接被拒绝，heal 后普通、两阶段 XA、一阶段 XA 全部追平且无重复。该版本
切片通过，但分布式 fencing/共识、多节点分区晋升和完整版本/拓扑矩阵仍未闭环。Evidence:
`reports/compatibility/p2-p4-official-source-network-partition-current-continuation1664.txt`。

Continuation 1665 fresh-ran the targeted I_S/P_S engine regression:
`go test ./server/innodb/engine -run 'TestInformationSchema|TestPerformanceSchema' -count=1`，通过。
这只证明现有切片未回归，不改变 P1 的 partial 状态；全量表列、运行时组件生命周期和完整权限
语义仍未闭环。Evidence:
`reports/compatibility/p1-information-schema-performance-schema-targeted-regression-current-continuation1665.txt`。

Continuation 1666 将 replication/XA 与普通客户端的稳定 transaction identity 写入同一条物理
`LOG_TYPE_TXN_COMMIT` WAL 记录，并通过 manager、engine 定向测试及 manager+engine 全包回归。
这收口了物理 WAL identity binding 子边界，但 storage/WAL/native-binlog/GTID/applied-state
仍不是单一物理提交点，因此 P2/P4 聚合项继续保持 partial。Evidence:
`reports/compatibility/p2-physical-commit-identity-wal-current-continuation1666.txt`。

Continuation 1667 又确认 XA 一阶段和已 prepare 的提交路径把规范化 XA XID 写入物理
`LOG_TYPE_TXN_COMMIT` WAL 记录；普通客户端仍写入稳定 commit key，复制回放仍写入 source
transaction identity。TDD 聚焦门禁和 engine 全包回归通过，但 storage/WAL/native-binlog/
GTID/applied-state 仍不是单一物理提交点，因此 P2/P4 聚合项继续保持 partial。Evidence:
`reports/compatibility/p2-xa-identity-wal-current-continuation1667.txt`。

Continuation 1668 修复 XA publication 失败后的恢复 identity：持久化 transaction journal 的
commit record 现在与物理 WAL 一样优先使用规范化 XA XID，避免崩溃后以普通客户端 commit key
重放 XA。该缺口先由测试复现，再通过 XA journal/WAL、复制 WAL 和普通客户端 WAL 聚焦测试及
engine 全包回归；统一 storage/WAL/native-binlog/GTID/applied-state 物理提交点仍未完成，
P2/P4 聚合项继续保持 partial。Evidence:
`reports/compatibility/p2-xa-journal-recovery-identity-current-continuation1668.txt`。

Continuation 1669 又把规范化 XA XID 写入物理提交前的 DML journal；恢复在尚未生成 commit
record 的更早崩溃窗口也不会退化为普通 client key。五个 identity 门禁和 engine 全包回归
通过；统一 storage/WAL/native-binlog/GTID/applied-state 物理提交点仍未完成，P2/P4 聚合项
继续保持 partial。Evidence:
`reports/compatibility/p2-xa-precommit-journal-identity-current-continuation1669.txt`。

Continuation 1670 对照官方 MySQL 5.7.44 和 8.4.11 原始 binlog，修正了
`XA_PREPARE_EVENT` 的物理布局：事件体使用 `one_phase + format_id + gtrid_length +
bqual_length + XID`，XA format-id 从 XA START/END 生命周期语句恢复。使用不同的
server-id=5701 和 format-id=1 做原始字段区分，确认四字节字段是 format-id；物理布局
TDD、XA source/decoder 聚焦回归和 `go test ./server/replication -count=1 -timeout 30m`
均通过。随后官方反向晋升脚本在 mysql:5.7.44、8.0.41、8.4.11 均端到端通过：官方源写入
xmysql、xmysql 重启后数据保留、提升为源、普通事务以及两阶段/一阶段 XA 写回官方目标。
fixture 目标表显式使用 utf8mb4，以满足 xmysql 默认 InnoDB 字符集对应的 TABLE_MAP
VARCHAR 元数据；P2/P4 聚合项仍保持 `partial`，因为 native binlog/GTID、崩溃恢复和晋升
的更广拓扑覆盖尚未完成。Evidence:
`reports/compatibility/p2-p4-xa-prepare-layout-current-continuation1670.txt`。

Continuation 1671 收口了 I_S Enterprise Firewall 两个虚拟表的字段形状缺口：
`MYSQL_FIREWALL_USERS` 的 `USERHOST`/`MODE` 和 `MYSQL_FIREWALL_WHITELIST` 的
`USERHOST`/`RULE` 现在都有显式 MySQL 契约，不再落入通用 NAME-derived metadata 回退。
由于 xmysql 不包含 Enterprise Firewall 运行时组件，表仍返回空集，不伪造 profile/allowlist
生命周期。扩展后的关键元数据审计和对应 I_S 回归通过；全量 I_S/P_S 运行时、权限和组件
语义仍保持 `partial`。Evidence:
`reports/compatibility/p1-information-schema-firewall-metadata-current-continuation1671.txt`。

Continuation 1672 修复了 P1 定向回归中的测试资源生命周期问题：规范化语句 instrument
名称测试不再为每条只读断言创建并延迟清理一个独立引擎，避免约 60 个并存引擎使后续实例
部分初始化并在关闭时误报系统表空间缺失。修复后单测、完整 I_S/P_S 定向回归，以及
replication/net 回归均通过；没有改变生产字典或存储逻辑。该记录只关闭测试资源回归，P1
全量官方表列、可选组件 runtime 和完整权限/生命周期语义仍保持 `partial`。Evidence:
`reports/compatibility/p1-engine-lifecycle-regression-current-continuation1672.txt`。

Continuation 1673 又修正了 `performance_schema.session_account_connect_attrs` 的账号范围：
同账号的实时会话属性现在可见，不同账号属性继续隔离；未认证会话保持仅当前会话可见。
TDD 回归、完整 I_S/P_S 定向回归均通过。该修复只关闭一个运行时可见性切片，完整表列、
可选组件生命周期和权限/角色语义仍保持 `partial`。Evidence:
`reports/compatibility/p1-performance-schema-session-account-attrs-current-continuation1673.txt`。

### P4：官方 MySQL 全量 fixture

当前官方 8.4.11 fixture 已覆盖若干单向、反向、XA、重启和晋升场景；更多版本、拓扑和
故障交错仍需外部 fixture 和独立门禁。

## 明确延期或排除

- FULLTEXT：按当前开发优先级延期。
- MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎，以及非 InnoDB 的 REPAIR TABLE/引擎转换：
  明确 out_of_scope。

## 最近切片

Continuation 1640 已验证 west/east 两个命名通道分别连接独立 native source，消费各自的
binlog、GTID、source UUID 和文件位置，应用结果不串通道。Evidence:
`reports/compatibility/p2-named-replication-channels-current-continuation1640.txt`。

Continuation 1641 已补齐 native 晋升后 source repoint 的本地持久化顺序和失败回滚：identity
写失败时不发布新内存 endpoint，并恢复旧 source URL。Evidence:
`reports/compatibility/p2-native-source-repoint-persistence-rollback-current-continuation1641.txt`。

Continuation 1642 已补齐 source/XA 的同进程幂等重试 state repair：首次 state 写失败后，
keyed transaction、XA PREPARE/COMMIT/ROLLBACK 重试会重新持久化 durable state，且不追加重复
binlog。Evidence:
`reports/compatibility/p2-source-idempotent-retry-state-repair-current-continuation1642.txt`。

Continuation 1643 已补齐客户端和复制回放事务的提交前恢复日志同步：物理存储提交前先保证
row/statement journal 落盘，提交后的 commit record/marker 屏障保持不变；同步失败时物理事务
仍保持 ACTIVE，可安全重试。Evidence:
`reports/compatibility/p2-precommit-journal-sync-current-continuation1643.txt`。

Continuation 1644 已刷新官方 MySQL 8.4.11 反向晋升、官方源 crash/reconnect（含 XA）和本地
外部进程 crash-recovery 矩阵（3 次重复）证据。Evidence:
`reports/compatibility/p2-p4-external-crash-official-current-continuation1644.txt`。
这些是 P2/P4 子门禁，不等价于完整统一提交协议、全故障交错、分布式网络分区或全版本互操作
完成，聚合项继续保持 `partial`。

Continuation 1645 已用 Docker mysql:8.4.11、Go mysql-driver、PyMySQL 和 Node.js/mysql2 跑完
当前定义的 17 个非 Connector/J 客户端用例，全部通过。Evidence:
`reports/compatibility/p3-client-full-defined-matrix-current-continuation1645.txt`。
认证插件/TLS、更多客户端版本和 ORM、扩展负向/网络故障以及受保护认证仍未完成，P3 聚合项
继续保持 `partial`。

Continuation 1646 已通过服务端 TLS/认证协议切片回归，覆盖 TLS 状态记录、`REQUIRE SSL/X509`、
`caching_sha2_password` 和 `sha256_password` 的 TLS/RSA 分支。Evidence:
`reports/compatibility/p3-tls-auth-slice-current-continuation1646.txt`。
该切片不是四类客户端的真实端到端 TLS 门禁，因此 P3 聚合项仍保持 `partial`。

Continuation 1647 已补齐 MySQL 协议级 SSLRequest 状态机：服务端先发送明文握手并声明
`CLIENT_SSL`，收到 32 字节 SSLRequest 后在同一 TCP 连接上升级为 `tls.Conn`，再继续解析后续
MySQL 包。配置解析、TLS 建连、会话读循环和全仓 compile-only 回归通过。Evidence:
`reports/compatibility/p3-mysql-protocol-tls-upgrade-current-continuation1647.txt`。
真实 Go mysql-driver、PyMySQL、Node.js/mysql2、mysql CLI 的 TLS/受保护凭据端到端矩阵尚未
完成，因此 P3 聚合项继续保持 `partial`。

Continuation 1647 的后续端到端验证已完成：在 TLS 服务配置和 CA 校验下，Go mysql-driver、
PyMySQL、Node.js/mysql2 以及 Docker mysql:8.4.11 提供的 mysql CLI 均通过当前定义的客户端
用例。Evidence:
`reports/compatibility/p3-client-tls-e2e-current-continuation1647.txt`。
这只关闭当前四类客户端的受保护 TLS 子门禁；更多客户端版本、ORM、认证插件、网络故障和集群
客户端拓扑仍未完成，P3 聚合项继续保持 `partial`。

随后使用 `dev_bypass_password_auth=false` 和现有 root 受保护凭据重跑，四类客户端的 TLS/认证
矩阵也全部通过。Evidence:
`reports/compatibility/p3-client-tls-protected-e2e-current-continuation1647.txt`。
因此当前定义的非 Connector/J 协议、TLS、受保护认证子门禁均已关闭；P3 聚合项仍只代表尚未
覆盖的更多客户端版本/ORM、认证插件、网络故障和集群客户端拓扑。

Continuation 1648 已补齐 `caching_sha2_password` 快速认证的 MySQL
AuthenticationMoreData `0x01 0x03` 响应，并通过服务端回归、TLS 最小探针、Go mysql-driver、
PyMySQL 和 Docker mysql CLI 的临时账号端到端验证。Node mysql2 的历史一次性
`connect ETIMEDOUT` 已在两次独立的新服务进程重跑中通过，未能复现；当前认证插件切片
可视为已通过，但非 Connector/J 全量矩阵仍因更多版本/ORM、负向/网络故障和集群拓扑
未完成而保持 `partial`。Evidence:
`reports/compatibility/p3-client-auth-plugin-matrix-current-continuation1648.txt`、
`reports/compatibility/p3-client-auth-plugin-node-rerun-current-continuation1648.txt`。

Continuation 1649 修正了 Windows 空密码开发夹具对 PyMySQL 环境变量的误判，并重新执行真实
进程级旧连接失效/服务重启/新连接恢复门禁；Go mysql-driver、PyMySQL、Node.js/mysql2 全部
通过。该子门禁已关闭，P3 聚合项仍因更多客户端版本/ORM、负向网络故障和集群拓扑保持
`partial`。Evidence:
`reports/compatibility/p3-client-restart-reconnect-current-continuation1649.txt`、
`reports/compatibility/client-restart-reconnect-current-continuation1649`。

Continuation 1650 重新执行官方 MySQL 8.4.11 反向晋升 fixture：官方源的普通事务、两阶段 XA、
一阶段 XA 均应用到 xmysql，xmysql 重启后无重复，随后晋升为 source，晋升后的普通事务和两种
XA 事务又成功应用到官方 MySQL target。该双向反向晋升子门禁通过，但 P2/P4 聚合项仍需完整
crash-kill、网络分区/fencing、更多官方版本及全量 GTID/XA 拓扑矩阵。Evidence:
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1650.txt`。

Continuation 1651 又通过官方 MySQL 8.4.11 source crash/reconnect fixture：xmysql 崩溃窗口前、
窗口中（含一阶段/两阶段 XA）和官方 source 重连后的事务均恢复并最终 exactly-once，无重复行。
这扩大了官方 source 故障覆盖，但仍不等价于全部 storage/WAL/native-binlog/applied-marker
交错、网络分区/fencing、更多版本和完整双向拓扑完成。Evidence:
`reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1651.txt`。

## 证据规则

代码测试通过只证明对应切片；只有覆盖同等范围的官方 fixture、客户端矩阵或运行时证据
通过，才能把聚合项从 partial 改为 implemented。

### Continuation 1736

本轮对原生 binlog/GTID、XA、relay 晋升和存储提交恢复边界做了只读审计，并重新执行复制包
全量回归与存储引擎复制/XA/恢复专项。当前实现已覆盖事务键幂等、logical relay 到 native
binlog 的部分写入重建、GTID 状态持久化与启动 reconciliation、native GTID index 重建、
XA prepare/commit/rollback 以及稳定事务身份重试。

新鲜证据：`server/replication` 全包通过（162.876 秒）；存储引擎复制/XA/恢复专项通过
（247.133 秒）。证据文件：
`reports/compatibility/p2-native-binlog-gtid-recovery-audit-current-continuation1736.txt`。

该轮没有发现可安全独立修复的遗漏小点，因此不虚报聚合项完成。P2/P4 仍为 `partial`：
尚未形成跨 storage/WAL、logical/native binlog、GTID/applied marker 和外部拓扑/fencing 的
单一物理提交点，也尚未完成完整官方版本、进程 kill、网络分区和双向拓扑互操作证明；FULLTEXT
仍 deferred，非 InnoDB 仍明确 out_of_scope。

### Continuation 1737

本轮补齐一个可独立验证的 Performance Schema statement instrument 语义：新增官方兼容的
`statement/sql/error` registry 行，并在 executor/worker 两条指标路径中把 SQL parser failure
归入该专用 instrument；普通执行错误仍按解析出的 statement instrument 统计。先红后绿的
`TestPerformanceSchemaParseErrorsUseErrorInstrument` 和聚焦 Performance Schema instrument
回归均通过，`server/observability/metrics` 全包也通过。Evidence:
`reports/compatibility/p1-performance-schema-parse-error-instrument-current-continuation1737.txt`。

该切片只关闭一个 parser-error instrument 边界，不代表完整 I_S/P_S 表覆盖、字段精度、锁/
等待/线程生命周期、组件/插件生命周期或权限语义完成，P1 聚合项继续保持 `partial`。

### Continuation 1738

本轮补齐二进制预处理协议的六个 Performance Schema command instrument：`Prepare`、
`Execute`、`Close stmt`、`Reset stmt`、`Long Data` 和 `Fetch`。协议事件不再错误归入
`statement/sql/stmt_*`，并且 `setup_instruments` 的 enabled/timed 设置会影响事件采集，
而 `Com_stmt_*` 全局状态计数继续保留。`server/net` 全包、metrics 全包和引擎专项回归通过。
Evidence: `reports/compatibility/p1-performance-schema-command-instruments-current-continuation1738.txt`。

该切片只关闭预处理协议 command instrument 边界；其它 COM command、完整命令生命周期、
全量 I_S/P_S 表字段和 P1 聚合项仍保持 `partial`。

### Continuation 1739

本轮补齐普通协议命令的 Performance Schema command instrument registry 和 handler 分类，
覆盖 `Ping`、`Init DB`、`Field List`、`Refresh`、`Statistics`、`Processlist`、`Kill`、
`Debug`、`Time`、`Change user`、`Binlog Dump`、`Table Dump`、`Connect`、`Connect Out`、
`Register Slave`、`Set option`、`Daemon`、`Sleep`、`Quit` 和 `Error` 等命令；普通 COM
事件保持 `statement/com/*` 命名，预处理命令与 SQL 查询继续走各自专用路径。引擎专项、
`server/net` 映射测试、`server/net` 全包和 metrics 全包均通过。Evidence:
`reports/compatibility/p1-performance-schema-common-command-instruments-current-continuation1739.txt`。

该切片只关闭普通 COM command 的 registry/handler 分类边界；`COM_QUERY` 的父级
`statement/com/Query` 生命周期、完整 command history/current 语义、全量 I_S/P_S 表字段
和 P1 聚合项仍保持 `partial`。

### Continuation 1740

本轮补齐 COM_QUERY 当前事件的初始 instrument：真实协议处理开始执行时，
`events_statements_current` 能显示 `statement/com/Query`；完成后仍由 SQL executor 写入
最终的 `statement/sql/*` 或 `statement/sql/error` 事件，不额外生成重复历史事件。现有
Performance Schema 当前事件专项、prepared/common command 专项、解析错误专项和
`server/net` 全包均通过。Evidence:
`reports/compatibility/p1-performance-schema-com-query-current-instrument-current-continuation1740.txt`。

该切片只关闭 COM_QUERY 当前事件初始分类边界；同一物理事件的完整可变生命周期、完整
command history/current 语义、全量 I_S/P_S 表字段和 P1 聚合项仍保持 `partial`。

### Continuation 1741

本轮修正合法 `COM_RESET_CONNECTION` 的 Performance Schema instrument：新增并接入
`statement/com/Reset connection`，不再错误归入 out-of-bound 的 `statement/com/Error`。
协议映射、setup row、statement summary、`server/net` 全包和 metrics 全包均通过。Evidence:
`reports/compatibility/p1-performance-schema-reset-connection-command-current-continuation1741.txt`。

该切片只关闭一个普通 COM 命令命名缺口；剩余命令矩阵、完整 command lifecycle/current-history
语义、全量 I_S/P_S 表字段和 P1 聚合项仍保持 `partial`。

### Continuation 1742

本轮补齐 `performance_schema_setup_actors_size` 和
`performance_schema_setup_objects_size` 两个 MySQL 8.4 全局变量，默认暴露 `-1` autosizing，
并按有效默认容量 100 行限制 `setup_actors/setup_objects` 插入。Performance Schema 全专项、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-setup-capacity-current-continuation1742.txt`。

该切片只关闭 setup 表容量和溢出边界；完整 I_S/P_S 表、组件、运行时和权限语义仍保持
`partial`。

### Continuation 1743

本轮补齐 MySQL 8.4 的 `performance_schema_show_processlist` 全局动态变量，默认 `OFF`；
开启后 `SHOW PROCESSLIST` 走 Performance Schema processlist 数据源并投影回现有传统结果形状。
专项 processlist、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-show-processlist-current-continuation1743.txt`。

该切片只关闭 processlist 变量和路由边界；完整 I_S/P_S 表字段、运行时统计、锁/等待/线程、
组件/插件生命周期及权限语义仍保持 `partial`。

### Continuation 1744

本轮补齐 MySQL 8.4 的 `performance_schema_accounts_size`、
`performance_schema_hosts_size` 和 `performance_schema_users_size` 启动变量，并从
`[performance_schema]` 配置加载。配置为 0 时关闭对应连接/状态摘要，正值限制摘要行数，
`-1` 保留 autosizing 语义。专项、完整 Performance Schema 族、manager 和 `server/net`
回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-connection-summary-capacity-current-continuation1744.txt`。

该切片只关闭连接摘要容量边界；完整 I_S/P_S 运行时、组件/插件生命周期、线程/锁/等待和
权限语义仍保持 `partial`。

### Continuation 1745

本轮补齐 `performance_schema_digests_size` 启动变量，并实现 digest summary 的 0 禁用、
正值容量限制和 -1 autosizing；容量在查询过滤之后应用，避免无关 schema 的排序位置影响
带条件查询。digest 专项与完整 Performance Schema 族均通过。Evidence:
`reports/compatibility/p1-performance-schema-digest-capacity-current-continuation1745.txt`。

该切片只关闭 digest summary 容量边界；digest 算法完全一致性、lost 计数器和完整
I_S/P_S 运行时、组件/权限语义仍保持 `partial`。

### Continuation 1746

本轮补齐 `performance_schema_max_sql_text_length` 启动变量，默认 1024 字节，并将配置
限制应用到 statement event 的 `SQL_TEXT` 与 digest 的 `QUERY_SAMPLE_TEXT`；截断保持 UTF-8
有效。专项、statement history/digest 回归和完整 Performance Schema 族均通过。Evidence:
`reports/compatibility/p1-performance-schema-sql-text-length-current-continuation1746.txt`。

该切片只关闭 SQL 文本存储长度边界；完整 digest 算法、其它 P_S 容量变量、I_S/P_S 运行时和
权限语义仍保持 `partial`。

### Continuation 1747

本轮补齐 MySQL 8.4 的 `performance_schema_error_size` 启动变量，默认按当前官方参考值暴露
5377，并从 `[performance_schema]` 配置加载。配置为 0 时关闭错误摘要，正值在查询条件过滤后
限制错误摘要行数，避免无关错误占用容量。错误摘要专项、完整 Performance Schema 族、manager、
`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-error-summary-capacity-current-continuation1747.txt`。

该切片只关闭错误摘要容量和 0 禁用边界；全量错误码目录、error log 语义，以及完整 I_S/P_S
运行时、锁/等待/线程、组件和权限语义仍保持 `partial`。

### Continuation 1748

本轮补齐 `performance_schema_max_metadata_locks` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `performance_schema.metadata_locks` 查询结果（先过滤后限行）。
专项 metadata lock owner/waiter 回归通过。Evidence:
`reports/compatibility/p1-performance-schema-metadata-lock-capacity-current-continuation1748.txt`。

该切片只关闭 metadata lock 容量边界；lost 计数器、精确 instrument 分配和完整 I_S/P_S 运行时、
锁/等待/线程、组件及权限语义仍保持 `partial`。

### Continuation 1749

本轮补齐 `performance_schema_max_table_handles` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `performance_schema.table_handles` 查询结果（先过滤后限行）。
专项、显式锁和隐式事务 table handle 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-table-handle-capacity-current-continuation1749.txt`。

1748/1749 批次后的完整 Performance Schema、manager、`server/net` 和 metrics release-gate
均通过。

该切片只关闭 table handle 容量边界；lost 计数器、精确 instrument 分配和完整 I_S/P_S 运行时、
锁/等待/线程、组件及权限语义仍保持 `partial`。

### Continuation 1750

本轮补齐 `performance_schema_session_connect_attrs_size` 启动变量，支持 `-1` autosizing 和
`[performance_schema] session_connect_attrs_size` 配置，并将每会话属性值限制应用到
`session_connect_attrs` 与 `session_account_connect_attrs`，保持顺序和 UTF-8 有效。专项、
既有连接属性回归、完整 P_S、manager、`server/net`、metrics 均通过。Evidence:
`reports/compatibility/p1-performance-schema-session-connect-attrs-capacity-current-continuation1750.txt`。

后续补充已将 `Performance_schema_session_connect_attrs_lost` 暴露到 P_S 状态表，并按每个发生
截断的会话计数一次；`_truncated`、精确协议字节预算和 error-log 副作用，以及完整 I_S/P_S
运行时、组件、锁/等待/线程和权限语义仍保持 `partial`。

### Continuation 1751

本轮补齐 `performance_schema_max_prepared_statements_instances` 启动变量，支持 `-1` autosizing、
0 禁用和正值容量限制，并将容量应用到 `prepared_statements_instances` 查询结果（先过滤后限行）。
预处理语句容量、实时库存、全会话库存、执行统计、完整 P_S、manager、`server/net` 和 metrics
回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-prepared-capacity-current-continuation1751.txt`。

该切片不宣称完成精确 `max_prepared_stmt_count` autosizing，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程和权限语义；prepared loss accounting 在 Continuation 1753 完成。

### Continuation 1752

本轮补齐 `performance_schema_max_program_instances` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `events_statements_summary_by_program` 查询结果（先过滤后限行）。
专项、完整 Performance Schema、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-program-capacity-current-continuation1752.txt`。

该切片不宣称完成 program instrument allocation/lost 语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程和权限语义。

### Continuation 1753

本轮补齐 `Performance_schema_prepared_statements_lost` 状态投影和预处理语句容量溢出计数；
同一溢出库存重复读取不会重复累加，prepared 专项、完整 P_S、manager、`server/net` 和 metrics
回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-prepared-lost-current-continuation1753.txt`。

精确的 MySQL prepared-statement 分配时机以及 `max_prepared_stmt_count` 自动扩容语义仍是 P1
partial；完整 I_S/P_S 运行时、组件、锁/等待/线程和权限语义也仍未完成。

### Continuation 1754

本轮补齐 `performance_schema_max_thread_instances` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `performance_schema.threads`；同时暴露并计数
`Performance_schema_thread_instances_lost`，重复读取同一溢出线程不会重复累加。专项、完整
P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-thread-capacity-current-continuation1754.txt`。

精确的 MySQL thread-instrument 分配时机和自动扩容语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程生命周期和权限语义仍保持 P1 partial。

### Continuation 1755

本轮补齐 `performance_schema_max_file_instances` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `performance_schema.file_instances`；同时暴露并计数
`Performance_schema_file_instances_lost`，重复读取同一溢出文件不会重复累加。专项、完整 P_S、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-file-capacity-current-continuation1755.txt`。

精确的 MySQL file-instrument 分配时机和自动扩容语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程生命周期和权限语义仍保持 P1 partial。

### Continuation 1756

本轮补齐 `performance_schema_max_socket_instances` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `performance_schema.socket_instances`；同时暴露并计数
`Performance_schema_socket_instances_lost`，重复读取同一溢出 socket 不会重复累加。专项、
完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-socket-capacity-current-continuation1756.txt`。

精确的 MySQL socket-instrument 分配时机和自动扩容语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程生命周期和权限语义仍保持 P1 partial。

### Continuation 1757

本轮补齐 `Performance_schema_program_lost` 状态投影和程序汇总容量溢出计数；同一溢出程序
重复读取不会重复累加，program 专项、完整 P_S、manager、`server/net` 和 metrics 回归均通过。
Evidence: `reports/compatibility/p1-performance-schema-program-lost-current-continuation1757.txt`。

精确的 MySQL program-instrument 分配时机和自动扩容语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程生命周期和权限语义仍保持 P1 partial。

### Continuation 1758

本轮补齐 `performance_schema_max_table_lock_stat` 启动变量，支持 `-1` autosizing、0 禁用和
正值容量限制，并将容量应用到 `table_lock_waits_summary_by_table`；同时暴露并计数
`Performance_schema_table_lock_stat_lost`，重复读取同一溢出表不会重复累加。专项、完整 P_S、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-table-lock-stat-capacity-current-continuation1758.txt`。

精确的 MySQL table-lock-stat 分配时机和自动扩容语义，以及完整 I_S/P_S 运行时、组件、
锁/等待/线程生命周期和权限语义仍保持 P1 partial。

### Continuation 1759

本轮补齐 `performance_schema_max_index_stat` 启动变量，支持 `-1` autosizing、0 禁用和正值
容量限制，并将容量应用到 `table_io_waits_summary_by_index_usage`；同时暴露并计数
`Performance_schema_index_stat_lost`，重复读取同一溢出索引统计不会重复累加。专项、完整
Performance Schema、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-index-stat-capacity-current-continuation1759.txt`。

精确的 MySQL index-stat 分配时机、真实索引身份解析和自动扩容语义，以及完整 I_S/P_S 运行时、
组件、锁/等待/线程生命周期和权限语义仍保持 P1 `partial`。本切片不改变全局矩阵计数；非
InnoDB 引擎及非 InnoDB 修复/引擎转换仍明确 out of scope。

### Continuation 1760

本轮补齐 `performance_schema_max_digest_length` 启动变量，默认 1024、支持 0..1048576，
并将独立字节限制应用到 digest 聚合、按 digest 直方图和 statement-event 的 `DIGEST_TEXT`。
专项、完整 Performance Schema、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-digest-length-current-continuation1760.txt`。

精确 MySQL digest tokenizer/hash 算法和全部 digest 字段副作用，以及完整 I_S/P_S 运行时、
组件、锁/等待/线程生命周期和权限语义仍保持 P1 `partial`；本切片不改变全局矩阵计数。

### Continuation 1761

本轮补齐 MySQL 8.4 剩余 Performance Schema 容量变量及官方 lost-status 名称表面，并为
`performance_schema_digests_size` 增加 `Performance_schema_digest_lost` 稳定计数。官方容量配置、
digest lost 专项、完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-official-capacity-surface-current-continuation1761.txt`。

类/实例、file handle、mutex/rwlock、meter/metric、table instance、statement stack 等真实
分配器生命周期与 lost 计数，以及 `max_digest_sample_age` 的精确重采样策略仍保持 P1
`partial`；全量 I_S/P_S 运行时、组件和权限语义也未宣称完成。

### Continuation 1762

本轮把 `performance_schema_max_digest_sample_age` 从配置/变量表面推进到运行时：digest
聚合现在在旧样本超过配置年龄时重新选择样本，同时保留 MySQL 的慢样本优先行为；启动配置
和动态 `SET GLOBAL` 均同步到 recorder。专项 digest/age 回归、完整 `TestPerformanceSchema`
族、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-digest-sample-age-current-continuation1762.txt`。

该切片只关闭 digest sample-age 的一个运行时策略；精确 MySQL tokenizer/hash、其余 digest
字段副作用、真实 instrument allocator/lifecycle 和各类 lost 计数，以及完整 I_S/P_S 表字段、
运行时、组件和权限语义仍保持 P1 `partial`。本切片不改变全局矩阵计数。

### Continuation 1763

本轮补齐 `Performance_schema_session_connect_attrs_longest_seen` 的真实运行时最大值：按
MySQL 连接属性的编码 key/value 缓冲区字节数记录已接受连接中的最大值，且在可配置的 P_S
截断之前记录。先红后绿的连接属性专项、完整 P_S、manager、`server/net` 和 metrics 回归
均通过。Evidence:
`reports/compatibility/p1-performance-schema-connect-attrs-longest-seen-current-continuation1763.txt`。

该切片只关闭一个连接属性 status 语义；完整 I_S/P_S 表字段、allocator/lifecycle/lost
计数、运行时统计、锁/等待/线程、组件和权限语义仍保持 P1 `partial`，全局矩阵计数不变。

### Continuation 1764

本轮补齐 `Performance_schema_metadata_lock_lost`：`max_metadata_locks` 容量在查询过滤后
应用，溢出的 metadata-lock 身份只计数一次，并在 `max_metadata_locks=0` 时仍能从
`performance_schema.global_status` 观察到。专项 metadata-lock/等待回归、完整 P_S、manager、
`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-metadata-lock-lost-current-continuation1764.txt`。

该切片只关闭 metadata-lock lost counter；其余 allocator/lifecycle/lost 家族、完整 I_S/P_S
表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，全局矩阵计数不变。

### Continuation 1765

本轮补齐 `Performance_schema_accounts_lost`、`Performance_schema_hosts_lost` 和
`Performance_schema_users_lost` 的真实容量溢出计数；账号/主机/用户身份稳定去重，过滤后
限容，重复读取不重复累加，并排除内部无用户身份的合成摘要行。连接汇总专项、完整 P_S、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-connection-summary-lost-current-continuation1765.txt`。

该切片只关闭三个 connection-summary lost counter；其余 allocator/lifecycle/lost 家族、
完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，
全局矩阵计数不变。

### Continuation 1766

本轮将 `performance_schema_max_statement_classes` 从变量表面推进到 statement instrument 注册容量：
按稳定注册顺序限容，容量为 0 时不暴露 `statement/*` instrument，超出容量的运行时 statement
instrument 不再视为已启用，并以真实排除数量提供 `Performance_schema_statement_classes_lost`。
statement-class 专项、完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-statement-classes-current-continuation1766.txt`。

该切片只关闭一个 statement-class allocator/capacity 语义；其余 allocator/lifecycle/lost 家族、
完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，
全局矩阵计数不变。

### Continuation 1767

本轮将当前已暴露的 stage/file/socket/memory instrument 接入对应的
`performance_schema_max_*_classes` 注册容量；容量为 0 时从 `setup_instruments` 移除，运行时
也视为未注册，并实现四个对应的 `*_classes_lost` 实际计数。专项、完整 P_S、manager、
`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-instrument-class-capacity-current-continuation1767.txt`。

该切片只关闭四类 instrument class capacity/lost 语义；其余 allocator/lifecycle/lost 家族、
完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，
全局矩阵计数不变。

### Continuation 1768

本轮将 `performance_schema_max_thread_classes` 接入 `setup_threads` 注册容量，并让前台
`thread/sql/one_connection`、后台 `thread/sql/main` 的运行时 instrument 状态遵守 class 是否
注册；`Performance_schema_thread_classes_lost` 改为实际排除数量。线程 class 专项、完整 P_S、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-thread-classes-current-continuation1768.txt`。

该切片只关闭 thread-class capacity/lifecycle 语义；其余 allocator/lifecycle/lost 家族、完整
I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，全局矩阵
计数不变。

### Continuation 1769

本轮将 `performance_schema_max_meter_classes` 和 `performance_schema_max_metric_classes` 接入
telemetry registry；容量限制在 `setup_meters`/`setup_metrics` 查询过滤前生效，并实现
`Performance_schema_meter_lost`、`Performance_schema_metric_lost` 实际计数。telemetry 专项、
完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-telemetry-classes-current-continuation1769.txt`。

该切片只关闭 telemetry class capacity/lost 语义；其余 allocator/lifecycle/lost 家族、完整
I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，全局矩阵
计数不变。

### Continuation 1770

本轮将 file/socket/memory class capacity 从注册表和运行时启用判断继续投影到对应的
instances/summary 查询；容量为 0 时这些运行时视图不再返回未注册类的行，默认容量路径保持不变。
专项、完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-instrument-runtime-capacity-current-continuation1770.txt`。

该切片只关闭三类 instrument runtime projection 语义；mutex/rwlock/condition/statement-stack
等仍无权威运行时数据源的视图、其余 allocator/lifecycle/lost 家族、完整 I_S/P_S 表字段和运行时
统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，全局矩阵计数不变。

### Continuation 1771

本轮补齐 `Performance_schema_table_handles_lost` 的真实计数：当已有真实显式锁或隐式事务
table handle 被 `performance_schema_max_table_handles` 淘汰时，按稳定句柄身份计数；句柄重新可见
后同步移除 lost 身份。专项、完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-table-handles-lost-current-continuation1771.txt`。

该切片只关闭 table-handle lost accounting 语义；其余 allocator/lifecycle/lost 家族、完整
I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，全局矩阵
计数不变。

### Continuation 1772

本轮补齐 `performance_schema_max_file_handles` 的运行时 lost accounting：使用现有
RuntimeRecorder 的 active file-handle 数量，按稳定 file/event/ordinal 身份识别容量溢出，
并在读取 `global_status` 时提供 `Performance_schema_file_handles_lost`。专项、完整 P_S、
manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-file-handles-lost-current-continuation1772.txt`。

该切片只关闭 file-handle capacity/lost accounting 语义；其余 allocator/lifecycle/lost 家族、
完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，
全局矩阵计数不变。

### Continuation 1773

本轮补齐真实 trigger stack 路径上的 `performance_schema_max_statement_stack` 溢出计数：
当 `ExecutionContext.triggerStack` 达到配置容量时，累计
`Performance_schema_nested_statement_lost`，并保持既有触发器递归保护行为不变。专项、
AFTER trigger 行为回归、完整 P_S、manager、`server/net` 和 metrics 回归均通过。完整 P_S
首次回归还发现并修复了两个官方 rwlock lost 状态行缺失的问题。Evidence:
`reports/compatibility/p1-performance-schema-statement-stack-lost-current-continuation1773.txt`。

该切片只关闭 statement-stack lost accounting 语义；其余 allocator/lifecycle/lost 家族、
完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1 `partial`，
全局矩阵计数不变。

### Continuation 1774

本轮将 `performance_schema_max_table_instances` 接入现有 RuntimeRecorder 的真实
table-I/O 对象来源：按对象类型/schema/table 去重并执行容量投影，超出容量的对象累计
`Performance_schema_table_instances_lost`，table 和 index I/O 汇总均遵守该容量。专项、
table-I/O 生命周期回归、完整 P_S、manager、`server/net` 和 metrics 回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-table-instances-lost-current-continuation1774.txt`。

该切片只关闭 recorder-backed table-instance capacity/lost 语义；其余 allocator/lifecycle/lost
家族、完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍保持 P1
`partial`，全局矩阵计数不变。

### Continuation 1775

本轮把 InnoDB SELECT 执行器已经确定的物理索引身份传入 RuntimeRecorder，
`performance_schema.table_io_waits_summary_by_index_usage` 不再把该类真实访问统一折叠为空
`INDEX_NAME`；table scan 和没有权威索引身份的旧调用继续使用空索引行。索引身份、table-I/O
截断及受影响包回归通过。Evidence:
`reports/compatibility/p1-performance-schema-index-identity-current-continuation1775.txt`。

该切片只关闭 executor/recorder 的 index identity 投影语义；完整 MySQL index-stat 选择、连接
场景的逐表索引分配、全量 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、组件及权限语义仍
保持 P1 `partial`，全局矩阵计数不变。

### Continuation 1776

本轮补齐可截断 Performance Schema setup 表的 `DROP` 权限语义：认证会话在
`setup_actors`/`setup_objects` 的 TRUNCATE 分支先执行表级 DROP 检查；无权限时不改变运行时配置，
授予 DROP 后才允许清空。`setup_consumers`/`setup_instruments`/`setup_threads` 的既有拒绝边界
保持不变。专项和完整 P_S 回归通过。Evidence:
`reports/compatibility/p1-performance-schema-truncate-drop-privilege-current-continuation1776.txt`。

该切片只关闭 setup-table TRUNCATE 权限语义；全量 I_S/P_S 运行时、组件生命周期、其余权限/角色
边界仍保持 P1 `partial`，全局矩阵计数不变。

### Continuation 1777

重跑此前 Node.js/mysql2 认证插件失败场景时确认旧 TLS 证书已过期，先更换当前有效的临时
localhost 证书，再通过 `caching_sha2_password`、`sha256_password` 及既有客户端边界矩阵。
Node.js/mysql2 全部场景通过，关闭该认证插件子门禁；P3 聚合仍为 `partial`，因为更多客户端版本、
ORM/连接池、协议负例、集群拓扑和长期故障场景仍未完成。Evidence:
`reports/compatibility/p3-client-node-auth-rerun-current-continuation1777.txt`。

### Continuation 1778

补齐 `COM_QUERY` 的 Performance Schema 父级命令 instrument 完成态：网络协议层现在在命令结束时
记录 `statement/com/Query`，引擎原有的 `statement/sql/*` 事件继续独立保留，避免把协议命令和 SQL
语义混成一条记录。专项 `server/net` 回归通过；受影响包级回归中 `server/net` 和 metrics 通过，
`server/innodb/engine` 在显式 45 分钟上限超时，因此不作为全包绿灯证据。Evidence:
`reports/compatibility/p1-performance-schema-com-query-command-lifecycle-current-continuation1778.txt`。

该切片只关闭 COM_QUERY 父级命令生命周期；完整 I_S/P_S 表字段和运行时统计、锁/等待/线程精度、
组件生命周期及权限/角色语义仍为 P1 `partial`。非 Connector/J 客户端矩阵继续纳入 P3，
XA/binlog/GTID/崩溃恢复/提升继续纳入 P2/P4；Fulltext 延后，非 InnoDB 不纳入范围。

### Continuation 1779

完成 P2 原生复制回归和官方 fixture 环境审计：`server/replication` 全量回归通过，覆盖
native binlog、GTID、XA、relay/restart、promotion/fencing、rotation、purge 和 exactly-once
状态转换；官方 MySQL 互操作测试全部因当前环境未提供
`XMYSQL_OFFICIAL_BINLOG_URL` / `XMYSQL_OFFICIAL_BINLOG_DSN` 而跳过，不能记为 PASS。Evidence:
`reports/compatibility/p2-replication-regression-official-fixture-audit-current-continuation1779.txt`。

该轮只强化了 P2 的 xmysql-native 证据，官方 MySQL XA/binlog/GTID/崩溃恢复/提升互操作仍为
P2/P4 `partial`，待受保护官方 fixture 可用后才能关闭；P1/P3、Fulltext 和非 InnoDB 范围不变。

### Continuation 1780

重新执行 P1 I_S/P_S、权限和角色回归：`go test -p 1 ./server/innodb/engine -run
'^(TestInformationSchema|TestPerformanceSchema|TestRole|TestApplicableRoles|TestMandatoryRoles|TestInformationSchemaPrivilege)'
-count=1 -timeout 30m` 通过，engine 用时 1669.007s。该证据确认当前已实现切片稳定，
但不关闭完整 I_S/P_S 的组件运行时来源、字段权威值、锁/等待/线程精度和全部权限角色生命周期。
Evidence: `reports/compatibility/p1-information-performance-regression-current-continuation1780.txt`。

### Continuation 1781

刷新非 Connector/J 客户端环境矩阵：Go、PyMySQL、Node.js/mysql2 运行时可用；当前主机没有
`mysql.exe`，因此 CLI 诊断为 `SKIPPED_ENVIRONMENT`。本次只运行 diagnostic-only，不启动服务端、
不执行 SQL，也没有写入密码；P3 完整矩阵继续保持 `partial`，待受保护 live-server 条件满足后
继续执行真实客户端 SQL、连接池、协议负例、版本和集群故障场景。Evidence:
`reports/compatibility/p3-client-environment-matrix-audit-current-continuation1781.txt`。

### Continuation 1783

在隔离的 xmysql-server 实例上真实执行非 Connector/J 客户端矩阵：Go/go-sql-driver/mysql、
PyMySQL、Node.js/mysql2 的全部定义 runner cases 通过，覆盖连接认证、DDL/DML、prepared
statement、事务、savepoint、NULL/类型、元数据、多结果/错误、错误码、wire value、多会话池和
重连。MySQL CLI 因 `mysql.exe` 缺失且 Docker CLI fallback 不可用而跳过。Evidence:
`reports/compatibility/p3-client-live-matrix-go-python-node-current-continuation1783.txt`。

该轮关闭了三类可用客户端的 live smoke slice，但没有关闭 P3 全量矩阵；CLI、版本组合、生产
TLS/认证、长期连接池/故障切换、协议负例和集群端点场景仍需继续验证，P3 聚合项保持 `partial`。

### Continuation 1782

本轮专项回归确认 `mutex_instances`、`rwlock_instances`、`cond_instances` 当前只有官方列形状和
无运行时组件时的空集投影；源码没有可提供对象身份、锁持有线程、等待状态和 allocator/lost
计数的权威同步对象生命周期。没有因此伪造 mutex/rwlock/condition 行，也没有把 Go `sync`
原语误映射为 MySQL instrument。专项形状/空组件回归通过。Evidence:
`reports/compatibility/p1-performance-schema-sync-instance-source-audit-current-continuation1782.txt`。

该轮没有生产代码变更；要关闭这部分 P1，下一步必须先建立同步对象生命周期和等待事件的真实
来源，再补 capacity、instance、wait、thread-owner、history 与 lost status 的红/绿回归。完整
I_S/P_S、组件、锁/等待/线程和权限语义仍为 `partial`，全局矩阵计数不变。

### Continuation 1784

本轮建立了第一个权威同步对象运行时来源：`performance_schema.rwlock_instances` 现在从
`tableDDLCoordinator` 的真实表级 DDL `sync.RWMutex` 注册表投影对象身份、写锁持有线程和读锁计数；
`performance_schema_max_rwlock_instances` 会限制实例容量并累计
`Performance_schema_rwlock_instances_lost`，关闭对应 `setup_instruments` instrument 后实例和 lost
计数同步清空。先失败后修复的专项测试、既有 P_S 定向回归及完整 `^TestPerformanceSchema` 回归
全部通过，完整回归耗时 965.358s。Evidence:
`reports/compatibility/p1-performance-schema-rwlock-instances-current-continuation1784.txt`。

该切片只推进了 P1 的 rwlock 实例真实来源；mutex/condition 仍因缺少权威生命周期来源保持空集，
完整 I_S/P_S 表字段、运行时统计、锁/等待/线程精度及组件/权限语义仍为 `partial`。非 InnoDB
继续不纳入范围；P3 非 Connector/J 全量客户端矩阵和 P2/P4 XA/binlog/GTID/崩溃恢复/提升官方
互操作仍按各自门禁继续推进。

### Continuation 1785

本轮为 P3 非 Connector/J 客户端矩阵增加 `COM_RESET_CONNECTION` live slice：PyMySQL 使用原始
协议命令、Node.js/mysql2 使用 `connection.reset()`，两者均验证 reset 后用户变量被清理；服务端
同时修复了动态用户变量残留在直接 session parameter、未被 `ResetSession` 清理的问题。Go/
go-sql-driver/mysql 保持既有用例通过，但其公开 `driver.SessionResetter` 不发送该协议命令，故不
伪造 Go 的 reset 协议覆盖。Evidence:
`reports/compatibility/p3-client-session-reset-current-continuation1785.txt`。

MySQL CLI 因当前主机没有 `mysql.exe` 且 Docker fallback 不可用而为 `SKIPPED_ENVIRONMENT`；
版本组合、TLS/认证插件、ORM/连接池、协议负例、长期故障切换和集群端点仍未完成，P3 聚合项
继续保持 `partial`。Fulltext 仍延后，非 InnoDB 仍不纳入范围。

### Continuation 1786

本轮为 P3 非 Connector/J 客户端矩阵增加真实 `COM_PING` 门禁：Go/go-sql-driver/mysql 的
`database/sql` `Ping`、PyMySQL `connection.ping(reconnect=False)` 和 Node.js/mysql2
`connection.ping()` 均通过；此前的 `COM_RESET_CONNECTION` 也继续通过。先失败后补齐
runner 的专项证据见 `reports/compatibility/p3-client-protocol-ping-current-continuation1786.txt`。

MySQL CLI 仍因缺少 `mysql.exe` 且 Docker fallback 不可用而跳过；P3 全量客户端、版本/TLS/
认证、ORM/连接池、协议负例、长期故障切换和集群端点仍为 `partial`。

### Continuation 1787

本轮在 PyMySQL live runner 中加入真实 `COM_INIT_DB` 门禁：`connection.select_db()` 后，
`SELECT DATABASE()` 返回目标库；Go、PyMySQL、Node.js/mysql2 的最终矩阵保持通过。
服务端网络层回归 `go test ./server/net -count=1 -timeout 10m` 通过（14.421s）。

MySQL CLI 因当前环境缺少 `mysql.exe` 且 Docker fallback 不可用，仍为
`SKIPPED_ENVIRONMENT`；P3 全量客户端矩阵继续保持 `partial`。Evidence:
`reports/compatibility/p3-client-protocol-init-db-current-continuation1787.txt`。

### Continuation 1788

本轮在 Node.js/mysql2 live runner 中加入真实 `COM_CHANGE_USER` 门禁：调用
`connection.changeUser()` 后校验目标数据库，并保留此前 Go/PyMySQL/Node.js 通过结果。
先失败后实现的证据见 `reports/compatibility/p3-client-protocol-change-user-current-continuation1788.txt`。

MySQL CLI 仍因缺少 `mysql.exe` 且 Docker fallback 不可用而跳过；P3 全量客户端矩阵、
版本/TLS/认证、ORM/连接池、协议负例、长期故障切换和集群端点仍保持 `partial`。

### Continuation 1789

本轮补齐 `performance_schema.mutex_instances` 的真实执行器来源：account、DDL coordinator、
global read-lock state、active query 和 XA 五个 `sync.Mutex` 实例现在可查询，并验证了
`performance_schema_max_mutex_instances` 容量、`Performance_schema_mutex_instances_lost`、
instrument 开关和实例身份。`LOCKED_BY_THREAD_ID` 保持 NULL，因为 Go mutex 没有可验证的
MySQL thread owner；同时补齐 `max_mutex_classes` / `max_rwlock_classes` 对
`setup_instruments` 和 class lost 统计的约束；condition 实例仍不伪造。

专项及交叉回归均通过，证据见
`reports/compatibility/p1-performance-schema-mutex-instances-current-continuation1789.txt`。
完整 I_S/P_S 字段、运行时统计、等待/线程精度和权限语义仍使 P1 聚合项保持 `partial`。

### Continuation 1790

本轮补齐原生 binlog 解码器对 MySQL `BEGIN_LOAD_QUERY_EVENT(17)` 和
`EXECUTE_LOAD_QUERY_EVENT(18)` 的已知控制事件消费，覆盖顶层、事务 payload 内嵌和
row-image 解码路径；未知事件拒绝行为保持不变。专项测试及完整 replication 回归通过。

Evidence: `reports/compatibility/p2-native-control-events-current-continuation1790.txt`。
该增量不改变官方 MySQL XA/binlog/GTID、崩溃恢复和提升互操作仍待 live fixture 验证的状态。

### Continuation 1791

本轮补齐 `performance_schema.cond_instances` 的第一个真实运行时来源：全局读锁 gate
现在持有并广播一个实际使用的 `sync.Cond`，查询可返回其对象身份；同时接入
`max_cond_classes`、`max_cond_instances`、instrument 开关以及
`Performance_schema_cond_classes_lost` / `Performance_schema_cond_instances_lost`。
失败优先测试先确认旧实现没有实例行，修复后 condition 专项及完整
`go test ./server/innodb/engine -run '^TestPerformanceSchema' -count=1 -timeout 30m`
通过（968.176s）。Evidence:
`reports/compatibility/p1-performance-schema-condition-instances-current-continuation1791.txt`。

该切片只关闭全局读锁 condition instance 的本地来源；完整 I_S/P_S 表列、所有等待生产者、
线程 owner 精度、组件生命周期和完整权限/角色语义仍为 P1 partial。P2/P4 官方 XA/binlog/
GTID/崩溃恢复/提升和 P3 全量客户端矩阵边界不变。

### Continuation 1792

本轮为全局读锁 gate 接入真实等待生命周期：被 `FLUSH TABLES WITH READ LOCK` 阻塞的写入
现在可在 `events_waits_current` 中按 session/thread 看到 condition wait，释放锁后进入
`events_waits_history` / `events_waits_history_long`，并汇总到按线程和全局 wait summary。
专项、相关等待/MDL 回归及完整 `^TestPerformanceSchema` 回归均通过，证据见
`reports/compatibility/p1-performance-schema-global-read-lock-waits-current-continuation1792.txt`。

该增量只关闭全局读锁这一等待生产者；完整 I_S/P_S 表列、其余等待生产者、线程 owner 精度、
组件生命周期和完整权限/角色语义仍为 P1 partial。P2/P4 官方 XA/binlog/GTID/崩溃恢复/
提升、P3 全量非 Connector/J 客户端矩阵仍待独立验收；Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1854

本轮补齐 `mysql.procs_priv` 的 `HOST`、`USER`、`DB`、`ROUTINE_NAME` 的 `IN`、`NOT IN`、
NULL 谓词，保留等值、`LIKE` 和 JDBC 兼容列投影。参数/例程回归通过，证据见
`reports/compatibility/p1-mysql-procs-priv-predicate-filters-current-continuation1854.txt`。

该切片只关闭 `mysql.procs_priv` 谓词边界；完整 mysql.* 表覆盖、例程/权限生命周期、
I_S/P_S runtime、官方 MySQL 互操作和非 Connector/J 客户端矩阵仍保持原状态。Fulltext
继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1877

本轮修复角色授权表和 MySQL 系统表的空字符串谓词：`=`/`LIKE ''` 现在与“没有谓词”区分，
`mysql.role_edges`、`mysql.server_cost` 等查询不再错误返回全量行；通配符和非空等值/LIKE
语义保持不变。专项回归通过，证据见
`reports/compatibility/p1-mysql-metadata-empty-string-predicates-current-continuation1877.txt`。

该切片只关闭一个本地元数据过滤边界；完整 I_S/P_S 组件运行时、字段精度、权限生命周期、官方
XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵仍保持 partial。Fulltext
继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1878

本轮修复 `INFORMATION_SCHEMA.PARAMETERS` 的 schema/routine 空字符串谓词：只有实际出现的
`SPECIFIC_SCHEMA`、`ROUTINE_SCHEMA`、`SPECIFIC_NAME`、`ROUTINE_NAME` 条件参与匹配，`=`/`LIKE ''`
不再被当作无过滤。专项例程回归通过，证据见
`reports/compatibility/p1-information-schema-parameters-empty-string-predicates-current-continuation1878.txt`。

该切片只关闭一个本地 PARAMETERS 过滤边界；完整 I_S/P_S 组件运行时、字段精度、权限生命周期、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 全量客户端矩阵仍保持 partial。Fulltext
继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1879

通用 INFORMATION_SCHEMA 元数据视图现在区分缺少过滤条件与显式空字符串条件：
`TABLE_SCHEMA/TABLE_NAME/COLUMN_NAME/... = ''`、`LIKE ''` 只匹配真实空值，不再把空字符串
误判为无过滤并返回全量元数据。专项 I_S 元数据族和全仓编译通过，证据见
`reports/compatibility/p1-information-schema-metadata-empty-string-predicates-current-continuation1879.txt`。

该切片只关闭一个共享元数据谓词边界；完整 I_S/P_S 表字段、组件/权限/线程运行时语义、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1880

`INFORMATION_SCHEMA.PARAMETERS` 也已修复 `PARAMETER_NAME/PARAMETER_MODE` 的显式空字符串
`=`/`LIKE` 语义：procedure 参数使用空模式精确匹配，function return 的 NULL 参数名/模式不
会错误匹配空字符串。专项 PARAMETERS、相关例程族和全仓编译通过，证据见
`reports/compatibility/p1-information-schema-parameters-empty-string-predicates-current-continuation1880.txt`。

该切片只关闭一个 PARAMETERS 谓词边界；完整 I_S/P_S 表字段、组件/权限/线程运行时语义、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1882

INFORMATION_SCHEMA 专用目录过滤器现在保留显式空字符串语义：view table/routine usage、
`KEYWORDS.WORD`、空间单位、空间参考系统和几何列的 `= ''`/`LIKE ''` 不再被当成缺少谓词；
真实空值 SRS 行仍可被精确区分。专项回归通过，证据见
`reports/compatibility/p1-information-schema-specialized-empty-string-predicates-current-continuation1882.txt`。

该切片只关闭一组共享专用过滤解析边界；完整 I_S/P_S 表字段、组件/权限/线程运行时语义、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1881

INFORMATION_SCHEMA 权限视图现在区分缺少谓词与显式空字符串谓词：`TABLE_SCHEMA = ''`、
`PRIVILEGE_TYPE LIKE ''` 不再恢复已有权限行。专项权限回归通过，证据见
`reports/compatibility/p1-information-schema-privilege-empty-string-predicates-current-continuation1881.txt`。

该切片只关闭一个共享权限视图谓词边界；完整 I_S/P_S 表字段、组件/权限/线程运行时语义、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1793

本轮补齐 `events_waits_summary_by_instance` 对全局读锁 condition wait 的实例级汇总，当前
等待和完成历史均使用与 `cond_instances` 相同的稳定实例身份。失败优先测试先确认等待期间
实例汇总为空，修复后完整 `^TestPerformanceSchema` 回归通过（84.678s）。Evidence：
`reports/compatibility/p1-performance-schema-global-read-lock-instance-summary-current-continuation1793.txt`。

该切片仍不代表完整 I_S/P_S 聚合完成；其余等待生产者、全量字段、线程 owner 精度、组件
生命周期和权限/角色语义继续为 P1 partial。P2/P4/P3、Fulltext 和非 InnoDB 边界不变。

### Continuation 1794

本轮按 MySQL 8.4 wait-event 对象契约修正全局读锁 condition wait：
`OBJECT_SCHEMA`、`OBJECT_NAME`、`OBJECT_TYPE` 均为 `NULL`，`OBJECT_INSTANCE_BEGIN` 与
`performance_schema.cond_instances` 的实际 `sync.Cond` 身份一致。失败优先测试、对象契约
专项和完整 `^TestPerformanceSchema` 回归均通过，证据见
`reports/compatibility/p1-performance-schema-global-read-lock-wait-object-contract-current-continuation1794.txt`。

该切片只修正一个等待生产者的对象字段精度；P1 全量 I_S/P_S、其余等待生产者、线程 owner、
组件生命周期和权限/角色语义仍为 partial。P2/P4/P3、Fulltext 和非 InnoDB 边界不变。

### Continuation 1795

本轮补齐全局读锁 condition wait 的 per-thread history 容量：设置
`performance_schema_events_waits_history_size=1` 后，`events_waits_history` 只保留同一线程
最新的一条等待；`history_long` 继续使用全局长历史边界。失败优先测试和完整
`^TestPerformanceSchema` 回归通过，证据见
`reports/compatibility/p1-performance-schema-global-read-lock-history-capacity-current-continuation1795.txt`。

短/长历史的独立底层存储在容量差异场景仍需继续收口；P1 其它 I_S/P_S、等待生产者、线程
owner、组件生命周期和权限/角色语义仍为 partial，P2/P4/P3、Fulltext、非 InnoDB 边界不变。

### Continuation 1796

本轮完成全局读锁 condition wait 的短/长历史独立底层保留：当
`history_size=3`、`history_long_size=1` 时，短历史保留 3 条、长历史保留 1 条，且按完成
序列稳定返回。失败优先测试和完整 `^TestPerformanceSchema` 回归通过（86.198s），证据见
`reports/compatibility/p1-performance-schema-global-read-lock-independent-history-current-continuation1796.txt`。

该切片只关闭全局读锁等待生产者的历史容量边界；P1 全量 I_S/P_S、其余等待生产者、线程
owner、组件生命周期和权限/角色语义仍为 partial，P2/P4/P3、Fulltext、非 InnoDB 边界不变。

### Continuation 1797

本轮补齐全局读锁 condition wait 的历史 truncate 作用域：truncate 短历史不清长历史，
truncate 长历史不清短历史；已有锁管理器和 MDL 历史 reset 行为保持不变。失败优先测试及
完整 `^TestPerformanceSchema` 回归通过（86.732s），证据见
`reports/compatibility/p1-performance-schema-global-read-lock-history-truncate-current-continuation1797.txt`。

该切片只关闭一个等待生产者的 reset 语义；P1 全量 I_S/P_S、其余等待生产者、线程 owner、
组件生命周期和权限/角色语义仍为 partial，P2/P4/P3、Fulltext、非 InnoDB 边界不变。

### Continuation 1798

本轮补齐 `events_waits_summary_by_instance` 的独立 truncate 分发和全局读锁实例摘要
reset：by-instance truncate 清零实例摘要，但不清 global/thread 摘要；global summary
truncate 对 by-instance 的既有零行投影保持不变。失败优先专项、相关回归及完整
`^TestPerformanceSchema` 回归通过（87.267s），证据见
`reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-truncate-current-continuation1798.txt`。

该切片只关闭一个摘要表 reset 语义；P1 全量 I_S/P_S、其余等待生产者、线程 owner、
组件生命周期和权限/角色语义仍为 partial，P2/P4/P3、Fulltext、非 InnoDB 边界不变。

### Continuation 1799

本轮将 by-instance reset 扩展到完成的 record-lock、MDL 和全局读锁等待：只过滤
truncate 前的已完成记录，保留新的等待，并继续隔离 global/thread 摘要。相关专项和
完整 `^TestPerformanceSchema` 回归通过（86.872s），证据见
`reports/compatibility/p1-performance-schema-wait-summary-instance-reset-current-continuation1799.txt`。

该切片只关闭 wait-summary-by-instance 的完成等待 reset 隔离；P1 全量 I_S/P_S、其余
等待生产者、线程 owner、组件生命周期和权限/角色语义仍为 partial，P2/P4/P3、Fulltext、
非 InnoDB 边界不变。

### Continuation 1800

本轮固化嵌套角色的可见性边界：`SET ROLE ALL` 后，
`INFORMATION_SCHEMA.ENABLED_ROLES` 只报告直接启用的父角色；父角色继承的子角色权限仍
通过 `ROLE_TABLE_GRANTS` 进入有效权限图。专项回归通过（5.742s），证据见
`reports/compatibility/p1-nested-role-enabled-vs-inherited-privilege-current-continuation1800.txt`。

该切片只关闭一个角色可见性回归边界，不改变完整 I_S/P_S、全部权限视图、组件生命周期、
P3 全量客户端矩阵和 P2/P4 XA/binlog/GTID/崩溃恢复/提升的聚合状态。

### Continuation 1801

本轮修正全局读锁 condition wait 的 `OBJECT_INSTANCE_BEGIN` 权威来源：
`cond_instances`、`events_waits_current/history`、`events_waits_summary_by_instance` 以及
summary truncate 保留的零行现在共用真实 `sync.Cond` 对象身份。失败优先测试先红后绿，
专项回归通过（19.132s），完整 `TestPerformanceSchema` 回归通过（1007.013s），证据见
`reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-identity-current-continuation1801.txt`。

该切片只关闭一个 P1 对象身份精度缺口，不改变完整 I_S/P_S、组件生命周期、权限语义、
P3 客户端矩阵和 P2/P4 复制互操作的聚合状态。

### Continuation 1802

本轮补齐连接属性缓冲超限时的 MySQL 语义：按 length-encoded 键和值计算容量，
在缓冲区有空间时投影 `_truncated` 属性及实际丢弃字节数；容量不足以容纳标记时仍只保留
lost 计数，不伪造标记。连接属性专项回归通过（9.960s），相关账户属性/全量连接属性子集
回归通过（19.434s），证据见
`reports/compatibility/p1-performance-schema-session-connect-attrs-truncated-current-continuation1802.txt`。

该切片只关闭一个连接属性运行时投影边界，不改变完整 I_S/P_S、组件生命周期、权限语义、
P3 客户端矩阵和 P2/P4 复制互操作的聚合状态。

### Continuation 1803

本轮补齐 `CURRENT_ROLE()` 的账号格式：默认 `sql_quote_show_create=ON` 时，限定角色返回
`` `role`@`host` `` 并以逗号连接；设置为 `OFF` 后返回未加引号的限定角色。专项会话元数据
及系统变量回归通过（engine 28.553s，manager 0.212s），证据见
`reports/compatibility/p1-current-role-account-format-current-continuation1803.txt`。

该切片只关闭一个角色可见性格式边界，不改变完整角色图、全部权限视图、完整 I_S/P_S、
P3 客户端矩阵和 P2/P4 复制互操作的聚合状态。

### Continuation 1804

本轮补齐 `ROLES_GRAPHML()` 的角色可见性边界：普通账户返回 MySQL 兼容的空 GraphML，
具备 `ROLE_ADMIN` 的会话可看到持久化账户节点及直接角色边；同时 Dispatcher 会保留权威引擎
预先准备的 GraphML，不再把已授权结果覆盖为空图。引擎角色专项、plan/dispatcher 回归通过，
证据见 `reports/compatibility/p1-roles-graphml-role-admin-boundary-current-continuation1804.txt`。

该切片只关闭角色图输出与入口传递边界，不改变完整 I_S/P_S、全部权限/组件语义、P3 客户端矩阵
以及 P2/P4 XA/binlog/GTID/崩溃恢复/提升的聚合状态。

### Continuation 1805

本轮统一 Dispatcher 与引擎入口的 `CURRENT_ROLE()` 账号格式：默认
`sql_quote_show_create=ON` 时返回反引号限定角色，切换为 `OFF` 后返回未加引号的限定角色。
失败优先测试和 Dispatcher 全包回归通过，证据见
`reports/compatibility/p1-current-role-dispatcher-format-current-continuation1805.txt`。

该切片只关闭一个系统函数入口一致性边界，不改变完整 I_S/P_S、角色/权限聚合、P2/P3/P4
互操作和 Fulltext/非 InnoDB 范围。

### Continuation 1806

本轮补齐角色元数据视图对 `USER`、`HOST`、`GRANTEE_HOST` 和 `DEFAULT_ROLE` 的等值/LIKE
过滤；错误账号、主机和默认角色条件不再被忽略。失败优先专项及角色相关回归通过
（217.132s）；engine 全包回归运行超过十分钟未产生退出码，已中止且不记录为 PASS，证据见
`reports/compatibility/p1-role-metadata-account-filters-current-continuation1806.txt`。

该切片只关闭一个角色元数据谓词边界，不改变完整 I_S/P_S、角色/权限/组件生命周期、
P3 全量客户端矩阵和 P2/P4 官方复制互操作的聚合状态。

### Continuation 1807

本轮为 `performance_schema.mutex_instances` 接入真实 owner sidecar：
`active_query` 和账户变更路径在已知连接线程号时投影 `LOCKED_BY_THREAD_ID`；其他无法从
Go 原语可靠推导 owner 的 mutex 仍返回 `NULL`。失败优先专项及同步实例、账户、KILL QUERY
回归通过（37.407s），证据见
`reports/compatibility/p1-performance-schema-mutex-owner-current-continuation1807.txt`。

该切片只关闭两个 executor mutex 路径的 owner 投影，不改变完整 I_S/P_S、等待/线程生命
周期、组件/权限语义以及 P2/P3/P4 聚合状态。

### Continuation 1808

本轮补齐 `performance_schema.host_cache` 的时间字段边界：认证失败记录已有权威的最早/最晚
观测时间，现在同时投影到 `FIRST_SEEN/LAST_SEEN`；没有时间源的活动 host 仍返回 `NULL`，
不伪造连接生命周期。失败优先专项及相关 host-cache 回归通过（9.300s），证据见
`reports/compatibility/p1-performance-schema-host-cache-seen-times-current-continuation1808.txt`。

该切片只关闭一个 host-cache 时间投影边界，不改变完整 I_S/P_S 运行时、组件/权限语义和
P2/P3/P4 聚合状态。

### Continuation 1809

本轮补齐 `performance_schema.setup_actors` 的 `ROLE` 运行时匹配：规则现在同时按
`HOST`、`USER` 和会话 `active_roles` 选择，`ROLE='%'` 继续匹配无激活角色的会话，具体角色
规则只作用于对应激活角色，并保留具体规则优先级。失败优先测试先复现无角色会话错误继承
具体角色规则，随后专项、setup_actors/setup_threads 相关回归及完整 `TestPerformanceSchema`
族通过（923.700s），证据见
`reports/compatibility/p1-performance-schema-setup-actors-active-role-current-continuation1809.txt`。

该切片只关闭 setup_actors 的 active-role 选择边界，不改变完整 I_S/P_S 字段精度、组件运行时、
权限聚合、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1810

本轮继续收口 `setup_actors` 的角色来源：当会话没有显式 `active_roles` 参数时，现在复用
`ENABLED_ROLES` 的权威来源，使用账户持久化的 `DefaultRoles` 并合并 mandatory roles；显式空
切片仍表示 `SET ROLE NONE`，不会被默认角色覆盖。失败优先测试先复现默认角色会话错误落入
通配规则，随后专项、相关 setup/ENABLED_ROLES 回归及完整 `TestPerformanceSchema` 族通过
（915.818s），证据见
`reports/compatibility/p1-performance-schema-setup-actors-default-role-current-continuation1810.txt`。

该切片只关闭 setup_actors 默认角色来源边界，不改变完整 I_S/P_S 字段精度、所有组件运行时、
权限聚合、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1811

本轮补齐 `INFORMATION_SCHEMA.EVENTS.STATUS` 的事件状态投影：`ALTER EVENT ... DISABLE` 后返回
`DISABLED`，重新 `ENABLE` 后返回 `ENABLED`。失败优先专项和事件相关回归通过，证据见
`reports/compatibility/p1-information-schema-events-disabled-status-current-continuation1811.txt`。

该切片只关闭事件状态投影边界，不改变完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1812

本轮补齐 `INFORMATION_SCHEMA.EVENTS.STARTS/ENDS` 的定义边界投影，与现有 `SHOW EVENTS`
结果保持一致。失败优先专项和事件相关回归通过，证据见
`reports/compatibility/p1-information-schema-events-start-end-current-continuation1812.txt`。

该切片只关闭 STARTS/ENDS 投影边界，不改变完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1813

本轮补齐 `INFORMATION_SCHEMA.EVENTS.ON_COMPLETION` 的 `PRESERVE` 投影；未显式设置时仍返回
`NOT PRESERVE`。失败优先专项和事件相关回归通过，证据见
`reports/compatibility/p1-information-schema-events-completion-policy-current-continuation1813.txt`。

该切片只关闭 ON_COMPLETION 投影边界，不改变完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1814

本轮补齐 `INFORMATION_SCHEMA.EVENTS.DEFINER` 与 `EVENT_COMMENT` 的持久化对象投影，覆盖
自定义 DEFINER 和 COMMENT。失败优先专项及事件相关回归通过，证据见
`reports/compatibility/p1-information-schema-events-definer-comment-current-continuation1814.txt`。

该切片只关闭 DEFINER/EVENT_COMMENT 投影边界，不改变完整 I_S/P_S 字段精度、组件/权限语义、
P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1815

本轮补齐 `INFORMATION_SCHEMA.EVENTS.CREATED/LAST_ALTERED`：创建时可见，旧对象回退到创建时间，
`ALTER EVENT` 和重命名事件持久化独立修改时间。失败优先专项及事件相关回归通过，证据见
`reports/compatibility/p1-information-schema-events-timestamps-current-continuation1815.txt`。

该切片不宣称 `LAST_EXECUTED` 等运行时字段完成；完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1816

本轮将 `INFORMATION_SCHEMA.EVENTS.LAST_EXECUTED` 接入真实 SQL EVENT scheduler：事件执行前
持久化执行时间，查询返回 MySQL DATETIME 形状。失败优先专项及完整事件调度回归通过，证据见
`reports/compatibility/p1-information-schema-events-last-executed-current-continuation1816.txt`。

该切片只关闭本地事件调度的 LAST_EXECUTED 投影边界，不改变完整 I_S/P_S 字段精度、组件/权限
语义、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围。

### Continuation 1817

本轮补齐 `INFORMATION_SCHEMA.ROUTINES.CREATED/LAST_ALTERED`，覆盖过程/函数创建和 ALTER
后的时间变化，并兼容旧对象回退。失败优先专项及 routine 相关回归通过，证据见
`reports/compatibility/p1-information-schema-routines-timestamps-current-continuation1817.txt`。

该切片只关闭 routine 时间投影边界；完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4 互操作
以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1818

本轮补齐存储对象创建会话的 `sql_mode/time_zone/character_set_client/collation_connection` 持久化
与 I_S/SHOW 投影，覆盖过程、触发器和事件，并保留旧元数据回退。失败优先专项及共享回归通过，
证据见 `reports/compatibility/p1-stored-object-session-metadata-current-continuation1818.txt`。

该切片只关闭存储对象会话元数据投影边界；完整 I_S/P_S 字段精度、组件/权限语义、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1819

本轮补齐 `INFORMATION_SCHEMA.EVENTS.ORIGINATOR`：创建事件时持久化创建者的 `server_id`，
查询优先使用持久化值；旧事件元数据回退当前实例 `server_id`。失败优先专项和
事件/routine/trigger 共享回归通过，证据见
`reports/compatibility/p1-information-schema-events-originator-current-continuation1819.txt`。

该切片只关闭一个事件元数据字段边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1820

本轮修正 `INFORMATION_SCHEMA.ROUTINES.ROUTINE_DEFINITION`：投影 routine body，保留
`SHOW CREATE` 的完整 CREATE 语句。失败优先专项和 routine 相关回归通过，证据见
`reports/compatibility/p1-information-schema-routines-definition-current-continuation1820.txt`。

该切片只关闭一个 routine 元数据字段边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1821

本轮补齐 `INFORMATION_SCHEMA.TRIGGERS.ACTION_ORDER`，复用实际触发器执行使用的
`FOLLOWS/PRECEDES` 拓扑顺序投影一基序号，并从 `ACTION_STATEMENT` 中移除顺序前缀。失败优先专项和触发器/存储对象回归通过，证据见
`reports/compatibility/p1-information-schema-triggers-action-order-current-continuation1821.txt`。

该切片只关闭一个 trigger 元数据字段边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1822

本轮补齐视图创建会话的 `character_set_client/collation_connection` 持久化与
`INFORMATION_SCHEMA.VIEWS`、`SHOW CREATE VIEW` 投影，旧视图保留 utf8mb4 回退。专项和视图相关
回归通过，证据见
`reports/compatibility/p1-information-schema-views-session-metadata-current-continuation1822.txt`。

该切片只关闭一个 view 会话元数据边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1823

本轮补齐 `INFORMATION_SCHEMA.VIEWS.IS_UPDATABLE`：单表直接列投影的视图返回 `YES`，聚合、连接、
分组、集合或子查询等复杂定义保守返回 `NO`。专项和 view 相关回归通过，证据见
`reports/compatibility/p1-information-schema-views-updatability-current-continuation1823.txt`。

该切片只关闭一个 view 元数据字段边界，不宣称完整 view DML 重写或所有 MySQL view corner cases；
完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1824

本轮修正 `INFORMATION_SCHEMA.EVENTS.EVENT_DEFINITION`：使用事件调度解析得到 `DO` 后的事件体，
而不是返回完整 `CREATE EVENT`；`SHOW CREATE EVENT` 和 scheduler 仍使用完整定义。严格事件回归通过，
证据见 `reports/compatibility/p1-information-schema-events-definition-current-continuation1824.txt`。

该切片只关闭一个 event 定义投影边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1825

本轮重新验证本地 P2 原生复制能力：XA、native binlog、GTID、恢复、promotion 和 fencing
专项通过（102.199 秒）。官方 MySQL 交易、GTID、XA 互操作测试因官方 fixture 环境变量未配置而
跳过，Docker 也未提供可用 server 端响应；因此本轮只加强本地证据，不把 P2/P4 标记为完成。
证据见 `reports/compatibility/p2-native-replication-local-regression-current-continuation1825.txt`。

### Continuation 1826

本轮修正 `INFORMATION_SCHEMA.PARAMETERS` 的函数返回值行：native 投影的
`PARAMETER_NAME` 现在为 `NULL`，而 JDBC 兼容投影继续提供 `COLUMN_NAME=RETURN_VALUE`。
失败优先专项和参数/例程回归通过，证据见
`reports/compatibility/p1-information-schema-parameters-return-name-current-continuation1826.txt`。

### Continuation 1827

本轮补齐 native `INFORMATION_SCHEMA.PARAMETERS` 的 `ORDINAL_POSITION`、`PARAMETER_NAME`、
`PARAMETER_MODE` 过滤；函数返回值筛选不再混入输入参数。专项和参数/例程回归通过，证据见
`reports/compatibility/p1-information-schema-parameters-filters-current-continuation1827.txt`。

### Continuation 1828

本轮收紧 PERFORMANCE_SCHEMA 配置表 UPDATE 的列语义：`setup_consumers` 只接受
`ENABLED`，`setup_instruments/setup_objects` 接受 `ENABLED/TIMED`，`setup_actors/setup_threads`
接受 `ENABLED/HISTORY`。专项和全部 setup 回归通过，证据见
`reports/compatibility/p1-performance-schema-setup-column-semantics-current-continuation1828.txt`。

### Continuation 1829

本轮补齐 native `INFORMATION_SCHEMA.PARAMETERS` 的 `IS NULL`/`IS NOT NULL` 过滤语义：函数返回值行的
`PARAMETER_NAME`、`PARAMETER_MODE` 为 NULL，普通参数可被 `IS NOT NULL` 正确筛选；`ORDINAL_POSITION`
仍按非 NULL 的 0/正数处理。专项和参数/例程/存储对象回归通过，证据见
`reports/compatibility/p1-information-schema-parameters-null-predicates-current-continuation1829.txt`。

该切片只关闭一个 PARAMETERS 谓词边界；完整 I_S/P_S 字段精度、组件/权限生命周期、P2/P3/P4
互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1830

本轮补齐 Performance Schema 对 XA mutex 的运行时 owner 投影：XA PREPARE、XA RECOVER、
prepared/suspended XA 状态访问以及持久化恢复路径统一经过 owner-aware 锁封装；在 XA PREPARE
持有 `xaMu` 期间，`performance_schema.mutex_instances.LOCKED_BY_THREAD_ID` 现在返回实际
MySQL 会话线程 ID。失败优先、XA 全量和同步实例回归通过，证据见
`reports/compatibility/p1-performance-schema-xa-mutex-owner-current-continuation1830.txt`。

该切片只关闭 XA mutex owner 可观测性边界；完整 I_S/P_S 字段精度、所有锁/等待/线程生命周期、
组件/权限聚合、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1831

本轮补齐 INFORMATION_SCHEMA 权限视图的 `IS NULL`/`IS NOT NULL` 谓词：
`TABLE_PRIVILEGES`、`COLUMN_PRIVILEGES`、`SCHEMA_PRIVILEGES` 和 `USER_PRIVILEGES` 现在会按
投影字段的 NULL 状态过滤，而不会把 NULL 谓词静默忽略。失败优先专项和权限视图回归通过，
证据见 `reports/compatibility/p1-information-schema-privilege-null-predicates-current-continuation1831.txt`。

该切片只关闭权限视图 NULL 谓词边界；完整角色/权限图、I_S/P_S 字段精度和运行时语义、
P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1832

本轮补齐 executor-owned mutex 的 Performance Schema 等待生命周期：阻塞等待进入
`events_waits_current`，完成后进入 `events_waits_history`/`events_waits_history_long`，并按真实
`OBJECT_INSTANCE_BEGIN` 进入 `events_waits_summary_by_instance`；该汇总的 TRUNCATE 也会清除 mutex
历史贡献。XA mutex owner、同步实例和等待视图
专项回归通过，证据见
`reports/compatibility/p1-performance-schema-mutex-wait-lifecycle-current-continuation1832.txt`。

该切片只关闭 executor-owned mutex 等待生命周期边界；完整 I_S/P_S 表字段、所有组件/权限/线程语义、
P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1833

本轮补齐 mutex wait summary 的维度 reset：global summary truncate 会排除旧 mutex 历史，thread
summary 查询正确传入 `thread` 维度，thread summary truncate 会返回零计数行且不清除 global summary。
等待/汇总专项回归通过，证据见
`reports/compatibility/p1-performance-schema-mutex-summary-dimension-reset-current-continuation1833.txt`。

该切片只关闭 mutex wait summary 的 global/thread reset 边界；完整 I_S/P_S 字段、组件/权限/线程语义、
P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1834

本轮验证 executor-owned mutex wait 在 account summary 中按真实 session user/host 投影，并验证
account summary 的独立 TRUNCATE 不影响 global summary。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-mutex-account-summary-current-continuation1834.txt`。

该切片验证 mutex wait 的 account/host/user 共享维度路径，不改变 P1 聚合项状态；完整 I_S/P_S、
组件/权限/线程语义、P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1835

本轮重新执行 P2 native binlog/GTID/XA/recovery/promotion 本地门禁，相关测试通过；官方 MySQL
交易、GTID、XA 互操作测试因 `XMYSQL_OFFICIAL_BINLOG_URL` 和
`XMYSQL_OFFICIAL_BINLOG_DSN` 未配置而跳过。该结果确认本地实现证据，但不能把官方互操作标记为
通过，证据见 `reports/compatibility/p2-native-local-and-official-fixture-audit-current-continuation1835.txt`。

当前范围内仍缺少可复现的官方 MySQL 拓扑和受保护凭据，因此 P2/P4 官方互操作只能保持 partial；
本地 native 状态机继续纳入后续实现审计。

### Continuation 1836

本轮验证 executor-owned mutex wait 的短历史与长历史容量独立生效，并验证动态缩小长历史容量会裁剪
已有记录。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-mutex-history-capacity-current-continuation1836.txt`。

该切片只关闭 mutex wait history capacity 边界，不改变 P1 聚合项状态；完整 I_S/P_S、组件/权限/线程语义、
P2/P3/P4 互操作以及 Fulltext/非 InnoDB 范围仍保持原边界。

### Continuation 1837

本轮补齐 `performance_schema.table_handles` 对普通语句锁生命周期的本地运行时投影：语句执行期间可见，
清理函数执行后消失；同时过滤观察 `INFORMATION_SCHEMA/PERFORMANCE_SCHEMA` 自身产生的虚拟元数据访问，
避免自观察行和虚增 `Performance_schema_table_handles_lost`。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-table-handles-statement-lease-current-continuation1837.txt`。

该切片只收口 table_handles 的一个生命周期缺口，不改变 P1 聚合项状态；完整 I_S/P_S 表字段覆盖、
组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1838

本轮补齐 `performance_schema.metadata_locks.LOCK_DURATION` 的生命周期语义：普通语句锁投影为
`STATEMENT`，事务内锁投影为 `TRANSACTION`，`LOCK TABLES` 投影为 `EXPLICIT`，等待中的锁也保留
对应 duration。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-metadata-lock-duration-current-continuation1838.txt`。

该切片只收口 metadata_locks 的 duration 语义，不改变 P1 聚合项状态；完整 I_S/P_S 表字段、组件/权限/
线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1839

本轮修正 `performance_schema.metadata_locks` 的容量语义：容量裁剪现在发生在 SQL 过滤之前，
`Performance_schema_metadata_lock_lost` 同时基于完整运行时 owner/waiter 集合计算，不会因为观察查询的
`WHERE` 谓词漏计或重新显示被容量淘汰的锁。
专项和 metadata-lock 全族回归通过，证据见
`reports/compatibility/p1-performance-schema-metadata-lock-capacity-filter-current-continuation1839.txt`。

该切片只收口 metadata_locks 的 capacity/lost 统计边界，不改变 P1 聚合项状态；完整 I_S/P_S 表字段、
组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1840

本轮将同一容量语义应用到 `performance_schema.table_handles`：容量裁剪发生在 SQL 过滤之前，
被淘汰的句柄不会被 `WHERE` 谓词重新找回，`Performance_schema_table_handles_lost` 仍按完整
运行时句柄集合统计。组合回归通过，证据见
`reports/compatibility/p1-performance-schema-table-handle-capacity-filter-current-continuation1840.txt`。

该切片只收口 table_handles/metadata_locks 的 capacity/filter 一致性，不改变 P1 聚合项状态；完整
I_S/P_S 表字段、组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1841

本轮修正 `performance_schema.prepared_statements_instances` 的容量语义：容量裁剪现在发生在 SQL
过滤之前，被淘汰的 prepared statement 不会被 `WHERE statement_id=...` 重新查出，
`Performance_schema_prepared_statements_lost` 仍按完整运行时 inventory 统计。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-prepared-capacity-filter-current-continuation1841.txt`。

该切片只收口 prepared statement inventory 的 capacity/filter 一致性，不改变 P1 聚合项状态；完整
I_S/P_S 表字段、组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1842

本轮将容量先于 SQL 过滤的语义扩展到 `performance_schema.socket_instances` 和
`performance_schema.file_instances`：稳定排序后的完整运行时集合先裁剪，选择性 `WHERE` 不会
重新显示被淘汰的实例，socket/file lost 统计仍基于完整集合。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-file-socket-capacity-filter-current-continuation1842.txt`。

该切片只收口 socket/file instance 的 capacity/filter 一致性，不改变 P1 聚合项状态；完整 I_S/P_S
表字段、组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1843

本轮修正 `performance_schema.events_statements_summary_by_program` 的容量语义：program summary
先按完整运行时集合裁剪，再应用 SQL 过滤；被淘汰的 program 不会被选择性查询复活，lost 仍按完整
集合统计。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-program-capacity-filter-current-continuation1843.txt`。

该切片只收口 program summary 的 capacity/filter 一致性，不改变 P1 聚合项状态；完整 I_S/P_S 表
字段、组件/权限/线程语义及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1844

本轮继续收口容量先于 SQL 过滤的运行时语义：`performance_schema.accounts/users/hosts`、
`table_lock_waits_summary_by_table`、table I/O 及 index usage 现在都先基于完整运行时集合执行
容量裁剪，再执行 `WHERE` 谓词；被淘汰的连接摘要、表锁摘要或 I/O/index 行不会被选择性查询复活，
连接/表锁的 lost 统计仍来自完整集合。专项回归通过，证据见
`reports/compatibility/p1-performance-schema-summary-io-capacity-filter-current-continuation1844.txt`。

该切片只收口上述运行时聚合的 capacity/filter 一致性，不改变 P1 聚合项状态；完整 I_S/P_S
表字段、组件/权限/线程生命周期语义、非 Connector/J 客户端矩阵及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1845

本轮修正 `performance_schema_error_size` 和 `performance_schema_digests_size` 的容量顺序：错误
摘要的 global/account/host/user/thread 维度及 digest summary 现在都先基于完整候选集合执行容量
裁剪，再应用 SQL 条件；选择性查询不会复活被淘汰行，digest lost 也按完整集合统计。回归通过，
证据见 `reports/compatibility/p1-performance-schema-error-digest-capacity-filter-current-continuation1845.txt`。

该切片仍不改变 P1 聚合项的 partial 状态；完整 I_S/P_S 表字段、组件/权限/线程生命周期语义、
非 Connector/J 客户端矩阵及官方 MySQL XA/binlog 互操作仍未收口。

### Continuation 1846

本轮修正 `performance_schema.threads` 的容量顺序：完整线程 inventory 先按 thread ID 稳定排序并
应用 `max_thread_instances`，之后才执行 SQL 条件；被淘汰线程不会被 `WHERE THREAD_ID=...`
复活，thread lost 仍基于完整运行时集合。回归通过，证据见
`reports/compatibility/p1-performance-schema-thread-capacity-filter-current-continuation1846.txt`。

该切片只收口 thread instance 的 capacity/filter 一致性，不改变 P1 聚合项状态；完整 I_S/P_S
字段、组件/权限/生命周期语义、非 Connector/J 客户端矩阵及 P2/P3/P4 互操作仍保持原边界。

### Continuation 1847

本轮重新执行了 1841-1846 的 Performance Schema 容量/过滤回归，以及本地 native
binlog/GTID/XA/recovery/promotion 回归，均通过。矩阵当前仍为 29 项：implemented=18、
partial=8、deferred=1、out_of_scope=2。该结果只确认本地实现和回归证据，不把缺少官方
MySQL 拓扑/凭据、缺失组件 runtime 或未完成的非 Connector/J 客户端矩阵误标为完成。
证据见 `reports/compatibility/p1-p2-capacity-and-native-regression-current-continuation1847.txt`。

Fulltext 继续 deferred；非 InnoDB 引擎继续 out_of_scope。

### Continuation 1848

本轮补齐角色元数据视图的谓词边界：`ENABLED_ROLES`、`APPLICABLE_ROLES`、
`ADMINISTRABLE_ROLE_AUTHORIZATIONS` 及 `ROLE_*_GRANTS` 现在共享支持 `IN`、`NOT IN`、
`IS NULL`、`IS NOT NULL` 的行匹配器，保留原有 `=`/`LIKE` 语义。先红后绿回归、角色/权限
全族回归以及完整 I_S/P_S 定向族均通过，证据见
`reports/compatibility/p1-information-schema-role-predicate-filters-current-continuation1848.txt`。

该切片只关闭角色元数据谓词边界；完整 I_S/P_S 表字段、组件 runtime、权限/生命周期总项、
官方 MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，
非 InnoDB 继续 out_of_scope。

### Continuation 1863

本轮补齐 `FLUSH OPTIMIZER_COSTS` 的显式分发和权限门禁，支持 `LOCAL` 与
`NO_WRITE_TO_BINLOG` 形式，并按官方语义检查 `FLUSH_OPTIMIZER_COSTS` 或 `RELOAD`。
专项权限回归通过，证据见
`reports/compatibility/p1-flush-optimizer-costs-privilege-dispatch-current-continuation1863.txt`。

该切片只关闭命令入口和权限语义；成本表的持久化 UPDATE/INSERT/DELETE 以及真正的代价模型
缓存重载仍未实现。完整 mysql.* 生命周期、完整 I_S/P_S runtime/权限语义、官方 MySQL
互操作和非 Connector/J 客户端矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1857

本轮补齐 Performance Schema setup 视图的谓词边界：`setup_consumers`、`setup_instruments`、
`setup_timers`、`setup_actors` 和 `setup_objects` 现在共享支持 `IN`、`NOT IN`、`IS NULL`、
`IS NOT NULL` 的匹配器，保留 `=`/`LIKE` 语义；`setup_actors`/`setup_objects` 的查询、更新、
删除路径使用同一套谓词逻辑。失败优先回归和 setup 相关族回归通过，证据见
`reports/compatibility/p1-performance-schema-setup-predicate-filters-current-continuation1857.txt`。

### Continuation 1858

本轮补齐 Performance Schema telemetry setup 视图的过滤边界：`setup_loggers`、`setup_meters`、
`setup_metrics` 共用的字符串过滤器现在支持 `NOT IN`、`IS NULL`、`IS NOT NULL`，并保留
`=`/`LIKE`/`IN` 语义；查询与更新路径复用同一套匹配逻辑。失败优先回归和 telemetry 相关族
回归通过，证据见
`reports/compatibility/p1-performance-schema-telemetry-predicate-filters-current-continuation1858.txt`。

### Continuation 1859

本轮补齐 `mysql.proxies_priv` 的目标列过滤：`PROXIED_HOST`、`PROXIED_USER`、`WITH_GRANT` 和
`GRANTOR` 不再静默忽略谓词，并复用 mysql 系统表的等值、`LIKE`、`IN`、`NOT IN`、NULL
匹配逻辑；代理生命周期回归通过，证据见
`reports/compatibility/p1-mysql-proxies-priv-target-filters-current-continuation1859.txt`。

该切片只关闭 `mysql.proxies_priv` 目标列过滤边界；完整 mysql.* 表/字段和权限生命周期、
完整 I_S/P_S 运行时语义、官方 MySQL 互操作及非 Connector/J 客户端矩阵仍保持原状态。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

该切片只关闭 telemetry setup 视图谓词边界；完整 I_S/P_S 字段、运行时生命周期、组件/权限
聚合、官方 MySQL 互操作和非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，
非 InnoDB 继续 out_of_scope。

### Continuation 1860

本轮补齐了存储映射中已有但元数据分发缺失的四张 MySQL 8.4 系统表：`mysql.component`、
`mysql.password_history`、`mysql.server_cost` 和 `mysql.engine_cost`。新增只读查询投影：成本表
返回官方默认成本行，组件/密码历史在当前没有对应 runtime/journal 时返回空集；四张表均支持
等值、`LIKE`、`IN`、`NOT IN` 和 NULL 谓词。同步加入 `INFORMATION_SCHEMA.TABLES/COLUMNS`
发现及列元数据。专项测试和相关账户/角色/代理/表元数据回归通过，证据见
`reports/compatibility/p1-mysql-cost-component-system-table-shapes-current-continuation1860.txt`。

该切片仍是只读系统表兼容：`INSTALL/UNINSTALL COMPONENT`、密码历史持久化、成本表写入及
`FLUSH OPTIMIZER_COSTS` 运行时语义尚未实现；完整 mysql.* 生命周期、完整 I_S/P_S runtime/权限
聚合、官方 MySQL 互操作和非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，
非 InnoDB 继续 out_of_scope。

该切片只关闭 setup 视图谓词边界；完整 I_S/P_S 字段、运行时生命周期、组件/权限聚合、官方
MySQL 互操作和非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1855

本轮重新执行完整 `TestInformationSchema|TestPerformanceSchema` 定向族，覆盖 1849-1854
新增的权限、角色、参数、例程谓词边界及既有 P1 回归，全部通过（2099.985 秒）。证据见
`reports/compatibility/p1-information-performance-predicate-regression-current-continuation1855.txt`。

该结果是当前本地回归证据，不改变 P1 聚合项的 partial 状态：组件 runtime、字段精度、权限
生命周期和官方 MySQL 外部 fixture 仍未全部闭环。Fulltext 继续 deferred，非 InnoDB 继续
out_of_scope。

### Continuation 1874

本轮补齐 Performance Schema 汇总视图的 NULL 谓词语义：共享过滤器现在支持
`IS NULL`/`IS NOT NULL`，并且通用投影路径对真实 NULL 值保持正确的三值逻辑。
新增阶段汇总和实际 NULL 字段回归，完整 P_S 测试族及全仓编译通过，证据见
`reports/compatibility/p1-performance-schema-summary-null-predicates-current-continuation1874.txt`。

该切片只关闭 P_S 汇总过滤器的 NULL 语义缺口；完整 I_S/P_S 组件运行时、字段精度、权限生命周期、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1872

本轮补齐账户失败登录策略：`CREATE/ALTER USER` 支持并持久化
`FAILED_LOGIN_ATTEMPTS` 与 `PASSWORD_LOCK_TIME N/UNBOUNDED`；认证失败达到阈值后会持久化计数并
临时或永久锁定账户，锁定状态在认证前拒绝请求，成功改密和 `ACCOUNT UNLOCK` 会清零计数并解除锁定。
`mysql.user`、`SHOW CREATE USER` 和认证账户映射均已覆盖该策略。专项、账户兼容回归和跨包编译通过，
证据见 `reports/compatibility/p1-failed-login-password-lock-current-continuation1872.txt`。

该切片关闭本地账户失败登录状态机缺口，但不改变全局聚合状态：完整 `mysql.*` 生命周期、全量
I_S/P_S runtime/权限语义、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 客户端矩阵
仍保持 partial 或待外部环境验证。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1873

本轮补齐过期密码受限会话：服务器声明并协商 `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS`；未声明该
能力的客户端继续收到 `ER_MUST_CHANGE_PASSWORD_LOGIN (1862)`，声明能力的客户端可在密码过期
后登录到受限会话。受限会话仅放行当前账户的 `SET PASSWORD` 及
`ALTER USER USER()/CURRENT_USER() IDENTIFIED BY ...`，并覆盖 COM_QUERY、COM_STMT_PREPARE、
COM_STMT_EXECUTE；改密成功后清除会话限制和持久化过期状态。显式过期与 lifetime 过期均接入，
认证切换路径也复用该状态机。专项及 auth/net/protocol/dispatcher 回归通过，证据见
`reports/compatibility/p1-expired-password-restricted-session-current-continuation1873.txt`。

该切片只关闭过期密码登录协商和本地受限改密边界；完整 `mysql.*` 生命周期、全量 I_S/P_S
runtime/权限语义、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector-J 客户端矩阵仍保持
partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1856

本轮执行全仓串行 Go 回归。`server/innodb/engine` 在 45 分钟时限内未完成，于
2702.580 秒超时并输出 goroutine dump；其余后续执行包通过。该结果记录为负向门禁，
不能宣称全仓 PASS；1849-1855 的定向 P1 回归证据保持有效。详见
`reports/compatibility/full-go-regression-current-continuation1856.txt`。

### Continuation 1851

本轮补齐 `mysql.user`、`mysql.global_grants`、`mysql.db`、`mysql.tables_priv`、
`mysql.columns_priv` 系统授权表的共享 `IN`/`NOT IN`/NULL 谓词匹配，并保持等值与 `LIKE`
语义。专项回归和账户/权限族回归通过，证据见
`reports/compatibility/p1-mysql-system-privilege-predicate-filters-current-continuation1851.txt`。

该切片只关闭系统授权表谓词边界；完整 mysql.* 表覆盖、权限生命周期总项、完整 I_S/P_S
字段/runtime、官方 MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续
deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1852

本轮补齐 `mysql.role_edges` 和 `mysql.default_roles` 的 `IN`、`NOT IN`、NULL 谓词匹配，
保留等值、`LIKE` 和稳定排序语义。角色专项及角色族回归通过，证据见
`reports/compatibility/p1-mysql-role-table-predicate-filters-current-continuation1852.txt`。

该切片只关闭 mysql 角色授权表的谓词边界；完整 mysql.* 表覆盖、角色/权限生命周期总项、
完整 I_S/P_S 字段/runtime、官方 MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1853

本轮补齐 `INFORMATION_SCHEMA.PARAMETERS` 的 `PARAMETER_NAME`、`ORDINAL_POSITION`、
`PARAMETER_MODE` 的 `IN`/`NOT IN` 谓词，普通参数和函数返回值行均覆盖，原有等值、`LIKE`
和 NULL 语义保持不变。参数/例程族回归通过，证据见
`reports/compatibility/p1-information-schema-parameters-in-predicates-current-continuation1853.txt`。

该切片只关闭 PARAMETERS 谓词边界；完整 I_S/P_S 字段/runtime、组件/权限生命周期、官方
MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1849

本轮补齐通用权限视图的 `IN`/`NOT IN` 谓词：`TABLE_PRIVILEGES`、`COLUMN_PRIVILEGES`、
`SCHEMA_PRIVILEGES`、`USER_PRIVILEGES` 共享的权限行匹配器现在覆盖 `GRANTEE`、对象字段、
`PRIVILEGE_TYPE` 和 `IS_GRANTABLE`，并保留原有等值、`LIKE`、NULL 语义。专项及角色/权限族
回归通过，证据见 `reports/compatibility/p1-information-schema-privilege-predicate-filters-current-continuation1849.txt`。

该切片只关闭权限视图谓词边界；完整 I_S/P_S 表字段、组件 runtime、权限/生命周期总项、
官方 MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，
非 InnoDB 继续 out_of_scope。

### Continuation 1850

本轮补齐通用元数据视图的 `IN`、`NOT IN`、`IS NULL`、`IS NOT NULL` 谓词，覆盖
`TABLE_SCHEMA`、`TABLE_NAME` 及共享元数据字段，并验证表、列、约束、目录和重命名回归。
证据见 `reports/compatibility/p1-information-schema-metadata-predicate-filters-current-continuation1850.txt`。

该切片只关闭元数据谓词边界；完整 I_S/P_S 表字段、组件 runtime、权限/生命周期总项、
官方 MySQL 互操作、非 Connector/J 客户端矩阵仍保持原状态。Fulltext 继续 deferred，
非 InnoDB 继续 out_of_scope。

### Continuation 1861

本轮补齐 `mysql.password_history` 的账户生命周期持久化：`ALTER USER ... IDENTIFIED BY`
和 `SET PASSWORD` 替换密码时，会把旧的非空密码哈希及 UTC 时间戳写入持久化账户元数据；
查询投影可返回 Host/User、时间戳和旧哈希，并保留密码字段过滤。专项密码历史回归和相关
账户、角色、代理、成本/组件、元数据回归均通过，证据见
`reports/compatibility/p1-mysql-password-history-persistence-current-continuation1861.txt`。

该切片只关闭密码历史的持久化读路径；密码复用/保留策略、完整 mysql.* 生命周期、组件
生命周期、成本表写入、完整 I_S/P_S runtime/权限语义、官方 MySQL 互操作和非 Connector/J
客户端矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1862

本轮补齐 `INSTALL COMPONENT`/`UNINSTALL COMPONENT` 的本地注册表生命周期：支持的
`file://...` URN 会持久化到 `mysql.component`，并按官方边界分别检查 INSERT/DELETE
权限；查询可返回组件 ID、组 ID 和 URN。组件专项回归及权限回归通过，证据见
`reports/compatibility/p1-mysql-component-registry-lifecycle-current-continuation1862.txt`。

该切片只实现注册表和权限语义，不加载/卸载原生组件二进制，也未实现依赖解析、组件服务激活、
INSTALL 的 SET/PERSIST 子句；完整 mysql.* 生命周期、完整 I_S/P_S runtime/权限语义、官方
MySQL 互操作和非 Connector/J 客户端矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1864

本轮补齐 `mysql.server_cost`/`mysql.engine_cost` 的持久化 INSERT、UPDATE、DELETE，支持
成本值恢复 NULL 默认值、引擎复合键、`last_update` 生成、表级 DML 权限和会话事务暂存；
`FLUSH OPTIMIZER_COSTS` 现在会把持久化成本重新加载到运行时优化器快照，序列扫描成本使用
`row_evaluate_cost`。专项成本 DML、事务、flush、组件/密码历史和优化器回归通过，证据见
`reports/compatibility/p1-mysql-optimizer-cost-dml-reload-current-continuation1864.txt`。

该切片仍未覆盖官方 MySQL 的完整成本模型告警、复制行为、所有表达式/IGNORE/REPLACE 变体和
所有计划算子的精确代价；完整 I_S/P_S、XA/binlog/GTID、全量客户端矩阵及官方互操作仍保持
partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1865

本轮补齐密码验证策略切片：新增全局动态变量 `password_require_current`，支持账户级
`PASSWORD REQUIRE CURRENT/OPTIONAL/DEFAULT` 持久化；`mysql.user` 可投影密码复用与当前密码
验证字段，`SHOW CREATE USER` 可重现策略；非特权会话修改自己的账户时，`ALTER USER` 与
`SET PASSWORD` 会要求匹配的 `REPLACE` 当前密码。密码历史数量/时间窗口校验继续生效。
专项密码策略、相关账户/系统表/组件/成本回归通过，证据见
`reports/compatibility/p1-password-verification-policy-current-continuation1865.txt`。

该切片只关闭密码管理策略的一组账户级边界；完整 `mysql.user` 生命周期/字段、二次密码语法、
全量 I_S/P_S、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 客户端矩阵仍保持
partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1866

本轮补齐 `mysql.user` 密码生命周期元数据：非空密码变更会持久化 UTC
`password_last_changed`，账户级 `PASSWORD EXPIRE INTERVAL N DAY` 会持久化并投影
`password_lifetime`，`SHOW CREATE USER` 可重现该选项；`PASSWORD EXPIRE NEVER/DEFAULT`
会清除账户级期限覆盖，同时新增动态全局变量 `default_password_lifetime`。
专项密码回归通过，证据见
`reports/compatibility/p1-mysql-user-password-lifetime-current-continuation1866.txt`。

该切片尚未实现密码到期调度/认证时强制过期，也未关闭完整 `mysql.user` 字段/生命周期、全量
I_S/P_S、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 客户端矩阵。Fulltext
继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1867

本轮扩展 `mysql.user` 默认投影，覆盖 MySQL 8.4 的完整静态权限列：RELOAD、SHUTDOWN、PROCESS、
FILE、REFERENCES、SHOW DATABASES、SUPER、临时表/锁表、复制、视图、例程、EVENT、TRIGGER、
TABLESPACE 等权限均从持久化全局授权和 ALL 展开结果生成；密码管理列和既有账户过滤保持不变。
专项静态权限列回归通过，证据见
`reports/compatibility/p1-mysql-user-static-privilege-columns-current-continuation1867.txt`。

该切片只关闭 `mysql.user` 静态列投影缺口；完整授权表生命周期、动态权限/角色边界、全量
I_S/P_S 字段和运行时语义、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 客户端
矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1868

本轮把密码期限从元数据接入认证路径：认证层读取持久化账户的
`password_last_changed`/`password_lifetime`，账户级期限优先于全局
`default_password_lifetime`；期限到期时返回既有过期密码错误，显式
`password_expired` 语义保持不变。旧账户缺少可解析变更时间时，不会仅因配置了期限而误判过期。
认证全包和引擎/认证联动回归通过，证据见
`reports/compatibility/p1-password-lifetime-auth-enforcement-current-continuation1868.txt`。

该切片只关闭 P_S 汇总过滤器的 NULL 语义缺口；完整 I_S/P_S 组件运行时、字段精度、权限生命周期、
官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵仍保持 partial。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1875

本轮修复 Performance Schema 全局/会话变量视图的空结果回退：`VARIABLE_NAME IN/NOT IN`、
`IS NULL/IS NOT NULL` 现在会保留真实空结果，不再在无匹配时恢复整张变量表；无变量名谓词时的
默认全量投影保持不变。聚焦、相关 P_S 回归、完整 P_S 测试族和全仓编译均通过，证据见
`reports/compatibility/p1-performance-schema-variable-filter-empty-result-current-continuation1875.txt`。

该切片不改变全局 partial 状态：完整 I_S/P_S 组件运行时、字段精度、权限生命周期、官方
XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵仍待继续推进。
Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1876

本轮修复 MySQL 系统成本表的 NULL 成员谓词：`mysql.server_cost`/`mysql.engine_cost` 中真实
`NULL` 值用于 `IN` 或 `NOT IN` 时按 MySQL 三值逻辑返回 UNKNOWN，因此不再错误保留
`NOT IN` 行；显式 `IS NULL`、`IS NOT NULL`、等值和 `LIKE` 语义保持不变。专项成本回归通过，
证据见 `reports/compatibility/p1-mysql-system-table-null-not-in-current-continuation1876.txt`。

该切片只关闭一个本地系统表谓词边界；完整 I_S/P_S 组件运行时、字段精度、权限生命周期、官方
XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵仍保持 partial。Fulltext
继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1869

本轮补齐 `SHOW CREATE USER` 的账户可见性和默认输出语义：查看其他账户现在要求对
`mysql` 系统 schema 具有 `SELECT`；当前账户可查看自身定义，但没有 `mysql.user`
`SELECT` 时认证哈希显示为 `<secret>`，拥有表级或更宽的 `SELECT` 后显示实际哈希；
同时支持 `CURRENT_USER()` 作为目标。默认账户选项现在完整输出 `REQUIRE NONE`、
`ACCOUNT UNLOCK`、`PASSWORD EXPIRE DEFAULT`、`PASSWORD HISTORY DEFAULT`、
`PASSWORD REUSE INTERVAL DEFAULT` 和 `PASSWORD REQUIRE CURRENT DEFAULT`。专项回归通过，
证据见 `reports/compatibility/p1-show-create-user-visibility-current-continuation1869.txt`。

该切片只关闭 `SHOW CREATE USER` 的权限、哈希脱敏、当前账户目标和默认子句边界；完整
`mysql.*` 生命周期、全量 I_S/P_S runtime/权限语义、官方 XA/binlog/GTID/崩溃恢复/提升
互操作及非 Connector/J 客户端矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB
继续 out_of_scope。

### Continuation 1870

本轮补齐账户属性 SQL 生命周期：`CREATE USER ... ATTRIBUTE '<JSON object>'` 会持久化到
`mysql.user.User_attributes`，`ALTER USER ... ATTRIBUTE` 可替换文档，`ATTRIBUTE DEFAULT`
可清除；`CREATE/ALTER USER ... COMMENT` 会写入同一 JSON 文档的 `comment` 键并保留其他
属性，`COMMENT` 与 `ATTRIBUTE` 同时使用会拒绝；非法 JSON 或非对象 JSON 会拒绝。
`ALTER USER USER()`/`CURRENT_USER()` 及 `SET PASSWORD FOR USER()`/`CURRENT_USER()` 也会解析到
当前认证账户。`INFORMATION_SCHEMA.USER_ATTRIBUTES` 和 `SHOW CREATE USER` 复用同一持久化文档。专项账户属性端到端回归通过，证据见
`reports/compatibility/p1-user-attributes-account-syntax-current-continuation1870.txt`。

该切片只关闭账户属性语法、持久化和投影边界；完整 `mysql.*` 生命周期、全量 I_S/P_S
runtime/权限语义、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 客户端矩阵仍
保持 partial。Fulltext 继续 deferred，非 InnoDB 继续 out_of_scope。

### Continuation 1871

本轮补齐账户 TLS 和资源限制元数据：`CREATE/ALTER USER` 支持并持久化 `REQUIRE CIPHER`、
`ISSUER`、`SUBJECT`，以及 `MAX_QUERIES_PER_HOUR`、`MAX_UPDATES_PER_HOUR`、
`MAX_CONNECTIONS_PER_HOUR`、`MAX_USER_CONNECTIONS`；`mysql.user` 投影和
`SHOW CREATE USER` 可回显这些属性，认证侧账户读取也已暴露四类限制。专项回归通过，证据见
`reports/compatibility/p1-account-tls-resource-limits-current-continuation1871.txt`。

本轮同时将 `MAX_USER_CONNECTIONS` 接入协议层真实认证会话计数，并将
`MAX_CONNECTIONS_PER_HOUR`、`MAX_QUERIES_PER_HOUR`、`MAX_UPDATES_PER_HOUR` 接入
解耦协议的普通查询和预处理执行路径，返回资源超限错误。其他协议适配器、警告行为及外部
客户端重试语义仍需矩阵级验收；新增 SSL/资源列的等值、`IN`、`IS NULL` 元数据谓词也已
纳入虚拟 `mysql.user` 投影。
完整 `mysql.*` 生命周期、全量 I_S/P_S runtime/权限语义、官方 XA/binlog/GTID/崩溃恢复/提升
互操作及非 Connector/J 客户端矩阵仍保持 partial。Fulltext 继续 deferred，非 InnoDB 继续
out_of_scope。
