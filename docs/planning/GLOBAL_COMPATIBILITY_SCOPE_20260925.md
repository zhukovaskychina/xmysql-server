# xmysql-server 全局 MySQL 兼容范围梳理

更新时间：2026-10-04

本文把已确认的兼容目标统一纳入一个全局任务边界。状态以源码、专项测试和
`reports/compatibility/scope-matrix-current-final.json` 为准；本轮最新增量证据见
`reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-identity-current-continuation1801.txt`；
`reports/compatibility/p1-performance-schema-session-connect-attrs-truncated-current-continuation1802.txt`；
`reports/compatibility/p1-current-role-account-format-current-continuation1803.txt`；
`reports/compatibility/p1-roles-graphml-role-admin-boundary-current-continuation1804.txt`；
`reports/compatibility/p1-current-role-dispatcher-format-current-continuation1805.txt`；
`reports/compatibility/p1-role-metadata-account-filters-current-continuation1806.txt`；
`reports/compatibility/p1-performance-schema-mutex-owner-current-continuation1807.txt`；
`reports/compatibility/p1-performance-schema-host-cache-seen-times-current-continuation1808.txt`；
`reports/compatibility/p1-performance-schema-setup-actors-active-role-current-continuation1809.txt`；
`reports/compatibility/p1-performance-schema-setup-actors-default-role-current-continuation1810.txt`；
`reports/compatibility/p1-information-schema-events-disabled-status-current-continuation1811.txt`；
`reports/compatibility/p1-information-schema-events-start-end-current-continuation1812.txt`；
`reports/compatibility/p1-information-schema-events-completion-policy-current-continuation1813.txt`；
`reports/compatibility/p1-information-schema-events-definer-comment-current-continuation1814.txt`；
`reports/compatibility/p1-information-schema-events-timestamps-current-continuation1815.txt`；
`reports/compatibility/p1-information-schema-events-last-executed-current-continuation1816.txt`；
`reports/compatibility/p2-native-replication-local-regression-current-continuation1825.txt`；
`reports/compatibility/p1-information-schema-parameters-return-name-current-continuation1826.txt`；
`reports/compatibility/p1-information-schema-parameters-filters-current-continuation1827.txt`；
`reports/compatibility/p1-performance-schema-setup-column-semantics-current-continuation1828.txt`；
`reports/compatibility/p1-information-schema-parameters-null-predicates-current-continuation1829.txt`；
`reports/compatibility/p1-performance-schema-xa-mutex-owner-current-continuation1830.txt`；
`reports/compatibility/p1-information-schema-privilege-null-predicates-current-continuation1831.txt`；
`reports/compatibility/p1-performance-schema-mutex-wait-lifecycle-current-continuation1832.txt`；
`reports/compatibility/p1-performance-schema-mutex-summary-dimension-reset-current-continuation1833.txt`；
`reports/compatibility/p1-performance-schema-mutex-account-summary-current-continuation1834.txt`；
`reports/compatibility/p2-native-local-and-official-fixture-audit-current-continuation1835.txt`；
`reports/compatibility/p1-performance-schema-mutex-history-capacity-current-continuation1836.txt`；
`reports/compatibility/p1-performance-schema-table-handles-statement-lease-current-continuation1837.txt`；
`reports/compatibility/p1-performance-schema-metadata-lock-duration-current-continuation1838.txt`；
`reports/compatibility/p1-performance-schema-metadata-lock-capacity-filter-current-continuation1839.txt`；
`reports/compatibility/p1-performance-schema-table-handle-capacity-filter-current-continuation1840.txt`；
`reports/compatibility/p1-information-schema-routines-timestamps-current-continuation1817.txt`；
`reports/compatibility/p1-stored-object-session-metadata-current-continuation1818.txt`；
`reports/compatibility/p1-information-schema-srs-id-like-filter-current-continuation1883.txt`；
`reports/compatibility/p1-p2-p3-fresh-gate-audit-current-continuation1884.txt`；
`reports/compatibility/p1-local-privilege-role-fresh-audit-current-continuation1885.txt`；
`reports/compatibility/p2-p4-official-mysql-fresh-interoperability-current-continuation1886.txt`；
`reports/compatibility/p3-non-connector-client-fresh-matrix-current-continuation1888.txt`；
`reports/compatibility/p3-client-fault-and-cluster-fresh-current-continuation1892.txt`；
`reports/compatibility/p1-final-boundary-audit-current-continuation1894.txt`；
`reports/compatibility/official-mysql-privilege-lifecycle-current/privilege-lifecycle-20261003-204707.json`；
`reports/compatibility/p1-performance-schema-native-lifecycle-current-continuation1900.txt`；
`reports/compatibility/full-go-regression-current-continuation1901.txt`；
以下历史证据仍保留用于追溯：
`reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1651.txt` 和
`reports/compatibility/p2-replication-directory-fsync-current-continuation1652.txt` 和
`reports/compatibility/p2-native-partial-transport-retry-current-continuation1654.txt` 和
`reports/compatibility/p3-client-network-fault-reconnect-current-continuation1655.txt` 和
`reports/compatibility/p3-mysql-cli-version-matrix-current-continuation1656.txt` 和
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1657.txt` 和
`reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1658.txt` 和
`reports/compatibility/p2-p4-official-reverse-promotion-mysql80-current-continuation1659.txt` 和
`reports/compatibility/p2-p4-official-source-crash-reconnect-mysql80-current-continuation1660.txt` 和
`reports/compatibility/p2-p4-official-reverse-promotion-script-regression-current-continuation1661.txt` 和
`reports/compatibility/p2-p4-official-source-network-partition-current-continuation1662.txt` 和
`reports/compatibility/p3-client-network-fault-reconnect-current-continuation1663.txt` 和
`reports/compatibility/p2-p4-official-source-network-partition-current-continuation1664.txt` 和
`reports/compatibility/p1-information-schema-performance-schema-targeted-regression-current-continuation1665.txt` 和
`reports/compatibility/p2-physical-commit-identity-wal-current-continuation1666.txt` 和
`reports/compatibility/p2-xa-identity-wal-current-continuation1667.txt` 和
`reports/compatibility/p2-xa-journal-recovery-identity-current-continuation1668.txt` 和
`reports/compatibility/p2-xa-precommit-journal-identity-current-continuation1669.txt`；仅有表名、
列名或单元测试通过，不代表已经达到 MySQL 完整语义。

全局优先级固定为：P0 先保证 MySQL 启动、核心 CRUD、基础集群和 Connector/J；随后推进
P1 完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA，P2 本地复制/XA/binlog/GTID/恢复与提升，
P3 非 Connector/J 客户端全量矩阵，P4 官方 MySQL 互操作。Fulltext 延后；非 InnoDB 引擎、
非 InnoDB 修复和引擎转换明确不纳入范围。P0 的已实现状态不覆盖后续聚合项。

本轮最新 P2 增量证据：`reports/compatibility/p2-replication-committed-context-marker-retry-current-continuation1697.txt`。
本轮最新 P1 增量证据：`reports/compatibility/p1-log-status-storage-engine-current-continuation1700.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt`。

本轮最新 P1 Performance Schema 增量证据：`reports/compatibility/p1-performance-schema-rwlock-instances-current-continuation1784.txt`。

本轮最新 P3 客户端增量证据：`reports/compatibility/p3-client-session-reset-current-continuation1785.txt`。

本轮最新 P3 客户端增量证据：`reports/compatibility/p3-client-protocol-ping-current-continuation1786.txt`。

本轮最新 P3 客户端增量证据：`reports/compatibility/p3-client-protocol-init-db-current-continuation1787.txt`。

PyMySQL 的 `select_db()` 已通过真实 `COM_INIT_DB` live slice；该结果只覆盖 Python
客户端协议路径，不改变非 Connector/J 全量客户端矩阵的 `partial` 状态。

本轮最新 P3 客户端增量证据：`reports/compatibility/p3-client-protocol-change-user-current-continuation1788.txt`。

Node.js/mysql2 的 `changeUser()` 已通过真实 `COM_CHANGE_USER` live slice；该结果只覆盖
Node.js 客户端协议路径，不改变非 Connector/J 全量客户端矩阵的 `partial` 状态。

本轮最新 P1 Performance Schema 增量证据：`reports/compatibility/p1-performance-schema-mutex-instances-current-continuation1789.txt`。

`performance_schema.mutex_instances` 已接入五个执行器真实 `sync.Mutex` 实例，并验证容量、
lost 计数和 instrument 开关；`max_mutex_classes` / `max_rwlock_classes` 也已约束
`setup_instruments` 与 class lost 统计。condition 实例以及完整 I_S/P_S 字段、运行时统计、
等待/线程精度和权限语义仍未完成，P1 聚合项继续为 `partial`。

本轮最新 P2 原生 binlog 增量证据：`reports/compatibility/p2-native-control-events-current-continuation1790.txt`。

原生解码器现在消费 `BEGIN_LOAD_QUERY_EVENT(17)` 和 `EXECUTE_LOAD_QUERY_EVENT(18)`，
覆盖顶层、事务 payload 和 row-image 路径；该本地解码增量不等同于官方 MySQL XA/binlog/
GTID、崩溃恢复和提升互操作已完成。

本轮最新 P1 Performance Schema 增量证据：`reports/compatibility/p1-performance-schema-condition-instances-current-continuation1791.txt`。

`performance_schema.cond_instances` 已接入全局读锁 gate 实际使用的 `sync.Cond`，并验证
对象身份、实例/class 容量、lost 计数和 instrument 开关；完整 I_S/P_S 运行时、等待/线程、
组件生命周期及权限语义仍保持 `partial`。

本轮继续补齐全局读锁等待生产者：阻塞写入的 condition wait 现在有当前事件、完成历史以及
按线程/全局 summary 的真实投影。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-waits-current-continuation1792.txt`。

本轮补齐 `events_waits_summary_by_instance` 对全局读锁 condition wait 的实例级当前/历史
汇总，实例身份与 `cond_instances` 保持一致。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-instance-summary-current-continuation1793.txt`。

本轮按 MySQL 8.4 wait-event 对象契约修正 condition wait 的对象字段与实例身份：同步对象的
schema/name/type 均为 NULL，实例地址与 `cond_instances` 一致。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-wait-object-contract-current-continuation1794.txt`。

本轮补齐全局读锁等待的 per-thread history 容量投影，`history_size` 与 `history_long_size`
的可见边界开始分离。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-history-capacity-current-continuation1795.txt`。

本轮完成全局读锁 condition wait 的短/长历史独立容量：per-thread history 不再被较小的
history_long 容量错误截断。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-independent-history-current-continuation1796.txt`。

本轮补齐全局读锁等待历史的短/长 truncate 独立作用域。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-history-truncate-current-continuation1797.txt`。

本轮补齐 `events_waits_summary_by_instance` 的独立 truncate 分发和全局读锁实例摘要
reset；by-instance truncate 不再影响 global/thread 摘要，global summary truncate 的
既有 by-instance 零行投影保持不变。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-truncate-current-continuation1798.txt`。

本轮将 by-instance reset 扩展到完成的 record-lock、MDL 和全局读锁等待，按 truncate
边界过滤历史并保留新等待；global/thread 摘要继续独立。证据：
`reports/compatibility/p1-performance-schema-wait-summary-instance-reset-current-continuation1799.txt`。

本轮补充嵌套角色可见性回归：`SET ROLE ALL` 后，`ENABLED_ROLES` 保留直接启用的父角色，
而父角色继承的子角色权限继续在 `ROLE_TABLE_GRANTS` 中可见。该结果只收口一个 P1 角色
语义边界；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、全权限视图和组件生命周期仍为
全局任务中的 `partial`。证据：
`reports/compatibility/p1-nested-role-enabled-vs-inherited-privilege-current-continuation1800.txt`。

本轮修正全局读锁 condition wait 的 `OBJECT_INSTANCE_BEGIN`：
`cond_instances`、wait current/history、`events_waits_summary_by_instance` 和 summary
truncate 零行现在共用真实 `sync.Cond` 对象身份。专项和完整 Performance Schema 回归均通过，
该切片不改变 P1 聚合项仍为 `partial` 的状态。证据：
`reports/compatibility/p1-performance-schema-global-read-lock-summary-instance-identity-current-continuation1801.txt`。

Go、PyMySQL、Node.js/mysql2 的真实 `COM_PING` smoke gate 已通过；P3 聚合项仍为 `partial`，
因为 CLI、版本/TLS/认证组合、协议负例、连接池、故障切换和集群端点矩阵尚未完成。

本轮已验证 PyMySQL 和 Node.js/mysql2 的 `COM_RESET_CONNECTION` live slice；Go 驱动的公开
`SessionResetter` 不发送该协议命令，因此没有将其误计入该 slice。P3 全量非 Connector/J
客户端矩阵仍为 `partial`，MySQL CLI、版本/TLS/认证、连接池、协议负例、故障切换和集群端点
仍待完成。

该切片已把真实 `tableDDLCoordinator` 表级 DDL rwlock 注册表接入
`performance_schema.rwlock_instances`，并验证实例容量、lost 计数及 instrument 开关；mutex/condition
仍没有权威同步对象生命周期来源，故不伪造实例行。P1 聚合项仍为 partial。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-statement-stack-lost-current-continuation1773.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-table-instances-lost-current-continuation1774.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-transaction-thread-mapping-current-continuation1732.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-wait-thread-mapping-current-continuation1733.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-prepared-command-status-current-continuation1734.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-prepared-command-session-status-current-continuation1735.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-digest-sample-age-current-continuation1762.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-connect-attrs-longest-seen-current-continuation1763.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-metadata-lock-lost-current-continuation1764.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-connection-summary-lost-current-continuation1765.txt`。

本轮最新 P3 环境证据：`reports/compatibility/p3-client-environment-matrix-audit-current-continuation1781.txt`。

本轮最新 P1 运行时来源审计：`reports/compatibility/p1-performance-schema-sync-instance-source-audit-current-continuation1782.txt`。

本轮最新 P3 live 客户端证据：`reports/compatibility/p3-client-live-matrix-go-python-node-current-continuation1783.txt`。

Continuation 1783 在隔离服务实例上通过了 Go、PyMySQL、Node.js/mysql2 的定义客户端场景；
MySQL CLI 仍因当前环境缺少 `mysql.exe` 且 Docker fallback 不可用而跳过。该证据关闭三类
客户端的 live smoke slice，但不等于完整非 Connector/J 矩阵完成，P3 继续保持 `partial`。

Continuation 1782 确认 mutex/rwlock/condition instance 当前只有官方形状和无组件时的空集投影，
尚无可追踪对象身份、owner/wait 状态及 allocator/lost 的真实同步对象来源；因此没有伪造运行时
行，也没有把 Go `sync` 原语冒充 MySQL instrument。该切片不改变 P1 聚合项的 `partial` 状态，
下一步需先建设真实同步对象生命周期，再收口实例、wait、history 和 lost 语义。

Continuation 1781 刷新了非 Connector/J 客户端环境状态：Go、PyMySQL 和 Node.js/mysql2
运行时可用，但当前主机没有 `mysql.exe`。该次命令是 diagnostic-only，不启动服务端、不执行
SQL，因此只能更新可执行条件证据，不能把完整 P3 矩阵改成 implemented；缺失 CLI、受保护凭据
或官方 fixture 时继续标记为环境未验证/外部待验证。

Continuation 1765 补齐了 Performance Schema accounts/hosts/users 容量溢出 status 的真实
计数，并修正内部空身份摘要不应生成匿名连接汇总行的问题；完整 P_S、manager、`server/net`
和 metrics 门禁通过。完整 I_S/P_S、P3 全客户端矩阵及 P2/P4 复制/XA/crash/promotion
聚合仍保持原状态。

Continuation 1764 补齐 `Performance_schema_metadata_lock_lost` 的真实容量溢出计数，并验证
`max_metadata_locks=0`、重复读取和过滤后限容行为；完整 P_S、manager、`server/net` 和
metrics 门禁通过。P1 I_S/P_S 聚合、P3 全客户端矩阵及 P2/P4 复制/XA/crash/promotion
聚合仍保持原状态。

Continuation 1763 补齐 `Performance_schema_session_connect_attrs_longest_seen` 的实际最大
编码缓冲区跟踪，并验证其在 P_S 截断前记录；完整 P_S、manager、`server/net` 和 metrics
门禁通过。该切片仍不改变完整 I_S/P_S、非 Connector/J 客户端、P2/P4 复制互操作以及明确
out-of-scope 项的聚合状态。

Continuation 1762 将 `performance_schema_max_digest_sample_age` 接入 digest recorder 的
实际重采样策略：旧样本超过配置年龄时可被新样本替换，动态 `SET GLOBAL` 与启动配置均生效；
完整 P_S、manager、`server/net` 和 metrics 门禁通过。该切片不改变 P1 I_S/P_S 聚合项的
`partial` 状态，也不改变非 Connector/J 客户端、P2/P4 复制互操作或明确 out-of-scope 项。

Continuation 1735 将六类 `Com_stmt_*` 预处理协议命令的身份维度接入
`status_by_thread` 与 `status_by_account/status_by_user`：有权威在线 session 时使用真实
连接 ID、用户和主机；身份缺失时只保留全局计数，不伪造线程。引擎身份维度专项和
`server/net` 回归均通过；完整 I_S/P_S、P2/P3/P4 聚合仍保持原状态。

Continuation 1734 补齐了二进制预处理协议到 `Com_stmt_*` 状态变量的运行时计数，覆盖
prepare/execute/close/reset/send-long-data/fetch，并通过引擎状态回归和预处理协议专项回归。
该切片只关闭 prepared-protocol status counter 缺口；完整 P_S 字段、组件生命周期、权限
语义和 P1/P2/P3/P4 聚合仍保持原状态。

Continuation 1733 将同一权威映射扩展到 `events_waits_current`、`events_waits_history` 和
按线程等待汇总，record-lock wait 的 THREAD_ID 在在线 session 存在时不再使用事务 ID 冒充。
六项专项回归（26.635 秒）及完整 `TestPerformanceSchema` 族（922.654 秒）通过；该切片
仍不等于完整 Performance Schema 表列、组件生命周期和权限语义完成。

Continuation 1732 完成了 record-lock 视图的权威事务到 session/thread 映射：
`performance_schema.data_locks` 与 `data_lock_waits` 在在线 session 注册存在时返回真实连接
线程 ID，没有映射时保持 `NULL`；不再把 InnoDB transaction ID 伪装成 THREAD_ID。该项不等于
完整 Performance Schema、锁元数据/值解析或 P1/P2/P3/P4 聚合完成。

本轮最新 P1 增量证据：`reports/compatibility/p1-variables-info-global-source-current-continuation1705.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-setup-instruments-null-flags-current-continuation1706.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-replication-empty-runtime-current-continuation1707.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-abstract-instrument-metadata-current-continuation1712.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-innodb-file-instrument-registry-current-continuation1713.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-thread-instrument-registry-current-continuation1714.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-windows-thread-registry-current-continuation1715.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-main-thread-fallback-current-continuation1716.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-main-thread-update-current-continuation1717.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-main-thread-runtime-fields-current-continuation1718.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-prepared-statements-all-live-sessions-current-continuation1721.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-event-history-all-live-threads-current-continuation1724.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-history-retention-defaults-current-continuation1725.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-history-size-variables-current-continuation1726.txt`。

本轮最新 P2 增量证据：`reports/compatibility/p2-native-endpoint-discovery-control-plane-current-continuation1727.txt`。

本轮最新 P2 增量证据：`reports/compatibility/p2-relay-state-persist-recovery-current-continuation1728.txt`。

本轮最新 P2 增量证据：`reports/compatibility/p2-native-pre-ga-row-events-current-continuation1729.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-innodb-locks-granted-snapshot-current-continuation1730.txt`。

本轮最新 P1 增量证据：`reports/compatibility/p1-performance-schema-data-lock-thread-identity-current-continuation1731.txt`。

Continuation 1731 修正 `performance_schema.data_locks` 的 record-lock 身份语义：没有
权威 transaction-to-thread 来源时，`THREAD_ID` 保持 NULL，不再冒充事务 ID；真实
`ENGINE_TRANSACTION_ID`、wait/history 和 metadata-lock owner thread 保持不变。

Continuation 1730 补齐 `INFORMATION_SCHEMA.INNODB_LOCKS` 对 LockManager 中 granted
record-lock snapshot 的实时投影，并覆盖无 wait edge 的已持有锁；P1 聚合项仍需完整
锁元数据、组件、生命周期、运行时字段和权限证据，继续保持 `partial`。

Continuation 1729 补齐 native binlog 解码器对 PRE-GA 行事件类型 20/21/22 的 v1 行布局、
事务内嵌套 payload 和 insert/update/delete 行像映射；专项测试先红后绿，整个
`server/replication` 包通过。该证据只关闭 legacy row-frame 解码缺口，官方 binlog/GTID
fixture、XA 互操作、crash-kill、晋升和 fencing 的 P2/P4 聚合项仍保持 `partial`。

Continuation 1727 补齐并回归 native endpoint 的控制面发现：replica 通过 peer 的
`/replication/status` 读取 source 的 `NativeEndpoint` 并执行 native source repoint，
内部凭据保留、持久化和对外状态脱敏，旧 source UUID 在切换后清除。专项重复 5 次及
整个 `server/replication` 包通过；跨节点多源调度、分布式 channel 编排、共识级 fencing
和完整官方 XA/binlog/GTID 拓扑仍保持 P2/P4 `partial`。

Continuation 1728 补齐晋升 relay 导入的持久化恢复证据：当 relay 的逻辑/native 投影已经
落盘而 promoted source 的 durable state 替换失败时，重启可从已提交 stream 恢复 upstream
GTID 与稳定 transaction key，重试导入不产生第二个逻辑 COMMIT。该切片仍不等于共识级
fencing、多源调度、分布式 channel 编排、完整官方 XA/binlog/GTID 互操作或 crash-kill
全矩阵，P2/P4 聚合项继续保持 `partial`。

## 本轮全局范围与优先级确认

以下边界统一纳入全局任务，不再作为“暂不考虑”的局部备注：

- **P0**：MySQL 服务可启动、核心 SQL/事务可用、集群复制与故障切换可用，以及
  **Connector/J** 连接、初始化、预编译、事务和元数据主路径。
- **P2**：native binlog/GTID、XA 与复制回放的统一提交边界、崩溃恢复、晋升、fencing
  和官方互操作；它是 P0 集群能力后的下一阶段收口项。
- **P1**：完整 `INFORMATION_SCHEMA` / `PERFORMANCE_SCHEMA` 的表、字段、运行时统计、
  锁/等待、线程生命周期、组件和权限语义。当前常用表与字段已实现，但全量官方语义仍是
  `partial`。
- **P3**：除 Connector/J 外的 MySQL CLI、Python、Go、Node.js 及 ORM/连接池/版本/网络
  故障的完整客户端兼容矩阵。已有基础矩阵和若干故障切片，但聚合项仍是 `partial`。
- **P4**：更多官方 MySQL 版本、双向拓扑、crash-kill、网络分区和 XA/binlog/GTID 的
  端到端 fixture 门禁。
- **明确排除**：MyISAM、ARCHIVE、CSV 等非 InnoDB 存储引擎，以及非 InnoDB 专用的
  `REPAIR TABLE`、引擎转换等能力；FULLTEXT 继续按既定延期项处理。

因此，状态判断不能只看“SQL 能返回”或单元测试通过：只有对应的真实运行时、恢复、
客户端和官方互操作证据齐备，才允许把聚合项从 `partial` 改为 `implemented`。

## Continuation 1718

补齐 `performance_schema.threads` server-main fallback 的 `PROCESSLIST_DB`、
`PROCESSLIST_TIME` 和 `RESOURCE_GROUP` 运行时投影，并同步覆盖两条 fallback 路径。专项
回归先红后绿，完整 Performance Schema 回归（734.861 秒，D: 临时目录）通过；真实 OS
线程号、主线程内存计数、完整后台线程生命周期、组件/权限语义和全量 I_S/P_S 仍保持
`partial`。

Evidence: `reports/compatibility/p1-performance-schema-main-thread-runtime-fields-current-continuation1718.txt`。

## Continuation 1717

补齐 `performance_schema.threads` 官方 server-main 行 `THREAD_ID=1` 的运行时更新语义：
`INSTRUMENTED` 和 `HISTORY` 可被更新并在查询中反映，单行影响数也正确返回。专项回归
先红后绿，完整 Performance Schema 回归（741.468 秒，D: 临时目录）通过；完整后台线程
生命周期、运行时统计、组件/权限语义和全量 I_S/P_S 仍保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-main-thread-update-current-continuation1717.txt`。

## Continuation 1716

修正 `performance_schema.threads` 无当前客户端会话时的 server-main fallback：从项目自定义
的 `xmysql/FOREGROUND/Sleep` 改为官方 MySQL 8.4 的 `thread/sql/main/BACKGROUND`，并让
`PROCESSLIST_ID` 和 `PROCESSLIST_COMMAND` 返回 `NULL`。专项回归先红后绿，完整
Performance Schema 回归（711.962 秒，D: 临时目录）通过；该切片只关闭一行线程形状差异，
完整后台线程生命周期、运行时统计、组件/权限语义以及全量 I_S/P_S 仍保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-main-thread-fallback-current-continuation1716.txt`。

## Continuation 1715

补齐官方 MySQL 8.4 Windows 条件注册的四个 `setup_threads` 行：
`thread/sql/con_named_pipes`、`thread/sql/con_shared_mem`、`thread/sql/con_sockets` 和
`thread/sql/shutdown_restart`。xmysql 按 `runtime.GOOS` 进行平台条件投影，当前 Windows
专项测试和完整 Performance Schema 回归（1139.440 秒）通过；listener、shutdown 和完整
线程生命周期/权限语义仍保持 P1 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-windows-thread-registry-current-continuation1715.txt`。

## Continuation 1714

补齐官方 MySQL 8.4 Unix server thread registry 中的六个 `setup_threads` 行：
`thread/sql/admin_interface`、`thread/sql/bootstrap`、`thread/sql/compress_gtid_table`、
`thread/sql/manager`、`thread/sql/parser_service` 和 `thread/sql/signal_handler`。
它们现在按官方 PSI flags 返回 `singleton/user` 属性，默认 `ENABLED=YES,HISTORY=YES`，
并通过专项及完整 Performance Schema 回归（1023.278 秒）。这只关闭 thread registry
缺口，完整后台线程生命周期、权限和运行时语义仍为 P1 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-thread-instrument-registry-current-continuation1714.txt`。

## Continuation 1713

补齐官方 MySQL 8.4 `setup_instruments` 的四个 InnoDB 文件注册行：
`innodb_tablespace_open_file`、`innodb_temp_file`、`innodb_arch_file` 和
`innodb_clone_file`。它们现在以 `ENABLED=YES,TIMED=YES` 出现在 setup registry 中；
先红后绿专项测试和完整 `TestPerformanceSchema` 族（750.467 秒）通过。该切片只关闭
注册表缺口，不等于 file instances、I/O runtime lifecycle、组件和权限语义完成，P1 聚合项
继续保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-innodb-file-instrument-registry-current-continuation1713.txt`。

## Continuation 1700

`performance_schema.log_status` 的 `STORAGE_ENGINES` 不再固定返回空对象：当本地 InnoDB
redo manager 可用时，xmysql 现在按 MySQL 8.4 契约返回
`{"InnoDB":{"LSN":...,"LSN_checkpoint":...}}`，数值直接来源于当前 redo manager。
该切片通过先红后绿的运行时回归和 redo/log_status 聚焦回归，但不改变 P1 聚合项仍为
`partial` 的结论；完整 I_S/P_S 运行时、组件生命周期和权限矩阵仍需继续覆盖。

Evidence: `reports/compatibility/p1-log-status-storage-engine-current-continuation1700.txt`。

## Continuation 1701

修正 `performance_schema.log_status.REPLICATION` 的官方 JSON 容器契约：MySQL 8.4 要求
该列是按 replication channel 组织的 JSON 数组。当前 runtime 尚无权威本地 relay-log
文件/位点时，xmysql 返回 `[]`，不再返回 `{"channels":[]}`，也不把 upstream source
坐标伪装成 relay 坐标。先红后绿回归通过；完整 relay channel lifecycle、XA/native
binlog/GTID 和官方双向互操作仍保持 `partial`。

Evidence: `reports/compatibility/p1-log-status-replication-json-shape-current-continuation1701.txt`。

## Continuation 1702

补齐 `transaction_read_only` 与兼容别名 `tx_read_only` 的 session-variable 投影：两者现在
都有 `OFF` 默认值，并在会话设置了对应值时从实时 session state 返回。聚焦回归先复现旧实现
漏行，再通过完整 `TestInformationSchema|TestPerformanceSchema` 定向族；该切片只补齐两个
动态变量行，完整系统变量清单、字段精度、组件生命周期和权限语义仍保持 `partial`。

Evidence: `reports/compatibility/p1-session-read-only-variable-aliases-current-continuation1702.txt`。

## Continuation 1703

补齐 `character_set_database` 与 `character_set_server` 的 session-variable 投影：已有会话
上下文中的动态值现在可以从 session-variable 视图读取，未覆盖时使用当前 utf8mb4 基线。
聚焦先红后绿通过；该切片不改变完整 charset/collation、系统变量清单和 P1 聚合项的
`partial` 状态。

Evidence: `reports/compatibility/p1-session-character-set-variable-scope-current-continuation1703.txt`。

## Continuation 1704

将 session-variable 视图的基线从手写十几个变量扩展为仓库现有 `SystemVariablesManager` 的
完整定义清单，并继续用实时 session 参数覆盖默认值；因此 `wait_timeout`、`innodb_page_size`
等已注册变量不再从 session 视图消失，`variables_by_thread` 也复用同一投影。聚焦回归和完整
Performance Schema 定向族（737.921 秒）均通过；MySQL 8.4 全量变量目录、精确元数据和
完整 P1 语义仍保持 `partial`。

Evidence: `reports/compatibility/p1-session-variable-manager-inventory-current-continuation1704.txt`。

## Continuation 1705

P1 继续收口 `performance_schema.variables_info` 的真实来源语义：系统变量管理器现在
记录全局变量的来源，初始定义显示 `COMPILED`，`SET GLOBAL` 修改后显示 `GLOBAL`；
持久化变量仍由 `SET PERSIST/PERSIST_ONLY` 的 durable metadata 覆盖为 `PERSISTED`，
并保留路径、设置用户和主机信息。新增红绿回归先确认旧实现把运行时修改错误报告为
`COMPILED`，随后通过变量/持久化聚焦门禁及完整 Performance Schema 定向族（705.749
秒）。

该切片只补齐一个 variables_info 来源状态，不代表完整 I_S/P_S 逐表运行时、组件生命
周期和权限矩阵完成；P1 聚合仍为 partial。P2/P4 官方 XA/binlog/GTID/复制/崩溃恢复/
晋升互操作、P3 全量非 Connector/J 客户端矩阵继续按原范围推进，FULLTEXT 继续 deferred，
非 InnoDB 引擎、专用 REPAIR TABLE 和引擎转换继续 out_of_scope。证据见
`reports/compatibility/p1-variables-info-global-source-current-continuation1705.txt`。

## Continuation 1706

P1 继续收口 `performance_schema.setup_instruments` 的字段值语义：对于没有权威 runtime
flag 的 instrument，`FLAGS` 现在返回 SQL `NULL`，与其可空的
`SET('controlled')` 元数据契约一致；不把未知属性合成成 `controlled`。先红后绿回归、
setup/variables 相关门禁和完整 Performance Schema 定向族均通过。P1 完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 逐表运行时、组件生命周期和权限矩阵仍为
partial；P2/P4 XA/binlog/GTID/复制/崩溃恢复互操作、P3 全量非 Connector/J 客户端矩阵
继续按原范围推进，FULLTEXT 继续 deferred，非 InnoDB 继续 out_of_scope。证据见
`reports/compatibility/p1-performance-schema-setup-instruments-null-flags-current-continuation1706.txt`。

## Continuation 1707

又收口了 P_S 复制视图的无 runtime 边界：standalone/source 节点没有已配置 replica
channel 时，`replication_applier_status`、`replication_connection_configuration` 和
`replication_connection_status` 返回空集，不再把空状态快照伪装成 channel；真实过滤规则
仍按其配置状态可见。专项回归和完整 Performance Schema 定向族通过，但这只是 P1/P2 的
一个局部空集语义切片，完整 I_S/P_S、native binlog/GTID/XA/crash recovery、P3 客户端
矩阵和 P4 官方双向互操作仍保持 `partial`。证据见
`reports/compatibility/p1-performance-schema-replication-empty-runtime-current-continuation1707.txt`。

## Continuation 1708

补齐一个有官方注册来源的 `setup_instruments.PROPERTIES` 值：
`wait/io/socket/sql/client_connection` 现在返回 `user`，对应 MySQL PSI 的
`PSI_FLAG_USER`；其余未建立权威 metadata 映射的 instrument 不做名称推断。专项 setup
回归和完整 Performance Schema 定向族通过，但完整 instrument metadata、组件生命周期、
I_S/P_S 逐表运行时和 P2/P3/P4 剩余范围仍保持 `partial`。证据见
`reports/compatibility/p1-performance-schema-client-socket-property-current-continuation1708.txt`。

## Continuation 1709

继续补齐一个有官方注册来源的 `setup_instruments` memory metadata 行：
`memory/sql/THD::main_mem_root` 现在返回 `PROPERTIES='controlled_by_default'`、
`FLAGS='controlled'`、`VOLATILITY=0` 以及官方 documentation。专项回归和完整
Performance Schema 定向族（60.333 秒）通过；其他没有权威来源的 instrument metadata
仍不做名称推断。完整 I_S/P_S 逐表运行时、组件生命周期、P2/P3/P4 互操作仍保持
`partial`。证据见
`reports/compatibility/p1-performance-schema-memory-instrument-metadata-current-continuation1709.txt`。

## Continuation 1710

补齐一个有官方 8.4 文档来源的 `setup_instruments` 注册/metadata 行：
`statement/abstract/Query` 现在返回 `PROPERTIES='mutable'`、`FLAGS=NULL`、
`VOLATILITY=0` 以及查询刚从网络接收、尚未完成语句类型细化的 documentation。专项
setup metadata 回归和完整 Performance Schema 定向族（786.386 秒，D: 临时目录）通过；
这只补齐注册/metadata，不代表 abstract statement 完整生命周期，完整 I_S/P_S、P2/P3/P4
仍保持 `partial`。证据见
`reports/compatibility/p1-performance-schema-abstract-query-instrument-current-continuation1710.txt`。

## Continuation 1711

补齐两个有官方 8.4 statement event 文档来源的 `setup_instruments` 注册行：
`statement/abstract/new_packet` 和 `statement/abstract/relay_log`，两者返回
`ENABLED=YES`、`TIMED=YES`。专项 setup 回归和完整 Performance Schema 定向族
（785.895 秒，D: 临时目录）通过；完整 abstract statement refinement/lifecycle、
I_S/P_S 逐表运行时及 P2/P3/P4 仍保持 `partial`。证据见
`reports/compatibility/p1-performance-schema-abstract-instrument-registry-current-continuation1711.txt`。

## Continuation 1712

补齐 `statement/abstract/new_packet` 与 `statement/abstract/relay_log` 的官方 metadata：
`PROPERTIES='mutable'`、`FLAGS=NULL`、`VOLATILITY=0` 以及官方 documentation。专项
metadata 回归和完整 Performance Schema 定向族（780.342 秒，D: 临时目录）通过；完整
statement refinement/lifecycle、I_S/P_S 逐表运行时和 P2/P3/P4 仍保持 `partial`。证据见
`reports/compatibility/p1-performance-schema-abstract-instrument-metadata-current-continuation1712.txt`。

## 1. 范围决策

| 优先级 | 范围 | 当前状态 | 结论 |
|---|---|---|---|
| P0 | MySQL 启动、协议、核心 CRUD、集群复制/故障切换、Connector/J | implemented | 已达到当前 P0 gate，Connector/J 与原有 P0 同级 |
| P1-A | INFORMATION_SCHEMA 全量表/列形状、过滤、权限可见性、运行时数据 | partial | 纳入全局任务，继续补齐，不以“可查询”作为完成条件 |
| P1-A | PERFORMANCE_SCHEMA 全量表/列形状、instrument/consumer、事件生命周期、运行时统计 | partial | 纳入全局任务，继续补齐；组件不存在时必须有明确兼容语义 |
| P2 | XA、原生 binlog、GTID、relay/applied state、崩溃恢复、提升/切换 | partial | 纳入全局任务，继续收敛持久化提交边界和恢复互操作 |
| P3 | 非 Connector/J 客户端完整矩阵：mysql CLI、Go、Python、Node.js 及集群端点场景 | partial | 纳入全局任务；定义的 smoke gate 已通过，但负向、重连、协议边界和完整集群客户端矩阵仍未完成 |
| P4 | 官方 MySQL fixture 的 XA/binlog/GTID/复制/崩溃恢复互操作 | partial | 已验证隔离的 xmysql -> 官方 MySQL XA 应用；全量历史追平、反向 XA、崩溃恢复等仍需验收 |
| deferred | FULLTEXT | deferred | 按当前决定暂不进入本轮实现 |
| out of scope | MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎，以及非 InnoDB 专用 REPAIR/引擎转换 | out_of_scope | 明确不纳入本项目兼容目标 |

执行顺序按当前交付目标固定为：P0（启动、核心 CRUD、集群、Connector/J）→ P2（本地原生复制/XA/binlog/GTID/恢复收口）→ P1（完整 I_S/P_S）→ P3（非 Connector/J 全量客户端）→ P4（官方 MySQL 双向互操作）。FULLTEXT 保持 deferred；非 InnoDB 保持 out_of_scope。

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
2. 非 Connector/J 客户端矩阵继续纳入全局 P3。定义的 smoke/cluster-endpoint gate 已
   `implemented`，已有证据覆盖 Docker MySQL 8.4.11 CLI、Go mysql driver、PyMySQL、Node
   mysql2，并覆盖源端和晋升端；负向、协议边界、更多类型/字符集、ORM 和完整故障恢复
   矩阵仍由 `non-connector-j-full-compatibility-matrix` 保持 `partial`。
3. XA 原生互操作继续纳入全局 P2/P4。本地 XA 状态机、native binlog 基线和官方 MySQL
   作为 source 的读取已有证据；官方双向 XA/binlog/GTID、重复投递、崩溃窗口、恢复后
   promotion/failover 仍未形成完整闭环，因此聚合项保持 `partial`。
4. 非 InnoDB 引擎不纳入本全局任务，状态为 `out_of_scope`。MyISAM、ARCHIVE、CSV、非
InnoDB 专用 `REPAIR TABLE` 和引擎转换均不作为剩余开发项。

### 2.2.2 最新增量（Continuation 1697）

复制 storage transaction 在物理 WAL 已提交但 applied/GTID marker 首次写入失败时，
现在可以在同一个 committed context 上重试并完成 journal/marker 发布；重试不会再次执行
物理 `TransactionManager.Commit`。该切片只关闭同进程 marker 重试窗口，P2/P4 的统一
storage/WAL/native-binlog/GTID/applied-marker 提交协议、完整 crash-kill/拓扑以及官方
MySQL 全量互操作仍保持 `partial`。

Evidence: `reports/compatibility/p2-replication-committed-context-marker-retry-current-continuation1697.txt`。

### 2.2.3 最新增量（Continuation 1698）

SQL EVENT 的 DML 结果现在通过内部 statement-result observer 在结果发布前收口
`events_statements_summary_by_program` 摘要，不再等待无关的 executeQuery 延迟 metrics
cleanup；事件生命周期回归同时等待数据行和摘要行，避免用异步 scheduler 的中间状态误判。
该切片收窄一个 P1 Performance Schema runtime publication 窗口，但完整 I_S/P_S 表、字段、
组件、运行时和权限语义仍保持 partial。

Evidence: `reports/compatibility/p1-event-program-summary-publication-current-continuation1698.txt`。

### 2.2.1 最新状态校正（Continuation 1655）

当前权威矩阵已更新为 `scope-matrix-current-continuation1655.json`：29 项中
18 项 `implemented`、8 项 `partial`、1 项 `deferred`、2 项 `out_of_scope`。
`caching_sha2_password` 快速认证的服务端协议切片、Go/PyMySQL/mysql CLI 和 Node.js/mysql2
的端到端认证插件切片均已有通过证据；Node 的历史一次性超时在两次独立新进程重跑中未复现。
P3 聚合项仍不能标记为完成，因为更广泛客户端版本/ORM、负向/网络故障和集群拓扑尚未覆盖。
P1 的完整 I_S/P_S 运行时/组件/权限语义、P2 的统一物理提交协议与完整 crash-kill/拓扑矩阵、
P4 官方 MySQL 双向 fixture 仍是全局未完成项；但 continuation 1650/1651 已新增官方
8.4.11 反向晋升和 source crash/reconnect 子门禁通过，不能替代完整拓扑和全交错矩阵。

Continuation 1652 又补齐了共享原子复制状态文件的 rename 后目录项持久化边界：Unix 使用目录
句柄 `Sync()`；Windows 尝试带 backup-semantics 的目录句柄 `FlushFileBuffers()`，对常见
文件系统的 `ERROR_ACCESS_DENIED` 按平台不支持处理为 best-effort，并验证目录同步调用及
失败传播。该切片关闭/收窄一个本地目录项崩溃窗口，但不改变 P2/P4 聚合项的 `partial` 状态，
因为统一 storage/WAL/native-binlog/applied-marker 提交协议、全 crash-kill/网络分区/拓扑和
完整官方双向 fixture 仍未完成。

Continuation 1653 的全仓 Go 串行回归（`go test -p 1 ./... -count=1 -timeout 90m`）全部通过，
确认 Continuation 1652 的复制状态目录持久化改动没有引入跨包回归；它不替代 P1/P2/P3/P4
各自所需的官方语义、外部客户端和分布式故障门禁。

Continuation 1654 又验证了 native 复制部分事务批次在 transport reset 后的重连恢复：relay
边界先持久化，剩余帧重试后只应用一次，GTID/source position 前进且历史 I/O 错误保留。该
切片收口一个具体网络故障边界，但不改变 P2/P4 聚合项的 `partial` 状态。

Continuation 1655 又验证了非 Connector/J 客户端的真实 TCP 断连恢复：xmysql 保持运行，由
本地 fault proxy 主动关闭 Go mysql-driver、PyMySQL、Node.js/mysql2 的活动连接，三者均在
旧查询失败后通过新连接恢复。该切片关闭一个 P3 网络故障子门禁，但不改变完整客户端矩阵的
`partial` 状态。

Continuation 1656 又用 Docker `mysql:5.7.44` 和 `mysql:8.0.41` 跑通当前定义的 16 个
CLI 用例，结合既有 `mysql:8.4.11` 证据形成 CLI 5.7/8.0/8.4 版本切片。该门禁使用开发
免密夹具，不能替代生产凭据和完整客户端版本/ORM/网络拓扑覆盖，因此 P3 聚合项继续保持
`partial`。

Continuation 1657 新鲜重跑官方 MySQL 8.4.11 反向晋升 fixture：官方 source 的普通事务、两阶段
XA 和一阶段 XA 均应用到 xmysql，xmysql 重启后无重复；xmysql 晋升为 source 后，普通/XA/一阶段
XA 又应用到官方 target。该子门禁通过，但不改变 P2/P4 聚合项的 `partial` 状态，因为完整
crash-kill、网络分区、更多版本和双向 GTID/XA 拓扑仍未完成。

Continuation 1658 新鲜重跑官方 MySQL 8.4.11 source crash/reconnect fixture：xmysql 重启后恢复
崩溃窗口前、窗口中的普通/XA/一阶段 XA 事务，随后在官方 source 重连后继续应用后续普通/XA
事务；GTID 连续且无重复。该子门禁通过，但完整 crash-kill、网络分区、更多版本和双向拓扑仍
未完成，P2/P4 继续保持 `partial`。

Continuation 1659 新鲜运行官方 MySQL 8.0.41 反向晋升 fixture：普通事务、两阶段 XA、一阶段 XA
均完成官方 source -> xmysql、重启无重复、xmysql 晋升 -> 官方 target 的闭环；fixture 已兼容
MySQL 8.0 的 `SHOW MASTER STATUS`。该版本子门禁通过，但 P2/P4 聚合项仍因更多版本、完整
crash-kill/网络分区和双向 GTID/XA 拓扑保持 partial。Evidence:
`reports/compatibility/p2-p4-official-reverse-promotion-mysql80-current-continuation1659.txt`。

Continuation 1660 新鲜运行官方 MySQL 8.0.41 source crash/reconnect fixture：xmysql 强制终止后恢复
崩溃前/崩溃窗口中的普通、两阶段 XA、一阶段 XA，官方 source 重启后继续追平后续普通/XA 事务，
GTID 连续且无重复。该版本子门禁通过，但 P2/P4 聚合项仍因更多版本、网络分区/fencing 和完整
双向 GTID/XA 拓扑保持 partial。Evidence:
`reports/compatibility/p2-p4-official-source-crash-reconnect-mysql80-current-continuation1660.txt`。

Continuation 1661 在 8.0.41 版本化状态查询修改后重新执行官方 MySQL 8.4.11 反向晋升 fixture，
普通、两阶段 XA、一阶段 XA 双向应用及重启去重均通过，确认 fixture 回退逻辑未破坏 8.4.11。
该回归不替代完整网络分区、fencing 和双向 GTID/XA 拓扑门禁，P2/P4 聚合项继续保持 partial。
Evidence: `reports/compatibility/p2-p4-official-reverse-promotion-script-regression-current-continuation1661.txt`。

Continuation 1662 新增官方 MySQL 8.4.11 真实进程持续网络分区/恢复：官方 source 保持在线，
proxy 关闭活动连接并拒绝新连接，分区期间 xmysql 保持断点，heal 后普通事务、两阶段 XA、一阶段
XA 全部追平且无重复。该子门禁通过，但不替代分布式 fencing/共识、多节点分区晋升和完整版本/拓扑
矩阵，P2/P4 聚合项继续保持 partial。Evidence:
`reports/compatibility/p2-p4-official-source-network-partition-current-continuation1662.txt`。

Continuation 1667 确认 XA 一阶段和已 prepare 的提交路径把规范化 XA XID 写入同一条物理
`LOG_TYPE_TXN_COMMIT` WAL 记录；focused identity tests 与 engine 全包回归通过。该切片只
收口 XA 的 WAL identity binding，不改变统一 storage/WAL/native-binlog/GTID/applied-state
提交协议仍未完成的结论。Evidence:
`reports/compatibility/p2-xa-identity-wal-current-continuation1667.txt`。

Continuation 1668 又修复 XA publication 失败后的 durable journal identity：恢复用的 commit
record 现在与物理 WAL 一样优先使用规范化 XA XID，避免把 XA 当普通 client key 重放。聚焦
测试和 engine 全包回归通过；该切片不改变 P2/P4 聚合项仍为 partial 的结论。Evidence:
`reports/compatibility/p2-xa-journal-recovery-identity-current-continuation1668.txt`。

Continuation 1669 又确认 XA DML journal 在物理提交前就使用规范化 XA XID，覆盖 commit record
尚未生成时的更早崩溃窗口。该切片通过 focused identity tests 与 engine 全包回归，但不改变
统一 storage/WAL/native-binlog/GTID/applied-state 物理提交协议仍未完成的结论。Evidence:
`reports/compatibility/p2-xa-precommit-journal-identity-current-continuation1669.txt`。

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
source position、HTTP pull position、prepared-XA 重试和恢复测试，以及 native applied
marker 在同进程重试时的状态收敛；仍未完成的是单一
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

当前矩阵：29 条记录，其中 27 条在范围内、2 条明确 out of scope；状态为：

- implemented：18
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

## Continuation 1562

继续补齐等待生命周期语义：`performance_schema.table_lock_waits_summary_by_table` 对已经
完成的 record-lock 与 metadata-lock wait，现在使用事件完成时保存的 `Instrumented/TIMED`
快照；后续把当前 instrument 改为 `TIMED=NO` 或 `ENABLED=NO`，不会重算计时或删除已经
产生的汇总。current wait 仍按当前配置判断。专项测试、manager 全包、完整 P_S 和 engine
全包回归均通过。

该切片关闭的是已完成 table-lock wait summary 的生命周期快照缺口；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表、字段、组件/权限语义，剩余 wait 类别，P2/P4
多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover 仍为 partial。
FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-wait-summary-instrument-snapshot-current-continuation1562.txt`。

## Continuation 1563

继续补齐 Performance Schema statement instrument 的最终命名与配置边界：已支持的
DDL、SHOW、SET、事务、授权和 XA 语句不再统一记录为首个 SQL 关键词，而是映射到
MySQL 风格的 `statement/sql/create_table`、`statement/sql/alter_table`、
`statement/sql/show_tables`、`statement/sql/begin`、`statement/sql/xa_start` 等
最终 instrument；`setup_instruments` 同步暴露这些行，因此按行更新 `ENABLED/TIMED`
可以控制对应语句采集；新增回归验证关闭 `statement/sql/create_table` 时真实 DDL 不入
汇总、重新开启后恢复。statement instrument 专项、完整 P_S 和 engine 全包回归均通过。

该切片关闭的是已支持语句的 canonical statement instrument 命名/配置缺口；完整
INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表、字段、组件/权限语义，剩余 instrument/stage/wait
类别，P2/P4 多通道复制、官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover
仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。
证据见 `reports/compatibility/p1-performance-schema-canonical-statement-instruments-current-continuation1563.txt`。

## Continuation 1564

继续补齐已有执行路径的 Performance Schema statement instrument 覆盖：存储对象 DDL、视图、
SQL PREPARE/EXECUTE/DEALLOCATE、CALL、表重命名、表锁、FLUSH/KILL、表维护、复制控制、
SHOW 扩展以及用户/角色语句现在映射到 MySQL 风格的 canonical `statement/sql/*` 名称，
包括 `create_view`、`create_procedure`、`call_procedure`、`prepare_sql`、`rename_table`、
`lock_tables`、`change_master`、`slave_start` 等；`insert ... select` 和
`replace ... select` 也分别使用 `insert_select`/`replace_select`。这些名称已注册到
`setup_instruments`，因此现有 runtime gate 可按 instrument 控制采集。

新增映射与 setup 行回归通过；完整 `TestPerformanceSchema` 用例通过（75.181s），engine
全包通过（446.339s）。本切片不宣称没有对应 xmysql runtime 的 Clone、Firewall、Keyring、
NDB、Enterprise scheduler 等组件表已完成；这些仍按真实组件是否存在分别处理，不能用合成
运行时行冒充官方语义。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 表、字段、组件/权限
语义，剩余 instrument/stage/wait 类别，P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复投递和
promotion/failover 仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用
REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-expanded-statement-instruments-current-continuation1564.txt`。

## Continuation 1565

补齐 Performance Schema 8.4 `setup_consumers` 的 `events_statements_cpu` 配置项：默认值为
`NO`，可以通过 `SELECT` 查看并通过 `UPDATE` 动态切换，且继续受现有 setup 表权限校验约束。
该 consumer 只表示 CPU 时间采集开关；当前 xmysql 尚未伪造 CPU 时间，`CPU_TIME` 仍保持
未采集时的 `NULL/0` 边界，因此不能把 CPU 指标本身宣称为已实现。

新增 consumer 默认值/查询/更新回归和完整 `TestPerformanceSchema` 回归通过（73.137s）。
该切片只关闭 setup consumer 注册与生命周期配置缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
表、字段、组件/权限语义，P2/P4 官方 XA/binlog/GTID、崩溃恢复、重复投递和 promotion/failover
仍为 partial。FULLTEXT 继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。证据见
`reports/compatibility/p1-performance-schema-statement-cpu-consumer-current-continuation1565.txt`。

## Continuation 1566

完成官方 MySQL 8.4.11 反向复制与运行时 Promote 验证：官方源的普通 InnoDB 事务和
XA 事务均被 xmysql 追平；停止官方源后 xmysql Promote 为 source；官方目标随后从
xmysql 读取 GTID/native binlog，并成功应用 Promote 后的行。过程中补齐认证结果权限
传播、Promote 后动态 source 绑定、所有启动角色安装 replication/XA commit hooks、普通
事务的 `QUERY_EVENT(BEGIN)`、MySQL 8.4 GTID event 元数据以及 `VARCHAR` TABLE_MAP
字节宽度。

证据：`reports/compatibility/p2-p4-official-reverse-replication-current-continuation1566.txt`，
fixture 结果目录为
`C:\\Users\\Administrator\\AppData\\Local\\Temp\\xmysql-reverse-promotion-20260928-114430`。
最新源码下 `server/net`、`server/replication` 和 `server/innodb/engine` 回归均通过，
其中 engine 全包耗时 435.977 秒。
官方反向 source/replica、XA、GTID、native row-event 和 runtime promotion 子门禁已验证，
但 P2/P4 聚合项仍保持 partial：崩溃窗口恢复、重启后的重复投递幂等、官方双向 XA 以及
更广泛的拓扑/故障切换场景尚未全部覆盖。完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
和非 Connector/J 客户端矩阵继续作为全局任务；FULLTEXT 继续 deferred，非 InnoDB
引擎、专用 REPAIR/转换继续 out of scope。

## Continuation 1567

执行外部进程级崩溃恢复矩阵 3 次：已提交数据重启后保留，未提交数据回滚，
半提交场景最多应用一次，DDL 和重建索引元数据在重启后仍可查询。证据见
`reports/compatibility/p2-external-process-crash-recovery-current-continuation1567.txt`。
该切片关闭 xmysql 本地外部 crash/recovery 子门禁，但官方 MySQL 重启后的重复投递、
native GTID/applied-state 崩溃窗口、双向 XA 和更广泛 promotion/failover 拓扑仍使
P2/P4 聚合项保持 partial。

## Continuation 1568

增强官方 MySQL 8.4.11 反向复制 fixture，加入 xmysql 进程重启后的重复拉取检查：
普通事务和 XA 事务先同步到 xmysql，xmysql 重启并重新连接后数据仍存在且没有重复，
随后 Promote 成 source，官方目标继续成功应用 Promote 后的 xmysql 事务。证据见
`reports/compatibility/p2-p4-official-reverse-restart-promotion-current-continuation1568.txt`。
该切片关闭官方 source → xmysql restart → promotion → official target 子门禁；
官方双向 XA、native GTID/applied-state crash-kill、任意重复投递和更广泛拓扑仍使
P2/P4 聚合项保持 partial。

## Continuation 1569

补齐 `performance_schema.setup_consumers.events_statements_cpu` 的实际运行时语义：
在 Windows/Linux 上按执行 OS 线程采集 CPU 时间，传递到 `events_statements_history*`
的 `CPU_TIME`，并累计到 statement summary/digest 的 `SUM_CPU_TIME`；consumer 关闭时
历史事件的 `CPU_TIME` 保持 `NULL`。专项测试、metrics 包回归、完整 `TestPerformanceSchema`、
engine 全包回归和 Linux 目标编译均通过。证据见
`reports/compatibility/p1-performance-schema-statement-cpu-time-current-continuation1569.txt`。

该切片只关闭 CPU_TIME 的运行时采集缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA
的组件、权限、instrument/stage/wait/thread/runtime 语义仍为 partial。P2/P4 官方双向 XA、
native GTID/applied-state crash-kill、任意重复投递和更广泛拓扑仍为 partial；FULLTEXT
继续 deferred，非 InnoDB 引擎及专用 REPAIR/转换继续 out of scope。

## Continuation 1570

继续补齐 P_S prepared statement 运行时统计：SQL `PREPARE/EXECUTE` 的嵌套执行上下文
现在把已采集的 CPU 时间累计到 `prepared_statements_instances.SUM_CPU_TIME`，不再固定
返回 0；新增 prepared statement 回归通过。协议 `COM_STMT_EXECUTE` 的独立 CPU 传播门禁
仍需单独补齐。证据见
`reports/compatibility/p1-performance-schema-prepared-cpu-time-current-continuation1570.txt`。

## Continuation 1571

补齐协议 prepared statement 的 CPU 统计：`COM_STMT_EXECUTE` 在
`events_statements_cpu` 开启时锁定执行 OS 线程采集 CPU 时间，协议层
`PreparedStatementManager` 现在保留 `CPUTimeTotal`，因此
`prepared_statements_instances.SUM_CPU_TIME` 可覆盖 Connector/J 风格的二进制预处理
语句。协议级回归和 net/protocol/compatibility 包回归通过。证据见
`reports/compatibility/p1-performance-schema-com-stmt-cpu-time-current-continuation1571.txt`。

## Continuation 1572

补齐 `events_statements_summary_by_program.SUM_CPU_TIME` 的真实运行时聚合：存储过程
从子语句 summary 累计 CPU 时间，存储函数、EVENT 和 TRIGGER 也在 CPU consumer 开启
时传递捕获值；普通 `executeQuery` 路径同步补上 statement summary 的 CPU 字段传播。
存储过程/函数专项和完整 `TestPerformanceSchema` 回归均通过。证据见
`reports/compatibility/p1-performance-schema-program-cpu-time-current-continuation1572.txt`。

该切片只关闭程序级 CPU 聚合缺口；完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA 的
组件、权限、instrument/stage/wait/thread/runtime 语义仍为 partial，P2/P4 官方
XA/binlog/GTID、崩溃恢复、重复投递和拓扑故障切换仍未完成。

## Continuation 1573

官方 MySQL 反向复制 fixture 现在额外验证 Promote 后的 xmysql XA 事务：官方源的
普通事务和 XA 事务先同步到 xmysql；xmysql 重启并 Promote 后，官方目标同时成功
应用 xmysql 的普通事务和 XA 事务。该路径形成了官方源 -> xmysql XA 与 xmysql
Promote -> 官方目标 XA 的双向子门禁。Evidence:
`reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1573.txt`。

native GTID/applied-state crash-kill、任意重复投递和更广泛 topology/failover
仍未关闭，P2/P4 聚合项继续保持 partial。

## Continuation 1574

补齐本地 replica 的 applied-marker 重试状态收敛：当存储回调已经成功、但最终
replica-state replacement 失败时，后续同进程重试会先提升已持久化 marker，再继续
处理后续事件；relay、native relay、table map、prepared-XA 和已应用行的内存重试视图
也会在每个成功持久化边界同步。新增 native 物理 binlog 回归，验证同进程重试与重启
重试都不会重复调用存储回调。Evidence:
`reports/compatibility/p2-native-applied-marker-same-process-current-continuation1574.txt`。

该切片关闭本地 native applied-marker 同进程/重启重试子门禁；官方 MySQL crash-kill、
任意重复投递和更广泛 topology/failover 仍未关闭，P2/P4 聚合项继续保持 partial。

## Continuation 1575

重新执行官方 MySQL 双向 XA/晋升门禁：官方源的普通事务和 XA 事务同步到 xmysql；
xmysql 重启后保留两条记录并完成 Promote；官方目标随后同时应用 xmysql 的普通事务和
XA 事务。该门禁结果为 PASS，但它是已有双向子门禁的最新复跑，不等于关闭官方
crash-kill、任意重复投递或更广拓扑组合。Evidence:
`reports/compatibility/p2-p4-official-bidirectional-xa-current-continuation1575.txt`。

## Continuation 1576

修复 `performance_schema.table_lock_waits_summary_by_table` 的最小等待时间
初始化：如果表行先由 reset/filter 快照创建，再收到第一条真实等待，
`MIN_TIMER_WAIT`、读/写和 metadata wait 的 `MIN_TIMER_*` 不再错误保持为 0。
单测和完整 `TestPerformanceSchema` 专项回归均通过。Evidence:
`reports/compatibility/p1-performance-schema-table-lock-minimum-current-continuation1576.txt`。

该切片只关闭一个 P_S 运行时统计正确性缺口；完整组件生命周期、所有等待分类、
权限语义和 I_S/P_S 聚合项仍保持 `partial`。

## Continuation 1577

修复全局及按实例 `events_waits_summary_*` 的首条等待初始化：reset 空行在收到
真实等待后不再把 `MIN_TIMER_WAIT`/`MAX_TIMER_WAIT` 锁在默认零值。通过
`TestPerformanceSchemaWaitSummaryTruncateResetsRows`、完整 `TestPerformanceSchema`
专项回归和 `server/innodb/engine` 全包回归。Evidence:
`reports/compatibility/p1-performance-schema-wait-minimum-current-continuation1577.txt`。

该切片只关闭一个 P_S 等待汇总正确性缺口；完整组件生命周期、所有等待分类、权限
语义和 I_S/P_S 聚合项仍保持 `partial`。

## Continuation 1578

修复按 account/host/user 维度重新聚合等待统计时的首条真实等待初始化：reset
保留的空线程行不再污染同一维度其他线程的 `MIN_TIMER_WAIT`/`MAX_TIMER_WAIT`。
通过 account 维度专项、完整 `TestPerformanceSchema` 和 `server/innodb/engine`
全包回归。Evidence:
`reports/compatibility/p1-performance-schema-wait-dimension-minimum-current-continuation1578.txt`。

该切片只关闭一个 P_S 维度聚合正确性缺口；完整组件生命周期、所有等待分类、权限
语义和 I_S/P_S 聚合项仍保持 `partial`。

## Continuation 1579

修复 `INFORMATION_SCHEMA.COLUMN_PRIVILEGES UNION ALL TABLE_PRIVILEGES` 原先固定
返回空结果的问题，改为组合两个真实权限视图的可见性、过滤和行投影。通过联合
查询专项、权限/角色专项、完整 `TestInformationSchema` 和引擎全包回归。
Evidence:
`reports/compatibility/p1-information-schema-privileges-union-current-continuation1579.txt`。

该切片只关闭一个 I_S 权限联合查询缺口；完整权限/角色语义、所有 SQL set-operation
形状以及 I_S/P_S 聚合项仍保持 `partial`。

## Continuation 1580

补齐 one-phase XA 的原生 binlog 重试恢复：当逻辑 `XA_PREPARE` 已落盘但原生
binlog 写入失败时，重试会从逻辑流重建原生文件，并恢复为同一条
`one_phase=1` 事务，不再追加重复事务。新增故障注入回归、原生/XA/崩溃/晋升专项
和完整 `server/replication` 回归均通过。Evidence:
`reports/compatibility/p2-one-phase-xa-native-retry-current-continuation1580.txt`。

该切片只关闭本地 one-phase XA native retry 子门禁；P2/P4 的完整 native binlog、
GTID、crash-kill、promotion 和官方 MySQL 互操作聚合项仍保持 `partial`。

## Continuation 1581

官方 MySQL 双向 fixture 现在同时覆盖 one-phase XA：官方源先向 xmysql 发送普通、
两阶段 XA 和 one-phase XA；xmysql 重启后保持三条记录并完成 Promote；官方目标随后
成功应用 xmysql 的普通、两阶段 XA 和 one-phase XA。该门禁结果为 PASS。Evidence:
`reports/compatibility/p2-p4-official-bidirectional-one-phase-xa-current-continuation1581.txt`。

该切片关闭官方双向 one-phase XA 子门禁，但 P2/P4 完整 crash-kill、GTID/位置、
拓扑故障切换和全量官方互操作聚合项仍保持 `partial`。

## Continuation 1582

补齐权限视图的多分支集合查询：`INFORMATION_SCHEMA.COLUMN_PRIVILEGES` 与
`TABLE_PRIVILEGES` 现在支持多段顶层 `UNION ALL`，并支持 `UNION`/`UNION DISTINCT`
的重复行消除。权限/角色专项和完整 `TestInformationSchema` 均通过。Evidence:
`reports/compatibility/p1-information-schema-privileges-union-distinct-current-continuation1582.txt`。

该切片只关闭权限视图集合查询的已验证子门禁；完整权限/角色可见性、全部 SQL
set-operation 组合及 I_S/P_S 聚合项仍保持 `partial`。

## Continuation 1583

外部进程崩溃恢复矩阵连续执行 3 次通过：已提交数据保留、未提交数据回滚、半提交
事务至多应用一次，DDL 元数据和索引重建元数据在重启后均可查询。Evidence:
`reports/compatibility/p2-external-process-crash-recovery-current-continuation1583.txt`。

该切片确认本地 redo/undo 和半提交 crash 子门禁，但 P2/P4 native GTID/applied-state、
官方 MySQL crash、promotion 和拓扑矩阵仍保持 `partial`。

## Continuation 1584

补齐权限视图的 `INTERSECT`/`EXCEPT` 集合语义：每个分支继续使用原有账户
可见性、过滤和授权行展开逻辑，新增受限账户的交集/差集回归，并通过权限/角色专项、
完整 `TestInformationSchema` 和 engine 包全量回归。Evidence:
`reports/compatibility/p1-information-schema-privileges-intersect-except-current-continuation1584.txt`。

该切片只关闭权限视图集合运算的已验证子门禁；完整权限/角色可见性、全部 SQL
set-operation 组合及 I_S/P_S 聚合项仍保持 `partial`。P3 非 Connector/J 客户端的
已定义 smoke gate 已通过，但全量客户端边界仍单独保持 `partial`。

## Continuation 1586

修复解耦网络处理器的认证字段边界：未指定默认库时，不再把
`mysql_native_password` 插件名写入当前 database；新增 no-default/empty-default
handshake 回归，并通过网络、协议和 replication 包专项回归。官方 MySQL -> xmysql
崩溃前查询可见性已通过，持久化 GTID 6-10、DDL 和崩溃前行 1/2 均有真实 fixture
证据。Evidence:
`reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1586.txt`。

已修复 native recovery 在持久化文件/位置可用时反复从空文件名 GTID dump
起点重放旧 metadata 的问题，并补齐运行时 `@@GLOBAL.gtid_executed` 的客户端可见性。
官方源停机期间产生的行 3/4（普通事务和 one-phase XA）现已在 xmysql 重启后完整追平；
`33489/3429/4429` fixture 返回 `PASS`，xmysql 的 GTID 为 `...:6-12`。
该 crash/reconnect 子门禁已关闭，但 P2/P4 聚合项仍保持 `partial`，因为双向
XA/binlog/GTID、提升/切换、重复投递和存储/WAL/applied-state 的其他边界仍需独立门禁。

## Continuation 1591

重跑非 Connector/J 客户端门禁并修正 Go runner 的 reconnect 检查：不再只复用
原连接执行 `Ping`，而是关闭旧 `database/sql` 句柄、重新打开连接并执行查询。
官方 MySQL 8.4.11 CLI、Go mysql driver、PyMySQL、Node mysql2 全部通过，包含
DDL/DML、预处理、事务、类型/字符集、元数据、savepoint、多结果/错误和物理重连。
Evidence:
`reports/compatibility/p3-non-connector-client-matrix-current-continuation1591.txt`。

这只关闭 P3 基础客户端与物理重连子门禁；负向/协议边界、TLS/认证插件、更多类型
和字符集、ORM 以及集群故障恢复仍使 P3 全量聚合项保持 `partial`。

## Continuation 1592

官方 MySQL -> xmysql 崩溃重连 fixture 扩展覆盖 xmysql 停机期间提交的普通事务、
one-phase XA 和两阶段 XA。三类事务均在 xmysql 重启后恢复，且 `@@GLOBAL.gtid_executed`
从源端 `...:1-14` 对应追平到 xmysql 的 `...:6-14`。Evidence:
`reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1592.txt`。

该切片关闭官方源 crash-window XA 子门禁；P2/P4 聚合项仍为 `partial`，任意重复投递、
全部存储/WAL/native-binlog/applied-marker 崩溃点、提升/故障切换拓扑和剩余双向官方
MySQL 矩阵仍需独立验证。

## Continuation 1594

官方 MySQL 反向提升 fixture 通过：官方源的普通事务、两阶段 XA、one-phase XA 已应用到
xmysql；xmysql 重启后无重复并完成 Promote；官方目标随后成功应用 xmysql 提升后的普通、
两阶段 XA 和 one-phase XA。Evidence:
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1594.txt`。

该切片关闭官方反向提升重启子门禁，但 P2/P4 聚合项仍为 `partial`，因为任意重复投递、
全部存储/WAL/applied-marker 崩溃点、完整拓扑故障切换和剩余官方双向矩阵仍未全部覆盖。

## Continuation 1595

新增 native duplicate-prefix retry 回归：副本先持久化事务前缀，重启后再次收到已持久化
前缀和提交帧，事务只应用一次；提交后的同一物理重试继续为 no-op。专项测试及
`server/replication`、`server/net` 回归均通过。Evidence:
`reports/compatibility/p2-native-duplicate-prefix-retry-current-continuation1595.txt`。

该切片关闭本地 native 重复前缀重试子门禁，但 P2/P4 聚合项仍为 `partial`，官方传输层
重复投递、全部存储/WAL/applied-marker 崩溃点和完整拓扑/官方互操作矩阵仍需验证。

## Continuation 1597

当前版本真实外部进程崩溃恢复矩阵连续 3 轮通过：已提交数据保留、未提交数据回滚、
半提交事务至多应用一次，DDL 和索引重建元数据在重启后仍可查询。Evidence:
`reports/compatibility/p2-external-crash-recovery-current-continuation1597.txt`。

该切片关闭当前外部进程 redo/undo/半提交 crash 子门禁，但 P2/P4 聚合项仍为 `partial`，
native replication/applied-marker 的全部传输边界、完整提升拓扑和官方互操作矩阵仍需验证。

## Continuation 1600

客户端集群故障切换 fixture 通过：MySQL CLI、Go mysql driver、PyMySQL、Node mysql2 均能
访问源端；副本追平 GTID 后源端停止并完成 Promote；四类客户端再次访问提升节点全部通过。
Evidence:
`reports/compatibility/p2-p3-client-cluster-failover-current-continuation1600.txt`。

该切片关闭客户端可见的集群端点故障切换子门禁，但 P3 全量客户端和 P2/P4 聚合项仍为
`partial`，负向/协议/TLS/认证插件/ORM、native 传输崩溃边界和完整官方拓扑矩阵仍需验证。

## Continuation 1602

本地 replication runtime 的 quorum/fencing 拓扑专项通过：单副本法定人数选举、重启后
fencing 状态和旧 epoch 拒绝、提升前隔离可达旧源均通过。Evidence:
`reports/compatibility/p2-local-topology-failover-current-continuation1602.txt`。

该切片关闭本地 quorum/fencing 拓扑子门禁，但 P2/P4 聚合项仍为 `partial`，多节点进程级
native 传输中断和完整官方 MySQL 拓扑矩阵仍需验证。

## Continuation 1604

扩展官方 MySQL → xmysql fixture：xmysql 保持运行时，官方源停止并重新启动；源恢复后
xmysql 自动重连并继续应用普通事务和两阶段 XA，行 1-7 均只出现一次。Evidence:
`reports/compatibility/p2-p4-official-source-transport-reconnect-current-continuation1604.txt`。

该切片关闭官方源 transport interruption/reconnect 子门禁，但 P2/P4 聚合项仍为 `partial`，
native 传输在全部 applied-marker/storage 崩溃边界和完整官方拓扑矩阵仍需验证。

## Continuation 1607

补齐 native 一次批量包含多个事务时的 source-position/applied-marker 故障窗口：在两笔
事务都完成 storage apply 和 durable applied marker 后，注入最终状态替换失败；重启后使用
同一 `ApplyNativeAtSource` 路径重试，两个事务均不重复执行，source position 只在整批状态
替换成功后发布。Evidence:
`reports/compatibility/p2-native-multi-transaction-source-position-current-continuation1607.txt`。

该切片只关闭本地多事务 native source-position 重试子门禁；所有 storage/WAL/native relay/
applied-marker 交错故障、完整多节点拓扑和官方 MySQL 矩阵仍保持 P2/P4 `partial`。

## Continuation 1606

补齐 P_S 组件型虚拟表的逐表 SELECT 权限回归：普通持久化账户访问
`component_scheduler_tasks`、`clone_status`、`keyring_keys` 时，在未授予对应表级
SELECT 前均被拒绝；逐表授权后可查询注册列，组件不存在时仍返回符合当前边界的空结果。
同时复跑 P_S/I_S、角色默认激活、mandatory role、`SET ROLE` 和角色可见性专项均通过。
Evidence: `reports/compatibility/p1-component-table-permissions-current-continuation1606.txt`。

该切片只关闭 P_S 组件表权限分发的局部门禁；Clone、Keyring、Enterprise Firewall、
component scheduler 的完整运行时生命周期仍需要权威组件或官方 Enterprise fixture，
因此 P1 聚合项仍为 `partial`。P2/P4 native XA/binlog/GTID/crash/promotion、P3 全量
非 Connector/J 边界矩阵仍保持 `partial`；FULLTEXT deferred，非 InnoDB out of scope。

## Continuation 1608

补齐本地 HTTP replication source 重启重连：replica 保持运行，source 使用同一数据目录和
replication UUID 重启后，普通事务与两阶段 XA 均继续应用，重启前事务没有重复。Evidence:
`reports/compatibility/p2-engine-source-reconnect-current-continuation1608.txt`。

该切片只关闭本地单副本 source restart/reconnect 子门禁；多节点进程级 transport 中断、所有
native relay/applied-marker/storage 崩溃交错和官方 MySQL 完整拓扑矩阵仍保持 P2/P4 `partial`。

## Continuation 1609

补齐带真实数据的三成员 quorum 自动故障切换：source 与两个 replica 先同步同一事务，source
停止后低 server-id 候选唯一晋升，另一 replica 保持从属，两个副本都只保留一条已应用记录。
Evidence: `reports/compatibility/p2-runtime-multi-replica-failover-data-current-continuation1609.txt`。

该切片只关闭本地进程内三成员 quorum/failover 数据子门禁；进程/容器级网络分区、完整 fencing
与 storage 崩溃交错、晋升后其他 replica 自动改指向，以及官方 MySQL 拓扑矩阵仍保持 P2/P4
`partial`。

## Continuation 1612

补齐非 Connector/J 客户端的负向协议错误契约：不存在表查询在服务端错误分类修复后，
Go mysql driver、PyMySQL 和 Node mysql2 均收到 MySQL `ER_NO_SUCH_TABLE`（1146，
SQLSTATE `42S02`）；mysql CLI 用例已加入同一门禁，但当前主机没有 mysql 可执行文件或
Docker CLI，因此结果记录为 `SKIPPED_ENVIRONMENT`，不伪造 PASS。Evidence:
`reports/compatibility/p3-client-negative-error-code-current-continuation1612.txt`；
`reports/compatibility/client-matrix-current-continuation1612`。

该切片只关闭 P3 负向错误码/SQLSTATE 子门禁；连接池、旧连接故障重试、TLS/认证插件、
更多类型/字符集、ORM、客户端版本边界和完整集群客户端矩阵仍保持 P3 `partial`。

## Continuation 1613

补齐 P3 多会话/连接池子门禁：Go 使用 `database/sql` 的两个并发连接，Node 使用
`mysql2` 原生 pool，PyMySQL 使用两个独立连接，三者均能同时执行查询并返回正确结果。
mysql CLI 没有连接池 API，因此不纳入该专用 case，也不将环境缺失伪装成 PASS。Evidence:
`reports/compatibility/p3-client-multi-session-pool-current-continuation1613.txt`；
`reports/compatibility/client-matrix-current-continuation1613`。

该切片只关闭 P3 多会话/连接池基础子门禁；旧连接故障重试、TLS/认证插件、更多类型/字符集、
ORM、客户端版本边界和完整集群客户端矩阵仍保持 P3 `partial`。

## Continuation 1615

补齐 P3 常见类型/元数据子门禁：跨客户端创建包含 `BIGINT`、`DECIMAL`、`DOUBLE`、`TEXT`、
`BLOB`、`TIMESTAMP` 的 InnoDB 表，并通过 `INFORMATION_SCHEMA.COLUMNS` 验证类型序列。
Go mysql driver、PyMySQL、Node mysql2 均通过；mysql CLI 因环境缺少客户端和 Docker 记录为
`SKIPPED_ENVIRONMENT`。Evidence:
`reports/compatibility/p3-client-extended-types-metadata-current-continuation1615.txt`；
`reports/compatibility/client-matrix-current-continuation1615`。

## Continuation 1624

补齐 P3 客户端真实值编码子门禁：通过 InnoDB 表实际写入并读取 `DECIMAL(10,2)`、`DOUBLE`、
`BLOB` 和 `DATE`，Go mysql driver、PyMySQL、Node.js/mysql2 均通过；mysql CLI 因环境缺少
客户端和 Docker 记录为 `SKIPPED_ENVIRONMENT`。同时修复 Go 客户端矩阵与独立重连夹具的构建
边界，避免两个 `main` 互相污染。Evidence:
`reports/compatibility/p3-client-wire-values-current-continuation1624.txt`；
`reports/compatibility/client-matrix-current-continuation1624`；
`reports/compatibility/client-restart-reconnect-current-continuation1624`。

该切片只关闭 P3 真实值编码基础子门禁；mysql CLI、TLS/认证插件、更多值/字符集、ORM、
客户端版本边界和完整集群客户端矩阵仍保持 P3 `partial`。

## Continuation 1625

补齐 P2 原生复制长连接在提升后的 source 刷新：在 `COM_REGISTER_SLAVE`、
`COM_BINLOG_DUMP` 和 `COM_BINLOG_DUMP_GTID` 到达时，从当前引擎重新取得复制 source，
不再依赖连接建立时缓存的旧指针。该切片通过网络层和 native binlog/GTID dump 回归，
但完整 native 拓扑、崩溃窗口和官方双向互操作仍保持 P2/P4 `partial`。Evidence:
`reports/compatibility/p2-native-session-source-refresh-current-continuation1625.txt`。

该切片只关闭 P2 原生复制长连接 source 刷新子门禁；完整 native 拓扑、崩溃窗口、网络分区和
官方双向互操作仍保持 P2/P4 `partial`。

## Continuation 1626

在当前机器可用 Docker 的条件下补跑官方 MySQL 8.4.11 互操作：

- 官方 MySQL 源 → xmysql 副本：xmysql 进程崩溃/重启、官方源停止/重连后，普通事务和 XA
  事务均恢复且无重复，PASS。
- 官方 MySQL 源 → xmysql 副本 → xmysql 提升 → 官方 MySQL 目标：普通事务、两阶段 XA、
  一阶段 XA 均通过 xmysql native binlog 继续复制，PASS。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1626.txt`；
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1626.txt`。

这两个专项门禁通过后，P2/P4 仍不能标记为 complete：完整双向拓扑、网络分区、更多官方
版本和全量故障窗口仍需继续验证。

同一轮 P1 回归也通过了现有权限、角色、组件生命周期和复制运行时切片：
`reports/compatibility/p1-privilege-role-component-regression-current-continuation1626.txt`。
这只证明已实现的语义切片没有回归，不改变 P1 全量 I_S/P_S 的 `partial` 状态。

## Continuation 1627

补齐 P3 客户端实测：在仓库受控免密测试配置下，mysql CLI（Docker MySQL 8.4.11）、Go
mysql driver、PyMySQL、Node.js/mysql2 均通过共享协议矩阵；随后在两节点 source/replica
拓扑中，四类客户端均通过 source 端点、replica 追平、source 停止后的 promoted 端点。

Evidence: `reports/compatibility/p3-client-matrix-dev-auth-current-continuation1627.txt`；
`reports/compatibility/p3-client-cluster-endpoint-current-continuation1627.txt`。

该结果关闭 P3 当前客户端 smoke/集群端点子门禁，但不等同于完整客户端兼容：真实密码插件、
TLS、ORM/版本边界、网络分区和全量负向矩阵仍保持 P3 `partial`。

## Continuation 1628

补齐正式 engine 配置到 replication runtime 的自动故障切换 wiring：`[replication] peers`、
`auto_failover`、`failure_timeout` 现在会被解析并传入运行时；三节点 engine 集成回归证明仅
通过配置即可在 source 失效后按 server-id 选举副本并保持另一副本为 replica。

Evidence: `reports/compatibility/p2-engine-replication-config-autofailover-current-continuation1628.txt`。

这关闭的是配置接线缺口，不改变 P2/P4 的聚合状态；native endpoint 广播、网络分区 fencing、
完整 native 拓扑与官方双向故障矩阵仍需继续验证。

## Continuation 1629

补齐 native endpoint 默认发现：具体 SQL 监听地址现在自动发布为 `mysql://host:port`，可供
晋升后副本自动重指向；`0.0.0.0`/`::` 等 wildcard 监听不会发布不可连接地址，仍需显式
配置 `native_endpoint`。相关 engine 回归通过。

Evidence: `reports/compatibility/p2-native-endpoint-advertisement-current-continuation1629.txt`。

这关闭 native endpoint 的默认发现缺口；容器/NAT/网络分区和官方完整 native 拓扑仍保持
 P2/P4 `partial`。

## Continuation 1689

补齐 native `ROTATE_EVENT` 本体的一致性门禁：每个 logical ROTATE 边界现在都会推导下一个
native 文件名，并比较完整 deterministic rotate body；checksum-valid 的轮转目标文件名
篡改会从 durable logical stream 重建。正常追加和恢复重建共用同一 rotate body 编码，
focused、replication 全包、engine replication/XA 及全仓串行回归通过。该切片只关闭一个
本地 native 轮转完整性边界，P2/P4 的统一物理提交、完整 crash-kill/拓扑和官方双向
XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-relay-rotate-event-integrity-current-continuation1689.txt`；
`reports/compatibility/full-go-regression-current-continuation1689.txt`。

## Continuation 1690

补齐 native binlog 物理帧的 short-write 检查：native 文件初始化、`ROTATE_EVENT` 和事务帧
写入统一检查 `io.Writer.Write` 返回的完整字节数，短写返回 `io.ErrShortWrite`，由现有
恢复/重建路径处理，不再静默留下截断帧。focused、replication 全包及 engine
replication/XA 回归通过；这只收口本地物理写入完整性，P2/P4 统一物理提交、完整 crash-kill/
拓扑和官方双向 XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-binlog-short-write-integrity-current-continuation1690.txt`。

## Continuation 1691

收口 pending native ROTATE 的同进程重试：当 logical ROTATE 已同步但 native `ROTATE_EVENT`
追加失败时，下一次 `Rotate()` 会从 durable logical stream 重建 native 投影并返回原事件，
不会追加第二个 logical rotation boundary。focused、rotation/restart/dump、replication 全包
及 engine replication/XA 回归通过。该切片只关闭本地 pending-ROTATE 幂等恢复边界，P2/P4
统一物理提交、完整 crash-kill/拓扑和官方双向 XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-rotate-retry-recovery-current-continuation1691.txt`。

## Continuation 1692

收口 native `binlog.index` 持久化失败后的同进程重试：当 logical ROTATE、native
`ROTATE_EVENT` 和下一个 native 文件已经落盘、但索引原子替换失败时，下一次 `Rotate()`
会比较 durable index 与 native 文件投影，修复索引并返回原事件，不会追加第二个 logical
rotation boundary 或第三个 native 文件。focused、replication 全包、engine replication/XA
及全仓串行回归通过。该切片只关闭本地 ROTATE/index 发布重试边界，P2/P4 统一物理提交、完整
crash-kill/拓扑和官方双向 XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-rotate-index-retry-recovery-current-continuation1692.txt`。

## Continuation 1693

收口 relay/import 的 durable GTID 索引重试窗口：当 logical relay 和 native binlog 已完整
落盘、但 `binlog.gtid.index` 原子发布失败时，下一次 `ImportEvents()` 会重建并持久化 GTID
索引后再确认幂等成功，不会留下 native 文件可读但 GTID 定位索引为空的状态。focused、
replication 全包、engine replication/XA 及全仓串行回归通过。该切片只关闭本地 native
relay/promotion GTID-index 发布边界，P2/P4 统一物理提交、完整 crash-kill/拓扑和官方双向
XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-gtid-index-import-retry-current-continuation1693.txt`。

## Continuation 1694

补齐 promotion/imported relay 的 native dump GTID 身份传播：晋升节点重新编码上游 relay 历史
时，native GTID_EVENT 仍携带上游 SID，而 source UUID 已切换为晋升节点身份。此前
`NativeDumpFileWithIntervals` 只在 SID 等于本机 UUID 时推进 executed set，可能造成跨文件续传
重复发送上游事务；现在按 wire SID 记录 canonical upstream UUID。focused、replication 全包、
engine replication/XA 及全仓串行回归通过。该切片只关闭 native dump 的 GTID 身份窗口，P2/P4
统一物理提交、完整 crash-kill/拓扑和官方双向 XA/binlog/GTID 互操作仍为 `partial`。

Evidence: `reports/compatibility/p2-native-dump-imported-gtid-executed-set-current-continuation1694.txt`。

## Continuation 1695

修复 native dump 从事务中间位置恢复时提前推进 executed GTID 的问题：位置跳过的 partial
transaction 现在必须先遇到完整 `XID_EVENT` 或 XA terminal query 才会写入调用方 executed
interval set。这样既保留 imported upstream SID 的身份传播，也避免后续完整重拉错误过滤未完成
事务。focused、net binlog/GTID、replication、engine replication/XA 及全仓串行回归通过；该切片
只关闭 native dump 的 partial-position GTID 窗口，P2/P4 聚合项仍为 `partial`。

Evidence: `reports/compatibility/p2-native-dump-partial-gtid-advancement-current-continuation1695.txt`。

## Continuation 1696

补齐 XA terminal 的 native dump 中间位置恢复：普通事务从 GTID_EVENT 之后开始仍视为 partial
并不推进 executed set；XA COMMIT/ROLLBACK 的 terminal query 在该位置下会被保留，因此现在
在 terminal query 到达时推进 terminal GTID。focused、net binlog/GTID、replication、engine
replication/XA 及全仓串行回归通过；该切片只关闭 native dump 的 XA terminal 位置窗口，P2/P4
聚合项仍为 `partial`。

Evidence: `reports/compatibility/p2-native-dump-xa-terminal-mid-resume-current-continuation1696.txt`。

## Continuation 1630

补齐 engine 配置级 quorum 安全门禁：配置一个不可达的第二副本后停止 source，存活副本在
超过 `failure_timeout` 后仍保持 replica，不会在网络分区时单节点自提升。该回归验证了
`conf.Cfg` 到 live replication runtime 的多数派约束。

Evidence: `reports/compatibility/p2-engine-replication-config-quorum-fencing-current-continuation1630.txt`。

这只关闭配置级“无 quorum 不晋升”子门禁；真实网络分区注入、并发 fencing race、完整 native
拓扑和官方双向 XA/binlog/GTID 矩阵仍保持 P2/P4 `partial`。

同轮串行回归 `server/conf`、`server/net`、`server/replication` 和 `server/innodb/engine`
全部通过（engine 448.330s）：`reports/compatibility/p2-p1-config-cluster-regression-current-continuation1630.txt`。

## Continuation 1631

重新执行官方 MySQL 8.4.11 互操作门禁：官方源崩溃/重连场景，以及官方源 → xmysql 重启/晋升
→ 官方目标的反向场景均 PASS，普通事务、两阶段 XA 和一阶段 XA 均完成恢复或继续复制。

Evidence: `reports/compatibility/p2-p4-official-source-crash-reconnect-current-continuation1631.txt`；
`reports/compatibility/p2-p4-official-reverse-promotion-current-continuation1631.txt`。

这些门禁刷新了当前证据，但不关闭完整双向拓扑、网络分区 fencing race、全部崩溃交错和更多
官方版本矩阵，P2/P4 仍保持 `partial`。

## Continuation 1632

补齐复制成员配置的自节点安全校验：`NewRuntime` 和运行中的 `UpdatePeers` 均拒绝与本地
replication listen endpoint 完全相同的 peer。这样不会把本节点算作远端可达成员，也不会
在自动故障转移时错误放大 quorum 分母；拒绝发生在持久化之前。专项测试和完整
`server/replication` 回归均通过。Evidence:
`reports/compatibility/p2-replication-self-peer-quorum-validation-current-continuation1632.txt`。

该切片只关闭“精确自节点 endpoint 不得进入成员集合”子门禁；host alias/NAT、真实网络分区、
并发 fencing race、完整多通道拓扑、native GTID/binlog 全部崩溃交错及官方版本广度仍保持
P2/P4 `partial`。

### Continuation 1736

原生 binlog/GTID、XA、relay 晋升和存储提交恢复边界完成一次新鲜审计。`server/replication`
全包回归通过（162.876 秒）；存储引擎复制/XA/恢复专项通过（247.133 秒）。当前局部能力
包括事务键幂等、native 投影重建、GTID/index 恢复、XA 两阶段状态恢复及稳定事务身份重试。

这不足以把 P2/P4 聚合项改为 `implemented`：跨 storage/WAL、logical/native binlog、
GTID/applied marker 和外部 topology/fencing 的统一物理提交点，以及完整官方版本/kill/
partition/双向拓扑互操作仍未完成。证据：
`reports/compatibility/p2-native-binlog-gtid-recovery-audit-current-continuation1736.txt`。

## Continuation 1633

修复晋升 fencing 的本地状态覆盖竞态：收集 peer ACK 后，如果本节点已经被更高 epoch 或
其他候选 UUID 围栏，晋升流程不再无条件清除 `fenced`，而是中止并保留围栏状态；无 peer
路径也使用同一保护。专项回归通过。Evidence:
`reports/compatibility/p2-fencing-epoch-race-guard-current-continuation1633.txt`。

这只关闭本地 fencing 状态覆盖子门禁；它不是共识协议，任意网络分区、进程暂停、同时外部
围栏、完整多节点拓扑和官方双向矩阵仍保持 P2/P4 `partial`。

## Continuation 1634

修复并发 fencing 持久化顺序：`applyFence` 现在在同一临界区内完成 epoch 校验、内存更新和
持久化替换，避免旧 epoch 的延迟写入覆盖新 epoch。并发请求后重启仍观察到最高 epoch 和
对应 owner。Evidence:
`reports/compatibility/p2-fencing-persistence-order-current-continuation1634.txt`。

该切片只关闭本地 fencing 文件回退风险；分布式共识、quorum lease、网络分区 fencing 和
完整拓扑仍保持 P2/P4 `partial`。

## Continuation 1635

补齐 fencing 持久化失败回滚：直接 fence、无 peer 晋升和带 peer 晋升在 fencing 文件写入
失败时都会恢复原有内存 epoch/owner，避免本进程与重启后的持久化状态分裂。Evidence:
`reports/compatibility/p2-fencing-persistence-rollback-current-continuation1635.txt`。

该切片只关闭本地文件写失败后的状态分裂；分布式共识、quorum lease、网络分区 fencing
和完整拓扑仍保持 P2/P4 `partial`。

## Continuation 1636

收紧 `CHANGE REPLICATION SOURCE` 的本地持久化边界：source 配置成功写入并通过 identity
清理后才发布新的内存 source；写入失败时旧 source URL 保持不变，避免当前进程和重启后的
source 不一致。Evidence:
`reports/compatibility/p2-source-config-persistence-rollback-current-continuation1636.txt`。

该切片只关闭本地 source 配置状态分裂；命名复制通道、分布式共识和完整拓扑仍保持 P2/P4
`partial`。

## Continuation 1637

补齐成员配置的一致性边界：启动加载 persisted members 时校验自节点 endpoint，`UpdatePeers`
先持久化再发布新内存 peer 集合，写失败时保留旧集合。Evidence:
`reports/compatibility/p2-members-persistence-rollback-current-continuation1637.txt`。

该切片只关闭本地 members 配置回退和自节点重载风险；host/NAT alias、分布式共识、命名通道
和完整拓扑仍保持 P2/P4 `partial`。

## Continuation 1638

补齐默认通道之外的命名复制通道第一段：运行时为每个命名通道建立隔离的 source/relay 状态
和生命周期，并持久化通道清单；SQL 层已接入 `CHANGE REPLICATION SOURCE/FILTER`、
`START/STOP/RESET REPLICA ... FOR CHANNEL` 以及 `SHOW REPLICA STATUS FOR CHANNEL`。
专项、复制包和引擎包全量回归均通过。Evidence:
`reports/compatibility/p2-named-replication-channels-current-continuation1638.txt`。

该切片只关闭“SQL/runtime 仅允许 default channel”的边界；每通道 native 协议注册与 binlog
dump 路由、独立 native endpoint 发现、多源调度、分布式 fencing、网络分区和完整 crash
矩阵仍保持 P2/P4 `partial`。

## Continuation 1639

Performance Schema 的复制连接、applier、filter 和 failover 视图现在从 default 加上所有
持久化命名通道生成独立行，并保留 `CHANNEL_NAME`；server-wide 的 `log_status` 等视图不被
重复展开。专项回归通过。Evidence:
`reports/compatibility/p1-p2-performance-schema-named-channel-views-current-continuation1639.txt`。

该切片只补齐本地命名通道的可观测性，不改变完整 P1 系统表/组件语义，也不等价于 native
endpoint 发现、分布式故障转移或官方多版本矩阵完成。

## Continuation 1640

补齐命名复制通道的 native 拉取/应用隔离：west/east 两个通道分别消费不同 native source，
并独立维护 source UUID、GTID、文件位置、applier 和 source URL；回归验证两边只应用各自事务。
MySQL 原生 COM_REGISTER_SLAVE/COM_BINLOG_DUMP 是连接级协议，因此不伪造 channel id；本地
命名 runtime 负责通道身份和状态隔离。Evidence:
`reports/compatibility/p2-named-replication-channels-current-continuation1640.txt`。

该切片只关闭本地 native 多通道拉取/应用隔离；native endpoint 发现、多节点调度、网络分区、
分布式 fencing、完整 crash-kill 矩阵和官方 XA/binlog/GTID 拓扑仍保持 P2/P4 `partial`。

## Continuation 1641

修复 native 晋升后 source repoint 的本地持久化顺序：先持久化脱敏 source URL 和 identity
清理，成功后才发布内存 native source；identity 写失败时恢复旧 source URL，避免当前进程和
重启状态分裂。Evidence:
`reports/compatibility/p2-native-source-repoint-persistence-rollback-current-continuation1641.txt`。

该切片只关闭本地 repoint 状态分裂，不等价于分布式共识、外部租约 fencing、网络分区安全或
官方多节点 native MySQL 故障转移完成。

## Continuation 1642

补齐 source/XA 的同进程幂等重试 state repair：keyed transaction、native one-phase XA、XA
PREPARE、COMMIT、ROLLBACK 在 binlog 已写入但 state 写失败后，重试会重新持久化 durable
state，并保持不重复追加。Evidence:
`reports/compatibility/p2-source-idempotent-retry-state-repair-current-continuation1642.txt`。

该切片只关闭本地 source state repair 窗口；storage、binlog writer、外部表提交的统一物理
原子性以及分布式 crash/network-partition 拓扑仍保持 P2/P4 `partial`。

## Continuation 1643

补齐客户端和复制回放事务的提交前恢复日志同步：物理存储提交前先保证 row/statement journal
落盘，提交后的 commit record/marker 屏障保持不变；同步失败时物理事务仍保持 ACTIVE，可安全
重试。复制包全量、引擎包全量和全仓 compile-only 验证通过。Evidence:
`reports/compatibility/p2-precommit-journal-sync-current-continuation1643.txt`。

该切片只关闭本地恢复日志先于物理提交的顺序窗口；storage/WAL、binlog writer、外部表、
applied marker 的统一物理原子性以及分布式 crash/network-partition 拓扑仍保持 P2/P4 `partial`。

## Continuation 1644

刷新当前外部证据：官方 MySQL 8.4.11 反向晋升、官方源 crash/reconnect（含 XA/one-phase XA）
和本地外部进程 crash-recovery 矩阵均通过；本地矩阵重复 3 次。该切片增强 P2/P4 子门禁，
但统一 storage/WAL/binlog/applied-marker 提交、完整 crash-kill 交错、分布式网络分区、全版本
和完整 XA/GTID 拓扑仍保持 `partial`。

Evidence: `reports/compatibility/p2-p4-external-crash-official-current-continuation1644.txt`。

## Continuation 1645

非 Connector/J 当前定义的 17 个客户端用例已在 loopback 开发测试配置下通过：Docker mysql
CLI、Go mysql-driver、PyMySQL、Node.js/mysql2 均通过连接认证、DDL/DML、预处理、事务、类型/字符集、
元数据、savepoint、多结果/错误、多会话池和重连等用例。认证插件/TLS、更多客户端版本/ORM、
扩展负向和网络故障、受保护认证仍未闭环，P3 聚合项保持 `partial`。

Evidence: `reports/compatibility/p3-client-full-defined-matrix-current-continuation1645.txt`。

## Continuation 1646

服务端 TLS/认证协议切片回归通过，覆盖 TLS 状态记录、`REQUIRE SSL/X509`、
`caching_sha2_password` 和 `sha256_password` 的 TLS/RSA 分支。该结果只证明服务端协议层，
不替代四类客户端的真实端到端 TLS 门禁，因此 P3 聚合项仍保持 `partial`。

Evidence: `reports/compatibility/p3-tls-auth-slice-current-continuation1646.txt`。

## Continuation 1647

补齐 MySQL 协议级 SSLRequest：服务端先发送明文握手并声明 `CLIENT_SSL`，收到 32 字节
SSLRequest 后在同一 TCP 连接上升级为 `tls.Conn`，再继续解析后续 MySQL 包。增加 `ssl`、
`ssl-cert`、`ssl-key`、`ssl-ca`、`ssl_require_client_cert` 配置解析，并通过配置、TLS 握手、
会话读循环和全仓 compile-only 回归。四类非 Connector/J 客户端的真实 TLS/受保护凭据矩阵仍
未完成，P3 全量聚合保持 `partial`。

Evidence: `reports/compatibility/p3-mysql-protocol-tls-upgrade-current-continuation1647.txt`。

同一 continuation 的真实客户端 TLS 子门禁也已通过：Go mysql-driver、PyMySQL、Node.js/mysql2
和 Docker mysql:8.4.11 的 mysql CLI 均使用 CA 校验连接 TLS 服务，并通过当前定义用例。更多
客户端版本、ORM、认证插件、网络故障和集群客户端拓扑仍未闭环，P3 聚合项保持 `partial`。

Evidence: `reports/compatibility/p3-client-tls-e2e-current-continuation1647.txt`。

随后以 `dev_bypass_password_auth=false` 和受保护 root 凭据重跑，四类客户端的 TLS/认证矩阵
也全部通过；当前定义的非 Connector/J 协议、TLS、受保护认证子门禁已关闭。更多客户端版本、
ORM、认证插件、网络故障和集群客户端拓扑仍未闭环，P3 聚合项保持 `partial`。

Evidence: `reports/compatibility/p3-client-tls-protected-e2e-current-continuation1647.txt`。

## Continuation 1648

补齐 `caching_sha2_password` 快速认证协议：服务端在验证 32 字节 fast response 后先发送
AuthenticationMoreData `0x01 0x03`，再发送最终 OK；服务端定向回归、TLS 最小探针、Go
mysql-driver、PyMySQL 和 Docker mysql CLI 认证插件路径通过。Node mysql2 在同一 AuthSwitch
场景仍 `connect ETIMEDOUT`，所以 P3 全量非 Connector/J 矩阵仍为 `partial`，Node 专项和更广
版本/ORM、负向/网络故障边界继续开放。

Evidence: `reports/compatibility/p3-client-auth-plugin-matrix-current-continuation1648.txt`。

## Continuation 1620

补齐 P3 旧连接故障与重连子门禁：每个客户端先保持一个真实 TCP/物理连接，控制器停止并
重新启动同一 xmysql 进程和数据目录；旧连接按预期失败，随后 Go mysql driver、PyMySQL、
Node.js/mysql2 均重新建立连接并成功执行 `SELECT 1`。Evidence:
`reports/compatibility/p3-client-restart-reconnect-current-continuation1620.txt`；
`reports/compatibility/client-restart-reconnect-current-continuation1620`。

该切片只关闭 P3 旧连接故障/重连基础子门禁；mysql CLI 当前仍因环境缺少客户端和 Docker
而未验证，TLS/认证插件、更多值/字符集、ORM、客户端版本边界和完整集群客户端矩阵仍保持
P3 `partial`。

## Continuation 1621

补齐 P2 晋升后的 HTTP 副本自动重指向：低 server-id 副本晋升后，其他存活副本通过控制面
发现新的 source，自动更新并持久化 source URL；随后从新 source 追加的事务能够继续应用且
不重复。Evidence: `reports/compatibility/p2-promotion-auto-repoint-current-continuation1621.txt`。

该切片只关闭现有 HTTP 复制拓扑的晋升后自动重指向子门禁；native MySQL binlog 仍需要可
发现的晋升后 MySQL binlog endpoint，完整 native/官方拓扑、崩溃窗口和双向互操作仍保持
P2/P4 `partial`。

## Continuation 1737

补齐一个 Performance Schema statement instrument 细节：注册 `statement/sql/error`，并让
SQL parser failure 在 executor/worker 指标路径和全局 statement summary 中使用该专用类型；
普通执行错误仍保留其 statement 类型。先红后绿的解析错误专项、聚焦 Performance Schema
instrument 回归及 metrics 全包通过。Evidence:
`reports/compatibility/p1-performance-schema-parse-error-instrument-current-continuation1737.txt`。

该切片不改变全局矩阵计数，也不代表完整 I_S/P_S 表列、运行时统计、锁/等待/线程生命周期、
组件/插件生命周期或权限语义完成；P1 聚合项仍为 `partial`。

## Continuation 1738

补齐六个预处理协议 command instrument：`statement/com/Prepare`、`Execute`、`Close stmt`、
`Reset stmt`、`Long Data` 和 `Fetch`；事件汇总保留官方 `statement/com/*` 名称，状态变量
仍投影到 `Com_stmt_*`。同时接入 command instrument 的 enabled/timed 设置，关闭 instrument
时不再生成事件但保留全局命令计数。Evidence:
`reports/compatibility/p1-performance-schema-command-instruments-current-continuation1738.txt`。

该切片不改变矩阵计数，也不代表其它 COM command、完整命令生命周期或 P1 全量 I_S/P_S
语义完成；P1 聚合项仍为 `partial`。

## Continuation 1739

补齐普通协议命令的 `statement/com/*` registry 与 handler 分类，并接入协议级事件记录；
覆盖 Ping、Init DB、Field List、Refresh、Statistics、Processlist、Kill、Debug、Time、
Change user、Binlog Dump、Table Dump、Connect、Connect Out、Register Slave、Set option、
Daemon、Sleep、Quit 和 Error。COM_QUERY 与预处理命令仍保留各自专用执行/统计路径，避免
重复计数。Evidence:
`reports/compatibility/p1-performance-schema-common-command-instruments-current-continuation1739.txt`。

该切片不改变全局矩阵计数，也不代表 `statement/com/Query` 父级生命周期、完整 command
history/current 语义、全量 I_S/P_S 表字段或 P1 聚合完成；P1 聚合项仍为 `partial`。

## Continuation 1740

补齐 COM_QUERY 在 `events_statements_current` 中的初始命令 instrument：协议 handler 将
`statement/com/Query` 上下文传给 active statement，executor 再按最终解析结果写入
`statement/sql/*` 或 `statement/sql/error`，不生成重复的完成事件。Evidence:
`reports/compatibility/p1-performance-schema-com-query-current-instrument-current-continuation1740.txt`。

该切片不改变全局矩阵计数，也不代表同一物理事件的完整可变生命周期、完整 command
history/current 语义、全量 I_S/P_S 表字段或 P1 聚合完成；P1 聚合项仍为 `partial`。

## Continuation 1741

修正合法 `COM_RESET_CONNECTION` 的 command instrument，注册并路由到
`statement/com/Reset connection`，不再错误使用 `statement/com/Error`。Evidence:
`reports/compatibility/p1-performance-schema-reset-connection-command-current-continuation1741.txt`。

该切片不改变全局矩阵计数，也不代表剩余命令矩阵、完整 command lifecycle/current-history
语义、全量 I_S/P_S 表字段或 P1 聚合完成；P1 聚合项仍为 `partial`。

## Continuation 1742

补齐 Performance Schema `setup_actors/setup_objects` 容量变量和默认容量限制：注册
`performance_schema_setup_actors_size`、`performance_schema_setup_objects_size`，默认值为
`-1` autosizing，并在有效默认容量 100 行时拒绝溢出插入。Evidence:
`reports/compatibility/p1-performance-schema-setup-capacity-current-continuation1742.txt`。

该切片不改变全局矩阵计数，也不代表完整 I_S/P_S 表、组件、运行时和权限语义完成；
P1 聚合项仍为 `partial`。

## Continuation 1743

补齐 Performance Schema `performance_schema_show_processlist` 动态变量：默认 `OFF`，
`SET GLOBAL` 后规范化为 `ON/OFF`，开启时 `SHOW PROCESSLIST` 使用 Performance Schema
processlist 数据源并保持传统返回形状。Evidence:
`reports/compatibility/p1-performance-schema-show-processlist-current-continuation1743.txt`。

该切片不改变全局矩阵计数，也不代表完整 I_S/P_S 表字段、运行时统计、锁/等待/线程、
组件/插件生命周期或权限语义完成；P1 聚合项仍为 `partial`。

## Continuation 1744

补齐 Performance Schema 连接摘要容量：注册并加载启动变量
`performance_schema_accounts_size`、`performance_schema_hosts_size`、
`performance_schema_users_size`，对 `accounts/hosts/users` 及对应 `status_by_*` 表实现
0 禁用、正值限行、-1 autosizing。Evidence:
`reports/compatibility/p1-performance-schema-connection-summary-capacity-current-continuation1744.txt`。

该切片不改变全局矩阵计数，也不代表完整 I_S/P_S 运行时、组件/插件生命周期、线程/锁/等待
和权限语义完成；P1 聚合项仍为 `partial`。

## Continuation 1745

补齐 Performance Schema `performance_schema_digests_size` 启动变量及 digest summary 容量
语义：0 禁用、正值限行、-1 autosizing，并在过滤后限行。Evidence:
`reports/compatibility/p1-performance-schema-digest-capacity-current-continuation1745.txt`。

该切片不改变全局矩阵计数，也不代表 digest 算法完全一致、lost 计数器或完整 I_S/P_S
运行时、组件和权限语义完成；P1 聚合项仍为 `partial`。

## Continuation 1746

补齐 `performance_schema_max_sql_text_length` 启动变量及 statement event `SQL_TEXT`、
digest `QUERY_SAMPLE_TEXT` 的字节长度限制，默认 1024，配置范围 0..1048576。Evidence:
`reports/compatibility/p1-performance-schema-sql-text-length-current-continuation1746.txt`。

该切片不改变全局矩阵计数，也不代表完整 digest 算法、其它 P_S 容量变量、I_S/P_S 运行时
和权限语义完成；P1 聚合项仍为 `partial`。

## Continuation 1747

补齐 `performance_schema_error_size` 只读启动变量与 `[performance_schema] error_size` 配置，
支持 0 禁用和正值容量限制，并通过错误摘要专项、完整 P_S 族、manager、net、metrics 回归。
该项不等于全量错误码目录或完整 error log 兼容；全量 I_S/P_S 运行时、组件/权限语义仍纳入
全局 P1 partial 任务。

## Continuation 1748

补齐 `performance_schema_max_metadata_locks` 启动变量与 `[performance_schema] max_metadata_locks`
配置，支持 autosizing、0 禁用和正值限行；metadata lock owner/waiter 专项通过。lost 计数器、
完整 instrument allocation 及全量 I_S/P_S 运行时语义仍纳入全局 P1 partial 任务。

## Continuation 1750

补齐 `performance_schema_session_connect_attrs_size` 启动变量和配置加载，并实现两个连接属性
表的 per-session 值字节限制；完整套件全部通过。`session_connect_attrs_lost`、`_truncated`、
已补充 `Performance_schema_session_connect_attrs_lost` 状态计数；`_truncated`、精确协议字节
语义和 error-log 副作用仍纳入全局 P1 partial 任务。

## Continuation 1751

补齐 `performance_schema_max_prepared_statements_instances` 启动变量与配置加载，实现预处理
语句实例表的 0 禁用和正值限行；预处理语句 lost 计数、精确 autosizing 与全量运行时语义仍
纳入全局 P1 partial 任务。

## Continuation 1752

补齐 `performance_schema_max_program_instances` 启动变量与配置加载，实现程序汇总表的 0
禁用和正值限行；专项、完整 P_S、manager、`server/net`、metrics 回归通过。Program instrument
allocation/lost 语义以及全量 I_S/P_S 运行时、组件、锁/等待/线程和权限语义仍纳入全局 P1
partial 任务。

## Continuation 1753

补齐 `Performance_schema_prepared_statements_lost` 状态投影和容量溢出计数，重复读取同一
溢出库存不会重复累加；prepared 专项、完整 P_S、manager、`server/net`、metrics 回归通过。
精确分配时机、`max_prepared_stmt_count` 自动扩容及全量 I_S/P_S 运行时语义仍纳入 P1 partial。

## Continuation 1754

补齐 `performance_schema_max_thread_instances` 启动变量、`threads` 容量限制和
`Performance_schema_thread_instances_lost` 状态计数；线程专项、完整 P_S、manager、
`server/net`、metrics 回归通过。精确分配时机、自动扩容和全量 I_S/P_S 运行时语义仍纳入
P1 partial。

## Continuation 1759

补齐 `performance_schema_max_index_stat` 启动变量、index-usage summary 容量限制和
`Performance_schema_index_stat_lost` 状态计数；index-stat 专项、完整 P_S、manager、
`server/net`、metrics 回归通过。精确分配时机、真实索引身份和自动扩容，以及全量 I_S/P_S
运行时语义仍纳入 P1 partial。非 InnoDB 引擎及非 InnoDB 修复/转换不纳入范围。

## Continuation 1760

补齐 `performance_schema_max_digest_length` 启动变量及 digest 聚合、直方图、statement-event
文本的统一字节限制；digest 专项、完整 P_S、manager、`server/net`、metrics 回归通过。精确
tokenizer/hash 及全量 I_S/P_S 运行时语义仍纳入 P1 partial。

## Continuation 1761

补齐 MySQL 8.4 Performance Schema 剩余容量变量和官方 lost-status 名称，并实现
`Performance_schema_digest_lost` 的 digest 容量溢出计数；专项、完整 P_S、manager、
`server/net`、metrics 回归通过。真实 instrument allocator/lifecycle、digest age-based
resampling 及全量 I_S/P_S 运行时语义仍纳入 P1 partial。

## Continuation 1758

补齐 `performance_schema_max_table_lock_stat` 启动变量、table lock summary 容量限制和
`Performance_schema_table_lock_stat_lost` 状态计数；table-lock 专项、完整 P_S、manager、
`server/net`、metrics 回归通过。精确分配时机、自动扩容和全量 I_S/P_S 运行时语义仍纳入
P1 partial。

## Continuation 1757

补齐 `Performance_schema_program_lost` 状态投影和程序汇总容量溢出计数，重复读取不会重复累加；
program 专项、完整 P_S、manager、`server/net`、metrics 回归通过。精确分配时机、自动扩容和
全量 I_S/P_S 运行时语义仍纳入 P1 partial。

## Continuation 1756

补齐 `performance_schema_max_socket_instances` 启动变量、`socket_instances` 容量限制和
`Performance_schema_socket_instances_lost` 状态计数；socket 专项、完整 P_S、manager、
`server/net`、metrics 回归通过。精确分配时机、自动扩容和全量 I_S/P_S 运行时语义仍纳入
P1 partial。

## Continuation 1755

补齐 `performance_schema_max_file_instances` 启动变量、`file_instances` 容量限制和
`Performance_schema_file_instances_lost` 状态计数；file 专项、完整 P_S、manager、
`server/net`、metrics 回归通过。精确分配时机、自动扩容和全量 I_S/P_S 运行时语义仍纳入
P1 partial。

## Continuation 1749

补齐 `performance_schema_max_table_handles` 启动变量与 `[performance_schema] max_table_handles`
配置，支持 autosizing、0 禁用和正值限行；显式锁及隐式事务 table handle 专项通过。lost 计数器、
完整 instrument allocation 及全量 I_S/P_S 运行时语义仍纳入全局 P1 partial 任务。

## Continuation 1766

补齐 `performance_schema_max_statement_classes` 的 statement instrument 注册容量、运行时可用性和
`Performance_schema_statement_classes_lost` 实际计数；statement-class 专项、完整 P_S、manager、
`server/net`、metrics 回归通过。其余 I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、
锁/等待/线程、组件/权限语义仍纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/
崩溃恢复/提升仍为 P2/P4，非 InnoDB 仍不纳入范围。

## Continuation 1767

补齐当前已暴露的 stage/file/socket/memory instrument class 注册容量、运行时可用性和四个
`*_classes_lost` 状态计数；专项、完整 P_S、manager、`server/net`、metrics 回归通过。其余
I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、组件/权限语义仍
纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，
非 InnoDB 仍不纳入范围。

## Continuation 1772

补齐 `performance_schema_max_file_handles` 和 `Performance_schema_file_handles_lost`：使用真实
文件 recorder 的 active open count 统计容量溢出，专项、完整 P_S、manager、`server/net`、
metrics 回归通过。其余 I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、
锁/等待/线程、组件/权限语义仍纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，
XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，非 InnoDB 仍不纳入范围。

## Continuation 1773

补齐真实 trigger stack 路径上的 `performance_schema_max_statement_stack` 溢出计数和
`Performance_schema_nested_statement_lost` 投影；同时修复完整 P_S 回归发现的官方 rwlock
lost 状态行缺失。专项、完整 P_S、manager、`server/net`、metrics 回归通过。其余 I_S/P_S
全量语义、allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、组件/权限语义仍纳入
P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，
非 InnoDB 仍不纳入范围。

## Continuation 1774

补齐 `performance_schema_max_table_instances` 在现有 table-I/O recorder 上的真实容量投影，
并实现 `Performance_schema_table_instances_lost` 累计计数；table/index I/O 汇总遵守该容量。
专项、完整 P_S、manager、`server/net`、metrics 回归通过。其余 I_S/P_S 全量语义、
allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、组件/权限语义仍纳入 P1 partial；
非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，非 InnoDB 仍不纳入范围。

## Continuation 1771

补齐 `Performance_schema_table_handles_lost` 的真实 table handle 淘汰计数，并保持显式锁、隐式
事务 lease 与容量为 0 的空结果语义；专项、完整 P_S、manager、`server/net`、metrics 回归通过。
其余 I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、组件/权限语义
仍纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，
非 InnoDB 仍不纳入范围。

## Continuation 1775

补齐 InnoDB SELECT 执行器到 RuntimeRecorder 的真实物理索引身份传递，并在
`table_io_waits_summary_by_index_usage` 中按索引身份分行记账；table scan 和无权威索引身份
仍保留空索引行。索引身份与受影响 P_S/table-I/O 回归通过。其余 I_S/P_S 全量语义、完整
index-stat 选择/连接分配、运行时统计、锁/等待/线程、组件/权限语义仍纳入 P1 partial；非
Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，非 InnoDB 仍不纳入
范围。

## Continuation 1776

补齐 Performance Schema `setup_actors`/`setup_objects` 的 TRUNCATE 表级 DROP 权限检查，保持
`setup_consumers`、`setup_instruments`、`setup_threads` 的官方拒绝边界。新权限专项和完整 P_S
回归通过。其余 I_S/P_S 运行时、组件/权限/角色全量语义仍为 P1 partial；非 Connector/J 客户端
矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，Fulltext 延后，非 InnoDB 不纳入范围。

## Continuation 1777

Node.js/mysql2 的 `caching_sha2_password` / `sha256_password` TLS 子门禁在刷新有效临时证书后通过，
此前失败被确认为过期测试证书而非协议失败。P3 全量非 Connector/J 矩阵仍保持 `partial`，
其余客户端版本、ORM/连接池、协议负例、集群拓扑和长期故障场景继续纳入全局任务；P1/P2/P4、
Fulltext 与非 InnoDB 范围不变。Evidence:
`reports/compatibility/p3-client-node-auth-rerun-current-continuation1777.txt`。

## Continuation 1778

补齐网络层 `COM_QUERY` 的 Performance Schema 父级命令 instrument 完成态，完成后保留
`statement/com/Query`，并与引擎的 `statement/sql/*` 事件分开记账。专项 `server/net` 通过；
受影响包级回归中 `server/net` 与 metrics 通过，engine 包在 45 分钟测试上限超时，故不宣称
全包通过。P1 全量 I_S/P_S、组件/权限/角色语义仍为 partial；P3 非 Connector/J 全矩阵、
P2/P4 XA/binlog/GTID/恢复/提升仍未完成；Fulltext 延后，非 InnoDB 不纳入范围。Evidence:
`reports/compatibility/p1-performance-schema-com-query-command-lifecycle-current-continuation1778.txt`。

## Continuation 1779

`server/replication` 全量回归通过（163.789s），覆盖 xmysql-native binlog、GTID、XA、relay/restart、
promotion/fencing、rotation、purge 与 exactly-once 状态转换。官方 MySQL fixture 选择性测试共 6 项，
因当前环境缺少 `XMYSQL_OFFICIAL_BINLOG_URL` / `XMYSQL_OFFICIAL_BINLOG_DSN` 全部跳过；因此官方
XA/binlog/GTID/崩溃恢复/提升互操作继续保持 P2/P4 partial，不能宣称通过。Evidence:
`reports/compatibility/p2-replication-regression-official-fixture-audit-current-continuation1779.txt`。

## Continuation 1780

P1 I_S/P_S、权限和角色定向回归通过：`server/innodb/engine` 退出码 0，用时 1669.007s。
这刷新了当前已实现切片的证据，但完整 I_S/P_S 组件运行时来源、字段权威值、锁/等待/线程精度
及全部权限/角色生命周期仍保持 partial；P2/P4 官方互操作、P3 全量非 Connector/J 矩阵、
Fulltext 与非 InnoDB 边界不变。Evidence:
`reports/compatibility/p1-information-performance-regression-current-continuation1780.txt`。

## Continuation 1770

补齐 file/socket/memory instrument class capacity 到 instances/summary 运行时投影：容量为 0 时
对应查询不返回未注册类行，专项、完整 P_S、manager、`server/net`、metrics 回归通过。其余
I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、组件/权限语义仍
纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，
非 InnoDB 仍不纳入范围。

## Continuation 1768

补齐 `performance_schema_max_thread_classes` 的 `setup_threads` 注册容量、前台/后台线程 class
运行时可用性和 `Performance_schema_thread_classes_lost` 实际计数；专项、完整 P_S、manager、
`server/net`、metrics 回归通过。其余 I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时
统计、锁/等待/线程、组件/权限语义仍纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，
XA/binlog/GTID/崩溃恢复/提升仍为 P2/P4，非 InnoDB 仍不纳入范围。

## Continuation 1769

补齐 telemetry registry 的 meter/metric class 容量和 `Performance_schema_meter_lost`、
`Performance_schema_metric_lost` 实际计数；专项、完整 P_S、manager、`server/net`、metrics
回归通过。其余 I_S/P_S 全量语义、allocator/lifecycle/lost 家族、运行时统计、锁/等待/线程、
组件/权限语义仍纳入 P1 partial；非 Connector/J 客户端矩阵仍为 P3，XA/binlog/GTID/崩溃恢复/
提升仍为 P2/P4，非 InnoDB 仍不纳入范围。

## Continuation 1804

补齐 `ROLES_GRAPHML()` 的角色可见性边界：普通账户返回空 GraphML，`ROLE_ADMIN` 会话可读取
持久化账户节点和直接角色边；Dispatcher 会保留权威引擎准备的 GraphML 会话值。引擎、plan、
dispatcher 回归通过。完整角色/权限视图、全量 I_S/P_S、P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-roles-graphml-role-admin-boundary-current-continuation1804.txt`。

## Continuation 1805

统一 Dispatcher 与引擎入口的 `CURRENT_ROLE()` 限定账号格式：默认变量值输出反引号账号，
关闭 `sql_quote_show_create` 后保留裸账号格式；失败优先测试和 Dispatcher 全包回归通过。
完整角色/权限视图、I_S/P_S、P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-current-role-dispatcher-format-current-continuation1805.txt`。

## Continuation 1806

补齐角色元数据视图对 `USER`、`HOST`、`GRANTEE_HOST` 和 `DEFAULT_ROLE` 的等值/LIKE 过滤；
错误账号、主机和默认角色条件不再被忽略。失败优先专项及角色相关回归通过（217.132s）；
engine 全包回归运行超过十分钟未产生退出码，已中止且不记录为 PASS。完整角色/权限视图、
I_S/P_S、P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-role-metadata-account-filters-current-continuation1806.txt`。

## Continuation 1807

为 `performance_schema.mutex_instances` 接入真实 owner sidecar：`active_query` 和账户变更
路径在已知连接线程号时投影 `LOCKED_BY_THREAD_ID`；其他无法从 Go 原语可靠推导 owner 的
mutex 仍返回 `NULL`。失败优先专项及同步实例、账户、KILL QUERY 回归通过（37.407s）。
完整 I_S/P_S、等待/线程生命周期、组件/权限语义和 P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-mutex-owner-current-continuation1807.txt`。

## Continuation 1808

补齐 `performance_schema.host_cache` 的时间字段边界：认证失败记录已有权威的最早/最晚
观测时间，现在同时投影到 `FIRST_SEEN/LAST_SEEN`；没有时间源的活动 host 仍返回 `NULL`，
不伪造连接生命周期。失败优先专项及相关 host-cache 回归通过（9.300s）。完整 I_S/P_S、
组件/权限语义和 P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-host-cache-seen-times-current-continuation1808.txt`。

## Continuation 1809

补齐 `performance_schema.setup_actors` 的 `ROLE` 运行时匹配：规则现在同时按 `HOST`、`USER`
和会话 `active_roles` 选择，`ROLE='%'` 继续匹配无激活角色的会话，具体角色规则只作用于
对应激活角色，并保留具体规则优先级。失败优先测试先复现无角色会话错误继承具体角色规则，
随后专项、setup_actors/setup_threads 相关回归及完整 `TestPerformanceSchema` 族通过（923.700s）。
完整 I_S/P_S、组件/权限语义和 P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-setup-actors-active-role-current-continuation1809.txt`。

## Continuation 1810

继续收口 `performance_schema.setup_actors` 的角色来源：会话没有显式 `active_roles` 参数时，
现在复用 `ENABLED_ROLES` 的权威来源，使用账户持久化的 `DefaultRoles` 并合并 mandatory roles；
显式空切片仍表示 `SET ROLE NONE`，不会被默认角色覆盖。失败优先测试先复现默认角色会话错误
落入通配规则，随后专项、相关 setup/ENABLED_ROLES 回归及完整 `TestPerformanceSchema` 族
通过（915.818s）。完整 I_S/P_S、组件/权限语义和 P2/P3/P4 聚合仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-setup-actors-default-role-current-continuation1810.txt`。

## Continuation 1811

- [x] `INFORMATION_SCHEMA.EVENTS.STATUS` 反映事件对象的持久化禁用状态，
  `ALTER EVENT ... DISABLE/ENABLE` 的查询结果分别为 `DISABLED`/`ENABLED`。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-disabled-status-current-continuation1811.txt`。

## Continuation 1812

- [x] `INFORMATION_SCHEMA.EVENTS.STARTS/ENDS` 反映事件定义中的调度起止边界，并与
  `SHOW EVENTS` 保持一致。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-start-end-current-continuation1812.txt`。

## Continuation 1813

- [x] `INFORMATION_SCHEMA.EVENTS.ON_COMPLETION` 反映显式 `PRESERVE` 策略，默认策略仍为
  `NOT PRESERVE`。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-completion-policy-current-continuation1813.txt`。

## Continuation 1814

- [x] `INFORMATION_SCHEMA.EVENTS.DEFINER` 与 `EVENT_COMMENT` 复用持久化事件对象状态，
  支持自定义 DEFINER 和 COMMENT。
- [x] 失败优先专项和事件相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-definer-comment-current-continuation1814.txt`。

## Continuation 1815

- [x] `INFORMATION_SCHEMA.EVENTS.CREATED/LAST_ALTERED` 反映持久化创建及修改时间，
  覆盖旧对象回退和 ALTER/rename 更新。
- [x] 失败优先专项和事件相关回归通过。
- [ ] `LAST_EXECUTED` 等运行时字段、完整 I_S/P_S 字段精度、组件/权限生命周期、
  非 Connector/J 客户端矩阵以及 P2/P4 官方复制互操作仍保持未完成。

Evidence: `reports/compatibility/p1-information-schema-events-timestamps-current-continuation1815.txt`。

## Continuation 1816

- [x] `INFORMATION_SCHEMA.EVENTS.LAST_EXECUTED` 由真实 SQL EVENT scheduler 执行回调持久化，
  并以 DATETIME 形状可查询。
- [x] 失败优先专项及完整事件调度相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-last-executed-current-continuation1816.txt`。

## Continuation 1817

- [x] `INFORMATION_SCHEMA.ROUTINES.CREATED/LAST_ALTERED` 复用持久化创建及修改时间，
  覆盖过程/函数 ALTER 路径和旧对象回退。
- [x] 失败优先专项及 routine 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-routines-timestamps-current-continuation1817.txt`。

## Continuation 1818

- [x] 存储对象创建会话的 `sql_mode/time_zone/character_set_client/collation_connection`
  已持久化并投影到相关 I_S/SHOW 路径，覆盖过程、触发器和事件。
- [x] 失败优先专项及共享事件/routine/trigger 回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-stored-object-session-metadata-current-continuation1818.txt`。

## Continuation 1819

- [x] `INFORMATION_SCHEMA.EVENTS.ORIGINATOR` 持久化并投影事件创建者的 `server_id`；旧事件
  元数据使用当前实例 `server_id` 兼容回退。
- [x] 失败优先专项和事件/routine/trigger 共享回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-originator-current-continuation1819.txt`。

## Continuation 1820

- [x] `INFORMATION_SCHEMA.ROUTINES.ROUTINE_DEFINITION` 投影 routine body；完整 CREATE 语句仍由
  `SHOW CREATE` 提供。
- [x] 失败优先专项和 routine 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-routines-definition-current-continuation1820.txt`。

## Continuation 1821

- [x] `INFORMATION_SCHEMA.TRIGGERS.ACTION_ORDER` 复用 FOLLOWS/PRECEDES 的实际执行顺序，投影
  一基序号；`ACTION_STATEMENT` 不再包含顺序前缀。
- [x] 失败优先专项和触发器/存储对象回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-triggers-action-order-current-continuation1821.txt`。

## Continuation 1822

- [x] 视图创建会话的 `character_set_client/collation_connection` 已持久化，并投影到
  `INFORMATION_SCHEMA.VIEWS` 与 `SHOW CREATE VIEW`；旧元数据保留回退值。
- [x] 视图专项及相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-views-session-metadata-current-continuation1822.txt`。

## Continuation 1823

- [x] `INFORMATION_SCHEMA.VIEWS.IS_UPDATABLE` 对可证明的单表直接列投影视图返回 `YES`，对聚合、
  连接、分组、集合和子查询等复杂视图返回 `NO`。
- [x] 失败优先专项和 view 相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-views-updatability-current-continuation1823.txt`。

## Continuation 1824

- [x] `INFORMATION_SCHEMA.EVENTS.EVENT_DEFINITION` 返回 `DO` 后的事件体；完整 CREATE 语句继续由
  `SHOW CREATE EVENT` 提供。
- [x] 失败优先专项和严格事件回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 客户端矩阵以及
  P2/P4 官方复制互操作仍保持未完成；相关聚合项继续标记为 `partial`。

Evidence: `reports/compatibility/p1-information-schema-events-definition-current-continuation1824.txt`。

## Continuation 1825

- [x] 本地 XA、native binlog、GTID、恢复、promotion 和 fencing 专项回归通过（102.199 秒）。
- [x] 官方 MySQL 集成测试的跳过原因已确认：`XMYSQL_OFFICIAL_BINLOG_URL`、
  `XMYSQL_OFFICIAL_BINLOG_DSN` 未配置，且当前 Docker 没有可用 server 端响应。
- [ ] 官方 MySQL XA/binlog/GTID、崩溃恢复和提升互操作仍未验证；P2/P4 聚合项继续保持
  `partial`，不能以本地回归替代官方拓扑证据。

Evidence: `reports/compatibility/p2-native-replication-local-regression-current-continuation1825.txt`。

## Continuation 1826

- [x] `INFORMATION_SCHEMA.PARAMETERS` 函数返回值行的 native `PARAMETER_NAME` 已修正为
  `NULL`；JDBC `COLUMN_NAME=RETURN_VALUE` 兼容投影保持不变。
- [x] 失败优先专项以及参数/例程相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期、非 Connector/J 矩阵和 P2/P4 官方互操作
  仍未完成；相关聚合项继续保持 `partial`。

Evidence: `reports/compatibility/p1-information-schema-parameters-return-name-current-continuation1826.txt`。

## Continuation 1827

- [x] native `INFORMATION_SCHEMA.PARAMETERS` 支持 `ORDINAL_POSITION`、`PARAMETER_NAME`、
  `PARAMETER_MODE` 过滤，函数返回值筛选不再包含输入参数。
- [x] 失败优先专项以及参数/例程回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期和其他 P1/P2/P3/P4 聚合项仍未完成。

Evidence: `reports/compatibility/p1-information-schema-parameters-filters-current-continuation1827.txt`。

## Continuation 1828

- [x] 收紧 Performance Schema setup 表的可更新列集合，非法 `TIMED/HISTORY` 更新现在被拒绝。
- [x] 失败优先专项和全部 `TestPerformanceSchemaSetup*` 回归通过。
- [ ] 完整 P_S 组件生命周期、字段精度和权限聚合仍未完成，聚合状态保持 `partial`。

Evidence: `reports/compatibility/p1-performance-schema-setup-column-semantics-current-continuation1828.txt`。

## Continuation 1829

- [x] native `INFORMATION_SCHEMA.PARAMETERS` 支持 `IS NULL`/`IS NOT NULL` 过滤，正确区分函数返回值行
  与普通参数的 NULL/非 NULL 名称和模式。
- [x] 失败优先专项以及参数/例程/存储对象相关回归通过。
- [ ] 完整 I_S/P_S 字段精度、组件/权限生命周期和其他 P1/P2/P3/P4 聚合项仍未完成。

Evidence: `reports/compatibility/p1-information-schema-parameters-null-predicates-current-continuation1829.txt`。

### Continuation 1830

- [x] XA 业务路径和 XA 持久化恢复路径统一使用 owner-aware mutex 封装。
- [x] XA PREPARE 持锁期间，`performance_schema.mutex_instances.LOCKED_BY_THREAD_ID`
  可观察到实际会话线程 ID；失败优先、XA 和同步实例回归通过。
- [ ] 完整 P_S 锁/等待/线程生命周期、字段精度、组件/权限聚合和 P2/P3/P4 互操作仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-xa-mutex-owner-current-continuation1830.txt`。

### Continuation 1831

- [x] INFORMATION_SCHEMA 四类权限视图支持 `IS NULL` / `IS NOT NULL` 谓词，按实际投影值的 NULL
  状态进行过滤。
- [x] 失败优先权限视图专项和权限视图回归通过。
- [ ] 完整角色/权限图、I_S/P_S 字段精度和运行时语义、P2/P3/P4 互操作仍未完成。

Evidence: `reports/compatibility/p1-information-schema-privilege-null-predicates-current-continuation1831.txt`。

### Continuation 1832

- [x] executor-owned mutex 阻塞等待进入 `events_waits_current`，完成后进入短/长历史视图。
- [x] mutex 等待按稳定 `OBJECT_INSTANCE_BEGIN` 进入 `events_waits_summary_by_instance`。
- [x] `events_waits_summary_by_instance` 的 TRUNCATE 会清除 mutex 历史贡献并保留零计数行。
- [x] XA mutex owner、同步实例和等待视图相关回归通过。
- [ ] 完整 I_S/P_S 字段、组件/权限/线程生命周期以及 P2/P3/P4 互操作仍未完成。

Evidence: `reports/compatibility/p1-performance-schema-mutex-wait-lifecycle-current-continuation1832.txt`。

### Continuation 1833

- [x] global/thread wait summaries now apply mutex-history reset cutoffs independently.
- [x] thread summary dispatch carries the `thread` dimension, and thread truncate preserves the zero row without clearing global summary.
- [ ] Complete I_S/P_S fields, components, permissions, threads and P2/P3/P4 interoperability remain unfinished.

Evidence: `reports/compatibility/p1-performance-schema-mutex-summary-dimension-reset-current-continuation1833.txt`。

### Continuation 1834

- [x] executor-owned mutex waits are verified in account summary with session user/host identity.
- [x] account summary truncate is isolated from the global mutex summary; host/user share the same dimension path.
- [ ] Complete I_S/P_S, component/permission/thread semantics and P2/P3/P4 interoperability remain unfinished.

Evidence: `reports/compatibility/p1-performance-schema-mutex-account-summary-current-continuation1834.txt`。

### Continuation 1835

- [x] Native binlog/GTID/XA/recovery/promotion local regression passed.
- [x] Official MySQL integration tests were explicitly audited and skipped only because the required fixture variables are unset.
- [ ] Official MySQL XA/binlog/GTID/crash/promotion interoperability remains partial until a reachable reproducible fixture exists.

Evidence: `reports/compatibility/p2-native-local-and-official-fixture-audit-current-continuation1835.txt`。

### Continuation 1836

- [x] Verified independent `events_waits_history` and `events_waits_history_long` capacities for executor-owned mutex waits.
- [x] Verified dynamic long-history capacity reduction trims retained mutex wait history.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; remaining table/field, component, permission and lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-mutex-history-capacity-current-continuation1836.txt`。

### Continuation 1837

- [x] Exposed ordinary statement-acquired table locks in `performance_schema.table_handles` for the statement lifetime.
- [x] Cleared the ephemeral row during statement cleanup and preserved explicit/transaction lease behavior.
- [x] Filtered virtual metadata-schema self-observation so table-handle loss accounting remains stable.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; full table/field coverage and remaining runtime component semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-table-handles-statement-lease-current-continuation1837.txt`。

### Continuation 1838

- [x] Preserved `STATEMENT`, `TRANSACTION`, and `EXPLICIT` metadata-lock duration through owner/waiter snapshots.
- [x] Verified ordinary statement, transaction-held, and `LOCK TABLES` projections in `performance_schema.metadata_locks`.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; full table/field coverage and remaining runtime lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-duration-current-continuation1838.txt`。

### Continuation 1839

- [x] Applied metadata-lock capacity before SQL filtering and made lost accounting use the full runtime collection.
- [x] Added a filtered-read regression proving the retained row stays visible, the evicted row does not reappear, and `Performance_schema_metadata_lock_lost` is one.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; full table/field coverage and remaining runtime lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-metadata-lock-capacity-filter-current-continuation1839.txt`。

### Continuation 1840

- [x] Applied table-handle capacity before SQL filtering, matching metadata-lock behavior.
- [x] Verified an evicted handle cannot be resurrected by a selective query and lost accounting remains one.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; full table/field coverage and remaining runtime lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-table-handle-capacity-filter-current-continuation1840.txt`。

### Continuation 1841

- [x] Applied `performance_schema.prepared_statements_instances` capacity before SQL filtering.
- [x] Verified an evicted prepared statement cannot be resurrected by a selective query while
  `Performance_schema_prepared_statements_lost` remains based on the complete live inventory.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete table/field coverage and remaining
  component, permission and lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-prepared-capacity-filter-current-continuation1841.txt`。

### Continuation 1842

- [x] Applied socket/file instance capacity before SQL filtering and retained full-set lost accounting.
- [x] Verified filtered reads cannot resurrect evicted socket or file instances.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete table/field and remaining runtime
  component, permission and lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-file-socket-capacity-filter-current-continuation1842.txt`。

### Continuation 1843

- [x] Applied program-summary capacity before SQL filtering and retained full-set lost accounting.
- [x] Verified a selective query cannot resurrect an evicted program summary.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete table/field and remaining runtime
  component, permission and lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-program-capacity-filter-current-continuation1843.txt`。

### Continuation 1844

- [x] Applied connection-summary capacity before SQL filtering and retained complete-set lost accounting.
- [x] Applied the same ordering to table-lock summaries, table I/O, and index-usage summaries.
- [x] Verified retained rows remain visible and evicted rows are not resurrected by selective predicates.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete table/field coverage and remaining
  component, permission, thread, and lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-summary-io-capacity-filter-current-continuation1844.txt`。

### Continuation 1845

- [x] Applied error-summary capacity before predicates for global and identity dimensions.
- [x] Applied digest capacity before predicates and retained complete-source lost accounting.
- [x] Verified selective reads cannot resurrect evicted error or digest rows.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete table/field and remaining runtime
  lifecycle semantics are not closed.

Evidence: `reports/compatibility/p1-performance-schema-error-digest-capacity-filter-current-continuation1845.txt`。

### Continuation 1846

- [x] Applied thread-instance capacity before SQL filtering on the complete sorted processlist inventory.
- [x] Verified an evicted thread cannot be resurrected by a selective `THREAD_ID` predicate.
- [ ] Keep the aggregate P1 I_S/P_S requirement partial; complete field and runtime lifecycle semantics
  are not closed.

Evidence: `reports/compatibility/p1-performance-schema-thread-capacity-filter-current-continuation1846.txt`。

### Continuation 1847

- [x] Re-ran the Performance Schema capacity/filter regression family from continuations 1841-1846.
- [x] Re-ran the local native binlog/GTID/XA/recovery/promotion regression family.
- [x] Confirmed the matrix remains 18 implemented, 8 partial, 1 deferred, and 2 out of scope.
- [ ] Keep official MySQL interoperability, missing component runtimes, and the incomplete
  non-Connector/J client matrix partial; keep Fulltext deferred and non-InnoDB out of scope.

Evidence: `reports/compatibility/p1-p2-capacity-and-native-regression-current-continuation1847.txt`。

### Continuation 1848

- [x] Added `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL` matching for role metadata views.
- [x] Preserved equality/LIKE behavior and applied the matcher across role projections.
- [x] Passed the focused RED/GREEN regression, role/privilege regression family, and full
  `TestInformationSchema|TestPerformanceSchema` family.
- [ ] Keep the aggregate P1 requirement partial pending complete official field/runtime/component
  evidence.

Evidence: `reports/compatibility/p1-information-schema-role-predicate-filters-current-continuation1848.txt`。

### Continuation 1849

- [x] Added `IN` and `NOT IN` matching to the generic INFORMATION_SCHEMA privilege views.
- [x] Preserved equality, LIKE, and NULL predicate behavior.
- [x] Passed focused and role/permission regression families.
- [ ] Keep the aggregate P1 requirement partial pending complete official field/runtime/
  component evidence.

Evidence: `reports/compatibility/p1-information-schema-privilege-predicate-filters-current-continuation1849.txt`。

### Continuation 1850

- [x] Added `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL` matching to shared metadata filters.
- [x] Passed focused metadata and related tables/columns/constraints/catalog regressions.
- [ ] Keep the aggregate P1 requirement partial pending complete official field/runtime/
  component evidence.

Evidence: `reports/compatibility/p1-information-schema-metadata-predicate-filters-current-continuation1850.txt`。

### Continuation 1851

- [x] Added shared `IN`/`NOT IN`/NULL matching for mysql privilege metadata tables.
- [x] Preserved equality and LIKE behavior and passed the account/permission regression family.
- [ ] Keep complete mysql.* coverage and the aggregate P1 requirement partial pending
  full field/runtime/component and official MySQL evidence.

Evidence: `reports/compatibility/p1-mysql-system-privilege-predicate-filters-current-continuation1851.txt`。

### Continuation 1852

- [x] Added `IN`, `NOT IN`, and NULL predicate matching to mysql.role_edges and mysql.default_roles.
- [x] Preserved equality/LIKE behavior and passed the role-focused regression family.
- [ ] Keep complete mysql.* and aggregate P1 lifecycle/field/runtime coverage partial.

Evidence: `reports/compatibility/p1-mysql-role-table-predicate-filters-current-continuation1852.txt`。

### Continuation 1853

- [x] Added `IN` and `NOT IN` matching to INFORMATION_SCHEMA.PARAMETERS fields.
- [x] Applied it to ordinary parameters and function return rows while retaining equality/LIKE/NULL.
- [x] Passed parameter/routine regression coverage.
- [ ] Keep complete I_S/P_S field/runtime/component and lifecycle coverage partial.

Evidence: `reports/compatibility/p1-information-schema-parameters-in-predicates-current-continuation1853.txt`。

### Continuation 1854

- [x] Added `IN`, `NOT IN`, and NULL matching to mysql.procs_priv routine filters.
- [x] Preserved equality/LIKE and JDBC-compatible projections.
- [x] Passed the parameter/routine regression family.
- [ ] Keep complete mysql.* and aggregate I_S/P_S runtime/lifecycle coverage partial.

Evidence: `reports/compatibility/p1-mysql-procs-priv-predicate-filters-current-continuation1854.txt`。

### Continuation 1855

- [x] Re-ran the complete targeted INFORMATION_SCHEMA/PERFORMANCE_SCHEMA regression family.
- [x] Confirmed the 1849-1854 predicate changes do not regress existing P1 role, permission,
  runtime, capacity, and metadata behavior.
- [ ] Keep the aggregate P1 requirement partial pending complete component/runtime/field/lifecycle
  and official MySQL evidence.

Evidence: `reports/compatibility/p1-information-performance-predicate-regression-current-continuation1855.txt`。

### Continuation 1856

- [x] Ran the full serial Go command and captured all post-timeout package results.
- [ ] Do not claim repository-wide PASS: `server/innodb/engine` timed out at 2702.580s;
  the targeted I_S/P_S gate remains the authoritative evidence for continuations 1849-1855.

Evidence: `reports/compatibility/full-go-regression-current-continuation1856.txt`。

### Continuation 1857

- [x] Added shared `IN`, `NOT IN`, `IS NULL`, and `IS NOT NULL` predicate matching to
  `performance_schema.setup_consumers`, `setup_instruments`, `setup_timers`, `setup_actors`,
  and `setup_objects`, retaining equality/LIKE behavior.
- [x] Reused the actor/object matcher across setup SELECT, UPDATE, and DELETE paths.
- [x] Passed the focused RED/GREEN test and setup regression family.
- [ ] Keep the aggregate I_S/P_S requirement partial pending complete field/runtime/component/
  permission lifecycle coverage and official MySQL evidence.

Evidence: `reports/compatibility/p1-performance-schema-setup-predicate-filters-current-continuation1857.txt`。

### Continuation 1858

- [x] Added `NOT IN`, `IS NULL`, and `IS NOT NULL` matching to the shared
  Performance Schema telemetry filter used by `setup_loggers`, `setup_meters`, and
  `setup_metrics`, while retaining equality/LIKE/IN behavior.
- [x] Passed focused telemetry predicate and telemetry regression tests.
- [ ] Keep the aggregate I_S/P_S requirement partial pending complete field/runtime/component/
  permission lifecycle coverage and official MySQL evidence.

Evidence: `reports/compatibility/p1-performance-schema-telemetry-predicate-filters-current-continuation1858.txt`。

### Continuation 1859

- [x] Applied mysql system-table predicates to `mysql.proxies_priv` target/grant columns
  (`PROXIED_HOST`, `PROXIED_USER`, `WITH_GRANT`, and `GRANTOR`), not only proxy account
  `HOST`/`USER`.
- [x] Preserved equality/LIKE/IN/NOT IN/NULL matching and passed proxy lifecycle regression.
- [ ] Keep complete mysql.* table/field and privilege-lifecycle coverage partial; official
  MySQL interoperability remains unverified.

Evidence: `reports/compatibility/p1-mysql-proxies-priv-target-filters-current-continuation1859.txt`。

### Continuation 1860

- [x] Added read projections for `mysql.component`, `mysql.password_history`,
  `mysql.server_cost`, and `mysql.engine_cost`.
- [x] Exposed MySQL 8.4 cost defaults, empty component/password-history runtime
  boundaries, predicate filtering, and INFORMATION_SCHEMA table/column discovery.
- [x] Passed the focused and related account/role/proxy/metadata regression gate.
- [ ] Keep system-table write lifecycle, complete mysql.* coverage, aggregate
  I_S/P_S runtime/permission semantics, and official MySQL evidence partial.

Evidence: `reports/compatibility/p1-mysql-cost-component-system-table-shapes-current-continuation1860.txt`。

### Continuation 1861

- [x] `mysql.password_history` now records the previous non-empty password hash
  and UTC timestamp for supported `ALTER USER` and `SET PASSWORD` changes.
- [x] Password-history rows are read from durable account metadata and retain
  native filtering behavior.
- [ ] The global P1 system-metadata requirement remains partial: password
  reuse/retention policy, complete mysql.* lifecycle, component lifecycle,
  optimizer-cost writes, full I_S/P_S runtime/permission semantics, and
  official MySQL interoperability are still open.

Evidence: `reports/compatibility/p1-mysql-password-history-persistence-current-continuation1861.txt`。

### Continuation 1862

- [x] Implemented durable `INSTALL COMPONENT`/`UNINSTALL COMPONENT` registry
  rows for supported `file://...` URNs.
- [x] Enforced INSERT/DELETE permissions on `mysql.component` and projected
  component IDs, group IDs, and URNs through the virtual system table.
- [ ] Native component loading/unloading, dependency and service activation,
  INSTALL SET/PERSIST, full mysql.* lifecycle, and official interoperability
  remain partial.

Evidence: `reports/compatibility/p1-mysql-component-registry-lifecycle-current-continuation1862.txt`。

### Continuation 1863

- [x] Added explicit `FLUSH OPTIMIZER_COSTS` dispatch with `LOCAL` and
  `NO_WRITE_TO_BINLOG` forms.
- [x] Enforced the `FLUSH_OPTIMIZER_COSTS` or `RELOAD` privilege boundary.
- [ ] Durable optimizer-cost table mutations and the real session-visible
  in-memory cost-model reload remain open.

Evidence: `reports/compatibility/p1-flush-optimizer-costs-privilege-dispatch-current-continuation1863.txt`。

### Continuation 1864

- [x] Added durable INSERT/UPDATE/DELETE semantics for `mysql.server_cost` and
  `mysql.engine_cost`, including transaction staging and table privileges.
- [x] `FLUSH OPTIMIZER_COSTS` now reloads the persisted model into the runtime
  optimizer snapshot; sequential-scan costing consumes `row_evaluate_cost`.
- [ ] Full MySQL cost-model warning/replication/plan-operator parity remains
  open, as do the broader I_S/P_S, XA/binlog, client-matrix, and official
  interoperability requirements.

Evidence: `reports/compatibility/p1-mysql-optimizer-cost-dml-reload-current-continuation1864.txt`。

### Continuation 1865

- [x] Added the global dynamic `password_require_current` policy and durable
  account-level `PASSWORD REQUIRE CURRENT`, `OPTIONAL`, and `DEFAULT` state.
- [x] Projected password verification/reuse policy fields through `mysql.user`,
  rendered them in `SHOW CREATE USER`, and enforced `REPLACE` verification for
  self-service `ALTER USER`/`SET PASSWORD` changes.
- [ ] Full mysql.user lifecycle/field parity, secondary-password clauses,
  complete I_S/P_S semantics, official replication/XA interoperability, and
  the non-Connector/J client matrix remain open or externally blocked.

Evidence: `reports/compatibility/p1-password-verification-policy-current-continuation1865.txt`。

### Continuation 1866

- [x] Added durable `password_last_changed` and account `password_lifetime`
  metadata, plus the dynamic `default_password_lifetime` variable.
- [x] Added `PASSWORD EXPIRE INTERVAL N DAY` persistence, projection, and
  `SHOW CREATE USER` rendering; `NEVER`/`DEFAULT` clear the account override.
- [ ] Password expiration scheduling/enforcement and complete `mysql.user`
  lifecycle/field parity remain open, along with the broader I_S/P_S,
  replication/XA, client-matrix, and official interoperability requirements.

Evidence: `reports/compatibility/p1-mysql-user-password-lifetime-current-continuation1866.txt`。

### Continuation 1867

- [x] Expanded the default `mysql.user` projection to all MySQL 8.4 static
  privilege columns, backed by durable global grants and existing ALL expansion.
- [x] Preserved the password-management fields and account filtering in the
  expanded projection.
- [ ] Full grant-table lifecycle, dynamic privilege/role edge semantics, full
  I_S/P_S field/runtime parity, official replication/XA interoperability, and
  the non-Connector-J client matrix remain open or externally blocked.

Evidence: `reports/compatibility/p1-mysql-user-static-privilege-columns-current-continuation1867.txt`。

### Continuation 1868

- [x] Connected durable `password_last_changed` and `password_lifetime` to
  authentication; account lifetime overrides the global default.
- [x] Authentication rejects accounts whose configured password lifetime has
  elapsed while preserving explicit `password_expired` behavior.
- [ ] Expired-password warning/change-flow protocol details and complete
  mysql.user/I_S/P_S parity remain open, as do official replication/XA and
  full non-Connector-J client-matrix gates.

Evidence: `reports/compatibility/p1-password-lifetime-auth-enforcement-current-continuation1868.txt`。
### Continuation 1869

`SHOW CREATE USER` 已补齐 MySQL 8.4 的账户可见性、哈希脱敏、`CURRENT_USER()` 目标解析和
默认账户选项输出，专项回归通过。该切片不改变全局聚合状态：完整 I_S/P_S 运行时与权限
语义、非 Connector/J 全量客户端矩阵、XA/native binlog/GTID/崩溃恢复/提升官方互操作仍需
继续推进；Fulltext 延后，非 InnoDB 不纳入范围。
### Continuation 1870

账户属性语法已补齐：CREATE/ALTER USER 的 JSON ATTRIBUTE 和 COMMENT 可持久化、清除/更新，USER()/CURRENT_USER() 目标可用于改密和账户属性，并在 mysql.user、
INFORMATION_SCHEMA.USER_ATTRIBUTES 和 SHOW CREATE USER 中保持一致。专项回归通过；全局
I_S/P_S 运行时、非 Connector/J 客户端矩阵、XA/native binlog/GTID/崩溃恢复/提升官方互操作
仍保持未完成边界。

### Continuation 1878

`INFORMATION_SCHEMA.PARAMETERS` 已修复 schema/routine 空字符串 `=`/`LIKE` 谓词：只有查询中
实际出现的 `SPECIFIC_SCHEMA`、`ROUTINE_SCHEMA`、`SPECIFIC_NAME`、`ROUTINE_NAME` 条件才参与
匹配，空字符串不再被当作无过滤。专项例程回归通过；全局 I_S/P_S、P2/P3/P4 仍保持未完成边界。

### Continuation 1883

`INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS.SRS_ID` 现在按 MySQL 的 `LIKE` 通配语义
匹配：`srs_id LIKE '4%'` 可以命中内置 4326，精确数值过滤及显式空字符串语义保持不变。
专项回归、完整 I_S 族和全仓编译通过；该切片只关闭一个 P1 目录过滤缺口，P1/P2/P3/P4
聚合项仍保持未完成边界。

### Continuation 1879

通用 INFORMATION_SCHEMA 元数据过滤现在区分“没有谓词”和显式空字符串谓词：
`TABLE_SCHEMA/TABLE_NAME/COLUMN_NAME/... = ''`、`LIKE ''` 不再恢复全量元数据行；缺少谓词仍保持
全量查询。专项 I_S 元数据回归和全仓编译通过；完整 I_S/P_S 运行时、权限/组件生命周期、
P2/P3/P4 仍保持未完成边界。

### Continuation 1880

`INFORMATION_SCHEMA.PARAMETERS` 现在也区分 `PARAMETER_NAME/PARAMETER_MODE` 缺少谓词与
显式 `= ''`/`LIKE ''`：procedure 参数使用空模式精确匹配，function return 的 NULL 参数名/模式
不会错误匹配空字符串。专项 PARAMETERS、相关例程族和全仓编译通过；完整 I_S/P_S、P2/P3/P4
仍保持未完成边界。

### Continuation 1882

INFORMATION_SCHEMA 专用目录过滤器现在保留显式空字符串语义：view table/routine usage、
`KEYWORDS.WORD`、空间单位、空间参考系统和几何列的 `= ''`/`LIKE ''` 不再被当成缺少谓词；
真实空值 SRS 行仍可被精确区分。专项 I_S 回归通过；完整 I_S/P_S、组件/权限生命周期、
P2/P3/P4 仍保持未完成边界。

### Continuation 1881

INFORMATION_SCHEMA 权限视图现在区分缺少谓词与显式空字符串谓词：`TABLE_SCHEMA = ''`、
`PRIVILEGE_TYPE LIKE ''` 不再恢复已有权限行。专项权限回归通过；完整 I_S/P_S、组件/权限
生命周期、P2/P3/P4 仍保持未完成边界。

### Continuation 1877

角色授权表和 MySQL 系统表现在区分“没有谓词”和“谓词值为空字符串”：`=`/`LIKE ''` 不再
被当作无过滤条件，因此 `mysql.role_edges`、`mysql.server_cost` 等查询不会错误恢复全量行；
通配符及非空等值/LIKE 语义保持不变。专项角色/账户回归通过；全局 I_S/P_S、P2/P3/P4 仍保持
未完成边界。

### Continuation 1871

账户 TLS 与资源限制元数据已接通：`REQUIRE CIPHER/ISSUER/SUBJECT`、四类 `WITH` 资源限制可
持久化，并通过 `mysql.user`、`SHOW CREATE USER` 和认证侧读取验证；四类限制已接入解耦协议
的认证、普通查询和预处理执行路径。其他协议适配器、警告/重试边界、完整 I_S/P_S、非
Connector-J 全量客户端矩阵及 XA/native binlog/GTID/崩溃恢复/提升官方互操作仍保持未完成边界。

### Continuation 1872

账户失败登录策略已接通本地持久化和认证状态机：支持失败次数阈值、按天临时锁定、
`UNBOUNDED` 锁定、成功认证/改密/解锁清零，并在 `mysql.user` 与 `SHOW CREATE USER` 中投影。
专项账户回归和跨包编译通过；该切片仍不足以把全局 P1 聚合项标记为完成。

### Continuation 1873

过期密码协议闭环已补齐：支持 `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS` 的客户端进入受限会话，
不支持的客户端返回 1862；受限会话只允许当前账户改密，并覆盖文本协议和预处理协议，成功改密
后解除限制。该局部边界已通过专项及 auth/net/protocol/dispatcher 回归，但全局 I_S/P_S、非
Connector-J 客户端矩阵、XA/native binlog/GTID/崩溃恢复/提升官方互操作仍保持未完成；Fulltext
延后，非 InnoDB 不纳入范围。

### Continuation 1874

Performance Schema 汇总过滤器已支持 `IS NULL`/`IS NOT NULL`，实际 NULL 投影值也按
MySQL 三值逻辑处理；专项、完整 P_S 回归和全仓编译均通过。该局部修复不改变全局聚合状态：
完整 I_S/P_S 运行时与权限语义、非 Connector/J 全量客户端矩阵、XA/native binlog/GTID/
崩溃恢复/提升官方互操作仍需继续推进；Fulltext 延后，非 InnoDB 不纳入范围。

### Continuation 1875

Performance Schema 全局/会话变量视图已修复空结果回退：`VARIABLE_NAME` 的 `IN/NOT IN` 和
`IS NULL/IS NOT NULL` 无匹配时返回空集，不再恢复完整变量列表；无该谓词时仍保持全量投影。
聚焦、相关回归、完整 P_S 测试族和全仓编译通过。全局 I_S/P_S、P2/P3/P4 仍保持未完成边界。

### Continuation 1876

MySQL 系统成本表的 NULL 成员谓词已修复：`mysql.server_cost`/`mysql.engine_cost` 中真实
`NULL` 值参与 `IN` 或 `NOT IN` 时按三值逻辑判定为 UNKNOWN，不再错误保留 `NOT IN` 行；
显式 `IS NULL`、`IS NOT NULL`、等值和 `LIKE` 语义保持不变。专项成本回归通过；全局
I_S/P_S 运行时、官方 XA/binlog/GTID/崩溃恢复/提升互操作及非 Connector/J 全量客户端矩阵
仍保持未完成边界。
