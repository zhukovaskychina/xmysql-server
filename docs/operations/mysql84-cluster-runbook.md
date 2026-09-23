# XMySQL P1 主从集群运行手册

## 范围

P1 提供 1 主多从的逻辑复制路径：source 在本地事务提交后写入 JSONL binlog 和 GTID 状态；replica 通过 HTTP 控制面拉取事件，并使用自身 SQL 执行器原子回放提交事务。

当前版本支持：

- 1 主 2 从及以上拓扑；
- GTID 持久化、断线重连和重复应用去重；
- replica 重启后从上次 position 继续拉流；
- replica 默认只读；
- `POST /replication/promote` 手动提升 replica；
- `GET/POST /replication/members` 查询或替换运行时成员列表，列表持久化到 `replication/members.json`；
- source/replica 状态查询。

当前版本不宣称：多主写入、分布式事务、自动选主、脑裂防护、MySQL 原生 binlog 格式兼容或完整存储对象复制。`CREATE/DROP DATABASE`、基础 `CREATE/DROP/ALTER/TRUNCATE/RENAME` DDL 已按提交事务复制；复杂在线 DDL、完整对象依赖和 DDL 原子回滚仍不在当前范围。

## 配置

在 `my.ini` 中加入：

```ini
[replication]
role = source                         ; standalone/source/replica
uuid = cluster-node-1
server_id = 1
listen_address = 127.0.0.1:4401
source_url =
poll_interval = 500ms
read_only = false
```

replica 示例：

```ini
[replication]
role = replica
uuid = cluster-node-2
server_id = 2
listen_address = 127.0.0.1:4402
source_url = http://127.0.0.1:4401
poll_interval = 500ms
read_only = true
```

每个节点必须使用独立的 `datadir`、`innodb.data_dir`、`server_id` 和 `uuid`。首次启动 replica 前，需要通过备份恢复或初始化脚本准备与 source 一致的初始数据；P1 当前会复制已提交 DML 和基础逻辑 DDL，复杂 DDL/对象仍需人工同步。

## 控制面

```text
GET  http://node:4401/replication/status
GET  http://node:4401/replication/members
POST http://node:4401/replication/members  {"peers":["http://node:4402","http://node:4403"]}
GET  http://node:4401/replication/binlog?position=4
POST http://node:4402/replication/promote
```

`status` 至少检查 `role`、`executed_gtids`、`source_position`、`replication_lag_seconds` 和 `last_error`。
`members` 返回本节点及每个 peer 的 `reachable`、角色、UUID、GTID 和错误信息；更新接口是 replace-all，提交前会校验绝对 HTTP(S) URL 并去重。

## 启动顺序

1. 启动 source，确认 `/replication/status` 返回 `role=source`。
2. 使用一致的初始数据目录启动所有 replica。
3. 确认 replica 返回 `role=replica`，且 `source_url` 可访问。
4. 在 source 提交一笔 DML。
5. 检查所有 replica 的业务查询和 `executed_gtids`。
6. source 故障时停止旧 source，确认没有其他 source 继续写入，再对目标 replica 执行 promote。
7. 通过提升后的 MySQL 端口执行写入，并记录切换报告。

## 验证

运行复制层和真实引擎集成测试：

```powershell
go test ./server/replication -count=1
go test ./server/innodb/engine -run TestEngineSourceReplicaReplicatesCommittedDML -count=1 -v
```

运行 Connector/J P0 门禁时必须显式注入隔离端点：

```powershell
$env:XMYSQL_JDBC_URL = 'jdbc:mysql://127.0.0.1:3314?useSSL=false&allowPublicKeyRetrieval=true'
$env:XMYSQL_JDBC_USER = 'root'
$env:XMYSQL_JDBC_PASSWORD = 'root@1234'
Set-Location jdbc_client
mvn test -Pjdbc-connectivity
```

## 故障处理

- `last_error` 非空：先检查 source URL、控制端口和 source 日志，再确认 replica 数据目录可写。
- GTID 不前进：检查 source 是否有新的 COMMIT 事件，以及 replica 是否仍处于 `role=replica`。
- replica 写入被拒绝：这是默认只读行为；完成 promote 后再路由写流量。
- 不得在旧 source 未隔离时 promote replica，否则可能产生双主写入和数据分叉；自动脑裂治理属于 P3。
- 自动提升仍只提供多数派、source 不可见和确定性候选保护；没有外部租约或磁盘 fencing 时，网络分区期间不能宣称达到共识级脑裂防护。
