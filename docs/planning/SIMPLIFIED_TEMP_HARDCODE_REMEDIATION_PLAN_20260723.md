# XMySQL 简化/临时/硬编码整改计划（2026-07-23）

## 1. 背景

本计划来自 2026-07-23 对当前源码的审核扫描。

扫描口径：

```bash
rg -n \
  --glob '!**/*_test.go' \
  --glob '!server/innodb/sqlparser/dependency/querypb/query.pb.go' \
  "简化|临时|硬编码|hard.?cod|stub|placeholder|bypass" \
  server client cmd logger util
```

当前结果：

- 命中 311 行。
- 涉及 99 个非测试源码文件。
- 主要集中在：
  - `server/innodb/manager`：115 行
  - `server/innodb/engine`：55 行
  - `server/innodb/plan`：44 行
  - `server/innodb/storage`：33 行
  - `server/dispatcher`：15 行
  - `server/protocol`：9 行

这些命中不全是缺陷。以下内容不作为本轮整改对象：

- 测试、demo、历史报告中的 mock 或临时目录。
- MySQL 语义中的 temporary table、placeholder 等正常术语。
- 查询优化器中“表达式简化”这类正常算法名称。
- 已明确是文档解释用途、不会进入运行路径的示例代码。

本计划只处理会影响运行行为、生产就绪判断、协议兼容、事务恢复、权限边界或数据正确性的点。

## 2. 整改目标

1. 去掉用户请求链路中的硬编码响应、临时 session、默认用户/主机推断。
2. 收敛协议层简化实现，至少保证 JDBC 常用路径下的包序号、列定义、认证解析、字符集处理有明确边界。
3. 补齐 checkpoint、buffer pool、MVCC、B+Tree、页记录格式中的占位行为，避免假成功。
4. 将统计、优化器、碎片整理中的估算行为标成可接受的非 P0 边界，避免被误判为生产能力。
5. 每个整改项必须有测试或验证脚本，禁止只改注释。

## 3. 优先级

| 优先级 | 范围 | 完成要求 |
|---|---|---|
| P0 | 用户请求链路、认证权限、事务恢复、数据页写入、B+Tree 记录结构、checkpoint | 修复实现并补测试；不能用文档说明代替 |
| P1 | JDBC 兼容、协议细节、元数据展示、统计信息、索引估算 | 明确行为边界，核心路径补测试 |
| P2 | 性能估算、demo、长期优化项 | 保留边界说明，不阻塞 P0 |

## 4. P0-01 请求链路去硬编码和临时 session

涉及文件：

- `server/dispatcher/enhanced_message_handler.go`
- `server/dispatcher/message_handler.go`
- `server/net/decoupled_handler.go`
- `server/auth/engine_access.go`

审核发现：

- `HandleQueryWithRealSession` 中 host 缺失时默认 `127.0.0.1`。
- `SELECT 1` 返回硬编码响应，绕过真实执行链路。
- `handleQueryMessage` 创建临时 session，注释明确“实际应该从消息中获取”。
- 权限检查只取第一个权限。
- `extractUserFromSessionID` 和 `extractHostFromSessionID` 从 sessionID 推断身份，失败默认 `root` / `127.0.0.1`。
- auth query 仍创建临时会话。

整改要求：

1. 所有 COM_QUERY 必须使用连接对应的真实 `server.MySQLServerSession`。
2. `SELECT 1` 走真实 SQL 执行链路；如果保留 fast path，只能放在协议探活分支，并且返回格式必须与真实 SELECT 一致。
3. user、host、database 从认证结果和真实 session 读取，禁止从 sessionID 猜。
4. 权限检查必须覆盖 SQL 所需权限集合，不能只取第一个权限。
5. 临时 session 只能用于后台内部查询，并且必须在类型和命名上与真实客户端 session 区分。

建议测试：

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
$GO_BIN test ./server/dispatcher ./server/net ./server/auth -count=1 -v
$GO_BIN test ./server/dispatcher -run 'Test.*Auth|Test.*Privilege|Test.*SelectOne|Test.*Session' -count=1 -v
```

完成定义：

- 同一连接执行 `USE db; CREATE TABLE ...; SELECT 1;` 时 session 状态连续。
- 非 root 用户不能因为 sessionID 缺失被默认成 root。
- 权限不足时返回明确 MySQL 错误，不走硬编码成功响应。

## 5. P0-02 认证与协议包格式收敛

涉及文件：

- `server/protocol/parser.go`
- `server/protocol/mysql_codec.go`
- `server/protocol/encoder.go`
- `server/protocol/charset_manager.go`
- `server/protocol/handshake.go`
- `server/net/decoupled_handler.go`
- `server/conf/config.go`

审核发现：

- auth response 被直接转换为字符串。
- MySQL 响应包序号大量使用固定值。
- 列定义仍是简化结构。
- 字符集转换基本未实现，当前多数情况直接返回原数据。
- `dev_bypass_password_auth` 仍存在，虽然非本地监听会被关闭。

整改要求：

1. auth parser 只解析认证响应字段，不把 challenge response 当明文密码。
2. 密码验证统一由 auth service 完成，按插件区分 `mysql_native_password` 和 `caching_sha2_password`。
3. 每个连接维护响应序号，结果集列、行、EOF/OK 包连续递增。
4. 列定义必须包含 JDBC 需要的 schema、table、name、type、flags、charset、length。
5. 字符集转换至少实现 `utf8` / `utf8mb4` 验证；不支持的转换返回明确错误。
6. `dev_bypass_password_auth=true` 只能用于本地调试配置，并在启动日志中明确标记。

建议测试：

```bash
$GO_BIN test ./server/protocol ./server/net ./server/auth -count=1 -v
cd jdbc_client && mvn test -Pjdbc-connectivity
```

完成定义：

- JDBC 连接认证不依赖明文密码字符串。
- 结果集包序号能通过 JDBC PreparedStatement 和普通 Statement 读取。
- 非本地监听时即使配置 bypass 也不能免密。

## 6. P0-03 Checkpoint、恢复和写入边界

涉及文件：

- `server/innodb/engine/storage_integrated_checkpoint.go`
- `server/innodb/engine/recovery_adapters.go`
- `server/innodb/manager/crash_recovery.go`
- `server/innodb/manager/redo_log_manager.go`
- `server/innodb/manager/transaction_manager.go`

审核发现：

- checkpoint 表空间信息返回空列表。
- 活跃事务返回空列表。
- dirty page `ModifyCount` 固定为 1。
- flush score 使用固定分数或 LSN 代理。
- checkpoint checksum 直接使用 LSN。
- recovery 路径存在“页面不存在则创建”“直接应用日志数据到页面”等简化语义。

整改要求：

1. checkpoint record 必须记录真实 dirty page、active transaction、tablespace 信息。
2. sharp checkpoint 期间写入必须阻塞或返回明确错误，失败后必须恢复写入。
3. checkpoint checksum 使用稳定序列化内容计算，不能只取 LSN。
4. recovery replay 必须基于 redo record 类型解析，不允许把 redo payload 直接覆盖页面。
5. 页面不存在、LSN 过旧、checksum 不匹配必须有明确错误或跳过规则。

建议测试：

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*Checkpoint|Test.*Recovery|Test.*WAL' -count=1 -v
$GO_BIN test ./server/innodb/manager -run 'Test.*Crash|Test.*Redo|Test.*Transaction' -count=1 -v
```

完成定义：

- checkpoint 文件中能看到真实 dirty page 和事务信息。
- 模拟 checkpoint 失败后，后续写入不被永久阻塞。
- redo 重放幂等，重复 replay 不改变已应用页面。

## 7. P0-04 B+Tree、页记录和索引路径去占位

涉及文件：

- `server/innodb/manager/enhanced_btree_index.go`
- `server/innodb/manager/bplus_tree_manager.go`
- `server/innodb/manager/enhanced_btree_adapter.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/storage/wrapper/record/row_secondary_index_leaf_row.go`
- `server/innodb/storage/wrapper/record/row_secondary_index_internal_row.go`
- `server/innodb/storage/wrapper/page/fsp_page_wrapper.go`
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`
- `server/innodb/storage/wrapper/page/blob.go`

审核发现：

- 索引列类型固定为 `VARCHAR`。
- `SimpleRow` 多个方法返回 0、空字符串或 no-op。
- 页面插入使用固定 user record offset。
- child page 解析返回 `pageNo + 1`。
- 二级索引 leaf row 读取主键返回 0。
- BLOB 页面部分方法直接返回 nil。
- FSP page wrapper 有 ID 固定为 0 的解析路径。

整改要求：

1. B+Tree 页必须通过真实页头、record header、slot/page directory 定位记录。
2. 索引列类型从表元数据读取，支持至少 int、varchar、decimal、bool、null。
3. `SimpleRow` 只能作为测试或适配层内部类型；生产写入路径必须使用真实 row。
4. child page、next page、primary key、record offset 必须从页面内容解析。
5. 未实现的 page/blob 操作返回明确错误，不能返回 nil 假成功。

建议测试：

```bash
$GO_BIN test ./server/innodb/manager -run 'Test.*BTree|Test.*Index|Test.*Page' -count=1 -v
$GO_BIN test ./server/innodb/storage/wrapper/... -count=1
$GO_BIN test ./server/innodb/engine -run 'Test.*Index|Test.*StorageIntegrated' -count=1 -v
```

完成定义：

- 多行同页插入、查询、删除不会互相覆盖。
- 二级索引能返回真实主键并回表。
- 索引 split/merge 后 child page 指针来自页面内容。

## 8. P0-05 MVCC、Undo、事务回滚去空实现

涉及文件：

- `server/innodb/storage/store/mvcc/mvcc.go`
- `server/innodb/storage/format/mvcc/record_version.go`
- `server/innodb/manager/undo_log_manager.go`
- `server/innodb/manager/undo_segment.go`
- `server/innodb/manager/storage_manager.go`
- `server/innodb/storage/store/mvcc/isolation.go`

审核发现：

- `storage/store/mvcc/mvcc.go` 文件声明为 empty stubs。
- `record_version.go` 的事务可见性是简化版本。
- undo rollback 仍有“标记版本为已回滚”的简化语义。
- storage manager 的事务日志和 rollback 是简化实现。

整改要求：

1. 明确保留一个 MVCC 实现入口，删除或下线空 stub 文件。
2. ReadView 可见性必须使用事务 ID、活跃事务列表、提交状态判断。
3. Undo rollback 至少支持 insert 回滚删除、update 回滚旧值、delete 回滚恢复。
4. rollback 不能只改内存标记，必须影响后续读取结果。
5. savepoint rollback 需要和 undo 语义一致。

建议测试：

```bash
$GO_BIN test ./server/innodb/manager -run 'Test.*MVCC|Test.*Undo|Test.*Rollback|Test.*Savepoint' -count=1 -v
$GO_BIN test ./server/innodb/storage/store/mvcc ./server/innodb/storage/format/mvcc -count=1 -v
```

完成定义：

- `BEGIN; INSERT; ROLLBACK; SELECT` 不再看到已回滚行。
- `SAVEPOINT; UPDATE; ROLLBACK TO SAVEPOINT` 恢复旧值。
- 多连接读写在已支持隔离级别下结果明确。

## 9. P1-01 SHOW、元数据和系统变量默认值

涉及文件：

- `server/innodb/engine/executor.go`
- `server/innodb/engine/select_executor.go`
- `server/dispatcher/system_variable_engine.go`
- `server/innodb/manager/info_schema_manager.go`
- `server/innodb/manager/info_schema_generators.go`
- `server/innodb/manager/schema_manager.go`

审核发现：

- `SHOW VARIABLES`、`SHOW STATUS` 有常见变量/状态的简化返回。
- `select_executor` 在权限表查询中使用默认 user/host。
- info schema 存在默认 schema 或用户表归属简化判断。
- schema manager 的数据字典删除回滚只记录警告。

整改要求：

1. 元数据查询优先读取真实 schema manager / info schema 数据。
2. 没有真实来源时返回明确错误或标记为 unsupported，不能假成功。
3. mysql.user 查询不能默认 user/host 造成权限误判。
4. DDL rollback 失败必须反映到调用方。

建议测试：

```bash
$GO_BIN test ./server/dispatcher ./server/innodb/engine -run 'Test.*Show|Test.*SystemVariable|Test.*InfoSchema|Test.*Auth' -count=1 -v
cd jdbc_client && mvn test -Pjdbc-connectivity
```

## 10. P1-02 优化器、统计和代价估算边界

涉及文件：

- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/plan/join_order_optimizer.go`
- `server/innodb/plan/join_order_optimizer_helpers.go`
- `server/innodb/plan/index_pushdown_optimizer.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/engine/select_executor.go`

审核发现：

- 行数、选择率、分组数、join 选择率大量使用固定比例。
- statistics collector 有模拟数据。
- parallel plan 使用占位键、占位行。
- select executor 对单表、索引扫描、WHERE 字符串转换有简化处理。

整改要求：

1. P0 阶段允许保守表扫描，但必须明确不宣称 CBO 完整可用。
2. 统计信息没有真实来源时，计划输出必须带 degraded 标记或日志。
3. 影响结果正确性的简化必须优先修复；只影响性能的估算可排入 P1。
4. parallel plan 的占位结果不能进入用户查询结果。

建议测试：

```bash
$GO_BIN test ./server/innodb/plan ./server/innodb/engine -run 'Test.*Optimizer|Test.*Statistics|Test.*Select|Test.*Parallel' -count=1 -v
```

## 11. P1-03 存储管理、碎片整理、Insert Buffer

涉及文件：

- `server/innodb/manager/tablespace_defragmenter.go`
- `server/innodb/manager/ibuf_manager.go`
- `server/innodb/manager/system_space_manager.go`
- `server/innodb/manager/system_page_initializer.go`
- `server/innodb/manager/page_fix_simplified.go`
- `server/innodb/manager/storage_manager.go`
- `server/innodb/storage/io/io_optimizer.go`

审核发现：

- defragmenter 的页面遍历、空洞统计、填充率、耗时估算较多简化。
- ibuf merge 只是占位，未解析真实页面格式。
- system space manager 存在占位组件和固定表 ID。
- IO optimizer 的实际读写函数仍为模拟实现。

整改要求：

1. 对生产路径会调用的函数，补真实实现或返回明确 unsupported。
2. 对只用于观测/估算的函数，输出中标注 estimate，不参与正确性判断。
3. I/O 读写路径必须接真实 storage provider，不得只返回模拟数据。
4. system space 固定 ID 逻辑要替换为持久化字典或可验证分配器。

建议测试：

```bash
$GO_BIN test ./server/innodb/manager ./server/innodb/storage/io -run 'Test.*Space|Test.*Defrag|Test.*IBuf|Test.*IO' -count=1 -v
```

## 12. P2-01 低风险简化项保留规则

以下命中可以暂不整改，但必须维持边界说明：

- `cmd/demo_*`、`cmd/test_*` 中的模拟代码。
- hash/checksum 工具中的兼容 fallback，但必须在函数名或注释中说明算法强度。
- varchar optimizer 中的 RLE 压缩和 BlobID 简化映射。
- 成本估算中的经验值，只要不影响查询结果正确性。
- 临时表、临时段、临时文件等真实业务语义。

## 13. 执行顺序

建议按以下顺序推进：

1. P0-01 请求链路去硬编码和临时 session。
2. P0-02 认证与协议包格式收敛。
3. P0-03 Checkpoint、恢复和写入边界。
4. P0-04 B+Tree、页记录和索引路径去占位。
5. P0-05 MVCC、Undo、事务回滚去空实现。
6. P1-01 SHOW、元数据和系统变量默认值。
7. P1-02 优化器、统计和代价估算边界。
8. P1-03 存储管理、碎片整理、Insert Buffer。

每完成一个 P0 项，必须更新对应计划或状态文档，记录：

- 修改文件。
- 新增测试。
- 验证命令。
- 仍保留的简化边界。

## 14. 全量回归门禁

P0 全部完成后运行：

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
$GO_BIN test ./server/dispatcher ./server/net ./server/protocol ./server/auth -count=1
$GO_BIN test ./server/innodb/engine ./server/innodb/manager ./server/innodb/storage/... -count=1
cd jdbc_client && mvn test -Pjdbc-connectivity
```

如果涉及恢复、并发、发布证据，再补跑：

```bash
scripts/crash_recovery_drill.sh
scripts/concurrency_validation.sh
scripts/run_p0_delivery_candidate.sh
```

## 15. 退出标准

本计划完成需要满足：

1. P0 文件中不再存在会导致假成功、静默跳过、默认 root、默认 host、硬编码结果集的实现。
2. `rg` 扫描中剩余的 `简化/临时/硬编码/stub/placeholder/bypass` 都有明确分类：已修复、测试专用、demo 专用、P1/P2 边界、或真实业务语义。
3. 核心 Go 测试和 JDBC connectivity 测试通过。
4. README 或 P0 状态文档同步更新当前能力边界，不能把估算能力写成已完成能力。
