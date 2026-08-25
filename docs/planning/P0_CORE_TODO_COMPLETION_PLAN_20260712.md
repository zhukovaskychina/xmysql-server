# XMySQL Server P0 核心 TODO 完成计划（2026-07-12）

## 1. 目标

本计划聚焦当前 P0 中最影响正确性的核心 TODO：

- 补齐 DML 主路径，避免 INSERT/UPDATE/DELETE 出现空实现、静默跳过或误删整页。
- 接入 SHOW 元数据链路，让基础 SQL 可用性闭环。
- 补齐 Sharp Checkpoint 写阻塞语义，避免检查点期间写入边界不清。
- 补齐 Buffer Pool 页面驱逐和 Enhanced B+Tree 索引重建/删除页面的最低可用行为。
- 拆分 `server/innodb/engine` 测试，形成可重复、可定位、不会长时间无输出的 P0 验证命令。

非目标：

- 不在本轮追求完整 MySQL 兼容。
- 不在本轮做大规模架构重写。
- 不把长期优化项伪装成已完成；无法生产化的能力必须保留明确边界和测试证据。

## 2. 当前判断

当前项目已能 `go build ./...`，但 P0 仍未闭环。关键阻塞点集中在：

- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/show_executor.go`
- `server/innodb/engine/storage_integrated_checkpoint.go`
- `server/innodb/manager/buffer_pool_manager.go`
- `server/innodb/manager/enhanced_btree_manager.go`
- `server/innodb/engine` 包整体测试耗时不可控

## 3. 执行顺序

### P0-01 DML 算子闭环

涉及文件：

- `server/innodb/engine/dml_operators.go`
- 必要时补充 `server/innodb/engine/dml_operators_test.go`

当前问题：

- `findDuplicateRecord` 直接返回 `findDuplicateRecord not fully implemented`。
- `insertRow` 仅记录日志后返回 `nil`，未做真实插入。
- `updateRecord` 仅记录日志后返回 `nil`，未做真实更新。
- `parseInsertRows` 返回空列表，导致 INSERT 语句无法产生真实行。
- `updateInPlace`、`deleteOldRecord`、`insertNewRecord`、`deleteRecord` 存在简化实现。

实现要求：

- `parseInsertRows` 从 `sqlparser.Insert` 中解析列和值，按表 schema 生成行数据。
- `insertRow` 至少完成数据校验、主键/唯一键冲突判断、聚簇索引写入、二级索引同步的最低可用路径。
- `findDuplicateRecord` 基于主键和唯一索引查找冲突记录，返回结构化错误或目标记录。
- `updateRecord` 更新目标记录，并在索引列变更时同步索引。
- INSERT、ON DUPLICATE KEY UPDATE、UPDATE、DELETE 的错误路径不能静默吞掉。

完成定义：

- INSERT 能真实写入一行并可被查询路径读到。
- 重复主键/唯一键返回明确错误或触发 ON DUPLICATE KEY UPDATE。
- UPDATE 命中目标行后修改实际值。
- DELETE 命中目标行后只删除目标记录。
- 至少覆盖成功路径、重复键、空行、schema 不匹配、索引列更新这些测试。

建议验证：

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
$GO_BIN test ./server/innodb/engine -run 'Test.*DML|Test.*Insert|Test.*Update|Test.*Delete' -count=1 -v
```

### P0-02 存储 DML Helper 行级读写修正

涉及文件：

- `server/innodb/engine/storage_integrated_dml_helper.go`
- 必要时补充 `server/innodb/engine/storage_integrated_dml_helper_test.go`

当前问题：

- `findRowsToUpdateInStorage` 在无 WHERE 时直接返回空结果。
- `findRowsToDeleteInStorage` 在无 WHERE 时直接返回空结果。
- `readRowFromStorage` 假设整个页面就是一行。
- `markRowAsDeletedInStorage` 通过清空页面表示删除，可能误伤同页其他记录。

实现要求：

- 明确无 WHERE 行为：支持全表扫描，或在当前阶段返回明确错误；禁止静默空结果。
- 行读取必须基于 `pageNo + slot` 定位目标记录。
- 删除必须只标记目标记录，不清空整页。
- 更新/删除前必须保留 old values，用于索引同步和回滚语义。

完成定义：

- 单页多行场景下，删除其中一行不影响其他行。
- WHERE 主键命中时能定位目标行。
- 无 WHERE 行为有明确测试。
- 读取失败、slot 越界、空页、损坏页有明确错误。

建议验证：

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*StorageIntegrated.*DML|Test.*Row.*Storage' -count=1 -v
```

### P0-03 SHOW 元数据链路

涉及文件：

- `server/innodb/engine/show_executor.go`
- 可能涉及 metadata/schema manager 接口
- 必要时补充 `server/innodb/engine/show_executor_test.go`

当前问题：

- `Schema()` 返回 `nil`。
- `showDatabases`、`showTables`、`showColumns` 返回“暂未接入真实元数据字典”。

实现要求：

- `Schema()` 按 SHOW 类型返回正确输出列。
- `SHOW DATABASES` 从真实 schema/metadata 来源读取数据库列表。
- `SHOW TABLES` 读取当前数据库下表列表。
- `SHOW COLUMNS` 读取目标表列定义，包括字段名、类型、NULL、Key、Default、Extra 的最低可用值。
- 元数据管理器缺失时返回明确错误，不返回假成功。

完成定义：

- `SHOW DATABASES`、`SHOW TABLES`、`SHOW COLUMNS` 都能返回真实结果。
- 空库、空表、表不存在、未选择数据库场景有明确错误或空结果语义。
- 输出 schema 与实际行列数一致。

建议验证：

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*Show|Test.*SHOW' -count=1 -v
```

### P0-04 Sharp Checkpoint 写阻塞

涉及文件：

- `server/innodb/engine/storage_integrated_checkpoint.go`
- 可能涉及事务管理器、写入门禁或持久化管理器

当前问题：

- `WriteSharpCheckpoint` 中写阻塞和解阻塞仍是 TODO。
- 错误路径下是否恢复写入状态不明确。

实现要求：

- Sharp checkpoint 开始时阻塞新写入。
- Flush 和 checkpoint record 写入完成后恢复写入。
- 中间任一步失败都必须恢复写入状态。
- 重入调用、并发调用要有明确行为：要么串行化，要么返回明确错误。

完成定义：

- checkpoint 期间新写入被阻塞或明确失败。
- checkpoint 成功后写入恢复。
- checkpoint 失败后写入也恢复。
- 并发 sharp checkpoint 有单测覆盖。

建议验证：

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*Checkpoint|Test.*Sharp' -count=1 -v
```

### P0-05 Buffer Pool 页面驱逐策略

涉及文件：

- `server/innodb/manager/buffer_pool_manager.go`
- 必要时补充 `server/innodb/manager/buffer_pool_manager_test.go`

当前问题：

- `evictPage` 仍是 TODO，当前返回 `nil`。

实现要求：

- 优先驱逐未 pin 的干净页。
- 没有干净页时，选择可驱逐脏页，先 flush 再驱逐。
- pinned 页面不可驱逐。
- 驱逐必须维护 page table、LRU list、dirty 状态和统计信息一致。

完成定义：

- buffer pool 满时可腾出页面。
- pinned 页面不会被误驱逐。
- dirty 页面被驱逐前会刷盘。
- 无可驱逐页面时返回明确失败，不死循环。

建议验证：

```bash
$GO_BIN test ./server/innodb/manager -run 'Test.*BufferPool|Test.*Evict|Test.*LRU' -count=1 -v
```

### P0-06 Enhanced B+Tree 索引重建与删除页面

涉及文件：

- `server/innodb/manager/enhanced_btree_manager.go`
- 可能涉及 `enhanced_btree_index.go`、metadata manager、page allocator

当前问题：

- `RebuildIndex` 直接返回 `index rebuild not implemented yet`。
- `DropIndex` 删除索引页面逻辑仍是 TODO。

实现要求：

- `RebuildIndex` 支持按 index metadata 重新构造索引结构。
- 重建过程必须保证失败不破坏旧索引。
- `DropIndex` 删除元数据前必须释放或标记回收索引页面。
- 删除索引后再次查询该 indexID 应明确报错。

完成定义：

- 可创建索引、写入数据、重建索引、再次查询成功。
- 重建失败时旧索引仍可用或错误边界明确。
- 删除索引后元数据和页面资源状态一致。

建议验证：

```bash
$GO_BIN test ./server/innodb/manager -run 'Test.*EnhancedBTree|Test.*RebuildIndex|Test.*DropIndex' -count=1 -v
```

### P0-07 拆分 engine 包测试

涉及文件：

- `scripts/` 下新增或调整 P0 测试脚本
- 可新增 `docs/planning/P0_ENGINE_TEST_SPLIT_MANIFEST.md`

当前问题：

- `go test ./server/innodb/engine` 整包运行时间不可控。
- 长时间无输出时难以判断是正常慢测、死锁、IO 卡住还是测试设计问题。

实现要求：

- 将 engine 测试拆分为独立 suite：DML、SHOW、checkpoint、executor、persistence、query。
- 每个 suite 有独立命令、超时时间、日志输出。
- 总入口脚本按 suite 串行执行，失败时停止并输出失败 suite。
- 慢测必须显式标记或移入单独命令。

完成定义：

- P0 验证不再依赖裸跑 `go test ./server/innodb/engine`。
- 每个 suite 在合理时间内输出结果。
- 失败时能定位到具体 suite 和测试名。
- 文档记录每个 suite 的覆盖范围和命令。

建议脚本：

```bash
scripts/p0_engine_test_suites.sh
```

建议命令：

```bash
$GO_BIN test ./server/innodb/engine -run 'Test.*DML|Test.*Insert|Test.*Update|Test.*Delete' -count=1 -timeout=60s -v
$GO_BIN test ./server/innodb/engine -run 'Test.*Show|Test.*SHOW' -count=1 -timeout=30s -v
$GO_BIN test ./server/innodb/engine -run 'Test.*Checkpoint|Test.*Sharp' -count=1 -timeout=60s -v
$GO_BIN test ./server/innodb/engine -run 'Test.*Persistence|Test.*WAL' -count=1 -timeout=120s -v
$GO_BIN test ./server/innodb/engine -run 'Test.*Executor|Test.*Select|Test.*Query' -count=1 -timeout=120s -v
```

## 4. 总体验收

本轮完成后，至少要满足：

- `go build ./...` 通过。
- DML、SHOW、Checkpoint、Buffer Pool、Enhanced B+Tree 的定向测试通过。
- P0 engine suite 脚本可重复运行，失败可定位。
- 以下硬未实现文本清零或移出生产路径：
  - `findDuplicateRecord not fully implemented`
  - `SHOW DATABASES 暂未接入真实元数据字典`
  - `SHOW TABLES 暂未接入真实元数据字典`
  - `SHOW COLUMNS 暂未接入真实元数据字典`
  - `TODO: 实现写操作阻塞机制`
  - `TODO: 解除写操作阻塞`
  - `TODO: 实现页面驱逐策略`
  - `index rebuild not implemented yet`

建议总验证命令：

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}
$GO_BIN build ./...
$GO_BIN test ./server/dispatcher -count=1
$GO_BIN test ./server/innodb/manager -run 'Test.*BufferPool|Test.*Evict|Test.*LRU|Test.*EnhancedBTree|Test.*RebuildIndex|Test.*DropIndex' -count=1 -timeout=120s -v
$GO_BIN test ./server/innodb/engine -run 'Test.*DML|Test.*Insert|Test.*Update|Test.*Delete|Test.*Show|Test.*SHOW|Test.*Checkpoint|Test.*Sharp' -count=1 -timeout=180s -v
```

## 5. 风险与处理

- DML 现有实现路径不止一条，改动前必须先确认生产入口，避免只修到旁路。
- 行级读写如果底层 page/record API 不足，不要继续扩大假实现；应先补最小 page slot 读写接口。
- SHOW 元数据不要硬编码模拟结果，必须接真实 metadata/schema manager。
- Sharp checkpoint 如果没有现成写门禁，应新增小范围门禁对象，不要把阻塞语义散落到各写路径。
- Buffer Pool 驱逐涉及状态一致性，必须用测试覆盖 page table、dirty、pin、LRU 变化。
- 索引重建不能原地破坏旧索引；最低要求是失败后状态可解释、可回滚或旧索引仍可用。

## 6. 建议提交切分

1. `p0-dml-operators`
2. `p0-storage-dml-helper`
3. `p0-show-metadata`
4. `p0-sharp-checkpoint-write-gate`
5. `p0-buffer-pool-eviction`
6. `p0-enhanced-btree-index-maintenance`
7. `p0-engine-test-suites`

每个提交必须包含对应测试或验证命令记录。
