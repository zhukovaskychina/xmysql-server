# 数据库管理器与表管理器分析

## 1. 概述

本文档分析 xmysql-server 中**数据库管理器**与**表管理器**的现状：是否实现、职责划分、依赖关系及集成缺口，并给出更新建议与实现说明。

## 2. 结论摘要

| 组件           | 是否实现 | 实现形式/位置 | 说明 |
|----------------|----------|----------------|------|
| **表管理器**   | ✅ 已实现 | `TableManager`（`server/innodb/manager/table_manager.go`） | 表元数据/统计/索引缓存，Create/Drop/Get 表，依赖 InfoSchemaManager + 可选 StorageManager/TableStorageManager |
| **数据库管理器** | ✅ 功能已有，无独立类型 | **SchemaManager**（`server/innodb/manager/schema_manager.go`）承担数据库级管理 | CreateDatabase/DropDatabase/GetAllSchemas/GetSchemaByName、数据字典持久化、与 SimpleDatabase 协作 |
| **统一入口**   | ✅ 已修复 | `StorageManager` 支持注入并返回子管理器 | 引擎在 `initQueryExecutor` 中注入 TableManager/TableStorageManager/IndexManager/TransactionManager/BTreeManager，集成层可通过 `GetTableManager()` 等获取 |

---

## 3. 表管理器（TableManager）

### 3.1 实现位置与职责

- **文件**: `server/innodb/manager/table_manager.go`
- **职责**:
  - 表元数据缓存（`tableMetaCache`）、表统计信息缓存（`tableStatsCache`）、表索引缓存（`tableIndexCache`）
  - **CreateTable** / **DropTable**（委托 `InfoSchemaManager`，并维护缓存）
  - **GetTable**、**GetTableMetadata**、**GetTableIndices**、**GetTableStats**、**UpdateTableStats**、**RefreshTableMetadata**
  - 可选：**GetTableBTreeManager**、**GetTableStorageInfo**（依赖 `TableStorageManager`）

### 3.2 依赖关系

```
TableManager
  ├── schemaManager   metadata.InfoSchemaManager  （必需）
  ├── storageManager  *StorageManager             （可选，用于 TableStorageManager）
  └── tableStorageManager *TableStorageManager    （可选，由 NewTableManagerWithStorage 内建）
```

### 3.3 创建与注入

- 在 **enginx**（`server/innodb/engine/enginx.go`）中通过 `manager.NewTableManagerWithStorage(e.infoSchemaManager, e.storageMgr)` 创建。
- 通过 `QueryExecutor.SetManagers(..., tableManager, ...)` 注入到 **XMySQLExecutor**。
- **已**在 `initQueryExecutor` 中通过 `e.storageMgr.SetTableManager(tableManager)` 注入到 **StorageManager**，集成层可从 `StorageManager.GetTableManager()` 获取。

### 3.4 使用方

- `StorageAdapter`、`UnifiedExecutor`、`SelectExecutor`、`DMLExecutor`、`StorageIntegratedDMLExecutor` 等均依赖 `*manager.TableManager`。
- 集成层 `ExecutionEngineIntegrator` 通过 `eei.storageManager.GetTableManager()` 获取；注入后该路径可拿到有效实例。

---

## 4. 数据库管理器（概念对应：SchemaManager）

### 4.1 是否有独立的 “DatabaseManager”

- 代码中**没有**名为 `DatabaseManager` 的类型。
- **数据库（Schema）级管理**由 **SchemaManager** 统一承担，可视为当前实现中的“数据库管理器”。

### 4.2 SchemaManager 的职责（数据库级）

- **文件**: `server/innodb/manager/schema_manager.go`
- **主要能力**:
  - **CreateDatabase** / **DropDatabase**（含 `ifNotExists` / `ifExists`）
  - **GetSchemaByName**、**GetAllSchemas**、**GetAllSchemaNames**
  - **GetAllSchemaTablesByName**、**GetTableByName**、**GetTableExist**
  - 从文件系统与数据字典加载/同步数据库：**loadDatabase**、**loadSingleDatabase**、**syncDatabasesWithDataDictionary**
  - 持久化数据库元数据：**persistDatabaseMetadata**、**writeDatabaseToDataDictionary**、**removeDatabaseFromDataDictionary**
- **数据结构**: `schemaMap map[string]metadata.Database`，值为 `SimpleDatabase` 等实现。

### 4.3 SimpleDatabase 与表文件

- **SimpleDatabase** 负责单库下表级文件与缓存：
  - 表定义与表空间：`CreateTable`、`DropTable`、`createTableFile`、`createTablespace`、`deleteTablespace`
  - 表列表与缓存：`ListTables`、`ListTableName`、`GetTable`、`loadTableFromFilesystem`、`loadAllTablesFromFilesystem`

### 4.4 与 InfoSchemaManager 的关系

- **InfoSchemaManager** 实现接口 `metadata.InfoSchemaManager`，面向“逻辑 Schema/Table”和 information_schema 查询。
- **TableManager** 依赖的是 **InfoSchemaManager**，而不是直接依赖 SchemaManager。
- 引擎层（enginx）当前使用 **InfoSchemaManager**（基于 DictionaryManager + SpaceManager + IndexManager），与 **SchemaManager**（基于文件系统 + 数据目录）是两套数据源；两者在“数据库/表”的创建与删除上需要保持一致，否则会出现元数据不一致。

---

## 5. 其他相关管理器

| 组件 | 职责 | 与数据库/表管理的关系 |
|------|------|------------------------|
| **TableStorageManager** | 表名 ↔ SpaceID/根页等存储信息映射，系统表预置映射 | 被 TableManager 用于 GetTableBTreeManager / GetTableStorageInfo |
| **DictionaryManager** | 数据字典（表定义等） | InfoSchemaManager 从字典加载表/Schema 信息 |
| **StorageManager** | 表空间、段、缓冲池、页、系统表空间、字典、系统变量等；并持有可选注入的 TableManager/TableStorageManager/IndexManager/TransactionManager/BTreeManager | 引擎初始化时通过 SetXxx 注入，GetXxx 返回注入实例（未注入时为 nil） |

---

## 6. 问题与缺口

### 6.1 ~~StorageManager 未暴露子管理器~~（已修复）

- **原问题**: GetTableManager() 等均返回 `nil`，集成层无法从 StorageManager 获取实例。
- **现状**: StorageManager 已支持 SetXxx/GetXxx，引擎在 initQueryExecutor 中完成注入，集成路径可正常获取。

### 6.2 数据库管理器命名与统一入口

- “数据库管理器”功能由 **SchemaManager** 实现，但无 `DatabaseManager` 类型，命名上不够直观。
- 若希望“数据库管理器”有统一入口，可任选其一：
  - **方案 A**：在文档中明确 **SchemaManager = 数据库管理器**，不新增类型。
  - **方案 B**：新增 **DatabaseManager** 类型，委托 SchemaManager 的 CreateDatabase/DropDatabase/GetSchema 等，对外提供统一接口。

### 6.3 执行器与 SchemaManager 的衔接

- 部分 DDL（如 `dropDatabaseImpl`）在 executor 中直接用文件系统操作，未经过 SchemaManager，可能导致与 SchemaManager 的 schemaMap/数据字典不同步。建议 CREATE/DROP DATABASE 统一走 SchemaManager。

---

## 7. 已完成的更新（管理器注入）

为修复集成层从 StorageManager 获取管理器的缺口，已做如下更新。

### 7.1 StorageManager 支持可选管理器注入

- 在 **StorageManager** 中增加可选字段（均为指针，可为 nil）：
  - `tableManager *TableManager`
  - `tableStorageManager *TableStorageManager`
  - `indexManager *IndexManager`
  - `transactionManager *TransactionManager`
  - `btreeManager basic.BPlusTreeManager`
- 新增 **SetXxx** 方法，供引擎在初始化时注入：
  - `SetTableManager(*TableManager)`
  - `SetTableStorageManager(*TableStorageManager)`
  - `SetIndexManager(*IndexManager)`
  - `SetTransactionManager(*TransactionManager)`
  - `SetBTreeManager(basic.BPlusTreeManager)`
- 原有 **GetXxx** 方法改为返回上述字段值，不再恒返回 nil。

### 7.2 引擎侧注入（enginx）

- 在 **initQueryExecutor** 中，创建完 **TableManager**、**TableStorageManager** 后，调用：
  - `e.storageMgr.SetTableManager(tableManager)`
  - `e.storageMgr.SetTableStorageManager(tableStorageManager)`
  - `e.storageMgr.SetIndexManager(e.indexManager)`
  - `e.storageMgr.SetTransactionManager(e.txManager)`（enginx 持有 `e.txManager`，已注入）
  - `e.storageMgr.SetBTreeManager(e.btreeMgr)`
- 这样通过 `StorageManager` 获取 TableManager/TableStorageManager/IndexManager/TransactionManager/BTreeManager 的集成路径即可拿到正确实例。

### 7.3 数据库管理器命名（可选）

- 若采用**方案 B**，可新增 `DatabaseManager` 结构体，内部持有 `*SchemaManager`，对外提供：
  - CreateDatabase / DropDatabase / GetSchemaByName / GetAllSchemas 等委托方法。
- 若采用**方案 A**，仅在文档与注释中说明 **SchemaManager 即数据库管理器**，无需改代码。

---

## 8. 模块与文件索引

| 角色 | 文件/包 |
|------|----------|
| 表管理器 | `server/innodb/manager/table_manager.go` |
| 数据库级管理（SchemaManager） | `server/innodb/manager/schema_manager.go` |
| 表存储映射 | `server/innodb/manager/table_storage_mapping.go` |
| 存储管理器 | `server/innodb/manager/storage_manager.go` |
| 信息模式/字典视角 | `server/innodb/manager/info_schema_manager.go`，`metadata.InfoSchemaManager` |
| 引擎初始化与注入 | `server/innodb/engine/enginx.go` |
| 执行器使用 TableManager | `server/innodb/engine/executor.go`，`select_executor.go`，`storage_adapter.go`，`unified_executor.go` |
| 集成层获取管理器 | `server/innodb/integration/execution_engine_integration.go` |

---

## 9. 审核意见（2025-03-01）

### 9.1 文档与代码一致性

- **已修正**：结论摘要、3.3/3.4、第 5 节表格、第 6.1 节、7.2 节中“修复前”表述已更新为与当前实现一致（统一入口已修复、注入关系已写明）。

### 9.2 代码问题（与文档无关，建议单独修）

- **TableManager.CreateTable 错误**（`table_manager.go`）：第 68–69 行、78–85 行使用字面量 `"table_name"`，应使用参数 `table.Name`（或从 `table` 取表名），否则创建任意表都会检查/缓存为同一 key，属于逻辑 bug，建议单独提交修复。

### 9.3 可选改进

- **StorageManager Get 方法并发**：当前 `GetTableManager()` 等直接读字段未加锁；同文件内 `GetSystemSpaceManager()` 等使用了 `sm.mu.RLock()`。若希望风格一致或未来允许运行期替换管理器，可对 Get/Set 加 RLock/Lock；若仅启动时注入、之后只读，现状可接受。
- **SchemaManager 与执行器 DDL**：文档 6.3 已说明 `dropDatabaseImpl` 等直接操作文件系统未经 SchemaManager，建议后续将 CREATE/DROP DATABASE 统一走 SchemaManager，以保持 schemaMap 与数据字典一致。

### 9.4 结论

- 分析结论正确：表管理器已实现，数据库级管理由 SchemaManager 承担，无独立 DatabaseManager 类型。
- 管理器注入方案已实现且与文档一致；文档已按“修复后”状态更新，可直接作为当前架构说明使用。

---

## 10. 修订记录

| 日期 | 说明 |
|------|------|
| 2025-03-01 | 初版：分析数据库/表管理器现状；补充 StorageManager 管理器注入方案与引擎侧注入说明。 |
| 2025-03-01 | 审核：更新结论摘要与各节表述与代码一致；补充审核意见（文档一致性、TableManager.CreateTable bug、可选改进）。 |
