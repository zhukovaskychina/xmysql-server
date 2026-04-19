# JDBC 集成测试失败修复计划

**状态**：草案（基于 `mvn test` 全量结果与 Surefire 报告归纳）  
**基线**：全量约 `Tests run: 102, Failures: 1, Errors: 79, Skipped: 1`；`mvn test -Pjdbc-connectivity` 已通过（连接 + 系统变量路径）。  
**目标**：在保持 **JDBC 可连、协议层可用** 的前提下，逐步使全量 `jdbc_client` 测试绿或明确「裁剪范围」。

---

## 1. 背景与范围

### 1.1 已验证能力（不必重复证明）

- 监听 `localhost:3309`（与 `BaseIntegrationTest` / `JdbcConnectionTest` 一致）。
- `JdbcConnectionTest`、`SystemVariableTest`：握手、会话变量、`SHOW`/`SET` 等路径可用（见 profile `jdbc-connectivity`）。

### 1.2 全量失败性质

失败集中在 **连上之后的 SQL 语义、DDL/DML、结果集类型、测试清理方式**，不是「Connector/J 连不上」。

### 1.3 文档范围

- **内核**：DDL/DML、存储集成、协议/结果集。  
- **测试/工程**：`TRUNCATE` 替代、库表隔离、Surefire 稳定性。  
- **不在本计划内**：更换 JDBC 驱动大版本、分布式、非单机目标。

---

## 2. 失败分类（事实来源）


| 类别                  | 典型异常 / 现象                                                | 主要涉及测试类                                                                           |
| ------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------- |
| A. `TRUNCATE` 未实现   | `unsupported DDL action: truncate`                       | `DMLOperationsTest`、`PreparedStatementTest`、`TransactionTest`、`PerformanceTest` 等 |
| B. `INSERT` + 主键/索引 | `生成主键字节失败: 未找到主键列或主键值`                                   | `DataTypeTest`、`SelectQueryTest`、`JoinQueryTest`、`IndexAndConstraintTest` 等       |
| C. `INSERT` 列元数据    | `列数量不匹配: 期望 0，实际 N`                                      | `IndexAndConstraintTest` 等                                                        |
| D. DDL / 多库语义       | `database '…' does not exist`、`table '…' already exists` | `DDLOperationsTest`                                                               |
| E. 结果集类型            | `Not a navigable ResultSet`（对 `SHOW`/查询用了滚动 API）         | `DDLOperationsTest`                                                               |
| F. 测试残留             | 同上「表已存在」、库名冲突                                            | 多类（与 D 交叉）                                                                        |


---

## 3. 修复原则

1. **先稳定清理与前置条件**（减少「表已存在」噪声），再判内核缺陷。
2. **先高扇出、低复杂度**：能用一个改动解锁多类测试的优先（如 `TRUNCATE` 或统一改为 `DELETE`）。
3. **每条合并前**：`mvn test -Pjdbc-connectivity` 必须通过；全量 `mvn test` 目标增量下降（或文档登记已知裁剪）。
4. **验收口径**：见第 6 节；禁止仅「手测通过」而无 Surefire 数字。

---

## 4. 分阶段计划

### 阶段 0：基线与可重复性（0.5～1 天）

**目标**：同一数据目录、连续两次全量失败「类型」一致，不因脏数据漂移。


| 任务                | 说明                                                                | 产出                   |
| ----------------- | ----------------------------------------------------------------- | -------------------- |
| 0.1 固定联调配置        | 使用仓库内 `conf/jdbc_local.ini`（或等价）统一端口、datadir、日志路径                 | 文档化启动命令              |
| 0.2 全量前清理策略       | 评估：独立 datadir per CI job；或测试套件 `@BeforeAll` 建库 + `@AfterAll` DROP | 设计决策记录               |
| 0.3 Surefire 分类统计 | 脚本或表格：按异常 message 聚合计数                                            | 附于本文或 `target/` 生成报告 |


**验收**：两次连续 `mvn test` 的失败 message 分布一致（或仅剩内核类错误）。

---

### 阶段 1：`TRUNCATE` 与表清空（高优先级，1～3 天）

**目标**：消除最大量的 `unsupported DDL action: truncate`。

**方案二选一（或组合）：**


| 方案    | 工作项                                                            | 优点             | 缺点               |
| ----- | -------------------------------------------------------------- | -------------- | ---------------- |
| 1A 内核 | 实现 `TRUNCATE TABLE`（解析 + 执行 + 与存储层语义对齐）                        | 兼容真实 JDBC 应用习惯 | 工作量较大，需权限与锁语义定义  |
| 1B 测试 | 将 `clearTable` 等改为 `DELETE FROM` 或 `DROP TABLE IF EXISTS` + 重建 | 快、易绿           | 与生产语义不完全一致，需文档说明 |


**推荐顺序**：若短期要绿 CI → **1B 先落地** +  backlog **1A**；若以产品对齐 MySQL → 优先 **1A**。

**验收**：`DMLOperationsTest`、`PreparedStatementTest`、`TransactionTest`、`PerformanceTest` 中因 `truncate` 导致的 ERROR 显著下降或为 0。

---

### 阶段 2：`INSERT` / 主键 / 索引元数据（中高优先级，3～10 天）

**目标**：解决 `未找到主键列或主键值` 与 `列数量不匹配: 期望 0`。


| 任务            | 说明                                                         |
| ------------- | ---------------------------------------------------------- |
| 2.1 元数据链路     | 从 `CREATE TABLE` 到执行器/存储集成：`Table`/`Index`/`PK` 在内存模型中是否完整 |
| 2.2 INSERT 路径 | 显式列清单与隐式列、自增、复合主键各一条最小用例（Go 单测 + JDBC）                     |
| 2.3 二级索引更新    | 「更新索引失败」栈对应模块：主键字节编码与索引条目一致性                               |


**验收**：`DataTypeTest`、`SelectQueryTest`、`JoinQueryTest` 至少 **setup 阶段 INSERT** 不再因主键/列数失败；`IndexAndConstraintTest` 部分用例恢复。

---

### 阶段 3：DDL / 多库 / `SHOW` 与 ResultSet（中优先级，2～7 天）

**目标**：减少 `DDLOperationsTest` 中的 `Not a navigable ResultSet`、`database does not exist`、重复建表。


| 任务                         | 说明                                                                      |
| -------------------------- | ----------------------------------------------------------------------- |
| 3.1 ResultSet 类型           | 服务端对 `SHOW DATABASES` 等是否返回 **TYPE_SCROLL_INSENSITIVE** 或测试改为仅 `next()` |
| 3.2 `CREATE/DROP DATABASE` | 与 `USE`、默认库、information_schema 行为对齐测试断言                                 |
| 3.3 幂等与清理                  | `DROP DATABASE IF EXISTS` / `DROP TABLE IF EXISTS` 与测试命名空间              |


**验收**：`DDLOperationsTest` Errors 大幅下降；唯一 Failure（`testDropDatabaseIfExists` 等）按断言逐条对齐。

---

### 阶段 4：回归门禁与文档（持续）


| 任务                  | 说明                                                                                                                                                                                                                                                                                                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 4.1 CI 建议           | PR 阶段跑 `mvn test -Pjdbc-connectivity`；全量 `mvn test` 夜间或主干门禁                                                                                                                                                                                                                                                                                                 |
| 4.2 文档交叉引用（**已落地**） | `jdbc_client/TEST_README.md`、`快速参考.md`、`测试总结.md`；根 `README.md`；`docs/README.md`、`DOCUMENTATION_INDEX.md`；`protocol/` 两修复文 + `PROTOCOL_DOCUMENTATION_INDEX.md`；`protocol-reports/JDBC_PROTOCOL_ANALYSIS.md`；`SESSION_`*、`SHOW_DROP`、`HANDSHAKE`、`ADVANCED_PROTOCOL`、`development/JDBC_DDL_TEST_RESULTS.md` — 均链至本文并注明 `jdbc-connectivity` / `jdbc_local.ini` |
| 4.3 与路线图对齐          | 若存在 TXN/EXE/STG 任务 ID，在实现 PR 中引用                                                                                                                                                                                                                                                                                                                            |


---

## 5. 风险与依赖


| 风险                        | 缓解                               |
| ------------------------- | -------------------------------- |
| 修复 `INSERT` 引入回归          | 每阶段增加/收紧 Go 层单测 + JDBC 最小用例      |
| `TRUNCATE` 语义与事务/MVCC 不一致 | 先文档声明「阶段 1B 非完整 TRUNCATE」，再实现 1A |
| 多库与 datadir 布局            | 与 `schema_manager`、系统表空间路径统一评审   |


---

## 6. 验收标准（汇总）


| 里程碑 | 命令                                                           | 通过条件                                           |
| --- | ------------------------------------------------------------ | ---------------------------------------------- |
| M0  | `mvn test -Pjdbc-connectivity`                               | `BUILD SUCCESS`，0 Failures / 0 Errors          |
| M1  | `mvn test -Dtest=DMLOperationsTest,...`（阶段 1 相关类）            | 无 `truncate` 类 ERROR（若选 1A/1B 达成）              |
| M2  | `mvn test -Dtest=SelectQueryTest,JoinQueryTest,DataTypeTest` | setup INSERT 成功为主                              |
| M3  | `mvn test -Dtest=DDLOperationsTest`                          | `Not a navigable` 与库不存在类问题可控                   |
| M4  | `mvn test`                                                   | **目标**：`Errors` 接近 0；若仍裁剪，须在本文与 TEST_README 登记 |


---

## 7. 修订记录


| 日期         | 修订内容                                                            |
| ---------- | --------------------------------------------------------------- |
| 2026-04-19 | 初版：基于全量 JDBC 失败分类与分阶段修复路线                                       |
| 2026-04-19 | 补充：文档交叉引用一次性收敛（根 README、jdbc_client 中文档、协议/JDBC 长文、修复计划 4.2 状态） |


---

## 8. 相关文件索引

- JDBC 连接专项：`jdbc_client/pom.xml`（profile `jdbc-connectivity`）  
- 集成基类：`jdbc_client/src/test/java/com/xmysql/server/test/BaseIntegrationTest.java`  
- 本地联调配置：`conf/jdbc_local.ini`  
- Surefire 报告：`jdbc_client/target/surefire-reports/TEST-*.xml`  
- 测试说明与速查：`jdbc_client/TEST_README.md`、`jdbc_client/快速参考.md`、`jdbc_client/测试总结.md`；交互菜单 `jdbc_client/run-tests.sh` / `run-tests.bat`（选项 **13** = `jdbc-connectivity`）  
- 仓库入口：`README.md`；文档中心：`docs/README.md`、`docs/DOCUMENTATION_INDEX.md`  
- 协议与 JDBC：`docs/protocol/PROTOCOL_DOCUMENTATION_INDEX.md`、`JDBC_CONNECTION_FIX_SUMMARY.md`、`JDBC_CONNECTION_COMMUNICATION_FIX.md`；`docs/protocol-reports/JDBC_PROTOCOL_ANALYSIS.md`  
- 会话与 DDL 上下文：`docs/SESSION_STATE_AND_PROTOCOL.md`、`docs/SESSION_CONTEXT_DESIGN.md`、`docs/SHOW_DROP_IMPLEMENTATION.md`、`docs/HANDSHAKE_FILES_MERGE.md`、`docs/ADVANCED_PROTOCOL_FEATURES.md`  
- 历史 DDL 结果（与全量现状对照）：`docs/development/JDBC_DDL_TEST_RESULTS.md`

