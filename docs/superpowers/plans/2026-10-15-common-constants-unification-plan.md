# common 常量分批统一方案

> **本方案已被后续全量要求替代。** 用户明确要求全部完成，当前实施范围以[全量计划](2026-10-15-common-constants-unification-full-plan.md)和[全量结果](2026-10-15-common-constants-unification-full-result.md)为准。下文保留分批方案及T1阶段记录，不再表示当前停止条件。

> **状态：T1首批已实施并完成本批验证（2026-10-15）。** 11个事务常量已统一数值来源，manager保留转发；详见[T1实施记录](2026-10-15-common-constants-unification-t1-result.md)。后续批次仍为候选，原全工程同时迁移的安排由本方案替代。
>
> 全量[排查清单](2026-10-15-common-constants-unification-inventory.md)及[JSON证据](2026-10-15-common-constants-unification-inventory.json)继续保留；除首批明确列出的事务常量外，清单里的旧目标和处置均为历史建议，不作为当前执行指令。

## 1. 目标和本次调整

长期目标仍是共享常量有唯一数值来源，实际调用方逐步统一引用。实施按常量族拆成可以独立验证、独立撤回的小批次，每次只处理一个明确问题。

当前只细化首批事务常量。后续批次是候选路线，不要求一次完成全部1216个常量，也不因一个批次完成而自动开始下一批。

用户于2026-10-15要求开始重构，现已按本方案完成T1首批。实施不包含提交、推送、部署或生产数据修改。

调整原则：

- 第一阶段统一数值来源；调用方名称迁移在独立批次完成。
- 允许临时兼容别名，但别名只能引用唯一来源，不能重新写数字；必须登记删除条件。
- 保留函数、字典、字段类型、协议行为、磁盘布局、默认策略及既有权限位。
- 无引用只作为调查证据，不自动触发删除。
- 每批记录准确文件范围；出现范围之外的改动需求时先修订该批范围，不顺带修改。
- 保护用户原有修改，不进行整仓格式化或机械数字替换。

## 2. 首批：只统一事务常量的数值来源

### 2.1 当前问题

`server/common/const_trx.go:3` 定义 `TRX_STATE_NOT_STARTED = 1`，工程内未发现消费者。

`server/innodb/manager/transaction_manager.go` 维护实际使用的事务枚举：未开始0、活动1、准备2、提交3、回滚4；隔离级别从0到3。引擎、MVCC和测试大量使用manager名称。

直接把所有调用点迁入common会造成大范围差异。首批采用“两处定义变一处数值来源，manager名称转发”的方式，保持现有调用点和运行值。

### 2.2 文件范围和明确结果

**生产代码只允许修改两个文件：**

1. `server/common/const_trx.go`：提供下表11个常量的唯一数值/字符串定义。
2. `server/innodb/manager/transaction_manager.go`：原11个常量名保留，定义表达式改为对应的 `common.X`；只增加必要import，不修改事务实现。

| 常量 | common目标值 | 目标类型 / 不变量 |
|---|---:|---|
| `TRX_STATE_NOT_STARTED` | 0 | `uint8`，与原manager值一致 |
| `TRX_STATE_ACTIVE` | 1 | `uint8` |
| `TRX_STATE_PREPARED` | 2 | `uint8` |
| `TRX_STATE_COMMITTED` | 3 | `uint8` |
| `TRX_STATE_ROLLED_BACK` | 4 | `uint8` |
| `TRX_ISO_READ_UNCOMMITTED` | 0 | `uint8` |
| `TRX_ISO_READ_COMMITTED` | 1 | `uint8` |
| `TRX_ISO_REPEATABLE_READ` | 2 | `uint8` |
| `TRX_ISO_SERIALIZABLE` | 3 | `uint8` |
| `LONG_TXN_LEVEL_WARNING` | `"WARNING"` | 无类型字符串常量，保留原行为 |
| `LONG_TXN_LEVEL_CRITICAL` | `"CRITICAL"` | 无类型字符串常量，保留原行为 |

manager转发示例：`TRX_STATE_ACTIVE = common.TRX_STATE_ACTIVE`。不再独立写 `iota` 或数字，避免产生第二套值。

**重要区别：** 首批后是“数值来源统一”，尚未达到“全部调用点直接引用common”；交付时必须如实区分。

`common.TRX_STATE_NOT_STARTED` 将由旧值1改为0，同时从无类型整数变为uint8；这是现有公开声明的调整，不能宣称所有导出API完全不变。工程内未找到旧common声明的使用者，实施前仍需检查新增引用及已知工程外消费者。如果已有消费者依赖旧值1，就不能直接按本批方案更改，须先调整兼容策略；manager现有状态值绝不能改变。

### 2.3 暂不做的改动

首批不迁移engine、MVCC、示例或测试里的manager调用名；不删除manager名称；不引入新的事务状态或隔离类型；不调整事务逻辑、持久化、错误处理、长事务算法和告警内容。

不修改其他common文件，不整理字段类型、页面常量、字符集、权限或SQLSTATE。

### 2.4 T1执行记录

- [x] 记录实施前HEAD、工作区改动、两个定义文件的哈希及旧common声明的全部引用；确认和方案基线一致。
- [x] 在未改代码前运行下列现有测试，记录退出码及结果。
- [x] 将11个现有manager常量的值迁入 `const_trx.go`，保留表中类型；manager改为直接引用common的转发定义。
- [x] 检查差异：只允许两个生产文件中的常量块、必要import和说明注释变化，任何函数体变化均不属于首批。
- [x] 运行同一组测试，并编译所有server包及其测试代码；核对状态、隔离和告警值未偏移。
- [x] 检查现有测试是否已覆盖常量兼容边界；新增 `server/common/const_trx_test.go`，独立列出预期0..4、0..3及两种字符串，检查common和manager的值、数字类型及字符串的无类型性质。
- [x] 在[T1实施记录](2026-10-15-common-constants-unification-t1-result.md)登记结果及11个manager临时别名，首批结束，不自动迁移其他常量族。

### 2.5 验证命令和判定

按实现前后各运行一次：

```powershell
go test ./server/common -count=1
go test ./server/innodb/manager -run '^(TestTransactionCommitPersistsStableIdentityInRedoRecord|TestTransactionIsolationAnomalyMatrix|TestTransactionManagerReportsActiveRollbacks|TestSavepoint_.*|TestMVCC_.*|TestLongTransaction.*|TestGetLongTransactions)$' -count=1
```

实施完成后做跨包编译：

```powershell
go test ./server/... -run '^$'
git diff --check
```

本机已发现的工具链为 `C:\Users\zx187\go\go1.27.2\bin\go.exe`；若PATH无go，使用完整路径，记录实际工具链。Go模块仍声明1.24，不因本次整理更新go.mod。

验收标准：

- 只有common定义事务数值；manager保留的11个名称全部为转发，无独立值。
- 原manager常量的值、静态类型和所有现有调用点保持一致。
- Begin/Commit/Rollback、隔离级别、MVCC、保存点及长事务相关检查无新增回归。
- 所有server包和测试能完成编译；已有失败或环境限制逐项记录，不能写成全部通过。
- 差异中没有用户原有文件被覆盖、无关代码变化或整仓格式化。

### 2.6 独立撤回方式

实施前保存两个目标文件的精确内容和哈希，若新增测试也登记其新文件状态。需要撤回时只逆向应用首批差异，恢复原有常量及必要import；如果文件随后有其他修改，按差异逐段撤回，不能用整文件覆盖抹掉他人修改。

本批不改数据，不涉及磁盘/数据库迁移；撤回不依赖后续批次。

## 3. manager临时别名的后续去向

临时别名集中保留在manager原常量位置，注明来源和迁移状态，禁止在其他包新增转发层。

| 子批次 | 工作 | 范围控制 | 别名是否保留 |
|---|---|---|---|
| T1 | 首批唯一数值来源 | 上述两个生产文件，必要时一个测试文件 | 保留11个manager转发名 |
| T2a | 迁移manager/MVCC内部调用 | 仅manager消费者；不和协议整理混做 | 保留，兼容engine等调用 |
| T2b | 迁移engine调用 | 仅事务状态/隔离/告警引用，不改执行逻辑 | 保留，直到全部其他调用迁完 |
| T2c | 迁移剩余测试和示例，确认接口边界 | 按一个目录一批，已有示例语法问题单独记录 | 符合下列条件后移除 |

删除条件：

1. 工程内生产代码、测试、可构建示例不再引用manager旧常量名称；无法解析的旧示例也完成定向引用核查。
2. 已知工程外兼容要求已经核实；若需保留公开别名，单独记录原因，不宣称旧名已全部清理。
3. 删除别名后完成相关包测试及server跨包编译。

后续调用迁移另行明确文件范围、基线和验证，不把本表视为立即执行指令。

## 4. 其他常量族的候选批次

每个候选批次在开始前重新检查源码和引用，并生成该批准确清单。不要直接执行全量附件中的历史删除或重构建议。

| 候选批次 | 只解决的问题 | 暂不包含 |
|---|---|---|
| P1 客户端能力位 | 合并common与protocol的CLIENT_*数值来源；必要时先保留转发名 | 握手能力增减、元数据协议行为改变 |
| P2 字段类型编号 | 合并Type*/COLUMN_TYPE_*/MYSQL_TYPE_*的同义编号，覆盖protocol、manager与sqltypes | 移动编码器函数、重做DDL/CAST默认表、改变querypb内部类型 |
| P3 列属性位 | 合并FLAG_*与common字段位及sqltypes四个私有位，保持原接口类型转换 | 清理解析器内部标志或改变权限位 |
| P4 服务器状态位 | 统一SERVER_STATUS等现有位的来源 | 改变EOF/OK/游标状态行为 |
| E1 错误号与默认状态 | ER_*引用现有Err*；HY000默认来源统一 | 删除错误目录、重新分类错误、改SIGNAL行为 |
| E2 SQLSTATE与异常控制固定值 | 固定状态、类别、handler动作和诊断项逐组命名 | 改变handler匹配优先级、增加支持分支或新控制模型 |
| B1 binlog事件号 | 让现有事件生成与读取引用同一事件目录，明确V1/V2 | 新增复制功能或全量替换所有数字 |
| S1 页面尺寸 | 合并确认同义的固定页尺寸、头尾尺寸 | 修改可配置页面大小或合并不同格式 |
| S2 页面类型与旧别名 | 迁移确认同义的FIL_PAGE/FILE_PAGE类型名称 | 变更类型编号或页面布局 |

P2与P3分别验证，不能因引用关系又合成一次大范围迁移。必要兼容别名只引用共同定义，并有自己的消费者迁移与删除条件。

每个批次的交付包括：该批旧名到新名、原值和原类型、具体文件、真实消费者、验证结果、未覆盖部分和独立撤回方式。文件范围明显扩大时继续拆批，不能以“只是常量替换”为由跳过检查。

## 5. 移出当前实施路线的独立事项

以下内容保留排查结果，后续单独提出需求和设计，不作为常量统一的附带任务：

- **字符集和排序规则数据重建：** common、protocol、I_S与SHOW仍需一起评估。ID255的PAD属性/Sortlen冲突、ID33的utf8/utf8mb3名字及不同可见集合继续保留为证据；本次不实现兼容投影或新的目录接口。
- **权限元数据重建：** 不实现StaticPrivilegeDefinitions，不重写SHOW PRIVILEGES，不批量删除权限集合或工具。
- **页面布局整理：** 旧BasePage的SpaceID偏移0、UnifiedPage的34属于不同格式；不创建统一布局API或迁移这些偏移。未来设计必须分别使用原字节样本验证。
- **字段默认值工具整理：** 不搬迁getColumnTypeInfo、不新增GetProtocolColumnTypeInfo，不删除util.go的DDL/CAST工具或旧转换函数。
- **无用代码和公开API删除：** RowType*、无调用helper、内部标志、旧系统表名等只作为候选清单，不自动移除。
- **行格式与错误修复：** 不给字典默认编码1补设未经确认的语义，不修复SIGNAL分类、SQLSTATE格式等相邻行为问题。

这些事项完成独立评估之前，不能将全工程描述为“常量已经全部统一”。

## 6. 全量附件的定位和使用方法

附件仍记录基线 `b15d304811beecdd673a1f48c3ef486723d14168` 的1576项排查信息：原common常量1216项、补充声明205项、类型转换表53处、SQLSTATE值45种、条件/控制字符串14种、字典与工具43项。

这组数字表示排查覆盖，不是本次改动量，也不是所有定义都要删除或搬迁。引用证据是源码静态证据，不代表运行入口可达。

本轮为每项增加批次状态：

- `first_batch_proposal`：仅首批事务常量，共12个原声明，对应11个目标值；仍为待实施方案。
- `future_batch_candidate`：重复定义的后续小批候选；旧目标需在该批开始前复核。
- `separate_design_required`：字符集、权限、页面布局、工具删除等独立事项。
- `catalog_retained_no_change_planned`：保留的完整目录或目前不安排修改的定义。

附件的 `historical_action/historical_target_*` 保留旧大方案的建议，便于追溯；当前动作只以 `current_action` 和主方案批次范围为准。Markdown旧目标列也标记为历史建议，不能直接作为实施清单。

每次开始新批次，只提取该批相关条目，检查源文件哈希、类型、值、消费者和实际差异；不要求先改完全部清单。

## 7. 方案评审和后续实施边界

当前交付包含T1的两个生产文件改动、一个兼容性测试文件及实施记录；全量附件及其JSON继续保留原基线排查证据。

T1已完成数值来源统一，现有调用方仍引用manager名称。其他调用迁移、协议常量或存储常量均需独立确定批次范围。任何批次都不要求以全工程重构作为完成前提。
