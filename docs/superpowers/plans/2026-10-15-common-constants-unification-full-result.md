# common 常量全量重构结果

按用户“都完成，而不是只完成一部分”的要求实施全量重构，不再以T1或候选批次作为停止条件。原1576项均已核对并记录最终来源、处理方式、保留理由和验证依据：[逐项JSON](2026-10-15-common-constants-unification-result.json)。没有以新加假引用、删除全部无调用API或合并不同语义数字来凑数量。

## 实现结果

| 范围 | 实现 |
|---|---|
| 事务状态、隔离和告警 | common唯一值来源；manager/MVCC、engine与原事务测试迁移到common；manager公开名称保留兼容转发，无独立值 |
| 协议能力位、字段编号、列标志、服务器状态 | 共享定义及生产消费者统一，保留common/protocol/manager旧API各自静态类型；querypb内部类型和转换规则不变 |
| 错误号与SQLSTATE | ER旧名引用Err来源；45种状态统一命名；237条错误码→状态映射完整快照不变，错误消息模板及可变目录查表保留 |
| 存储程序固定值 | SQLSTATE类别、条件名、handler动作、诊断项和内部控制信号统一；正则解析也消费同一名称；不改变匹配优先级或控制逻辑 |
| binlog | 原生生成与解码消费共同物理事件号，明确pre-GA/V1/V2/partial/GTID；两条生成路径的43字节FDE表统一来源并保持原字节 |
| 存储尺寸、页面类型与布局 | 共享固定尺寸/类型；三种页头分别命名，不混用SpaceID偏移0/34；独立原始头和16KB页样本验证 |
| 字符集和排序规则 | common固定记录派生原公开映射、engine四视图、protocol目录、dispatcher SHOW及schema支持策略；各入口ID33/255、子集、顺序、数值类型及真实编码行为由显式投影保留 |
| 权限 | 固定记录派生映射/集合/SHOW/I_S ALL/mysql.user标志/auth名称子集；动态登记仍运行时读取，保留PROXY/CONFIG及不同scope差异，不扩大授权 |
| 工具和公开目录 | DDL/CAST共享字段记录及覆盖策略，协议列定义也消费共享基础值并显式覆盖；旧列转换保留INT=INT24约定；命令号与名称集中且接入日志实际消费者；Unicode、公开错误和系统目录按职责保留 |

补充完成了原表未直接列出的实际消费者：`protocol/charset_manager.go`、`dispatcher/system_variable_engine.go`、`manager/schema_manager.go`、`engine/account_metadata_compatibility.go`及`auth/account_file_access.go`。这些路径没有继续维护另一套元数据事实。

## 兼容检查

[编译器API审计](2026-10-15-common-constants-unification-api-audit.json)实际读取当前Go编译器导出的常量，与原清单全部1216个common声明比较精确值及静态类型。没有意外变化；唯一明确调整仍是`common.TRX_STATE_NOT_STARTED`由无类型整数1改为uint8的0，manager原编码不变。可复核：

```powershell
$goExe = 'C:\Users\zx187\go\go1.27.2\bin\go.exe'
& $goExe run ./scripts/common_constants_audit -go $goExe -output audit.json
```

新增契约包括：协议握手/OK/游标字节、完整querypb类型转换、SQLSTATE全表摘要、三种原始页面布局、原元数据完整行及类型、256种byte字段默认值、协议46字符集/17排序规则与实际编码、不同入口子集、权限scope顺序和mysql.user逐列授权矩阵。

保留的旧公开常量名属于兼容接口，其表达式引用共同定义，不重新写值。没有将未知工程外依赖视为不存在；无引用的独立公开目录仍按原职责保留。缓冲池页数量、可配置页大小、querypb内部标志和不同磁盘格式不因数值相同被合并。

## 验证记录

工具链为`go1.27.2 windows/amd64`；`go.mod`仍为原版本。生产数据、外部数据库、提交、推送及部署均未执行。

- common、protocol、sqltypes、auth全套测试及存储全部包测试通过。
- 事务、MVCC、保存点、长事务、信息模式、账号、动态权限、字符集与权限消费者契约通过。
- 存储程序/Procedure/Signal/Diagnostics聚焦测试通过。
- replication和net全套测试通过。
- 全仓`go test ./... -run '^$'`编译检查通过，覆盖100个包。
- manager最终全套测试通过，未排除压缩测试；压缩封装与不压缩回退两条路径的测试均通过。
- engine单进程全套先后受3分钟、15分钟总时限影响，改为按完整清单分组执行1899个测试，逐个核对PASS/SKIP/FAIL和遗漏数量；没有省略测试以获得通过结果。
- engine最新完整清单分组执行：1897通过、2项原有跳过、0失败、0遗漏。两项调度测试只增加失败时的状态抓取，原断言和生产代码不变；通过过程中未触发抓取。原正常运行的1895通过/2失败也完整保留在[逐例记录](2026-10-15-common-constants-unification-engine-tests.json)中。
- 临时抓取代码已移出源码包，原测试文件按字节恢复；重新编译的正常测试程序与先前正常程序SHA-256完全一致。最终不带抓取的第18批100项全部通过，退出码0。
- `git diff --check`通过，改动Go文件的`gofmt -l`无输出；测试生成数据单独保存并恢复原本干净的已跟踪测试fixture。

原`TestCompressedStorageProviderUsesFixedPageEnvelope`在HEAD和重构后均失败，修正前生产文件及该测试的SHA-256与HEAD相同。32个`x`在当前工具链下可能压成更大的流，原代码按`MinSavings=0`正确回退原文，因此该样本不能保证进入压缩分支。最终只修正测试：用512个重复字节验证固定页封装、解压读回及压缩元数据，保留全部原断言；新增32个不重复字节验证原文回退及原样读回。生产压缩算法、设置和封装行为未改，两种相反分支均独立通过，manager全套也通过。

engine测试的限制与对照：

- `TestSQLEventExecutesDoStatementWhenSchedulerIsEnabled`和`TestSQLEventProgramSummaryTracksExecutionResults`在较早第18批中超时失败；两个用例单独同跑时，HEAD与当前均通过。HEAD同一100例非verbose运行有失败和通过两轮，匹配当前verbose/直接程序运行方式的一轮100项通过。后续当前同批3轮诊断、完整1899项诊断及最终正常100项均通过。未捕获到新的失败状态，故不声称原两次失败已完成因果归因或已由某项修改稳定修复；所有轮次均保留。
- 15分钟单进程运行还出现`TestEngineSourceReplicaReconnectsAfterSourceRestartWithoutDuplicateTransactions`的时间断言失败；完整分组中此例通过。单独对照时HEAD失败、当前通过，底层错误不同，不据此认定同一根因，也不把重试成功当成稳定性修复。
- 调度器、事件runtime、Close/ExecuteQuery与测试fixture等5个直接相关文件的SHA-256与HEAD相同。`Eventually`超时不取消或等待condition goroutine，故清理后的shutdown可能是次生症状。INSERT非原子写FRM与SELECT并发读解析的两处源码也与HEAD相同，是不完整JSON的合理机制，尚未证明是上述两次超时的直接原因。完整诊断按批保存了文件变化；固定`testdata/innodb`与`manager/data`来自原有使用固定/空配置的用例，未发现临时目录被覆盖或常量替换了动态尺寸字段。未改生命周期、写盘逻辑或超时阈值。
- 两项原有跳过为`TestVolcanoExecutor_BuildSelection`和`TestVolcanoExecutorBuild`，原因分别为需要实际manager实例和完整集成环境。

[运行测试对照记录](2026-10-15-common-constants-unification-runtime-comparison.json)保留全部轮次和源码哈希。全量代码范围、常量/目录兼容检查及最终验证已完成；历史时序不稳定性仍作为交付风险保留，不能把后续通过表述成调度器稳定性修复，也不能宣称完整单进程套件已稳定全绿。

独立审查核对了协议/错误/事务/binlog及扩展字符集、权限消费者。审查发现的事件号遗漏和FDE重复表已修复，修复后相关测试通过。未改事件调度和压缩逻辑来使测试变绿。

## 证据与文件保护

原始HEAD为`b15d304811beecdd673a1f48c3ef486723d14168`。历史清单保留原行号、值和哈希，最终JSON记录当前源码哈希，便于区分原基线和交付代码。

本地完整执行日志、逐项分工记录、修改前快照和独立原始HEAD对照目录位于`.superpowers/full-unification/`。IDE配置、Java示例和用户原有未跟踪目录不覆盖；IDE配置执行期间另有变化，未以旧快照覆盖，Java示例哈希与实施前相同。测试生成的工作区数据与源码/契约fixture分别处理。
