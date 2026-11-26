# XMySQL 数据访问与一致性契约

版本 0.8  统一 SQL 规范性设计草案  2026 年 11 月 25 日

## 当前已验证范围（2026-11-26）

- `VECTOR_SEARCH`、`CONTENT_TEXT_CHUNKS`、`FULLTEXT_TOKENS` 已有共享的固定列定义，支持不执行模型的 PREPARE 描述和带类型的空结果。真实 MySQL 8.4.13 的当前 50 条查询、文本及原生预处理两种协议共 100 项差分全部通过，包含错误结果元数据存在性对照；这只是有限矩阵，不代表完整 MySQL 兼容性。具体证据及范围见末尾更新和 `client_compatibility/MYSQL84_DIFFERENTIAL.md`。
- 原生 PREPARE 的 SELECT/WITH 元数据获取已改为读取 AST 与表目录，不执行查询或调用模型；已知 AI 函数能提前返回结果类型，未知静态类型则延后到 EXECUTE 返回元数据。JOIN 的 AI 结果类型推断也不再额外调用模型。覆盖范围和暂缺的静态元数据能力见末尾验收记录。
- 普通 Go、PyMySQL 和 Connector/J 客户端已验证 CONTENT CRUD、二进制元数据、有界分段读取和回滚；Go/JDBC 还验证了 2 MiB+37 字节长参数上传。具体驱动、协议与限制见末尾验收记录。
- 已验证停机物理备份恢复到独立新目录：源目录被移开，恢复库能够读取相同原始字节、摘要、逻辑版本、来源绑定块，并在后续原件更新时使派生块失效。分段文件被篡改时，JOIN 内容读取明确报错。
- 单列来源主键已在普通 SELECT、JOIN、INSERT 来源绑定及派生失效维护间统一；物理索引键格式保持原样。早期原型产生的不一致来源引用需要重新派生，本轮不自动重解释旧来源引用。
- 数据库 CONTENT 目录内新增持久维护队列：来源/处理器版本/配置摘要幂等入队、校验和与不可变代际、claim lease fencing、过期重领、heartbeat、退避重试、失败和完成状态均有测试。文本 CONTENT 的 SQL DML 意图随事务 journal 保存，回滚不入队，提交后入队，恢复路径可按持久意图补投；覆盖了事务提交/回滚、journal 往返和恢复重放定向测试。尚无工作线程消费、派生结果存储/发布与 source-version fencing、SQL 状态查询、失败任务运维重放、保留清理及跨实例复制，不能视为完整异步处理或 U05 完成。
- 当前验证不等于在线备份、崩溃恢复、续传、多客户端并发、内存上界、全部多模态解析或端到端 RAG 质量已完成。完整目标规范及早期进度记录保留在下文；判断当前实现状态时应结合最近的验收记录。

本文约束普通 Java 和 Python 数据库客户端最终如何通过一套 SQL 访问结构化记录、CONTENT 列、派生视图、索引及模型上下文。原始、规范内容、检索和上下文表示由同一数据库管理。本版保留前版的完整性、新鲜度、一致性、类型保真和表示转换要求，收敛专用 API 与行业命名；配套[内核建设计划](2026-11-25-xmysql-multimodal-ai-agent-plan.md)定义产品范围与工作包。规范中的类型、函数、目录和状态是目标契约；当前分支仅实现其中一部分原型，尚未完成端到端验收。`content.SegmentStore` 提供数据库目录内的不可变分段写入和范围读取，`SegmentStore.NewWriter` 可增量接收分块并限制单段内存；被识别为 `CONTENT_FROM_BYTES(?, '<literal MIME>')` 的预处理长参数现已接入增量写入器，并受 `[innodb] content_max_object_bytes` 单对象上限（默认 1 TiB）约束。`COM_STMT_SEND_LONG_DATA` 不返回响应，传输错误暂存到语句并在对应 execute 响应中报告；reset、statement close 和连接关闭会尝试中止未提交的 writer，语句执行后未被 DML 行采纳的已发布对象会由协议层清理。引擎崩溃恢复后会清除未发布的 `.staging-*` 写入目录；已接入启动期已发布对象回收原型，在事务恢复后扫描已映射用户表中的 CONTENT 引用，只回收确认无引用的对象；存在保留 XA 清单或无法确认 CONTENT 表映射完整时会跳过。该路径尚未验证协议行为、内存上界和事务故障边界，也未纳入备份/快照保留规则，不能视作 U09 完成；其他长参数仍走既有缓冲路径。大于 8 KiB 的 SQL CONTENT 值会在行编码时写入该存储，SQL 切片/文本/全文函数可按引用读取。新建和更新对象引用随行变更登记，事务回滚路径会在恢复行后删除本事务创建的对象。直接整列投影目前限制为 4 MiB，需通过 `CONTENT_SLICE` 读取更大值；续传句柄、备份恢复和权限集成仍未实现。

主要访问方式为 MySQL 兼容 SQL。模型、解析器和后台维护是数据库内部执行部件；客户端不需要部署独立向量数据库或维护多个事实副本。大对象可使用数据库定义的标准流和 SQL 句柄，原件身份、权限、版本与读取语义仍归数据库管理。Agent 使用相同契约，不拥有另一个数据入口。

列语义元数据复用现有列 `COMMENT` 的持久化机制，以 `xmysql-semantic:v1:` 加 JSON 编码。机器目录 `information_schema.xmysql_column_semantics` 将结构化 COMMENT 展开为业务名、解释、单位、空值含义、范围、枚举映射、revision、来源及确认状态；普通 COMMENT 不会被推定为事实。表级 COMMENT 通过 `information_schema.xmysql_table_semantics` 暴露显式关系提示，包含两侧列、基数、含义及各关系自身的确认状态；它不替代外键或自动规划 JOIN。`SUGGESTED` 仅代表建议，模型来源不能直接标记为 `CONFIRMED`；确认定义需记录确认人。目录遵循基础表可见性和列 SELECT 权限；对当前会话不可见的关系目标会省略并标出裁剪。该实现只暴露当前 COMMENT 中的定义快照，历史 revision 保留和上下文构建器接入仍未实现。

2026-11-26 客户端流式路径验收进展：普通 `go-sql-driver/mysql` 通过预处理 INSERT 发送 2 MiB+37 字节 CONTENT；测试把驱动 `maxAllowedPacket` 限为 1 MiB，以实际触发多个 `COM_STMT_SEND_LONG_DATA` 包。执行包省略长参数值时，服务端按省略参数索引解析其余二进制参数，后续绑定数据库分段写入器生成的 CONTENT 引用。重复 5 次均成功。客户端随后以 1 MiB 的 `CONTENT_SLICE` 查询分段读取，在每个查询中用 `CONTENT_REVISION(body)=captured_revision` 固定版本条件，并核对总长度、SHA-256 及完整重组摘要。发现并修复了同连接数据包并发处理竞态：全局任务池仍用于跨连接并发，但单连接 SQL/协议命令按线序完成。此项证明当前 Go 驱动的长数据上传和有界 SQL 切片读取闭环；未证明 Java/Python 驱动行为、端到端堆内存上界、并发更新下的版本失配响应、重启续传、备份恢复或跨实例复制，U09 仍未完成。

本版补齐多路召回、重排、上下文、证据与上层回答核验契约，并保留 CONTENT、派生发布、持久增量及安全隔离要求。数据完整、语义证据充分和回答正确分别表述；普通 SQL 事务及排序语义保持不变。共 90 项验收映射，模型评分不替代确定性不变量，引用有效不等于结论正确。

当前 SQL 原型新增 `CONTENT_TEXT_BLOCK(content_column, ordinal, byte_offset, byte_length)`：输入必须是存储行中的显式文本 MIME CONTENT 列，单次读取范围不超过 4 MiB；函数校验 UTF-8，并返回包含当前行来源身份、内容修订、摘要和原件字节区间的 READY 内容块。该原型供 SQL 侧产生可追溯的文本块，调用者仍须选择块边界；不执行 PDF/OCR/Office 解析、自动分块或模型嵌入。尚待 SQL 客户端、范围边界和源更新失效行为验收。

内容块包络 v2 增加显式 `identity_version`，新块 ID 绑定文本、属性、类型化载荷摘要、父块 ID 和关系类型；v1 块仍按旧身份算法读取及往返，避免升级后失效。可选 `payload` 由 `schema_id`、`schema_version`、JSON `media_type`、JSON 原文和 SHA-256 组成，用于表达通用表格单元格、类型化结构值或处理器产生的其他结构；调用方可省略摘要，由数据库按载荷字节计算，提供摘要时则逐字节核验。`parent_id`/`relation` 仅接受 `contains`、`continues`、`derived_from`、`references` 关系。`CONTENT_BLOCK_PAYLOAD`、`CONTENT_BLOCK_PARENT_ID`、`CONTENT_BLOCK_RELATION` 等 SQL 访问器读取这些字段。首个标准载荷为 `xmysql.table@1`：列声明 SQL 类型、单位和含义；行及单元格可保存定位；单元格区分 `PRESENT`、源数据真实空值 `SOURCE_NULL`、源对象未含该属性 `SOURCE_MISSING`、未处理 `NOT_PROCESSED` 和类型转换失败 `CONVERSION_FAILED`。整数以 JSON 精确数字解析并按 SQL 整数宽度/有无符号校验，二进制以 Base64 表示；单张表上限 16,384 列、100,000 行及 200,000 单元格。`CONTENT_PARSE_CSV(content_column)` 可在普通 SELECT 中把当前行、最多 4 MiB 的 UTF-8 文本 CSV 转成来源绑定 `CONTENT` 表格块；字段保持字符串，不猜 SQL 类型，重复/空表头（空头生成 `column_N`）、不规则列数、超限内容会明确处理，数据行保存 CSV 字节区间。`CONTENT_PARSE_JSON_ARRAY(content_column)` 支持显式 JSON MIME 的非空对象数组；对象属性名按确定性顺序建列、单元格仍保持 JSON 类型和精确 JSON 值，重复键、大小写冲突、非对象行及超限输入明确报错，数据行记录原件字节范围。缺失属性与显式 `null` 分开表达。`CONTENT_PARSE_IMAGE(content_column)` 支持 PNG、JPEG、GIF：最多读取首 4 MiB 探测头部，检查声明 MIME 与检测格式一致，记录宽高并限制单边 100,000 像素、总像素 100,000,000。它不解码像素、不查 EXIF 朝向、不遍历 GIF 帧、不执行 OCR，也不验证完整文件；输出标明 `header_only` 与 `pixels_decoded=false`。图片块引用整张原图，需显式 `ALLOW_DECLARED_DERIVATION` 才能进入上下文，真实图像通过 `CONTENT_BLOCK_SOURCE_SLICE` 读取。上述是确定性内置适配器，不是 PDF/OCR/Office/音视频解析器；无图像语义检索文本，须由视觉嵌入/模型处理。`CONTENT_BLOCK_CONTEXT` 严格模式须与原件声明范围字节完全相同，派生模式显式标注未经核验。上述格式当前通过源码构建，尚无 SQL 行为或旧格式迁移验收。

引擎级物理备份现在只允许在引擎尚未启动或已成功关闭后执行，并通过生命周期互斥避免备份与 Start/Close 并发；引擎运行中、启动失败或关闭失败都会拒绝创建。停止前调用方仍须先停止 SQL 监听器。底层 `server/backup` 文件快照接口仍要求调用者自行确保整个数据目录停写；该改动不代表 CONTENT 事务目录、派生数据及对象恢复已完成端到端验收。

备份和复制范围说明：物理备份会递归收集数据目录中的常规文件，因此包括 `.xmysql-content` 的清单与段文件。现有 JSON 逻辑备份和 XMySQL 事务行事件只携带行值及其中的外置对象引用，没有附带外置对象字节或对象传输协议；因此单独使用逻辑备份/行事件不能在另一实例恢复外置 CONTENT。跨实例传输、重放幂等和对象发布与行事务原子关联仍属于 U09/U04 未完成范围。

## 一 修订目标与核心不变量

调用方必须知道自己读了哪个范围、哪个版本、处理了多少内容、遗漏了什么，以及能否继续读取或重试。数据库不以单一成功标志同时代表上传完成、解析完成、返回完整和模型结论正确。

1. 同一证据的元数据、正文、页面图像、音视频片段均绑定同一不可变来源版本。
2. 关系记录和媒体绑定在固定来源快照中选择；派生结果允许稍后生成，但其来源身份必须完全匹配。
3. 严格请求不因未就绪、预算不足、解析失败或断流而静默缩小范围。
4. 来源覆盖、处理覆盖、检索范围、传输覆盖和提取质量分别报告。
5. 新鲜度相对于明确的数据库提交或外部同步边界定义，不能只用响应时间代替数据时间。
6. 分页、续传、重试和任务恢复延续原始身份，不自动切换到新版本。
7. 每次读取使用当前有效授权；旧快照和旧句柄不延续已经撤销的权限。
8. 原始记录、提取结果、模型判断和人工确认分别保存，并保留来源关系。
9. 相同输入的结果复用与重新推理是不同操作；输入可复现不等于非确定模型输出必定相同。
10. 每个确定性不变量均有独立验收；OCR、检索及模型效果另以标注集度量。
11. 新内容与 AI 能力增量接入，保留当前已支持的 MySQL 语义及客户端访问；不得通过删除旧能力实现新功能。
12. 未显式调用新增能力的普通 SQL 不自动解析媒体、调用模型或产生外发及模型费用；AI 不可用不阻断基础服务。
13. 既有通过用例的新回归阻止发布；已有失败和未支持单列，不能假定上游 MySQL 全部能力已经具备。

### 分支兼容性契约

P0 固定当前源码与修改、旧能力状态及执行证据，覆盖已支持的 DDL DML、查询类型和函数、约束、事务、权限、协议、元数据、备份恢复与复制。以已有兼容矩阵和测试为入口，不把文档声明自动视为通过。既有失败需保留并定位，必要修复须公开语义影响，不通过冻结错误行为维持表面兼容。

新增能力关闭及开启时，对普通 SQL 比较结果、类型、明确顺序、重复与 NULL、精度、错误号、SQLSTATE、警告、事务和权限。新增关键字、函数注册与目录迁移不能无声占用旧标识符或改变原对象含义。普通 JDBC/Python 访问旧能力无需新 SDK；模型服务离线不影响基础启动和旧表读写，混合资源负载按固定预算验收。

新旧对象混合事务仍遵守原隔离和原子性，不自动提交或隐式拆分。新类型、备份和复制编码单列支持范围，旧类型链路保持回归。禁用功能不代表旧程序可以直接打开已升级格式，降级需兼容读者或经过验证的恢复路径。

## 二 来源快照与跨接口读取

### 1 读取会话

首次查询创建 ReadSession，固定 tenant_id、发起者、选择范围、source_snapshot_id、semantic_schema_version、scope_manifest_id、读取策略和 expires_at。清单包含关系记录版本、媒体绑定版本、媒体不可变版本及所需处理能力。权限指纹用于审计和发现变化，不是跳过实时鉴权的凭据。

source_snapshot_id 表示一致选择的原始数据状态。artifact_read_snapshot_id 表示实际读取已发布派生结果的状态，两者可以不同；证据一致性由来源清单和依赖版本校验保证。仅记录各表读取时间，不能代替一致来源快照。无法建立一致输入时，严格请求返回 CONSISTENCY_UNAVAILABLE；只有调用方显式选择 LIVE_PER_STEP 才允许跨步骤读取变化中的数据，并展示每一步的来源版本。

### 2 固定与变化检测

默认 PINNED 在请求被接受时捕获已提交范围，后续分页、上下文和媒体读取继续使用该范围。响应不声称包含捕获之后才提交的记录。

CURRENT_GUARDED 在固定输入基础上，于发布结果前检查依赖是否变化；检查范围包含新增或删除的范围成员、记录值、媒体绑定及语义定义。变化返回 INPUT_CHANGED，不能只比较已知对象版本而漏掉新插入的符合条件记录。

LATEST_AT_CAPTURE 表示捕获来源快照时可见的最新提交，不代表响应到达客户端那一刻的无限追新。AT_LEAST_RECEIPT 表示快照必须包含调用方给定的提交回执或更晚的兼容状态。外部目录是否同步完成单独给出 connector_checkpoint 和 sync_scope；无法证明覆盖时报告 UNKNOWN。

### 3 SQL 与内容通道

关系结果、内容视图、模型上下文和大对象读取均由 SQL 表达。连接内优先复用现有 MVCC 事务和游标；跨请求分页或大对象续读可取得数据库签发的 result_id 或 content_handle。内部 ReadSession 是该读取上下文的逻辑实现，不要求用户创建另一套会话资源或调用专用服务。

数据库在句柄内保存 source_snapshot_id、来源清单和 expires_at，调用方无需手工组合表版本与对象路径。大对象读取函数校验句柄绑定的内容 ID、版本、来源列和当前权限；不能用某个路径的当前字节替代已经固定的内容。

句柄续期只能延长原有来源的保留期，不能改变快照。过期或来源不再可合法保留时返回 READ_SESSION_EXPIRED 或 SOURCE_VERSION_UNAVAILABLE。普通 SQL 查询可以直接使用显式事务；可恢复的独立句柄属于可选扩展，SDK 不自动拼接不同来源上下文。

## 三 新鲜度与能力就绪

### 1 请求参数

以下维度独立设置，不将它们混成一个查询模式：

| 维度 | 建议字段 | 定义 |
| --- | --- | --- |
| 来源选择 | scope_policy | PINNED、CURRENT_GUARDED 或显式 LIVE_PER_STEP |
| 派生版本选择 | generation_policy | CAPTURED_SOURCE 或显式 LATEST_PUBLISHED；不能覆盖事务隔离 |
| 新鲜度 | freshness.mode | LATEST_AT_CAPTURE、AT_LEAST_RECEIPT 或 BOUNDED_STALENESS |
| 能力 | required_capabilities | ORIGINAL、TABLE、TEXT、FULLTEXT、VECTOR 或 SEMANTIC |
| 覆盖要求 | coverage_policy | REQUIRE_ALL 或 ALLOW_PARTIAL |
| 检索语义 | search_mode | RELATIONAL_EXACT、VECTOR_EXACT、VECTOR_APPROX 或 SEMANTIC |
| 预算与等待 | deadline、budget | 到期或预算不足返回可识别状态 |
| 检索策略 | retrieval_options | 通道、候选预算、融合、重排、上下文及补查上限；仅用于显式语义入口 |
| 回答要求 | answer_options | evidence_policy、输出 Schema 与 require_supported_answer；后者默认 false，不替代来源覆盖策略 |

BOUNDED_STALENESS 的上界针对声明范围内的更新覆盖前沿，不是最近某个 Worker 成功的时间。静态且没有更新的数据不能因处理时间久远就被误判为过期。外部源的未知同步进度不能自动视作满足新鲜度。

### 2 统一维护的内部状态

内容写入内部状态为 UPLOADING、STAGED、PUBLISHED、ABORTED、EXPIRED。STAGED 表示字节校验和持久化完成；PUBLISHED 表示源行、内容引用与派生维护意图已提交。这些是数据库内部状态，用户入口仍是内容列写入和事务提交。

派生视图及索引维护按能力保存 PENDING、RUNNING、READY、FAILED、REVIEW_REQUIRED、OBSOLETE。OCR READY 不代表 VECTOR READY，也不代表识别内容已由人工确认。每个失败项记录原因、重试类型和影响范围。

流水线和索引水位是对指定来源范围、分区及能力的无洞覆盖证明。并行任务乱序完成时，不能用最大完成序号推进水位。针对固定清单可直接核对每项依赖的处理状态；已失去当前绑定的历史来源仍按其合法历史请求处理。

### 3 派生身份与发布条件

派生身份 D 包含来源表列及主键、源行版本、content_id/revision、派生定义、处理器及配置、规范内容与分块、实际使用模型空间和语义版本。未参与计算的依赖不加入 D。attempt_id、租约和 fencing_token 属于运行尝试，generation_id 属于发布，不混作来源身份。

PENDING 经原子领取变为 RUNNING；结果持久化并校验当前令牌及依赖后，在短事务内写为 READY。质量不足转 REVIEW_REQUIRED；失败转 FAILED 并记录原因及重试资格。超时接管须提高 fencing_token 后回到 PENDING，旧 Worker 的返回只能被拒绝。FAILED 的有界重试建立新 attempt；人工纠正或依赖改变建立新 D，旧结果不覆盖。失去当前资格的状态为 OBSOLETE，合法历史证据按保留规则继续可读。

完整发布代际 G 保存来源清单、定义与能力集合、无洞前沿、结果清单摘要及前一代际。只有约定单位全部 READY 且满足质量要求，才可将 BUILDING 以目录 CAS 发布为 SERVING。旧代际为 RETIRED 并按读者保留；运行 READY 不自动切换服务指针。产物先持久化，目录事务后提交；崩溃不能暴露部分清单。重试核对已提交状态，不能重复切换或用过时令牌发布。

存在失败、待复核或未处理单位时，新完整代际不发布。显式部分查询可查看有覆盖诊断的结果，但不赋予“完整 SERVING”资格。排除失败单位需要建立可追溯的新范围，原请求不能因此被改写为成功。增量代际可复用旧产物，复用时仍校验 D，不重新推理源向量以伪造相同版本。

P2 代际按物化视图、定义版本及授权分区的定义范围维护。首期持久保存所需关系投影，包括主键、源行版本、视图所依赖字段的值及类型、内容引用；未变化投影可复用不可变记录。G 持有内容保留引用，并进入备份和恢复清单，不依赖普通 MVCC 历史版本长期存活。未保留字段不能从当前源行补读，应重新捕获当前来源或返回范围不支持。

同一视图、定义及分区发布前沿不得倒退，CAS 同时核对前驱、前沿和依赖版本。旧 G 在较新 G 发布后 CAS 失败，只可保留历史产物或废弃，禁止刷新前驱后再覆盖较新 G。新定义建立独立发布序列，显式校验后切换逻辑定义。跨分区读取需验证共同来源切点，无法证明一致则返回 CONSISTENCY_UNAVAILABLE。

普通派生视图名默认 CAPTURED_SOURCE，不隐式读取旧 G；P1 U08 已可直接读取所选来源的 READY 产物，P2 增加发布代际及完整屏障。查询所需范围全部 READY 即可严格返回，不因范围外坏文件失败。只有 LATEST_PUBLISHED 使用完整 SERVING 代际，不能将 BUILDING 子集当作已发布快照。

### 4 等待与未就绪

autocommit 独立查询或显式固定来源结果句柄可以先固定来源、结束内部短事务，再等待对应派生版本；就绪后建立能读取新产物的上下文，校验来源依赖与实时权限。用户显式 BEGIN 的事务保持原有隔离与快照，不隐式提交或替换；所需派生结果不可见时返回 NOT_READY。未提交源行不得发布给后台处理，原始值按读己之写规则读取。REQUIRE_ALL 未满足时返回 NOT_READY、PROCESSING_FAILED 或 QUALITY_REQUIREMENT_UNMET；不得返回普通空数组让调用方误判为没有记录。

CAPTURED_SOURCE 为默认派生版本选择，固定当前请求的来源；缺少派生值默认快速返回 NOT_READY。只有显式设置有限等待期限，autocommit 或固定来源句柄才等待；显式 BEGIN 仍按前述规则返回，不在事务内等待未来发布。等待到期返回 DEADLINE_EXCEEDED 或带原因的 NOT_READY，不无限阻塞。

ALLOW_PARTIAL 必须显式选择，返回结果逐项披露缺失及原因。权限不足的来源不纳入可见计数；响应不向无权调用方泄露隐藏对象身份或数量。

### 5 读取已发布代际

LATEST_PUBLISHED 显式选择当前读取快照中最新可见且满足所需能力的完整 SERVING 代际。它保留该代际的固定来源和定义，不同时声称 LATEST_AT_CAPTURE。调用方设置 BOUNDED_STALENESS 或 AT_LEAST_RECEIPT 时，先验证该 G 的覆盖是否满足；不满足返回 FRESHNESS_REQUIREMENT_UNMET，无可靠变化前沿则为 CONSISTENCY_UNAVAILABLE。若要求当前捕获来源，而已发布代际不覆盖它，返回 NOT_READY，不默默选旧代际。未要求新鲜度界限时，调用方必须显式接受 generation_as_of 历史读取。

返回 generation_id、source_snapshot_id、scope_manifest_id、source_frontier、captured_change_frontier、freshness_evaluation 和缺口。时间上界按连续覆盖前沿到捕获时未覆盖的更新衡量；静态内容不因发布时间久远而被判过期。分区前沿不能证明给定 SQL 范围时，只能报告 UNKNOWN，不能取最新一项成功时间冒充证明。

代际关系值与内容都从该来源清单读取，不能从当前源行补字段；与当前表 JOIN 按来源修订键校验，不匹配不静默漏行。显式事务仅能看到其快照可见代际。每次读取仍检验当前授权及历史保留许可，旧代际不能绕过撤权或强制删除。COMPLETE 仅针对该 G 的范围和能力，不代表全局最新。

在线检索配置可组合 LATEST_PUBLISHED、PINNED、BOUNDED_STALENESS、REQUIRE_ALL 与明确的近似检索。正式统计可组合 CAPTURED_SOURCE、PINNED、LATEST_AT_CAPTURE 和 RELATIONAL_EXACT；交互预览显式设 ALLOW_PARTIAL。它们是配置组合，不把严格、固定、有限过期和部分返回混成互斥的四种语义。

### 6 分段向量索引一致性

ANN 查询固定索引清单、向量空间、来源清单、delta 快照及覆盖前沿。基段不可变，delta 与删除记录在同一数据库中持久化；已完成嵌入与 delta 引用原子发布。源提交而嵌入未完成仍是能力缺口，内存缓存不能使之变成 READY。

基段候选与精确扫描 delta 按内容修订和删除记录裁定可见性、去重及排序；结果整体仍是 ANN。过滤后不足 K 要继续探索或按声明策略扫描，预算耗尽时披露不足原因。合并固定截止点 C，C 后变化留在 delta；新段、吸收范围、删除覆盖和前沿同一清单 CAS 发布。旧读者保持旧清单所需记录，不能提前截断 delta 或删除旧段。

默认不得跳过未合并增量。候选范围、过滤策略、delta 数量、段清单与近似标志在查询状态及 EXPLAIN 中可见；精确请求不能因资源紧张静默降为 ANN。

## 四 完整性与结果状态

### 1 四类覆盖与质量

| 维度 | 必须记录 | 可以作出的判断 |
| --- | --- | --- |
| source_coverage | 授权来源清单、范围规则、枚举是否结束 | 是否枚举了约定来源 |
| processing_coverage | 预期单位、成功、失败、未处理、待复核单位 | 每页、每行或每个区间的处理去向 |
| retrieval | 过滤范围、精确或近似、Top K、候选预算 | 检索采用何种边界，不能据此证明现实事实完整 |
| delivery | 分页、截断、总量是否已知、末尾完成标识 | 调用方是否拿到本次结果的完整传输 |
| quality | OCR、版面、抽取、时间对齐的评测或未知状态 | 质量满足哪项标准，不把运行完成当作内容正确 |

全部页面都有处理去向，仅能证明结构性覆盖，不证明每个字符都识别正确。向量精确 Top K 仅保证给定向量和候选集的距离结果；语义相关性及事实判断单独评测。页面或数据被预算截断时，必须显示截断范围及原因。

### 2 空值与无结果

普通 SQL NULL 保持原语义。AI 派生字段另附 value_state，区分 PRESENT、SOURCE_NULL、NOT_PROCESSED、EXTRACTION_FAILED、MODEL_UNKNOWN、REDACTED 和 OMITTED；不得只靠一个 JSON null 表达所有情况。权限限制导致的细项披露按可见性策略处理。

NO_MATCH 表示按已声明范围和方法未命中，不保证不存在现实相关内容。没有候选、没有可读证据、尚未就绪和模型无法判断是不同状态。任何“不存在”结论必须明确其范围、算法和证据边界。

### 3 错误封装

SQL 通过标准驱动的数据库错误号与 SQLSTATE 报告失败；详细状态可经同连接的查询结果 ID 和只读目录读取。逻辑错误包含 code、message、retryable、retry_after_ms、request_id 及可用的结果身份。可选传输桥接不能成为正确读取的必需入口，需要的信息按授权范围披露。

核心错误码为 EVIDENCE_REQUIREMENT_UNMET、FRESHNESS_REQUIREMENT_UNMET、RESOURCE_LIMIT_EXCEEDED、FORMAT_VERSION_UNSUPPORTED、INVALID_REQUEST、SCHEMA_MISMATCH、CAPABILITY_UNSUPPORTED、NOT_READY、PROCESSING_FAILED、QUALITY_REQUIREMENT_UNMET、INPUT_CHANGED、CONSISTENCY_UNAVAILABLE、READ_SESSION_EXPIRED、SOURCE_VERSION_UNAVAILABLE、CONTENT_INTEGRITY_ERROR、IDEMPOTENCY_CONFLICT、IDEMPOTENCY_RECORD_EXPIRED、STREAM_INCOMPLETE、BUDGET_EXCEEDED、DEADLINE_EXCEEDED 和 ACCESS_DENIED。

任务 execution_status 与结果 result_status 分开；result_status 为 COMPLETE、PARTIAL 或 UNAVAILABLE，COMPLETE 只代表满足已声明的数据访问契约，不代表模型结论正确。REQUIRE_ALL 模式不得以 PARTIAL 当成功结果。

### 4 表示转换与保真

原始表示负责保留可核验来源，规范内容表示负责段落、表格、媒体位置和类型化值，检索表示负责候选定位，任务上下文负责当前问题的实际输入。每层拥有独立版本和作用范围，不能把索引命中等价为内容已读取，也不能把模型可读视图等价为完整原件。

规范内容模型使用 content_revision 和块身份保存结构。Markdown、HTML、类型化 JSON 与模型消息从该内容版本形成不同视图，每个视图声明 rendering_profile_version、覆盖内容类型、格式转换及未表达信息。涉及精确金额、公式状态、坐标、合并单元格或时间范围的操作必须读取权威结构化内容，不能依赖重新解析 Markdown 恢复原始类型与精度。原始解析输出和文件仍保留，以支持规范模型无法表达的特性及后续重处理。

检索 chunk 记录 chunk_profile_version、content_revision、块范围、tokenizer、重复上下文及来源位置。模型 tokenizer 不同可能改变输入预算，不能用某个模型的 token_count 当作通用长度。拆分表格、章节或时间片段时记录父子和连续关系；重叠正文及重复表头标记 context_only。确定性统计按原始行或单元格身份去重，不能把重复 chunk 多算一次。

源嵌入是绑定内容版本、编码器和配置的持久派生值。精确扫描和 ANN 索引必须读取同一份值，不能因切换计划而重新调用模型生成源向量；表达式索引只作为该派生值的声明简写。查询向量在一条语句内固定一次，显式重新嵌入产生新派生版本。固定模型版本仍不能证明所有再次推理都逐位相同。

解析、分块、向量和上下文组成显式依赖图。更换解析器可能改变结构和块 ID，重新分块可能改变 chunk ID；新结果写入新版本，历史引用仍使用旧版本。索引替换依照覆盖屏障发布，不把旧向量映射到新正文；历史合法保留和即时撤权仍按读取契约处理。模型提示词变化是否导致重建向量，取决于编码输入的真实依赖，不能无条件全部重做或全部复用。

查询路径由问题及所需保证确定：明确页码、编号和短文档可以直接读取；精确统计走类型化关系计算；开放语义检索使用声明范围的全文或向量候选；视觉问题读取实际页面或图像。全量统计不得只对语义 Top K 内容求和。向量化是检索手段，不是所有文件进入模型的必需条件，也不能修复上游提取错误。

### 5 检索及回答质量契约

RAG 请求固定来源、检索策略、通道与各路候选预算、融合配置、重排模型及能力、上下文预算和证据策略。内核用 retrieval_profile 摘要保存依赖，调用方可直接通过 SQL 参数声明，无须创建独立资源。原问题和绑定的字段、编号、版本、数值、单位均保留，查询扩展不能改变明确约束。普通 SQL 的距离排序、精确聚合和精确性要求不能因 RAG 策略被静默改写。

候选保存来源修订、规范证据身份、位置、通道名次和各自原始评分，按身份融合而非只按文字相似合并。初期 RRF 配置版本固定，全文分数、向量距离和重排分数不直接相加。各通道遵守同一来源和授权范围，过滤后不足应有界继续探索或按声明策略回退；资源耗尽披露范围，不放宽条件。模型重排必须读取声明模态的实际内容，并逐项绑定候选 ID；超时、降级和不支持的表示显式可见。

context_manifest 固定实际进入模型的正文、结构、原图或片段、来源和转换版本、加入原因、质量、截断及未观察范围。表头、单位、父块、邻段及媒体前后文只能在原来源内按权限补全；补全重复项标记 context_only，不能重复计入统计。预算导致必要条件缺失时记为证据缺口，不把已经读取的少量内容宣称完整原件。

每次回答有独立 answer_status：ANSWERED、INSUFFICIENT_EVIDENCE、CONFLICTING_EVIDENCE、NEEDS_CLARIFICATION 或 UNASSESSED。result_status 继续表达数据访问契约是否完成：数据读取 COMPLETE 时仍可以没有可支持答案。未匹配不证明不存在，REQUIRE_ALL 不要求模型作肯定结论。调用方显式设置 require_supported_answer 后，无法满足证据策略须返回 EVIDENCE_REQUIREMENT_UNMET。证据评估按确定性规则、模型判断或人工确认分别留证，不把语义模型的通过结果等价为事实证明。

require_supported_answer=false 时，语义不足本身不阻止已满足数据访问契约的 COMPLETE 返回；为 true 且不足时，整个生成请求通过 SQL 错误失败，result_status=UNAVAILABLE，同时独立保留已完成的数据覆盖信息。不得一边以完整生成成功返回，一边在隐藏字段声明证据要求未满足。删除无依据主张后重新检查问题覆盖，必要部分缺失仍需返回不足或澄清，不能将残缺答案冒充完整回答。

金额、数量、日期、版本比较和全量聚合来自类型化 SQL 计算，返回范围、类型、单位及计算身份。生成结果包含 answer、claims、citations、computed_values、context_manifest_id、quality_assessment 和用量。重要 claim 关联实际输入证据或计算结果；引用位置有效和主张确实受支持分别核验。生成后的数字、单位、否定条件、适用范围、冲突和版本混配分别检查，不能单靠模型自报置信度通过。

quality_assessment 的方式为 NOT_RUN、RULE_CHECKED、MODEL_ASSESSED 或 HUMAN_VERIFIED，并声明已检项、缺口及最终文本摘要。ANSWERED 表示按声明策略形成回答，不保证所有断言真实，也不保证原始来源符合外部现实。证据不足的返回可为空答案、澄清或缺口说明；上层应用须保留其状态，不改写成确定结论。

| 内容与核验条件 | answer_status | require_supported_answer |
| --- | --- | --- |
| 仅确定性 SQL 结果及固定模板，适用规则全部通过 | ANSWERED，RULE_CHECKED | 对已检查的事实范围可满足 |
| 自由生成，未运行支持核验或仅检验引用存在 | UNASSESSED | 不满足；仅作明确未核验预览 |
| 重要主张完成声明支持检查，已声明关键条件无未决缺口 | ANSWERED，保留模型或人工评估方式 | 按版本化策略满足，不担保外部真实性 |
| 核验成功返回证据不足 冲突或需澄清 | 对应语义状态 | 不满足；按回答策略返回拒答或证据错误 |

支持策略最低检查来源和实际输入引用、当前权限、适用计算值与类型单位、重要主张支持关系及已声明问题条件。自由生成的关键主张检查结果须逐项为 PASS，FAIL 或 INDETERMINATE 均不能满足支持要求；纯 SQL 事实由确定性规则检查，不强迫语义模型介入。核验调用技术 UNKNOWN/ERROR 使严格模型步骤失败，不能替换成一条普通拒答来掩盖故障；成功返回的语义不足、冲突和澄清不视为技术 UNKNOWN。运行时识别条件及模型判断的局限必须保留，离线完整证据真值仅用于评测，不能声称线上已证明事实完整。

应用二次生成、摘要或改写时建立新生成版本并重新核验最终展示文本；旧文本的核验不得沿用到变化后的新文本。普通 JDBC/Python 客户端能读取所有质量字段及错误，不以必需专用 SDK 实现该契约。数据库无法控制应用在契约外自行改写的准确率，端到端验收必须包含最终展示结果。

EXPLAIN 与执行诊断记录各路命中、过滤淘汰、去重、融合、重排、补全、预算与截断及耗时，授权范围外身份与计数不可见。缓存依赖来源、语义、处理及检索策略和模型版本，每次命中重查权限。缺少阶段诊断不得把所有失败笼统归因于模型。

### 6 质量评测口径

ANN Recall at K 仅对比相同持久向量、距离和过滤范围的精确结果，分母采用可用候选数与 K 的较小值；范围为空单列，不记满分。证据 Recall at K 以独立标注的必要证据组为真值，组内允许可替代证据；另报告所有必要组同时命中的比例。Precision at K、nDCG 按固定相关性等级及有效 K 计分，未返回位置不作为相关，重复证据不重复得分。

没有必要证据组的反例不计算召回率，单列其拒答和错误肯定情况，不记作召回满分。RRF 名次从 1 开始，平滑常数及非负权重随策略冻结；不同来源、不同模态及必要证据组的去重规则随标注配置保存，避免重复块制造得分。

至少 200 道 RAG 问题中保留至少 40 道无匹配、不充分、冲突或歧义反例，另有至少 100 道 SQL 问题及至少 20 道反例；按原件与修订族切分校准和留出集。P2 与 P3 按模态范围分阶段验收，不以模型桩或未支持模态充数。冻结真值、样本量、分母、相关性等级、阈值及预算后再验收，按模态和问题类型报告置信区间。

已回答正确率为正确实质答案数除以给出实质答案数；正确覆盖率为可答问题中正确回答数除以标注可答问题数；回答覆盖率为实质答案数除以全部合法测试请求数。应拒答反例上的错误肯定率、超时和失败另列，分母为零报告不适用。主张支持率、引用有效率、定位正确率及上下文必要信息保留率分别报告，不能相互替代。准确性以授权来源和冻结参考答案为准，不声明超出来源的现实真实性。

固定来源、模型、数据及资源，对全文、向量、融合、重排和结构补全作逐步消融，检验各层收益及退化；最终应用展示文本纳入验收。校准集用于选策略，留出集用于独立验收，不能事后调参或删除难例改善成绩。

## 五 Excel 入库与类型保真

### 1 来源与结构

保存原始工作簿版本、Sheet 身份、单元格或区域映射及 import_profile_version。先形成工作簿清单，再识别一个或多个逻辑表；多级表头、合并单元格、隐藏行列、筛选行、空白区和重复表头按导入配置处理。被排除的区域必须可在授权清单中追溯，不能默认为不存在。

每条结构化记录保留 source_cell_range，表头定义与数据行分离。列类型推断作为建议，确认后固定版本；保留原始值、标准化值及转换状态。编号前导零、数值与日期混合、百分比、货币、不同单位和跨表关联不得无声强制转换。

### 2 公式与数值

公式分别保存 formula_text、cached_value、calculated_value、calculation_state 和计算配置版本。状态区分 FORMULA_ONLY、CACHE_UNVERIFIED、RECALCULATED、ERROR 和 EXTERNAL_DEPENDENCY_UNAVAILABLE。只读取缓存值不宣称已重新计算；数值是否最新取决于依赖和计算策略。外部引用失效不得自动替换为零。

MySQL 结果集保持 DECIMAL、BIGINT、日期时间和二进制的原生协议类型，由驱动映射为对应语言类型。只有导出 JSON 上下文时，DECIMAL 和可能超出 JSON 消费者安全范围的整数采用 Schema 声明的字符串编码。浮点与十进制分开，JSON 中 NaN 和无穷值显式编码状态或拒绝。日期、带偏移时间、无时区的本地日期时间分别使用不同逻辑类型，不擅自补成 UTC。

单位、枚举、业务口径、换算规则与汇率来源版本属于语义 Schema。聚合前验证币种、单位和口径；转换规则缺失时返回语义错误或按调用方显式规则分组，不直接相加。

### 3 提供给模型

先用 SQL 进行确定性筛选和聚合，结果附字段解释、单位、值状态、来源范围和快照。模型上下文过大时，返回可继续读取的结果句柄，或显式标记采样与截断。模型摘要不能取代精确统计。

## 六 扫描 PDF 与文档读取

### 1 按页识别处理方式

固定原始 PDF 版本并枚举所有页。逐页识别数字文本页、扫描图像页、混合页和带已有 OCR 文本层的扫描页；文本抽取为空仅是信号，不能作为空白证明。旧 OCR 层与图像可能不一致，需要记录提取来源并进行版面或抽样一致性检查。

数字文本优先直接提取文字及位置。扫描页渲染为页面图像，检测方向、倾斜和必要的图像预处理，再做版面分析及 OCR。混合页对图像区域补充 OCR，按区域和阅读顺序去重合并，避免数字文本与 OCR 文本重复两遍。

### 2 扫描页完整路径

```text
PDF 不可变版本与页清单
  → 页面渲染与方向处理
  → 版面区域和阅读顺序分析
  → OCR 文字及表格结构识别
  → 原页坐标映射与质量检查
  → 版本化内容块和来源图
  → 全文索引及可选向量索引
  → 原文与页面图像组合成模型输入
```

原始页面图像、预处理图像及其坐标变换分别可追踪。正文块保留 page_no、block_id、bbox、reading_order、文本、提取方式和质量状态。表格保留行列、合并关系、单元格来源和续表关系，不能只保存拼接字符串。印章、签名、图片和无法识别区域按区域记录，不以 OCR 文字代替其全部视觉信息。

### 3 页面状态与复核

每页采用 PENDING、RUNNING、EXTRACTED、REVIEW_REQUIRED、FAILED 或 CLASSIFIED_BLANK；空白分类保存方法和依据，有人工确认时另记 human_verified。每个预期页必须有状态。空白分类、OCR 空字符串、渲染失败和根本没处理是不同情况。

低分辨率、旋转、手写、复杂表格、金额和关键编号需要质量检查。OCR 置信分数不是事实正确率；视觉模型复核也不能自动当成人工确认。复核前后的文字各保留版本，任何纠正均产生新 Artifact 并触发依赖索引更新。

严格请求指定 processing_profile，例如“全部页面有处理去向、所有表格已识别或标明失败、关键金额已复核”。配置同时明确哪些状态可用于当前操作；“标明失败”只满足状态记录完整，不满足必须读取该页内容的能力要求。无法满足时返回质量或覆盖错误。即使该配置满足，也不宣称整份文档语义绝对完整。ALLOW_PARTIAL 返回可用块及缺失页、质量不足区域的清单。

### 4 检索与模型使用

按页读取、编号过滤和全文检索不要求向量化。需要语义查找时，对版本化内容块生成向量；适配器必须提供原文，视觉任务提供原始页面或区域图像。视觉嵌入属于另一个模型能力，不能把 PDF 原始字节当成普通文本嵌入。上下文标明 OCR 来源、原始页位置和质量状态，模型需要据此区分原始文字与识别结果。

例如某条款位于第 5 页，返回的 source 含内容 ID、内容版本、page_no、bbox 和提取版本。调用方可通过同一读取会话查看该版本原页，并准确定位区域。

## 七 多媒体内容读取

内容列描述符包含 content_id、content_version、来源表列及行版本、MIME、size_bytes、content_sha256 和定位信息。为兼容前版契约，asset_id 和 asset_version 仅是内容 ID 与版本的旧字段别名，不再代表独立资产业务模型。SQL 返回的 content_handle 是受控读取身份，续读固定同一内容版本。原件、缩略图、裁剪页和视频片段均有转换参数及来源关系。

SQL 大对象全量和范围读取必须绑定固定版本；续传校验版本和完整性，不能拼接多个版本。CONTENT_READ 等拟议函数通过普通 SQL 调用返回二进制片段；可选传输适配器沿用同一来源身份，不成为首期依赖。ETag 若由物理后端提供，只是条件读取标识，不统一等同于 SHA256。音视频片段读取明确请求半开区间及实际输出区间，受关键帧限制时返回实际边界及映射。字节范围不自动等价于时间切片。

帧采样、音轨选择、声道、转写覆盖区间和音画时钟偏移保留在处理清单中。某些片段没有被观察到，不可据此判断事件未发生。文本模型接收转写或描述时，应标记缺失的视觉、语调或其他模态信息。

模型接口可接收受控 URL 或由适配器上传字节，记录实际请求内容及哈希。只在 manifest 放一个 URL 不代表内容已经送达；无法确认媒体输入有效时，报输入错误或已声明的降级模式。不能声称能证明模型内部实际利用了每一个输入像素。

### 内容描述符编码约束

描述符包含 format_version、flags、content_id/revision、mime_type、size_bytes、content_sha256、storage_kind、manifest_id 和 ownership_ref。INLINE 时字节由描述符承载；MANAGED_SEGMENTS 时清单固定序号、长度、摘要及内部键。格式版本采用明确端序、整型边界及长度前缀，未知必需字段或非法长度拒绝，未知可选字段依版本规则跳过。P0 冻结编码测试样例，P1 才开放持久化写入；这里不指定 Go 内存布局为磁盘格式。

内容修订及段不可变，事务引用、发布状态和 GC 记录另存目录。原件完整摘要和各段校验分开，范围读取至少验证所触及段；完整回读核对长度及原件摘要。摘要、内容 ID 或去重命中都不能代替当前授权。P1 默认托管文件分段，不依赖任意外部路径；内联阈值与段大小基于基线测量。

数据库管理的 CONTENT 段存储根路径必须是数据目录内的真实目录；初始化时拒绝符号链接或普通文件作为存储根，避免对象写入绕出数据库数据目录。物理备份对该目录内文件按普通数据文件完整归档并校验摘要。

当前 `SegmentStore.WriteTransfer` 与 `ReadTransfer` 提供有界流式对象帧：XCT/1 头部记录 MIME、修订、字节数和 SHA-256，后接原始内容；接收端在长度和完整摘要均匹配前不发布对象，并生成本地对象 ID。`PutVerified` 提供相同的摘要校验导入能力。导入端生成本地对象 ID，调用方必须将返回的 `ObjectRef` 写回对应 CONTENT 行。该帧尚未连接逻辑备份或复制事务协议，也未定义身份认证、授权、重试去重与对象/行原子发布，不能据此认定跨实例对象传输已完成。

## 八 分页 重试 断流与取消

分页令牌绑定查询指纹、固定快照、稳定排序、语义 Schema、结果句柄和当前位置。排序必须有稳定的唯一并列键。分页前重查权限；变更后不能通过重新排序或静默跳过来维持假完整性，应返回 ACCESS_DENIED 并按策略废止句柄。总量未计算时返回 total_known=false，不伪造总数。

幂等身份为租户、发起者、操作类型与 idempotency_key，另保存规范化请求摘要。相同身份和相同请求返回既有任务或回执；请求内容不同返回 IDEMPOTENCY_CONFLICT。首次请求捕获的版本清单是幂等记录的一部分，重试不自动换成最新输入。超过公开的保留期后，自动重试返回 IDEMPOTENCY_RECORD_EXPIRED，由调用方明确决定是否新建操作。

该保证适用于声明支持幂等键的内容写句柄、维护操作或受控任务。未携带此类身份的普通 INSERT UPDATE 遵循原有 SQL 语义，不自动获得重复执行去重保证。提交结果未知时先按业务主键、操作回执或事务状态核对，不能盲目重试。

未知是否提交的写请求先查询操作状态，SDK 不盲目重发新键。上传块、内容续传和模型调用分别采用各自身份。外部推理支持幂等或结果复用时可减少重复，崩溃未知窗口仍可能产生重复外部调用，必须记账。

核心 SQL 路径依据标准驱动错误、游标正常结束及声明的结果状态判断完成；大对象句柄还校验固定长度与摘要。断流或驱动报错后不能把已收到部分当成完整结果，续读固定同一来源与排序位置。可选 JSON 桥接要求合法封装；可选 NDJSON 或批流才使用批序号和终结记录，缺少终结记录标记 STREAM_INCOMPLETE。这些桥接规则不要求普通 SQL 客户端安装专属 SDK。

断开连接仅终止客户端等待。SQL 取消操作或标准驱动取消请求返回可查询的 CANCEL_REQUESTED 状态，服务端通过状态转换屏障阻止新步骤和最终发布；已提交成功与取消按先后序明确裁定。远程调用仍可能运行和计费，另列 external_call_status，不能把 CANCELLED 解读为远端资源已全部停止。

## 九 权限 删除 缓存与来源

当前授权检查覆盖来源发现、关系查询、候选筛选、内容读取、上下文构建、模型发送及结果读取。固定快照不授予历史权限。已发出的内容无法通过撤权收回；向外部模型临时签发的读取凭据，其撤销保证必须与短有效期或实时鉴权能力一致。普通客户端走 SQL 内容读取，每次访问执行当前权限检查。

逻辑删除首先阻止新的来源发现和当前版本访问。历史读取仅在显式允许历史访问、当前授权仍有效且保留期未结束时成立。法律或强制清除策略可以使旧句柄失效，返回可识别错误；清除派生数据、缓存和任务输入也必须留下可审计的完成状态。

缓存与来源记忆依赖来源版本、语义 Schema、权限、模型、提示词和参数。相似问题只用于查找可能相关经验，不能证明两个查询结果可等价复用。来源删除或权限变化后，缓存命中仍要校验访问；不得把跨租户数据、隐藏来源数量或敏感摘要泄露给调用方。

事实、提取内容、模型断言和人工确认采用独立 assertion_kind，并保存生成方式及证据版本。文档和媒体中的指令性内容属于数据，不能提升执行权限。引用必须指向实际进入上下文的内容；引用存在也不证明结论真实。

### 安全与资源隔离契约

解析及解码运行在受限独立进程，输入为固定且只读内容，输出经内核校验才可提交；进程无目录写权限和数据库管理凭据。按文件签名、白名单、解压后总量、文件项数、页数、像素、时长、CPU、内存和墙钟时间限额处理。XLSX 禁用宏与自动外链，文件内容默认无网络访问，畸形输入不得使主 SQL 进程崩溃。超限记录 RESOURCE_LIMIT_EXCEEDED 及对应单位，不能返回成功空内容。

首期不接受任意文件路径或 URL 抓取；如以后开放远程导入，必须单独授权，限制目标、DNS 解析结果、重定向每一跳与网络出口。模型外发由执行身份、来源列策略及目的地白名单共同控制，凭据不得进入提示词、原件或普通日志。提示注入只能影响不可信模型输出，不能扩展 SQL、工具或数据外发权限；输出使用前仍过 AST、Schema、授权与预算校验。

后台作业和前台 SQL 分配独立并发、内存及 IO 预算；暂存、已提交原件、派生、delta、句柄和代际分别配额。预算原子预留，远程结果未知时保留账目。缓存及去重跨域不可见，句柄读取始终重新鉴权。相关负例按 P1 文件隔离、P2 模型及权限、P3 音视频和合并负载分阶段验收。

## 十 协议与返回示例

### 1 SQL 是统一入口

主要能力使用 MySQL 结果集、二进制参数、数据库错误及 SQL 函数。CONTENT 和 VECTOR 的跨线编码使用驱动能识别的 BLOB 或文本，配合类型元数据与显式转换。BIGINT、DECIMAL、二进制和时间在驱动层保持原生逻辑类型；JSON 上下文采用已声明的无损编码。

来源范围、新鲜度、所需能力和覆盖策略通过数据库查询选项绑定到语句或当前读取上下文。具体语法在实现前冻结；不得用私有协议尾包承载普通驱动无法读取的必要状态。状态信息可通过普通 SQL 查询系统只读视图。后续可选的 Arrow 或 HTTP 桥接沿用同一契约，不引入独立数据语义。

### 2 通用 SQL 示例

以下语法均为拟议扩展，尚未实现。大对象句柄由数据库签发并校验，客户端不能伪造路径。

```sql
INSERT INTO documents(id, body)
VALUES (?, CONTENT_FROM_BYTES(?, ?));

SELECT id, CONTENT_TEXT(body), CONTENT_METADATA(body)
FROM documents WHERE id = ?;

SELECT CONTENT_OPEN(body) AS content_handle
FROM documents WHERE id = ?;

SELECT CONTENT_READ(?, ?, ?) AS bytes;
```

CONTENT_READ 的参数为受控句柄、字节偏移和长度。句柄固定来源、版本和读取上限；句柄不存在、失效、来源已不可用或权限不足时抛出 SQL 错误，不能返回正常空字节。空对象与到达明确末尾采用已声明的长度和状态识别。所有函数都可由普通 JDBC 或 MySQL Python 驱动调用。

本分支另有一组正在实现的内容块基础函数，边界不同于上面的拟议句柄接口。`CONTENT_BLOCK_FROM_JSON(?)` 接受调用方提供的来源字段，这些字段只经过格式校验，因此结果状态为 `UNVERIFIED`，可作为普通数据读取，但不得作为已核验引用；`CONTENT_BLOCK_FROM_SOURCE(?, content_column)` 要求 SELECT 中的物理基表行及 typed CONTENT 列，列可用别名或表名限定。数据库从该行提取主键、表上下文、CONTENT 修订与摘要来构造来源绑定，JSON 只提供 `ordinal`、`kind`、`text`、`locator`、`processor_id/version` 与可选 `model_id/version`、`attributes`，结果状态为 `READY`。两者均计算块身份并保存为版本化二进制 `CONTENT` 值，MIME 固定为 `application/vnd.xmysql.content-block`；SQL 通过 `CONTENT_BLOCK_TEXT`、`CONTENT_BLOCK_ATTRIBUTES`、`CONTENT_BLOCK_STATE`、`CONTENT_BLOCK_KIND`、`CONTENT_BLOCK_SOURCE_*`、`CONTENT_BLOCK_PAGE`、`CONTENT_BLOCK_START_MS/END_MS`、`CONTENT_BLOCK_REGION_*` 读取内容和证据定位。`CONTENT_TEXT(content)` 仅解码明确的文本 MIME（`text/*`、JSON、XML、NDJSON、JSON-LD 和表单编码），剥离 UTF-8 BOM，拒绝非法 UTF-8、PDF/图像等非文本 MIME，并限制单次返回 4 MiB；它不执行 OCR、PDF 解析或媒体转写。`content.ChunkText` 与 `ChunkTextFromStore` 可把最多 4 MiB 的显式文本 CONTENT 按段落、句子、空白及 UTF-8 字符边界切成来源绑定块，提供字节上限、可选重叠、原件字节区间，并把分块参数与边界策略写入块属性；它是确定性字节分块原型，不是模型 token 计数器、文档解析器或 SQL 表值函数。`CONTENT_SLICE(content, offset, length)` 按 0-based 字节偏移读取 typed CONTENT 中的有界片段，单次最大 4 MiB；读取范围超出内容末尾时返回可判定长度的空片段，不会跨越或拼接不同修订。来源更新或删除时，DML 会在同一存储事务中把引用旧主键/修订/摘要的 READY 内容块和绑定向量置为 STALE，并记录派生行变更。当前已有数据库目录内的进程级反向映射，按来源版本筛选候选派生表；首次使用从持久行重建，提交后增量更新，每 256 次相关变更写一次检查点。重启后检查点不作为权威状态加载，而从行存重建；显式共享事务回退到全表扫描，候选表仍用逐行访问器检查所有行并仅暂存命中行。它还不是 B+Tree 管理的事务内持久索引，尚未执行 SQL/并发/事务/恢复验收；不代表计划中的 PDF/OCR、RAG 或句柄能力已实现。

当前 SQL 增加了 `CONTENT_TEXT_CHUNK(content_column, ordinal, max_bytes[, overlap_bytes[, min_boundary_bytes]])` 与 `CONTENT_TEXT_CHUNK_COUNT(content_column, max_bytes[, overlap_bytes[, min_boundary_bytes]])` 两个标量函数，以及 `TABLE(CONTENT_TEXT_CHUNKS(content_column, max_bytes[, overlap_bytes[, min_boundary_bytes]])) AS chunks` 表值函数。显式 `TABLE(...)` 包装用于避免扩展普通函数语法并保持解析器原有冲突基线。表值函数作为前置行源的相关展开，可用逗号或 `CROSS JOIN` 写法；每个块输出 `ordinal`、`text`、typed `block`、`byte_start`、`byte_end`，其中 `block` 绑定当前来源行主键、CONTENT 修订、摘要和原件字节定位。分块大小是 UTF-8 字节数而非模型 token 数，处理限于 4 MiB 文本和每值最多 65,536 块；内容超出边界或来源上下文缺失时必须报错，不能静默截断。表值函数当前只支持通用显式文本 CONTENT，不支持 PDF/OCR/Office 解析或音视频，且仅支持逗号/CROSS JOIN 相关展开；其他连接形态应显式拒绝。上述 SQL 路径可由普通 MySQL 驱动调用，但尚无 SQL 行为、兼容性与性能验收，不能视为 RAG 闭环。

安全边界：`CONTENT_BLOCK_FROM_JSON` 中的源主键、修订和摘要是调用方声明值，不能单独作为已验证引用，系统将其标为 `UNVERIFIED`，`CONTENT_BLOCK_MATCHES_SOURCE` 和 `CONTENT_BLOCK_VERIFIED_TEXT` 均不接受该状态。`CONTENT_BLOCK_FROM_SOURCE` 把来源绑定到 SELECT 当前行的主键和实际 CONTENT 值。DML 失效扫描是事务内的同步维护；无法为旧值或新值构造来源身份时，源更新/删除报错并回滚，避免在证据状态不明时提交。并发发布、历史快照和任何未被扫描注册表覆盖的来源仍须在读取时显式重验：将块与当前源行关联，并调用 `CONTENT_BLOCK_MATCHES_SOURCE` 或 `CONTENT_BLOCK_VERIFIED_TEXT`，后者按该 SELECT 当前可见行版本核验修订和摘要。旧版编码若缺少来源可信状态，也按 `UNVERIFIED` 读取以避免升级后误信。

`CONTENT_BLOCK_MATCHES_SOURCE(block_column, source_content_column)` 可在当前查询的物理基表行上下文中重建来源引用，并比较 schema/table/column、规范主键、修订号和摘要。两列可来自同一表，也可来自 JOIN 两侧并用别名限定；仅当块状态为 READY 且完全匹配时返回 TRUE；NULL 原件返回 FALSE，缺少物理行上下文则报错。`CONTENT_BLOCK_VERIFIED_TEXT(block_column, source_content_column)` 除上述来源身份核验外，还要求文本块带有字节起止定位、定位不超过单次读取上限，且块文本与当前原件该字节范围逐字节相同；状态不可信、来源不匹配、没有字节定位或内容不相同时返回 NULL。`CONTENT_BLOCK_CONTEXT(block_column, source_content_column)` 默认生成受 4 MiB 限制的 JSON 上下文项，包含来源表/列/主键、修订、摘要、定位、处理器和块 ID；只在文本或结构载荷与原件指定字节范围精确相等时声明 `SOURCE_MATCHED` 与 `EXACT_SOURCE_RANGE_MATCH`，返回 schema version 1。可显式传入 `ALLOW_DECLARED_DERIVATION`，允许有来源版本匹配且带字节、页、区域或时间定位的文本/结构化派生内容进入上下文；此时返回 schema version 2、状态 `SOURCE_VERSION_MATCHED` 与 `DECLARED_TRANSFORMATION_NOT_VERIFIED`，并设置 `requires_source_modality_input=true`，同时提供来源 MIME、模态类别和原件字节长度，供调用方选择实际媒体读取/预处理路径。该模式仅记录来源身份和调用方声明的处理器，不认证处理器或派生内容正确性；模型调用方应按策略读取实际原件媒体，不可把该状态提升为事实或已核验 OCR。两种模式均不证明语义相关性或回答正确。DML 会同步置失效并保留块行，暂未自动删除孤儿块。

`CONTENT_BLOCK_SOURCE_SLICE(block_column, source_content_column, offset, length)` 在同一物理行上下文中再次核验块来源绑定，然后从该修订的原件读取至多 4 MiB 原始字节；来源已变化时返回 NULL，返回类型为二进制 MEDIUMBLOB。该函数用于让多模态处理器读取真实媒体内容，不会将 PDF 页定位裁成页面图像，也不会把音视频时间区间转码成片段；客户端需结合上下文中的 locator 调用相应内部预处理器。大对象分段读取可以多次调用，但版本核验必须处于同一 SQL 一致性快照，避免拼接来源修订。

```sql
CREATE TABLE parsed_csv (id BIGINT PRIMARY KEY, parsed CONTENT);
INSERT INTO parsed_csv (id, parsed)
SELECT id, CONTENT_PARSE_CSV(csv_body) FROM source_csv;

SELECT CONTENT_BLOCK_PAYLOAD(p.parsed),
       CONTENT_BLOCK_CONTEXT(p.parsed, s.csv_body, 'ALLOW_DECLARED_DERIVATION')
FROM parsed_csv AS p
JOIN source_csv AS s ON s.id = p.id;

SELECT id, FULLTEXT_SCORE(CONTENT_BLOCK_RETRIEVAL_TEXT(parsed), 'target phrase') AS score
FROM parsed_csv
ORDER BY score DESC
LIMIT 20;
```

该示例中 CSV 内容块仍是派生表示；上下文输出不会把字段值升级为经过人工/模型核验的事实。`CONTENT_BLOCK_RETRIEVAL_TEXT(block)` 对 READY 文本块返回原文本，对 `xmysql.table@1` 生成确定性 Markdown 检索视图，列头附 SQL 类型、单位和含义，逐行保留来源定位，并为源空、源缺失、未处理和转换失败值输出不同标记；通用 JSON 载荷返回其规范 JSON 文本，图片元数据不当作图像语义文本返回。它只接受 UTF-8 且最多 4 MiB，不会静默截断或把 STALE/UNVERIFIED 块用于检索。该视图用于全文/模型召回，权威数值和状态仍以结构化 payload 为准。

对于 JSON 对象数组，可用 `CONTENT_PARSE_JSON_ARRAY(json_body)` 生成同一 `xmysql.table@1` 类型块。比如属性缺失会呈现为 `SOURCE_MISSING`，显式 JSON `null` 则为 `SOURCE_NULL`；嵌套数组/对象及数字仍保留 JSON 原值，不因外观被转换成其他 SQL 类型。只支持有显式 JSON MIME、非空、结构一致可投影的对象数组。

```sql
INSERT INTO derived_blocks(block_id, body)
VALUES (?, CONTENT_BLOCK_FROM_JSON(?));

SELECT CONTENT_BLOCK_TEXT(body), CONTENT_BLOCK_SOURCE_TABLE(body),
       CONTENT_BLOCK_SOURCE_REVISION(body), CONTENT_BLOCK_PAGE(body)
FROM derived_blocks
WHERE block_id = ?;
```

上面展示的是 SQL 入口形状；当前分支代码尚未通过实际客户端/事务往返验收。

来源绑定入口使用 SELECT 当前行上下文：

```sql
SELECT CONTENT_BLOCK_FROM_SOURCE(?, body)
FROM documents
WHERE id = ?;

SELECT CONTENT_BLOCK_TEXT(block_body)
FROM documents
WHERE CONTENT_BLOCK_MATCHES_SOURCE(block_body, body);

SELECT CONTENT_BLOCK_TEXT(b.body), CONTENT_BLOCK_PAGE(b.body)
FROM documents AS d
JOIN derived_blocks AS b ON b.source_id = d.id
WHERE CONTENT_BLOCK_MATCHES_SOURCE(b.body, d.body);

SELECT CONTENT_BLOCK_VERIFIED_TEXT(b.body, d.body) AS evidence_text
FROM documents AS d
JOIN derived_blocks AS b ON b.source_id = d.id
WHERE CONTENT_BLOCK_VERIFIED_TEXT(b.body, d.body) IS NOT NULL;
```

`CONTENT_BLOCK_FROM_SOURCE` 会拒绝没有主键、不是物理基表行、来源列不是 typed CONTENT 或者无法解析其表上下文的情况。JOIN 场景必须使用 `d.body` 这类限定列名；无限定列名在多表上下文中没有唯一来源，不能作为可信来源绑定。派生数据跨表物化后仍需按来源主键、修订和摘要在读取时重验。

向量来源绑定也沿用上述行上下文：`VECTOR_FROM_SOURCE(json, content_column)` 的 JSON 只包含 `model_id`、`model_version`、`space_id`、`source_modality` 和 `elements`，数据库把来源主键、CONTENT 修订与摘要直接绑定到当前行。普通 `VECTOR_FROM_ARRAY(...)` 生成无来源的 `UNVERIFIED` 向量；对持久证据检索应使用 `VECTOR_MATCHES_SOURCE(vector_column, source_content_column)` 或 `VECTOR_VERIFIED_DISTANCE(vector_column, query_vector, source_content_column[, metric])`。不匹配或未验证时谓词为 FALSE、可信距离为 NULL；STALE 向量直接参与 `VECTOR_DISTANCE` 会报错。向量二进制格式从 v1 升至 v2，旧向量读取为 `UNVERIFIED`。模型 ID/版本及共享空间仍由调用方声明，尚无模型目录对其背书。

`FULLTEXT_SCORE(text, query[, mode])` 使用数据库现有 Unicode 分词和停用词规则，返回命中词频整数；`mode` 为 `NATURAL` 或 `BOOLEAN`。CONTENT 输入通过相同的文本 MIME 与 UTF-8 校验（含 JSON/XML 等支持的文本类型），且受 4 MiB 单值上限；PDF、图片和非法编码须先由对应处理器生成文本，不能按字节强制解码。此分数供 SQL 窗口排名与 RRF 融合使用，不等价于 BM25，也不替代已有 `MATCH ... AGAINST` 查询兼容路径。可分别按全文分数降序和 `VECTOR_VERIFIED_DISTANCE` 升序生成候选排名，再用普通 SQL `UNION ALL`、`ROW_NUMBER` 和 `SUM(1/(k+rank))` 做 RRF；各通道在加入排名前必须执行相同的权限、来源版本与结构化条件过滤。当前分数计算按行执行，尚无融合优化器或 RRF 专用执行节点。

SQL 函数 `RRF_SCORE(k, rank1, rank2, ...)` 为同一候选的 1 到 32 个通道名次求 `SUM(1/(k+rank))`；名次从 1 开始，NULL 表示该候选未进入该通道。它是确定性标量函数，可同 `UNION ALL`、`ROW_NUMBER` 与 `GROUP BY` 组成普通 SQL 融合流程；不负责候选生成、跨通道去重、通道权重或优化计划。该执行路径尚未经过 SQL 行为验收。

下面展示同一 SQL 中的通用候选融合形状。`documents.body`、`derived_blocks.body` 和 `derived_blocks.embedding` 都是普通数据库列；文本候选和向量候选各自按通道排序，随后按 `block_id` 去重并用 RRF 合并。查询向量示例值由调用方的兼容编码器提供，不能由模型/空间字符串自我背书。

```sql
WITH lexical_candidates AS (
  SELECT b.block_id,
         ROW_NUMBER() OVER (
           ORDER BY FULLTEXT_SCORE(
             CONTENT_BLOCK_VERIFIED_TEXT(b.body, d.body), ?
           ) DESC, b.block_id
         ) AS channel_rank
  FROM documents AS d
  JOIN derived_blocks AS b ON b.source_id = d.id
  WHERE CONTENT_BLOCK_VERIFIED_TEXT(b.body, d.body) IS NOT NULL
    AND FULLTEXT_SCORE(CONTENT_BLOCK_VERIFIED_TEXT(b.body, d.body), ?) > 0
), semantic_candidates AS (
  SELECT b.block_id,
         ROW_NUMBER() OVER (
           ORDER BY VECTOR_VERIFIED_DISTANCE(
             b.embedding,
             VECTOR_FROM_ARRAY('text-encoder', 'v1', 'shared-space-v1', 'text', 0.12, 0.98),
             d.body
           ), b.block_id
         ) AS channel_rank
  FROM documents AS d
  JOIN derived_blocks AS b ON b.source_id = d.id
  WHERE VECTOR_MATCHES_SOURCE(b.embedding, d.body)
), candidate_ranks AS (
  SELECT block_id, channel_rank FROM lexical_candidates
  UNION ALL
  SELECT block_id, channel_rank FROM semantic_candidates
)
SELECT block_id, SUM(RRF_SCORE(60, channel_rank)) AS rrf_score
FROM candidate_ranks
GROUP BY block_id
ORDER BY rrf_score DESC, block_id
LIMIT 20;
```

该 SQL 形状尚未作为一条实际查询执行验收；首期每个通道仍可能扫描/排序完整候选范围，生产化需增加每通道有界候选、可解释执行计划和融合召回/延迟基准。

### 3 完整性元数据示例

下例为通过 SQL 读取状态视图或 CONTEXT_AGG 得到的逻辑元数据，不定义另一个 HTTP 服务。来源是通用 documents 表的固定行范围。

```json
{
  "result_id": "result-001",
  "result_status": "UNAVAILABLE",
  "source_snapshot_id": "snapshot-108",
  "scope": {"table": "documents", "predicate_hash": "sha256:..."},
  "coverage": {
    "source_coverage": {"enumeration": "COMPLETE", "eligible_rows": 12},
    "processing_coverage": {"ready_rows": 11, "pending_rows": 1},
    "retrieval": {"mode": "VECTOR_EXACT", "executed": false},
    "delivery": {"truncated": false, "terminal": true}
  },
  "error": {"code": "NOT_READY", "retryable": true},
  "items": []
}
```

REQUIRE_ALL 查询在此状态下必须同时以驱动可感知的 SQL 错误失败；状态视图只是解释，不用正常空结果代替错误。成功结果包含稳定来源身份和位置、类型与 value_state。模型上下文中的媒体项使用内容句柄或执行器已经读取的实际模态输入，不能要求用户同步外部媒体目录。

### 4 客户端验收

Java 与 Python 分别完成同一 DDL、提交与回滚、内容参数导入、派生关系 SELECT、精确与近似检索、模型上下文、大对象续读和删除。Java setBinaryStream 与长参数链路验证是否真实有界；Python 驱动验证二进制、DECIMAL、时间、错误和流终结。驱动不支持超大单参数时，使用同一数据库的 SQL 大对象写句柄，不能以增加业务上传服务替代这一验收。

### 5 核心 GenAI 补充契约

文档导入覆盖与 MySQL AI 26.10 对标的 PDF、PPTX、PPT、TXT、HTML、DOCX、DOC，各格式声明处理器版本和限制；不支持或失败必须可见，不能当作成功空文档。Office 转换禁宏、禁自动外链，保留原件与页、幻灯片或段落定位。RAG SQL 便利入口复用相同查询与权限体系，返回回答、实际引用、来源清单、覆盖、模型版本及用量；可以只取上下文，不强迫生成。

当前分支新增的 `AI_RAG_RESULT(question, schema, table, vector_index, embedding_model, generation_model, k[, EXACT|ANN])` 是该便利入口的首个 SQL 原型：仅允许无 FROM 的标量 SELECT；EXACT 默认，ANN 必须显式指定；先校验来源表 SELECT 权限和原生索引，再调用嵌入、向量检索、来源块/CONTENT 回读及生成。JSON 输出包含 `answer`、`answer_status`、`claims`、`citations`、`source_manifest`、`context_manifest_id`、`coverage`、`model_usage` 和 `citation_audit`。命中的引用绑定来源主键、CONTENT 修订/摘要、块 ID、模态定位、处理器/模型及向量空间/索引代次；多模态运行时须明确声明支持并接收经授权的原始媒体字节。没有候选或没有可送入模型的证据时返回 `INSUFFICIENT_EVIDENCE`，不调用生成；过期、不可读或不支持的证据失败关闭。模型输出必须是严格 JSON 的 claim 列表，每条 claim 带一个或多个证据 ID；数据库仅从结构化 claim 构造最终 `answer`，遇到格式错误、无引用或未知 ID 时不返回生成文本。生成结果仍标为 `UNASSESSED`，`claim_support_verified` 固定为 false：ID 属于模型输入不等于引用内容确实支持该主张，更不等于答案正确。该原型尚未通过 SQL 执行、普通驱动、权限绕过、并发更新、回滚/重启、真实多模态模型和质量集验收，不构成 A07 完成；后续还需重排、逐主张语义支持验证及拒答质量门槛。

自然语言 SQL 的生成与执行分离。模型只接触调用方可见的 Schema 及允许外发的解释；结果经 AST、类型、对象权限和当前任务只读策略检查。拒绝多语句、锁定读、文件输出及外发副作用，不能把任意 SELECT 当成无副作用。默认生成文本不执行，显式执行模式经同一执行器及预算限制运行。该能力属于核心 GenAI，不依赖完整 Agent 任务运行时。

同库多轮记录保存 conversation_id、turn_no、输入、输出、来源清单及模型版本。轮次唯一键、前轮版本及 CAS 控制并发，重试沿用调用身份，不重复追加已提交轮次。每轮重查当前授权及证据可用性；历史回答仅为对话内容，不替代原始证据。预算截断历史时披露范围，不能因历史中曾经出现敏感内容而豁免当前权限。

## 十一 复现清单与保留

为每次模型调用保存可授权读取的实际输入清单：来源与语义版本、选中记录和片段、转换参数、模板和提示词版本、模型精确标识及参数、模型输入与输出 Schema、工具结果、采样及截断、错误和用量。实际内容或可重建的不可变依赖必须在声明保留期内可用，只有哈希而无内容不能声称可以复现。

保留清单采用加密与单独访问审计，普通日志只记录摘要和引用。删除、撤权或保留期结束可以依法使复现内容不可用，审计记录该原因。供应商不暴露精确模型权重或行为版本时，明确 reproducibility_level=INPUT_ONLY；不得承诺输出完全相同。恢复已有步骤优先返回已保存输出，显式要求重新推理时创建新调用身份。

## 十二 审核项与验收映射

以下均为未来验收要求，本次未运行这些测试。阶段退出只要求本阶段与前置阶段适用场景通过，P4 不阻塞 P3。核心 SQL 路径必须验收，可选桥接仅在实现和对外提供后适用；P1 未就绪明确报错，P2 再增加等待和覆盖水位。确定性契约逐用例检查；内容识别与语义效果需独立标注集，不能用请求成功率代替。

| 编号 | 阶段 | 场景 | 必须满足 |
| --- | --- | --- | --- |
| C01 | P1 | PDF v1 已有派生正文后替换 v2 | v2 原件不得配 v1 正文；旧来源仅在合法固定版本模式可读 |
| C02 | P2 | 等待解析期间新增符合条件记录 | PINNED 范围稳定，CURRENT_GUARDED 发现范围变化 |
| C03 | P1 | 先查 SQL 后续读大对象 | 同一数据库来源清单与句柄，版本不符显式拒绝 |
| C04 | P2 | 乱序处理任务 | 水位不越过目标范围中的覆盖空洞 |
| C05 | P2 | 上传已提交而向量未就绪 | AT_LEAST_RECEIPT 加 VECTOR 严格请求等待或报错 |
| C06 | P1 | 一百页中两页失败 | P1 逐页状态存在且严格失败；P2 部分模式披露缺口 |
| C07 | P3 | Top K 与权限过滤 | 不把近似或不足 K 的结果冒充穷尽候选 |
| C08 | P2 | 分页中并发插入更新删除 | 稳定来源与排序，无静默漏行或重复 |
| C09 | P2 | 游标过期 | READ_SESSION_EXPIRED，不自动续接最新快照 |
| C10 | P1 | 上传提交后响应丢失 | 同身份重试返回原回执，不新增重复绑定 |
| C11 | P1 | 幂等键相同而内容不同 | IDEMPOTENCY_CONFLICT；过保留期不盲目重做 |
| C12 | P1 | SQL 游标或大对象读取中途断开 | 标准驱动报错且结果不完整，可选桥接另查终结记录 |
| C13 | P1 | SQL 范围续读期间内容更新 | 读取原版本或拒绝，不能拼接新旧字节 |
| C14 | P1 | 查询后撤权或逻辑删除 | 新下载、缓存和结果读取遵守当前策略，不延续旧权限 |
| C15 | P1 | Excel 公式缓存过期或外链失效 | 状态可见，不冒充重新计算或补零 |
| C16 | P1 | Decimal 前导零与本地时间 | Java/Python 往返类型、精度及语义一致 |
| C17 | P1 | 扫描页无文字层 | 实际进行渲染与 OCR；原件与页坐标可回查 |
| C18 | P1 | 混合页及旧 OCR 层 | 区域去重、阅读顺序及来源标明，无静默重复或旧层冒充新识别 |
| C19 | P1 | 空白 手写 旋转及损坏页 | 空白分类、待复核、失败、未处理分别标记 |
| C20 | P1 | PDF 表格与数字识别 | 单元格结构、原始区域及纠正版本可追溯 |
| C21 | P3 | 音视频片段抽样遗漏和时钟偏移 | 披露观察范围与时间误差，不能推出未观察事件不存在 |
| C22 | P2 | 模型无法访问媒体 URL | 输入失败或显式降级，不宣称已使用原始模态 |
| C23 | P4 | 取消与成功并发 | 终态有序裁定，远端未知调用及消耗仍可见 |
| C24 | P4 | 来源记忆和相似问题缓存 | 权限、数据及语义版本匹配后才复用，不泄露隐藏范围 |
| C25 | P2 | 模型推断与事实同时出现 | assertion_kind、来源和人工确认状态分别保存 |
| C26 | P2 | 重建一次模型调用 | 在保留期内还原实际输入与配置；无法复现时解释原因 |
| C27 | P1 | 同一内容生成 Markdown 与类型化 JSON | 内容版本一致；类型及未表达结构可见，源位置仍可读取 |
| C28 | P2 | 章节 表格或时段被拆成多个 chunk | 保留连续和父子关系，能够扩展到必要上下文 |
| C29 | P2 | 重叠窗口或表头重复 | 精确统计不重复计算，context_only 内容可识别 |
| C30 | P2 | 解析器或分块器升级 | 新旧身份和依赖隔离，旧索引不得引用新正文，历史来源可核验 |
| C31 | P2 | 固定编号查询与全量统计 | 可走确定性读取路径，不强制依赖向量 Top K 或模型摘要 |
| C32 | P1 | 原件存在规范模型无法表达的内容 | 转换诊断明确，原始内容可按授权读取，结果不声称无损 |
| C33 | P1 | Java Python 普通驱动导入和查询同一 CONTENT 列 | 同一 SQL 与原生数据库错误，无必需专用服务 |
| C34 | P2 | INSERT UPDATE DELETE 后派生视图及索引维护 | 数据库自动管理依赖，无应用双写和外部检索库同步 |
| C35 | P2 | 文本检索图片及图片检索图片 | 编码空间兼容，原图可读取且来源版本正确 |
| C36 | P3 | 文字检索音频视频片段 | 返回实际音轨或片段及正确时间定位，披露转写和抽样范围 |
| C37 | P3 | 同一行多模态内容参与语义计算 | 实际输入包含声明模态，能力不符显式报错或降级 |
| C38 | P1 | 大对象长参数及范围读取 | 客户端与服务端内存有界，断连清理，续读版本稳定 |
| C39 | P3 | PostgreSQL 加 pgvector 同条件对照 | 功能、质量、性能和组件成本分项，不省略对方现有能力 |
| C40 | P4 | Agent 与普通 SQL 读取相同结果 | 权限、版本、完整性和类型一致，Agent 无专属事实副本 |
| C41 | P1 | 显式事务写入后读取异步派生值 | 不隐式提交或换快照，未就绪报错，原始值可读 |
| C42 | P3 | 精确与 ANN 改变执行计划 | 共用持久源嵌入，查询向量每语句一次，计划切换不重推理 |
| C43 | P1 | 描述符未知版本 非法长度或摘要不符 | 未知必需格式拒绝，溢出不分配内存，原件及段校验失败可见 |
| C44 | P1 | Worker 超时后旧进程返回 | 接管提高令牌，旧结果拒绝，已提交结果不重复发布 |
| C45 | P2 | 部分产物完成与目录发布间崩溃 | 完整代际指针原子，重启只见旧或新清单，不混配 |
| C46 | P2 | 读取完整旧代际但来源又有提交 | 明示来源截止点，有限新鲜度不符报错，不宣称最新 |
| C47 | P2 | 一页失败阻止新代际 | 旧代际可按策略读；新代际不发布，重试或显式改范围 |
| C48 | P3 | ANN 基段加 delta 并发新增更新删除 | 共用持久源向量，删除与去重正确，不静默跳过增量 |
| C49 | P3 | 合并截止点前后并发提交且重启 | 清单与吸收范围原子切换，旧读者保留，恢复无漏重 |
| C50 | P1 | 畸形 PDF 压缩炸弹与路径输入 | 白名单及解压限额，隔离进程失败，SQL 主进程可用 |
| C51 | P2 | 原文指令诱导越权 SQL 或模型外发 | AST 授权和目的地策略拒绝，隐蔽来源与凭据不泄露 |
| C52 | P2 | 撤权后句柄 缓存或跨域去重命中 | 即时拒绝且不披露隐藏计数，历史代际无绕过权限 |
| C53 | P3 | 媒体解码与 delta 合并占满配额 | 有界队列及隔离生效，原件不丢，SQL P95 按固定负载测量 |
| C54 | P2 | 模型超时未知结果及批次并发重试 | 预留账目守恒，不重复释放；超限或取消明示状态 |
| C55 | P2 | 源行版本回收后重启并读旧代际 | 持久关系投影与内容引用可恢复，不从当前行补值 |
| C56 | P2 | 新旧完整代际并发完成 | 较旧 CAS 失败不得刷新前驱覆盖较新 G，服务前沿不倒退 |
| C57 | P2 | 全视图有坏文件但选中子范围已就绪 | CAPTURED_SOURCE 可严格返回该子范围；未完成 G 不作为发布入口 |
| C58 | P2 | 官方文档格式逐项导入 | PDF TXT HTML DOCX PPTX DOC PPT 均有原件、处理状态和定位，不支持不计为通过 |
| C59 | P2 | 单一 SQL 完成检索回答及引用回读 | 引用指向实际模型输入，版本一致，无证据不伪造引用 |
| C60 | P2 | 批量摘要与结构化抽取 | Schema、精确类型、逐项错误和重试身份可核验 |
| C61 | P2 | 自然语言生成越权或有副作用 SELECT | 生成执行分离，AST 权限拒绝多语句 锁定读 文件输出及外发 |
| C62 | P2 | 多轮追问并发 撤权与历史截断 | 轮次 CAS，当前权限有效，旧答案不当事实，截断可见 |
| C63 | P2 | 同内容重复导入 替换及显式删除 | 来源身份和 DML 依赖明确，对方重复导入语义单列比较 |
| C64 | P3 | 同文件同问题的 MySQL AI 功能对照 | 固定实际构建和模型，功能质量性能可靠性分别留证 |
| C65 | P3 | 普通 Java Python 完成核心 AI 闭环 | 同一数据库语义，无必需业务 SDK；例程名兼容范围明确 |
| C66 | P2 | 自然语言 SQL 正向正确性与歧义 | 独立标注集按实际结果等价性评估；过滤 JOIN 聚合 NULL 排序时间及合理拒答分项报告 |
| C67 | P1 | 分块切到表头 单位与条件边界 | 父子和相邻关系可追溯，缺失不伪装为空或完整 |
| C68 | P2 | 编号 版本与全量统计查询 | 使用精确关系语义，不由语义相似或 Top K 改写条件 |
| C69 | P2 | 全文与向量候选重复或量纲不同 | 同一来源范围，RRF 按排名融合，身份去重且保留通道贡献 |
| C70 | P3 | ANN 过滤后候选不足 | 有界继续探索或按声明回退，披露不足，不放宽权限或谓词 |
| C71 | P2 | 重排超时 能力不符或模型升级 | 实际输入逐项对齐，严格报错或显式降级，旧分数不跨版本复用 |
| C72 | P2 | 补全表头 邻段或媒体前后文超预算 | P2 文本图像、P3 音视频；同来源同授权，披露结构缺失及未观察范围 |
| C73 | P2 | 候选到上下文之间来源更新撤权 | 固定版本并重查权限，不能跨修订拼接或使用失效缓存 |
| C74 | P2 | 来源全部就绪但语义证据不足或冲突 | 数据 COMPLETE 与答案状态分开，按策略拒答 澄清或证据错误 |
| C75 | P2 | 分块 模型或检索策略升级 | 新摘要及依赖触发适用回归，不复用不兼容缓存或评分 |
| C76 | P3 | 近邻召回高但必要证据缺失 | ANN 与证据真值分开，多组证据完整率不能由近邻指标替代 |
| C77 | P2 | 逐阶段诊断和隐藏来源计数 | 命中淘汰重排补全可归因，诊断不能泄露无权范围 |
| C78 | P3 | 消融实验与留出集独立验收 | 同源修订不跨集，固定预算，报告收益 退化和不确定区间 |
| C79 | P2 | 距离 ORDER BY 与 RAG 语义重排同时存在 | 普通 SQL 距离顺序不被静默替换，语义排序须显式请求 |
| C80 | P2 | 模型解释精确金额 数量和日期计算 | 使用 SQL 权威结果，类型 单位 范围与精度保持，不重新心算 |
| C81 | P2 | 关键回答主张关联引用和计算结果 | 主张映射实际输入，按映射区分 ANSWERED 与 UNASSESSED；技术失败不伪装拒答 |
| C82 | P2 | 上层摘要或改写改变数字和限制条件 | 新文本新生成版本并重新核验，旧验证状态不得沿用 |
| C83 | P2 | 引用存在但原文不支持回答结论 | 独立反例要求修正或披露无法支持，引用有效不能直接判作主张核验通过 |
| C84 | P3 | 高拒答或超时制造表面准确率 | 固定全部问题账目，正确率 正确覆盖 回答覆盖及错误肯定分别报告 |
| C85 | P0 | 固定现有 MySQL 能力及已知缺口 | 记录实际版本 修改及测试；已支持 待测 已失败 未支持分别可追溯 |
| C86 | P1 | 旧表及 CONTENT 混合事务提交回滚 | 既有 SQL 行为及原子性保持，不隐式提交或绕过权限 |
| C87 | P1 | 新关键字 函数和目录影响旧语句 | 旧标识符和同名对象不被无声抢占，结果类型及错误语义兼容 |
| C88 | P2 | AI 关闭 断网 超时或 Worker 停止 | 普通服务和旧表查询继续工作，不隐式外发或模型收费 |
| C89 | P1 | 旧类型备份 复制及导入导出回归 | P1 起旧链路回归，新类型按阶段单列，禁用不等于可直接降级 |
| C90 | P3 | 旧功能集在开关及混合负载下执行 | 每阶段旧功能零新增回归，P3 综合资源验收，不跳过失败 |
| C91 | P1 | WAV `fmt `/`data` 边界、MIME、块对齐或 PCM 参数伪造 | 解析必须有界失败，不读取样本，不生成 READY 衍生块 |
| C92 | P1 | WAV 音频上下文组装 | 来源版本必须匹配；上下文标注未经验证转换并要求实际媒体输入；元数据不能冒充转写文本 |
| C93 | P2 | 压缩 WAV、RF64 或多 `data` 块 | 返回明确不支持，不猜测时长，不静默跳过音频块 |
| C94 | P1 | 同一原件多个文本块分别生成向量 | 每个向量绑定正确块 ID 和当前原件版本，不可仅凭维度或共享空间混淆块 |
| C95 | P1 | 块向量绑定旧修订、陈旧块或不同原件行 | 构造失败或匹配返回 FALSE/NULL；旧向量与块标识的 v1/v2 兼容读取保持明确 |
| C96 | P1 | BM25 单词得分及语料统计不匹配 | 按固定语料的独立公式核对得分、长短文归一化、零频词和无效统计拒绝 |
| C97 | P1 | SQL 词项展开及文档频率统计 | 表值 token 行必须和全文打分共用同一分词、停用词规则，位置从过滤后的零起序号稳定产生 |
| C98 | P2 | 显式 SQL 多模态嵌入 | `AI_EMBED_BLOCK` 在普通表、基础 JOIN 和派生表的显式投影中，仅当 runtime 已配置且来源修订匹配时调用；provider 收到实际文本或有界媒体字节并遵守取消；返回向量记录模型/版本/空间及来源块身份；无 provider、超限媒体、错误模态与中断均须可诊断 |
| C99 | P2 | 显式 SQL 文本生成与摘要 | `AI_GENERATE`/`AI_SUMMARIZE` 仅由显式投影调用，文本或文本 CONTENT 输入受 1 MiB 限制；运行时取消和提供方错误向上传递；普通、JOIN、派生查询路径语义一致；`AI_GENERATE_RESULT` 返回模型及 token 用量元数据；持久调用账本仍需单独完成 |
| C100 | P2 | 显式多模态块理解与证据回传 | `AI_DESCRIBE_BLOCK(block_column, source_content_column, model_alias)` 仅接受当前源行 READY 块；文本块传文本，图像/音频/视频块传当前原件实际字节、MIME 与块定位；图像/音频/视频块类型必须与原件模态一致；媒体源限制 16 MiB；`AI_DESCRIBE_BLOCK_RESULT` 返回文本、模型/用量、原件来源版本、块 ID、模态及原件定位；普通表、JOIN、派生投影一致，取消、错误、来源变更和超限均明确失败 |
| C101 | P2 | 视频帧作为可追溯模型上下文 | `CONTENT_BLOCK_CONTEXT(..., 'ALLOW_DECLARED_DERIVATION')` 接受与视频 CONTENT 当前版本绑定且带有效起止时间的视频帧块；上下文标明其需要实际原件模态输入，保留帧/片段时间位置；不能把帧元数据或描述文字当成已读取的视频像素 |
| C102 | P2 | 已确认语义目录进入模型上下文 | 只将当前源 CONTENT 列与源表中 `CONFIRMED` 定义传入块上下文及显式多模态描述请求；仅附带 `CONFIRMED` 且两端关联列均有 SELECT 权限的关系；保留定义版本、单位、口径、空值含义和确认来源；`SUGGESTED`/`DRAFT` 推断不得提升为事实 |

## 十三 公开依据

本契约是本项目的设计决策，以下资料用于界定现有机制及其边界，不证明 XMySQL 已具有这些能力。

- [PostgreSQL 事务隔离](https://www.postgresql.org/docs/18/transaction-iso.html)：连接内快照语义的参考。
- [S3 条件请求](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)：固定对象状态读取的参考。
- [BigQuery 元数据缓存](https://docs.cloud.google.com/bigquery/docs/metadata-caching-external-tables)：显式新鲜度约束的参考。
- [pgvector 过滤](https://github.com/pgvector/pgvector#filtering)：近似检索及过滤导致候选不足的边界。
- [openpyxl 教程](https://openpyxl.readthedocs.io/en/stable/tutorial.html)：公式文本与缓存值读取行为的参考。
- [pypdf 文本提取](https://pypdf.readthedocs.io/en/stable/user/extract-text.html)：扫描件、OCR 文本层与数字文本的区别；纯图像页需要 OCR，文本提取不能替代图像识别。
- [Debezium Outbox](https://debezium.io/documentation/reference/transformations/outbox-event-router.html)：事件身份、去重和顺序需要独立定义。
- [DBOS 持久执行](https://www.dbos.dev/durable-execution)：检查点、恢复及外部副作用未知窗口的边界。
- [MinerU 输出契约](https://opendatalab.github.io/MinerU/reference/output_files/)：阅读格式、结构化内容和中间结果的职责区分，具体能力按部署版本核对。

补充对照依据：[pgvector 官方项目](https://github.com/pgvector/pgvector)、[PostgreSQL 大对象](https://www.postgresql.org/docs/current/largeobjects.html)、[PostgreSQL 全文检索](https://www.postgresql.org/docs/current/textsearch.html)、[MySQL 长参数协议](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html)。

- [Lance 索引生命周期](https://lance.org/guide/indexing/)：版本化索引、未索引增量扫描与维护参考。
- [Qdrant 不可变结构](https://qdrant.tech/articles/immutable-data-structures/)：不可变段及合并机制参考，不作为 XMySQL 已实现证明。
- [OWASP 文件上传](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html)与 [SSRF 防护](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)：隔离、限额和网络边界参考。

补充核心功能依据和当前状态见 [MySQL AI 追平矩阵](2026-11-25-xmysql-mysql-ai-parity-matrix.md)，官方能力与本项目拟议扩展分开记录。

检索参考：[RRF 融合](https://learn.microsoft.com/en-us/azure/search/hybrid-search-ranking)、[语义二次排名](https://learn.microsoft.com/en-us/azure/search/semantic-search-overview)。本项目的候选预算、质量阈值及回答契约须自行实现和评测。

## RIFF/WAVE 元数据适配器

`CONTENT_PARSE_AUDIO(content_column)` 当前仅支持 RIFF/WAVE PCM 与 IEEE-float。它扫描有界数量的 chunk 头和最多 64 KiB 的 `fmt ` 块，不读取音频样本；核对 RIFF 边界、唯一 `fmt `/`data` 块、通道、采样率、位宽和 block alignment，输出样本帧数与基于帧数/采样率计算的时长。类型载荷为 `xmysql.audio@1`，音频块记录半开 `[0, duration_ms)` 时间范围（短于 1 ms 时没有可表达的毫秒级定位），并声明 `container_header_only`、`samples_decoded=false`、`transcription_available=false`。须通过 `CONTENT_BLOCK_CONTEXT(..., 'ALLOW_DECLARED_DERIVATION')` 才能将描述符带入上下文，模型输入仍需从来源 CONTENT 提供实际音频。压缩编码、RF64、多 data chunk、解码、分段、转写和语音语义检索不在此解析器范围。新增 content 层用例覆盖有效 PCM、RIFF/data 越界、MIME/codec/对齐错误、缺失或重复 chunk 及 chunk 数上限；engine SQL 用例通过 `INSERT ... SELECT` 持久化 READY 块、核对 1 秒定位，并确认派生上下文标出 `SOURCE_VERSION_MATCHED`、`DECLARED_TRANSFORMATION_NOT_VERIFIED`、实际音频模态输入要求及不可用转写状态。注入的测试 runtime 收到与原件逐字节相同的音频、MIME、时间定位和来源版本，并返回可追溯 JSON；畸形输入的 INSERT 失败且目标表没有派生行。该验证没有证明 WAV 样本解码或模型语音质量。

格式依据：[Microsoft RIFF chunk 说明](https://learn.microsoft.com/en-us/windows/win32/xaudio2/resource-interchange-file-format--riff-)与 [WAVEFORMATEX 采样格式约束](https://learn.microsoft.com/en-us/windows/win32/api/mmeapi/ns-mmeapi-waveformatex)。

## 向量绑定到具体内容块

`VECTOR_FROM_SOURCE(json, content_column)` 绑定原件行版本，但无法区分同一原件的多个分块。新增 `VECTOR_FROM_BLOCK(json, block_column, source_content_column)`：仅接受当前查询中的 READY 内容块和 typed CONTENT 列；要求内容块来源身份、主键、修订和摘要与当前关系行完全匹配，再将其块 ID 编入向量。`VECTOR_BLOCK_ID(vector_column)` 可供 SQL 关联检索命中和内容块。向量编码 v3 增加 32 字节 SHA-256 块 ID；v1/v2 保持读取，现有不含块绑定的值仍写为 v2。检索查询仍应同时校验 `VECTOR_MATCHES_SOURCE(vector, source)`、块 ID 等值及内容块 READY 状态。`AI_EMBED_BLOCK(block, source, model_alias)` 是显式投影原型，可通过服务器注入的 `EmbeddingRuntime` 对文本或受限实际媒体发起嵌入；暂无默认提供方、可持久化批量维护、ANN 索引及自动增量维护。本路径尚未进行 SQL、旧值往返或事务验收。

当前分支的 `VECTOR_SEARCH` 结果契约在既有字段尾部追加 nullable `source_block CONTENT`。命中向量含块 ID 时，执行器只在来源行内查找 READY 块，并同时核对块 ID、来源表/列/主键/修订/摘要；只有调用方对块列有 SELECT 权限才返回该块。块没有存入来源行、身份不匹配或调用方无列权限时返回 NULL，客户端仍可用 `source_block_id` 按自己的授权 SQL 定位；该字段不会把相似度提升为语义证据，也不证明生成答案正确。检索结果同时携带仅供执行器内部使用的来源行上下文和 `source_content` 到物理来源列的映射，因而同一 SELECT 可将 `v.source_block`、`v.source_content` 直接交给 `AI_DESCRIBE_BLOCK`、`AI_EMBED_BLOCK` 或 `CONTENT_BLOCK_CONTEXT`，继续核对来源并读取实际媒体；模型上下文附带已确认的源列语义及对当前会话可见的已确认关系提示，但不会自动读取关系目标行。engine 包 Go 构建已通过；该 SQL 组合路径及受限列权限场景仍待运行验收。

可用 `VECTOR_MATCHES_BLOCK(vector, block, source)` 原子校验向量、READY 内容块及当前来源行；`VECTOR_VERIFIED_BLOCK_DISTANCE(vector, query_vector, block, source[, metric])` 仅对这三者身份一致的候选返回距离，否则返回 NULL。它防止遗漏块 ID 比较造成同一文档内块级向量错配。两个算子仍需通过 SQL 行为和权限验收。

`FULLTEXT_TOKEN_COUNT(text)` 和 `BM25_TERM_SCORE(text, term, document_count, document_frequency, average_document_length[, k1[, b]])` 复用数据库同一 Unicode 分词器和停用词。后者实现 Okapi BM25 单词得分，默认 `k1=1.2`、`b=0.75`；输入语料统计应在同一 SQL 候选范围上计算，`document_frequency <= document_count`，平均长度必须大于零。调用方可按查询词对单词得分求和，再与向量通道名次用 `RRF_SCORE` 融合。定向回归现已用独立公式校验默认参数、长短文档归一化、缺词零分及无效文档频率拒绝，并通过普通 SQL 执行路径验证得分与分组文档频率。该函数不负责自动生成候选、语料统计或词干化，也不代表数据库已经具备原生 BM25 倒排索引。

`TABLE(FULLTEXT_TOKENS(text)) AS terms` 将相同分词/停用词处理结果展开为 `token` 与 `position` 两列；`position` 是过滤停用词后从 0 开始的 token 序号。它可作为 FROM 首行处理字面查询词，也可通过逗号或 `CROSS JOIN` 对前序文本列作受限 lateral 展开；每个输入最多 4 MiB、最多 262,144 个词。现在普通 SQL 回归已验证停用词过滤后的稳定位置、词频展开及以 `COUNT(DISTINCT source_id)` 聚合文档频率。解析器会将 `CROSS JOIN` 规范化为普通 `join`，执行器因此显式接受无 `ON`/`USING` 条件的内连接作为笛卡尔 lateral 连接，同时拒绝带条件及外连接形态。当前没有自动多词 BM25 聚合算子或倒排索引候选计划。

`AI_EMBED_BLOCK(block_column, source_content_column, model_alias)` 是显式 SELECT 投影函数，支持普通表、基础 JOIN 和派生表投影。结果记录在投影到派生表时保留每列对应的来源上下文，避免多来源 JOIN 只保留单个行身份。服务器必须通过 `XMySQLExecutor.SetEmbeddingRuntime` 安装受控 runtime；普通客户端无需调用专用 AI HTTP API，但尚未提供默认 provider、模型目录、凭据管理或计费配额目录。调用只接受当前行 READY 内容块，且块身份必须匹配来源列的当前主键、修订与摘要。文本块传递数据库生成的文本表示；图片、音频和视频块将当前来源的实际媒体字节及 MIME、块定位信息传给 runtime，媒体源上限为 16 MiB，超限明确报错。runtime 的模型 ID、版本、兼容空间和向量坐标会编码为带来源块身份的 VECTOR 值。调用跟随 SELECT 的 context 取消，但外部模型调用在客户端重试时可能重复；模型 ID/空间是已安装 runtime 的声明，仍需后续模型目录、空间签名与授权治理。上述路径只有服务代码构建证据，派生来源绑定、SQL 行为、协议、取消和实际多模态模型行为未验收。

`AI_GENERATE(prompt, model_alias)` 与 `AI_SUMMARIZE(text, model_alias)` 是显式文本生成投影，分别将任务标记为 `GENERATE` 和 `SUMMARIZE`，调用通过 `XMySQLExecutor.SetGenerationRuntime` 注入的服务器运行时。输入可为文本或明确的文本 MIME `CONTENT`，CONTENT 在数据库内经文本类型/UTF-8 解码，所有 prompt 上限为 1 MiB；函数不读取图片/音视频，也不会由普通 SELECT 隐式调用模型。SQL 输出为生成的 UTF-8 文本；`AI_GENERATE_RESULT(prompt, model_alias)` 输出包含文本、模型 ID/版本、请求 ID、token 用量和用量是否已知的 JSON 字符串，可由普通 SQL 持久写入 JSON/TEXT/CONTENT 列。调用元数据仍未自动写入持久账本。函数已接入普通表、JOIN 和派生查询投影路径，当前只有源码构建证据，模型提供方、调用取消、文本质量、SQL 行为、审计持久性与准确率均未验收。

`AI_DESCRIBE_BLOCK(block_column, source_content_column, model_alias)` 是显式多模态块理解投影。数据库先核验块为 READY，且绑定的 schema/table/column、主键、修订和摘要与当前关系行的 CONTENT 值完全匹配；文本、表格和结构化块传数据库生成的文本表示，图片区域、音频片段和视频帧传实际 CONTENT 原件字节及 MIME、定位和块 ID，不以路径或向量冒充模型看到的内容。当前媒体读取上限为 16 MiB，runtime 必须按模态和定位处理，超过上限明确失败。`AI_DESCRIBE_BLOCK` 返回生成文本；`AI_DESCRIBE_BLOCK_RESULT` 的 JSON 还携带 `source`、`block_id`、`modality`、`mime_type` 和 `locator`，并列模型 ID/版本及用量，便于保留证据链。调用服从查询取消，并在媒体读取前检查取消信号。该能力当前只有服务代码构建证据；具体模型对定位的遵循、实际 SQL、授权、审计、引用质量和多模态行为均待验收，仍没有持久模型调用账本或默认 provider。

如果源 CONTENT 列和表注释含 `xmysql-semantic:v1:` 定义，块上下文与显式多模态描述请求只携带格式验证通过且状态为 `CONFIRMED` 的源列语义、源表业务名和说明，以及逐条确认为 `CONFIRMED` 的关系定义。关系只有在当前 SQL 会话对源表和目标表的全部关联列均有 SELECT 或列级 SELECT 权限时才附带。字段语义包含业务名、描述、单位、空值含义、适用范围、枚举解释、修订、来源与确认人；`DRAFT`、`SUGGESTED`、`REJECTED` 与无效定义都不会进入可信上下文，由模型提出的建议仍需人工或可信导入流程确认。当前不自动把其他列定义或用户问题扩写进模型输入；多列语义及相关记录的上下文构建仍待实现。

## ISO-BMFF 视频容器元数据适配器

`CONTENT_PARSE_VIDEO(content_column)` 当前支持声明为 `video/mp4`、`video/quicktime` 或 `video/x-m4v` 的 ISO-BMFF 内容。解析器使用分段存储范围读取 box 头和 `mvhd` 所需字段，跳过 `mdat` 大载荷；校验 top-level box 边界、`ftyp`、`moov/mvhd`、版本、时基及时长溢出，并限制 top-level 与 `moov` 子 box 数量。结果是绑定当前内容来源修订/摘要的整片 `video_segment` 块，定位 `[0,duration_ms)`，属性明确 `decode_scope=container_metadata_only`、`frames_decoded=false`。不解码帧、不读取/拆分音轨、不转写、不验证编码支持，也不提供时间裁剪流。`AI_DESCRIBE_BLOCK` 与 `AI_EMBED_BLOCK` 只有在注入的 runtime 能按声明能力处理视频时才可能执行实际模型任务；当前接口把原始媒体和时间定位传给 runtime，尚未实现帧采样/片段裁剪或能力协商。全服务代码构建通过，SQL 行为、损坏文件/边界、取消和真实视频模型均待验收。

## 2026-11-26: cross-connection CONTENT slice revision regression

Added `TestMySQLDriverQueryCompatibility/content_slice_revision_fences_other_connection_changes` using two ordinary Go MySQL connections and 1,200,000-byte values. The reader obtains a revision and first slice, the other connection commits a replacement, and the old-revision continuation must return `sql.ErrNoRows`. A fresh revision reconstructs all replacement bytes; a rolled-back replacement preserves that revision and bytes; deletion stops continuation. The first run failed because `CONTENT_FROM_BYTES` updates retained revision 1: `applyUpdateExpressions` mistook every typed CONTENT value for an external reference whose identity should be preserved. The guard now preserves only actual external references, so inline constructor values receive the incremented row revision before segmentation.

Fresh verification: the focused real-driver test passed; the complete `server/net` package passed (21.924s); `go test -mod=readonly -p 1 ./server/innodb/engine -run 'Test(Content|LargeContent|.*Vector|.*Model)' -count=1 -timeout=5m` passed (2.738s), including the existing segmented-reference preservation and large-object restart tests. This is deterministic interleaving across two connections, not simultaneous-read stress or full engine regression. Assigning an already segmented CONTENT reference (including long-data replacements) still follows the existing identity-preservation path and needs separate row-version design and validation. It is not covered by this fix. Do not treat general streaming revision safety or U09 as complete.

## 2026-11-26: streamed CONTENT replacement and distinct row/object revisions

This follow-up supersedes the external-reference limitation in the preceding revision-regression note. A real prepared UPDATE with `maxAllowedPacket=1 MiB` first failed in pre-write constraint evaluation because `CONTENT_PARAM` had no execution-local context. UPDATE now binds current session expression values before constraint validation and cascade planning, as well as during the final write. Once that failure was fixed, the same client reproduced a reused revision 1 for a committed long-data replacement.

The logical CONTENT row revision is now independent of the immutable object manifest revision. SQL assignment and transient prepared uploads advance the logical revision while keeping object ID, manifest revision and manifest digest unchanged. Internal restoration of an existing reference retains its saved logical revision. Adopting a transient object clears only transient ownership state, retaining the assigned row revision. Storage encoding v3 adds an eight-byte object revision to the existing segmented envelope only when the two revisions differ; v1 inline and v2 segmented values remain readable and same-revision segmented values continue to use v2. Existing binaries without v3 support cannot read these new v3 records.

The real Go-driver regression covers both ordinary binary parameters and multiple long-data packets: first slice, committed replacement on another connection, rejection of old-revision continuation, full replacement reconstruction, rollback preserving committed revision/bytes, and deletion stopping continuation. Both transfer modes passed together (8.756s). Complete content and net packages passed (2.293s and 14.741s) before the final test-only expansion to both modes. Codec tests cover v2 compatibility, v3 round-trip, all truncated v3 prefixes, exact retained object identity and byte reads after reopening the segment store. Engine restart coverage now assigns an existing segmented value to a new row revision before closing and reopening the engine. Server build and vet for content/net/engine passed. Full engine regression is being recorded separately; this slice does not establish upload resume, Java/Python support, crash recovery, backup/restore or general streaming completion.

Verification follow-up for streamed replacements: the full engine run completed in 427.403s with one failure, `TestLargeContentExternalStorageSurvivesEngineRestart`. Its content and revision assertions succeeded; its final object-count assertion incorrectly counted `dependencies.ibd` and `dependencies.json`, which are created by the added UPDATE, as content objects. The assertion now checks the exact retained object-directory ID and excludes catalog files. The corrected CONTENT/vector/model engine group passed (2.372s). No production source changed after the full engine run; the full suite was not rerun after this test-only correction, so that run is retained as failed with a verified fixture correction rather than reported as fully green. Focused client transfer-mode tests, complete content/net packages, build, vet and changed-file diff checks passed as recorded above. Test-modified package data files were restored from pre-run snapshots after all test processes terminated.

## 2026-11-26: model SQL cancellation and ordinary-client KILL QUERY

Model entry points now recheck the statement context immediately after the inference adapter returns, before accepting either generated text or vectors. Cancellation/deadline errors take precedence over a late adapter result and retain the context error through `%w`. This covers query embeddings, block embeddings, generation/summary and multimodal block descriptions, including their JSON result forms. It prevents returning late success; it does not forcibly stop an adapter that never returns or provide a default timeout, call/token budget, provider implementation or durable call ledger.

A 32-case projection matrix covers eight expressions with active context, cancellation before the call (zero provider calls), cancellation during the call and deadline expiration during the call. Its adapters deliberately return success after cancellation; the original source accepted those results and the matrix failed. All 32 cases now pass. Six no-FROM SELECT cases also verify normal explicit generation and rejection before provider execution of WHERE, GROUP BY, HAVING, ORDER BY and LIMIT, which the existing constant evaluator does not execute. Ordinary constant expressions retain their existing evaluator. Plain no-FROM AI_EMBED/AI_GENERATE/AI_GENERATE_RESULT/AI_SUMMARIZE now use the model runtime and statement context; nested model expressions and richer constant-query clauses remain open.

The real Go MySQL driver test gets CONNECTION_ID(), starts a model SELECT, waits until the provider is running, cancels it with KILL QUERY from another connection, and verifies a SQL error rather than the late answer, then successfully runs SELECT 1 on the original connection. This exposed and fixed the system-variable dispatcher's CONNECTION_ID() returning its internal session string instead of the numeric transport connection ID. It also exposed the no-FROM constant SELECT path reporting unknown function AI_GENERATE, now resolved by explicit model dispatch. The wire test currently asserts cancellation error text and connection reuse; MySQL's exact interruption errno/SQLSTATE mapping still needs a separate compatibility check. The provider is a deterministic server-side test adapter, so these results do not establish real-model output quality or HTTP-provider cancellation.

Fresh checks: complete dispatcher (0.580s) and net (22.735s) packages passed; engine model/content/vector/query-semantics group passed (4.999s); final cancellation and no-FROM tests passed (2.470s); server build and engine/dispatcher/net vet passed; touched-file diff check passed. No full-engine rerun was performed in this slice. Test-modified package data files were restored byte-for-byte from pre-run snapshots after tests terminated. Stage 4 remains open.

## 2026-11-26: statement model-call budgets and deadlines

Added server-owned `ModelExecutionPolicy` with positive `MaxCalls`, `MaxInputBytes` and `CallTimeout` limits. Default explicit model SQL allowance is 64 attempted calls, 64 MiB aggregate input and 30 seconds per call. `XMySQLExecutor.SetModelExecutionPolicy` validates changes; policy snapshots are attached to query contexts and retained by forks/child SELECTs. Configuration changes affect new queries, without increasing a running query's allowance. SQL clients cannot raise these limits. This is currently a server-side configuration API alongside runtime injection; no SQL variable, configuration-file mapping or model catalog is supplied by this slice.

All embedding and generation entry points use the same reservation mechanism. Prompt/text plus actual media payload byte lengths are reserved before calling the adapter, under a mutex. Denied calls do not reach the adapter. Attempted calls retain their reservation after provider failure, cancellation or unknown outcome; there is no refund that would let same-statement retries escape the limit. Per-call deadlines inherit earlier query deadlines/cancellation. Late successful results are rejected after deadline expiry. The synchronous adapter must honor context cancellation to stop promptly; no detached goroutines are used to simulate cancellation of a permanently blocked provider.

The pre-wiring SQL regression reproduced two projections calling the provider despite a one-call/five-byte allowance. Verified SQL projection limits and context-fork retention; 20 concurrent attempts against an allowance of two produce exactly two provider calls and 18 denials even when those two calls fail. Embedding and generation share the same byte counter, including media bytes and exact-limit behavior; changing server policy does not expand an existing budget. Deadline tests reject success returned after expiry while leaving the parent statement context uncancelled.

Ordinary Go-driver verification covers text SQL, prepared SQL, UNION ALL branches sharing one statement budget, a fresh budget for the next statement, and a configured deadline rejecting late generation. Complete dispatcher and net packages passed (0.540s and 31.416s); final UNION-only test expansion passed (0.900s). Engine model/cancellation/constant-select/vector/query-semantics group passed (4.700s); server build, engine/net vet and touched-file diff checks passed. No full-engine rerun was performed in this slice. Test-modified package data was restored after test processes terminated.

Remaining governance: token/output/cost reservations, tenant/global concurrency and quotas, durable usage/unknown-outcome records, real-provider configuration, retry idempotency across statements/restart and SQL-visible budget reporting. This in-memory per-statement policy does not establish billing limits, durable accounting, full model governance or Stage 4 completion.

## 2026-11-26: startup INI configuration for model execution limits

The previous statement-budget slice exposed only an internal setter. Startup configuration now reads `[ai]` keys `max_calls_per_query`, `max_input_bytes_per_query` (raw bytes) and `call_timeout` (Go duration such as `750ms` or `30s`). NewCfg and the engine policy use one shared default definition: 64 calls, 67108864 bytes and 30 seconds. NewXMySQLExecutor snapshots the loaded configuration; existing partial programmatic Cfg values retain positive defaults for unspecified limits. The sample is in `conf/default.ini`. These limits govern explicit model SQL; loading configuration does not install a provider or invoke models.

Missing settings preserve defaults. Zero, negative, malformed and overflowing values log a warning and use the bounded default for that setting, matching the existing configuration loader's fallback convention. Zero does not disable inference. There is no file watcher/hot reload; restart or the existing server-side policy setter applies changes to future queries. SQL clients cannot increase these limits.

The original INI-to-executor test failed because an INI call limit of one was ignored. It now proves the loaded call limit rejects a second model projection, the 100-byte input limit rejects a 101-byte prompt before provider execution, and the configured 20ms deadline reaches the provider and rejects late success. Seven configuration cases cover missing/custom/partial settings, zero, negative, malformed and overflowing values. Complete conf and dispatcher packages passed (2.363s, 0.658s); engine model/vector/query-semantics group passed (5.095s); focused real-client cancellation and statement-budget checks passed (1.119s); server build and conf/engine/net vet passed.

The full net package run failed (25.416s) in the earlier large-CONTENT inline-parameter INSERT test: the driver hit its five-second read timeout and returned invalid connection. Its isolated rerun passed (7.728s for the entire multi-step test). This does not establish that the intermittent transfer failure is fixed, nor prove its cause; preserve the failure log and investigate separately. Do not report the complete net suite as passing for this snapshot. No transfer timeout or payload was relaxed in this slice. Full-engine regression was not rerun. Package data files changed by tests were restored from pre-run snapshots after tests ended. Stage 4 and the overall implementation remain open.

## 2026-11-26: remove media payload expansion from native prepared CONTENT SQL

Investigated the intermittent five-second inline-parameter CONTENT upload timeout without changing payload sizes or timeouts. Three pre-change real-driver profile runs used the same 1,200,000-byte payload and reported INSERT times of 3.596s, 2.187s and 3.036s; the complete group took 30.848s. CPU profiling attributed 15.80s cumulative / 37.29% of CPU samples to Tokenizer.scanString, with substantial copying and GC. Ordinary eligible CONTENT parameters were expanded into large SQL literals and repeatedly parsed, unlike already typed long-data parameters. The profile does not prove every possible cause of the earlier intermittent timeout, but identifies a measured avoidable cost on that exact path.

PreparedStatement.BindInlineContentParameters now binds ordinary string/binary parameters of eligible CONTENT_FROM_BYTES constructors with literal MIME through the existing execution-scoped CONTENT_PARAM representation. The bound SQL stays small; bytes are owned typed values, preserving existing DML revision assignment, segmentation, permissions and transaction behavior. Streamed bindings are retained, NULL/non-string validation and unrelated parameters keep their previous paths. Ordinary BLOB parameters and constructors whose MIME is not statically eligible retain their existing binder. Both normal and streamed values remain scoped to the current native handler execution. The scope clears its private parameter map after completion so transaction/session snapshots cannot retain large input payloads through that map. The legacy standalone protocol handler was not migrated to this execution-scoped optimization.

Under the same three-run profile command, INSERT times became 155ms, 161ms and 118ms; the complete group took 4.350s. Total sampled CPU decreased from 42.37s to 4.08s and string scanning left the top CPU entries. Input-packet log sizes remained approximately 32.73 MB in both runs, so packet logging was not removed to obtain the improvement. These are local diagnostic measurements with profiling, not a general throughput or latency guarantee. Profile binaries, logs and CPU files are under `C:/Users/zx187/AppData/Local/Temp/xmysql-upload-profile-fb9507b936bf472689da10675ac81731`.

After final parameter-scope cleanup, complete protocol, dispatcher and net packages passed (0.434s, 0.588s and 15.347s), including large upload/readback, revision fencing, rollback, deletion, permissions, model cancellation and budgets. New protocol tests verify SQL remains under 256 bytes for 1.2 MB binary content, exact owned payload bytes including escapes, parameters after CONTENT, NULL vs empty content, unchanged unrelated parameters and preexisting bindings. Server build, protocol/net vet, Go formatting and changed-file diff checks passed. No parser or engine source was changed in this slice and full engine regression was not rerun. Test-written package data was restored from pre-profile snapshots after all test processes terminated. This supersedes the previous snapshot's failing-net-suite status for this newly verified source; broader load, Java/Python client and recovery gates remain open.

## 2026-11-26: Python/JDBC CONTENT interoperability and metadata negotiation

Added reproducible opt-in external-client acceptance to the temporary Go listener fixture. Python uses pinned PyMySQL 1.2.4 and text-protocol parameters with binary prefix. Java uses the project's Connector/J 8.0.33, exercising both client-emulated prepared statements and confirmed native ServerPreparedStatement instances. The fixture supplies only its temporary loopback endpoint/database/test account; dependencies are explicitly configured and missing dependencies fail an enabled test. Normal test runs explicitly skip this optional external group. Run instructions are in `client_compatibility/CONTENT_ACCEPTANCE.md`.

The first external runs exposed real protocol defects despite the earlier Go-driver checks: binary CONTENT was advertised as generic BLOB with a UTF-8 character set, causing PyMySQL UnicodeDecodeError and incorrect JDBC type metadata; PREPARE always emitted metadata EOF packets even under CLIENT_DEPRECATE_EOF, causing Connector/J to parse an EOF as a column definition; binary execute results also emitted an extra metadata terminator, causing native JDBC result sets to appear empty. Native column definitions now retain binary/varbinary/blob family type codes, LONGBLOB for CONTENT/VECTOR, BinaryFlag and charset 63. PREPARE and binary execute omit metadata terminators only for negotiated EOF deprecation; legacy framing is retained otherwise. Protocol reference: https://dev.mysql.com/doc/dev/mysql-server/8.4.10/page_protocol_com_stmt_prepare.html . No driver assertions were relaxed to accept incorrect metadata or framing.

Both clients passed binary CRUD with all 256 byte values in 131109 bytes, binary LONGBLOB metadata, revision-fenced bounded slices, revision advancement on update, rollback and deletion. Native JDBC additionally passed setBinaryStream with 2097189 bytes, 256 KiB send chunks and a subsequent scalar parameter. The driver limit remains 1 MiB. An intermediate test incorrectly selected the whole 2 MiB row and hit the driver's PacketTooBigException; the final test checks SQL length and every byte through bounded CONTENT_SLICE calls instead, retaining the original upload size and packet limit. This is a corrected test workflow, not a server packet-limit change.

Fresh full run with external clients enabled: common 2.427s, protocol 0.536s, net 27.246s, all passed. Client subprocesses emitted PASS with exact driver versions. Added independent tests for binary column type/charset, PREPARE framing with and without EOF deprecation, binary row placement and contiguous packet sequence numbers. Server build, net/protocol vet and changed-file formatting/diff checks passed. Test-modified package files were restored from pre-run snapshots after all test processes terminated. No full-engine rerun was performed in this slice.

This supersedes earlier statements that Java/Python CONTENT CRUD had no client evidence, within the exact cases above. It does not establish all driver features, Python binary prepared statements (PyMySQL uses text), cross-client concurrent updates, authorization/revocation in those drivers, restart/resume, memory ceilings, backup/restore or broad MySQL equivalence. Native AI/multimodal implementation remains in progress.

## 2026-11-26: native CONTENT cold physical backup, restored provenance and dependency maintenance

Added `TestPhysicalBackupRestoresContentInIndependentDirectory`. It starts an engine, stores segmented textual CONTENT, advances its logical row revision to 2 without changing the immutable object revision, persists a source-bound block using ordinary INSERT SELECT, closes the engine and invokes its existing physical backup. The archive manifest must include segment files, the immutable object manifest and dependency catalog files. After archive verification, the test moves the closed source directory within its own temporary root, restores into a different directory, starts the restored engine and verifies exact original bytes, SHA-256, byte length, row revision 2 and source-verified block text through a JOIN. It corrupts a restored segment and requires a SQL error, restores those test bytes, then updates the source and requires the derived block to become STALE. The backup archive mechanism already traversed the complete data directory and required no replacement; this slice fixes the SQL/provenance/startup mechanisms needed to use its restored CONTENT data correctly.

The test exposed and drove these corrections:

- Simple INSERT SELECT used a DML-only expression evaluator without source identity or segment-store context. Native CONTENT/VECTOR/AI expression projections now use the existing general SELECT executor, retaining typed results and source context.
- Single-column source IDs differed between ordinary SELECT/source-bound INSERT and JOIN/derived invalidation. Source identities now consistently use the single-column storage bytes; multi-column source identities keep the existing tuple encoding. Physical B+Tree/index encodings are not changed. Same-row invalidation had the same defect and now uses the same helper; an independent test first reproduced READY remaining after a source change and now passes.
- JOIN source rows lacked segment-store execution context, while native scalar errors could fall through to a successful NULL. JOIN now carries data-directory, statement and session/authorization context; native content/vector/model expression errors propagate. Actual segment corruption is tested through SQL.
- Cold mapping reconstruction treated `.xmysql-content/dependencies.json` as a user table definition, preventing later DML after restore. Filesystem mappings and the CONTENT mapping-completeness scan now apply the schema manager's existing exclusion of `.`, `#` and `@` internal directories.
- Derived invalidation compared lowercase loaded column types against uppercase native type constants. It now normalizes those types, so restored CONTENT/VECTOR columns participate in maintenance.

Legacy prototype source references created using the inconsistent single-key JOIN encoding are not silently converted; they need rederivation against current source identities. This is a correctness change in the new provenance feature, not a physical index format change.

Fresh complete package run: engine passed in 550.711s (1970 top-level passes, 2 skips), manager in 15.092s (332 passes, 5 skips), backup in 3.448s (11 passes). Counts exclude nested subtests. The complete net package with PyMySQL 1.2.4 and Connector/J 8.0.33 enabled passed in 34.609s. Server build and engine/backup vet passed. Broader manager vet reported the preexisting `DefragmentationStats` lock-copy warning in `tablespace_defragmenter.go:328`; the same assignment was confirmed in HEAD and was not changed. Changed-file formatting/diff checks passed. Five test-modified fixture files were restored and all snapshotted baseline file hashes checked; no new fixture files remained in the inspected data directories.

The evidence proves this stopped-engine CONTENT backup/restore workflow and restored dependency behavior. It does not establish online backup consistency, interrupted transfer/restore recovery, logical dump/PITR integration, vector-index backup restoration, all media families or general backup/recovery completion. Stage 1's combined catalog/recovery work item and the overall AI/multimodal goal remain open.


## 2026-11-26: model-free JOIN typing and schema-only SELECT preparation

Fixed two distinct sources of unintended inference:

- JOIN projected its first row to infer result types before projecting the actual output rows. A two-row model query exhausted a two-call budget. Known model functions now expose their fixed SQL return type (VECTOR or VARCHAR) without executing the expression; empty JOIN results retain those types. The regression covers eight text, CONTENT, block-embedding and block-description expressions, both two-row and empty results. It does not establish single evaluation for arbitrary non-model functions or model expressions in sorting.
- Native COM_STMT_PREPARE previously executed a SELECT with NULL-bound parameters to obtain columns. Literal prompts therefore called the model during preparation. SELECT/WITH preparation now calls an authorized schema-only describer that reads the detached AST and existing table definitions. It does not dispatch the SELECT, evaluate expressions, scan rows or invoke a runtime. Constants, physical columns, root model functions, base/derived JOINs, homogeneous UNIONs, CTEs and scalar-subquery output types are described without result materialization. NATURAL/USING output shape and RIGHT JOIN ordering reuse execution's shared shape merger.

The dispatcher applies the existing real-session user/host/active-role authorization before catalog description; the describer also applies existing column privilege checks. Handler-level assertions verify both early model output aliases, VECTOR LONGBLOB/binary/charset-63 metadata, an allowed CONTENT table and withheld metadata for an unauthorized table. Real TCP driver tests first reproduced calls at PREPARE for constant, table, JOIN, UNION and derived model queries; they now require zero preparation calls and one actual provider call per EXECUTE output row.

Static description is deliberately incomplete. Unknown function types, heterogeneous set-operation types, views without a physical table definition and table-valued functions currently omit optional PREPARE result metadata; the existing execution response still supplies its result metadata. They never trigger a SELECT execution fallback. Some clients may defer getMetaData/cursor selection until execution for those shapes. Completing those static contracts and broader MySQL metadata parity remains work; this is not a claim of complete PREPARE compatibility. SHOW/DESCRIBE/EXPLAIN metadata handling remains on its existing path.

Fresh verification: all 137 selected engine top-level tests passed in 68.576s, covering model/native content/vector, JOIN, correlated/derived query and compatibility paths; the full net, dispatcher and protocol packages passed in 35.395s, 1.371s and 0.724s. Net included enabled PyMySQL 1.2.4 and Connector/J 8.0.33 (emulated and native prepared) CONTENT acceptance, including the 2,097,189-byte JDBC stream. Subsequent direct PREPARE-field/privilege assertions passed with the real-driver regression (1.504s). Server build and engine/net/dispatcher/protocol vet passed. The preceding full-engine run predates these metadata changes; no new full-engine claim is made. Red/green and JSON evidence: C:/Users/zx187/AppData/Local/Temp/xmysql-prepare-metadata-904e512d8bdd45f983c6967bec55eba3. Model-provider integration, complete metadata inference and the broader AI/multimodal plan remain open.

## 2026-11-26: declared native table-function schemas, empty results and live MySQL comparison

VECTOR_SEARCH (19 columns), CONTENT_TEXT_CHUNKS (5 columns) and FULLTEXT_TOKENS (2 columns) now define query-local result columns and types independently of arguments and row count. Their existing empty-source constructors and SELECT/WITH static description reuse those definitions. PREPARE can therefore describe a query-vector AI_EMBED inside VECTOR_SEARCH without invoking the provider or opening the index. Aliases, qualified stars, binary primary keys, distance and boolean retrieval flags keep their declared types; the appended nullable `source_block` column is declared CONTENT.

The derived JOIN executor reuses resolvable SQL/catalog metadata for empty and populated projections, preserving evaluated collation and source context. Empty native results now return a typed zero-row result set; they do not disappear into an OK response. A physical INT projected beside generated blocks remains INT, rather than being inferred as BIGINT from a Go int64. Unsupported static expressions and views still have metadata gaps; this is not a claim of complete empty-derived-query compatibility.

A real nonempty CONTENT_TEXT_CHUNKS query exposed a separate derivation defect: newTextChunkBlock added chunk parameters to Attributes after NewBlock had computed a v2 ID. Serialization rejected the inconsistent identity. It now finalizes the ID after the attributes are set. An independent content-package regression verifies chunk persistence/decoding, equality with ordinal lookup, different IDs for different settings even when the text is identical, and continued rejection of post-publication attribute tampering. The codec's validation was not relaxed.

The ordinary TCP driver's native prepared search now verifies early four-column metadata, zero embedding calls at PREPARE, one call at EXECUTE under a one-call budget, exact search values, and identical column types for filtered-empty results. Static and real-engine tests additionally cover full-text and lateral chunk empty results, stars, source INT preservation and model-free schema description. The provider used here is an injected deterministic test adapter; no real model-quality claim follows.

Docker became available during this run, with a cached mysql:8.4 image. Added opt-in same-SQL reference acceptance and a JSON report using a task-owned loopback MySQL 8.4.13 instance. All 34 successful-query value/name/order comparisons and both expected scalar-cardinality error-code/SQLSTATE comparisons agreed, but 8 of the 36 cases failed protocol type parity. They cover predicate BIGINT, correlated scalar projection types, and plain INT preservation through CTE/simple-derived paths, in text and native prepared modes. These failures remain actionable evidence, not skipped or normalized away. Setup, exact scope and mismatch table are in client_compatibility/MYSQL84_DIFFERENTIAL.md. The task-owned reference tables were removed and the --rm container stopped after capture.

Evidence root: C:/Users/zx187/AppData/Local/Temp/xmysql-native-table-schema-e1089b3c5e1c48dd985f091223a43f9e. The overall MySQL compatibility and AI/multimodal gates remain open, including the newly observed reference mismatches.

Fresh complete regression for this production-source snapshot: content passed 7 top-level tests (2.457s); engine passed 1973 with the same 2 existing Volcano environment skips (737.032s). The final complete net run, with PyMySQL 1.2.4 and Connector/J 8.0.33 enabled and the optional reference group explicitly skipped, passed 121 top-level tests (35.800s). Dispatcher and protocol passed 74 and 77 top-level tests (0.818s and 0.809s). Server build and content/engine/net/dispatcher/protocol vet passed. The separately enabled live-reference run intentionally remains failing: 28 of 36 leaf cases passed, 8 failed type parity, with full JSON retained. Three files changed by the full regression were restored, the turn's original fixture snapshots were restored, and no new fixture files remained in the inspected roots. Full-regression success does not supersede the live-reference failures.

## 2026-11-26: preserve declared types and labels through materialized and correlated projections

Closed the original eight live-reference type mismatches. Simple derived tables and materialized CTE projections now resolve against their input SQL schemas instead of the first Go value. The schema-only projection resolver is shared with PREPARE description, without replaying the inner query or invoking models. Known output types and column names survive empty inputs, NULL-first rows, stars and explicit aliases. Evaluated collation and source contexts are retained; aggregate execution keeps its existing path.

Correlated scalar projections obtain resolvable visible labels and types from the original SQL/catalog definition, before appended hidden ordering helpers. Bare column labels omit the table qualifier, explicit aliases remain intact, and hidden helper columns are removed as before. A full-client run caught the remaining qualified-label defect after type-only inspection; it was fixed and the complete reference cases rerun. Primitive SQL predicate results and TRUE/FALSE now report BIGINT on the direct projection paths. This changes result-expression metadata, not physical TINYINT columns or the declared TINYINT flags of native VECTOR_SEARCH.

Expanded the reference matrix to 26 SQL statements in text/native-prepared modes (52 cases), including empty CTE/derived/correlated results, NULL-first scalars, real TINYINT fields alongside predicates and multiple predicate operators. The final current-report.json proves 50 passing cases, including all original 36, and 2 remaining sorted-JOIN type failures. Both failures return the same values and labels: XMySQL BIGINT versus reference INT. Independent CLI metadata and EXPLAIN capture show the reference sorted JOIN uses temporary/filesort materialization and reports LONG, while the otherwise equivalent unsorted JOIN reports LONGLONG. No query-shape-specific conversion or INT/BIGINT normalization was added to conceal the difference; materialized result typing remains unresolved.

Verification scope: 228 affected engine top-level tests passed (181.637s) after the type changes. After the final label correction, 59 correlated/projection tests passed again (33.204s). The final full net run with enabled Python/JDBC and live MySQL comparison failed only in the two reference leaf cases and their parent groups; its other 120 top-level tests and both external clients passed. Therefore the complete net gate is failing, not green. Dispatcher and protocol passed (74/77 top-level tests, 1.611s/1.876s); final server build and engine/net/dispatcher/protocol vet returned zero. The earlier full-engine 1973-pass run predates these changes and is not a new full-engine result.

Evidence: C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-type-parity-3e40953543c746f4a1ecd10af57c7283, including verification-summary.json, current-report.json, red/intermediate/final logs and reference-plan-probe.log. The task-owned reference tables were removed, the --rm container stopped, and test-written fixtures restored with hash verification and no new files in the inspected roots. The complete MySQL/AI/multimodal goal remains active and incomplete.

## 2026-11-26: sorted JOIN result type parity follow-up

Closed the remaining two type mismatches in the then-current live reference set by applying an AST-level result schema rule: boolean projections over a sorted JOIN report INT, while the unsorted JOIN and single-source predicate paths retain BIGINT. The same rule feeds runtime JOIN result metadata and schema-only PREPARE description. Coverage includes equality, `IS NULL`, range predicates and ordering by source key, alias, ordinal and projected key, with ASC/DESC and LIMIT.

The differential set is now 34 SQL statements × text/native-prepared modes = 68 cases against MySQL 8.4.13. All 66 successful results match labels, protocol types and row values/order; both expected scalar cardinality errors match code 1242 and SQLSTATE 21000. The targeted engine projection regression and the full enabled wire differential passed. This matrix is a narrow compatibility gate, not a full MySQL conformance result.

Evidence: `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-followup-final.json` and `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-followup-final.log`. Query compatibility work remains broad; the entire plan and AI/multimodal gates remain incomplete.

## 2026-11-26: locale-aware Swedish collation follow-up

The existing `utf8mb4_swedish_ci` catalog entry now compares through a Swedish locale collator rather than accent-stripped Unicode lexical order. Case remains insensitive, Swedish letters retain distinct weights for comparison and DISTINCT grouping, and PAD SPACE trimming remains in effect. Storage-backed SQL verifies ordered values `a,z,å,ä,ö` and a DISTINCT count of five.

Live MySQL 8.4.13 differential now covers 36 SQL statements in text and native-prepared modes (72 cases). All 70 successful results match columns, types, values and ordering, and both expected cardinality errors match. Evidence is in `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-collation-final.json`. This does not cover all locale collations, MySQL-version weight tables, or mixed-collation coercibility.

## 2026-11-26: German DIN-2 collation follow-up

The declared Unicode `*_german2_ci` collations now normalize the documented German phonebook equivalences: `ä/ö/ü/ß` to `ae/oe/ue/ss`, and for Unicode charsets `æ/œ` to `ae/oe`. The `latin1_german2_ci` path deliberately does not add the Æ/AE or Œ/OE equivalences that MySQL excludes for latin1. Both ORDER BY and collation-aware DISTINCT consume the normalized key.

The MySQL 8.4.13 differential now has 38 SQL statements × two protocols = 76 cases. All 74 successful results match and both expected errors match code/SQLSTATE. Deterministic German2/Swedish/collation regressions and the live TCP differential pass. Official semantics: [MySQL Unicode Character Sets](https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html). This remains a targeted subset, not full Unicode collation parity.

## 2026-11-26: latin1 German2 distinction verified

Added `latin1_german2_ci` ORDER BY and DISTINCT cases to preserve its documented difference from Unicode German2: Æ/AE and Œ/OE are not equated in latin1. The final differential has 40 SQL statements × text/native-prepared = 80 cases; 78 successful results match and two expected errors match code/SQLSTATE. Engine regressions and live MySQL 8.4.13 comparison pass. Evidence: `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-german2-latin-final.json`. Other locales, complete collation weights and coercibility remain open.

## 2026-11-26: asynchronous text derivation result contract

Committed text CONTENT rows enqueue deterministic jobs keyed by source schema/table/column, canonical primary key, content revision/digest, processor version and chunk options. After startup recovery, the engine worker rereads the current row and checks that exact source identity before and after processing. It streams versioned block envelopes into one immutable object under the database CONTENT directory, then atomically makes a bounded (maximum 16 MiB) result manifest visible through a fenced queue generation. The manifest records source identity, processor version, object reference, and each block's stable ID, ordinal, offset and encoded length. Queue result bytes carry a SHA-256 digest and are readable only for a READY job marked as having a stored result.

`CONTENT_TEXT_CHUNKS(content_column, max_bytes[, overlap_bytes[, min_boundary_bytes]])` first looks up the exact source/configuration job. A valid READY result is read and each block is range-read, checksum-decoded and matched to the current source; when no usable READY result exists, the existing deterministic synchronous chunker remains the fallback. `CONTENT_MAINTENANCE_STATUS(content_column[, max_bytes[, overlap_bytes[, min_boundary_bytes]]])` reports `QUEUED`, `LEASED`, `READY`, `FAILED`, `NOT_QUEUED` or `NOT_APPLICABLE` for the current row version and does not create jobs. The worker uses leases, heartbeat renewal, bounded exponential retry, and terminal failure for stale sources or unsupported processor versions. Startup object reclamation preserves objects reachable from verified READY result manifests.

Content-package and focused engine tests cover result checksum/restart/fencing/size bounds, SQL status, persisted block reuse, stale-source failure, worker start/stop and object-GC retention. The full engine regression passed for this implementation (392.941 seconds). This contract does not yet include SQL retry or result-retention operations, cross-instance queue replication, model/embedding jobs, non-text parsers, or end-to-end RAG.

## 2026-11-26: BM25 and SQL token expansion acceptance

The new focused engine regressions independently calculate the Okapi BM25 expected score, compare equal-term short/long documents under length normalization, verify zero score when a term is absent, and require inconsistent corpus frequency to fail. A SQL-level query verifies `BM25_TERM_SCORE`, `TABLE(FULLTEXT_TOKENS(...))`, stopword-filtered zero-based positions, and grouped `COUNT(DISTINCT source_id)` document frequencies use matching token rules. This run exposed and fixed a lateral-function join bug: the SQL parser normalizes `CROSS JOIN` to `join`, so the executor now accepts the join only when it has no `ON`/`USING` condition. The BM25 formula and SQL token-statistics path are covered; automatic multi-term BM25 aggregation, inverted-index candidate execution, and live MySQL reference parity remain open.

## 2026-11-27: multi-term BM25 score expression

Added `BM25_SCORE(document, query, document_count, term_document_frequencies_json, average_document_length[, k1[, b]])`. It uses the existing Unicode tokenizer and stopword list, deduplicates query terms in query order, sums the same Okapi contribution used by `BM25_TERM_SCORE`, and validates a complete normalized-token-to-document-frequency JSON object. Duplicate, missing, extra, non-integer, or out-of-range statistics are rejected. Query/document text is capped at 4 MiB each, term statistics at 1 MiB, and query terms at 4,096. PREPARE describes the result as DOUBLE.

This adds multi-term scoring only; SQL still supplies corpus statistics and candidate enumeration, and no inverted-index or top-K retrieval plan is added. `go build -mod=readonly ./server/innodb/engine` is the only verification for this slice. No tests were run, so formula parity, SQL evaluation, NULL behavior and client metadata remain unverified; live MySQL parity is pending. This supersedes the prior note that automatic multi-term BM25 scoring itself was absent.
