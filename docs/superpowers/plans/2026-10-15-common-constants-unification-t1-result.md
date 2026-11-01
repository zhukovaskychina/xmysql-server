# 事务常量统一 T1 实施记录

> 本文为历史阶段记录。后续全量重构已继续迁移事务调用方及其他常量族，当前状态见[全量实施记录](2026-10-15-common-constants-unification-full-result.md)。

2026-10-15，按[分批方案](2026-10-15-common-constants-unification-plan.md)完成T1：common定义11个事务常量，manager原11个名称直接引用common，现有调用方及事务实现保持原样。后续批次未开始。

## 基线与文件

- HEAD：`b15d304811beecdd673a1f48c3ef486723d14168`，分支`dev`。
- 实施前两个目标文件未修改，SHA-256与排查JSON一致：
  - `server/common/const_trx.go`：`550a0be409aa9bd6dc2aafb1f5a162ecdf2e2482eb54a1298578af100e9b554e`。
  - `server/innodb/manager/transaction_manager.go`：`dd7e84b0120eef6e47ab968b95a0f12e442a0a48dbf4079a6a86e3a8b8b9b14e`。
- 生产代码仅修改上述两文件中的常量块、必要import和说明注释。常量块之后的事务声明和函数体经比较未改变。
- 新增`server/common/const_trx_test.go`，覆盖两个包公开常量的值和数字类型，并通过具名字符串类型赋值确认告警常量仍为无类型字符串。
- 原有IDE配置、Java示例及其他未跟踪文件未覆盖；原有两个已跟踪脏文件的哈希与实施前相同。

## manager兼容名称登记

下列名称均保留在`transaction_manager.go`原常量块中，表达式统一为`common.同名常量`，没有独立数值或iota。

| 名称 | 唯一来源的值 | 类型 |
|---|---|---|
| TRX_STATE_NOT_STARTED | 0 | uint8 |
| TRX_STATE_ACTIVE | 1 | uint8 |
| TRX_STATE_PREPARED | 2 | uint8 |
| TRX_STATE_COMMITTED | 3 | uint8 |
| TRX_STATE_ROLLED_BACK | 4 | uint8 |
| TRX_ISO_READ_UNCOMMITTED | 0 | uint8 |
| TRX_ISO_READ_COMMITTED | 1 | uint8 |
| TRX_ISO_REPEATABLE_READ | 2 | uint8 |
| TRX_ISO_SERIALIZABLE | 3 | uint8 |
| LONG_TXN_LEVEL_WARNING | WARNING | 无类型字符串 |
| LONG_TXN_LEVEL_CRITICAL | CRITICAL | 无类型字符串 |

删除条件仍依主方案第3节：工程内生产、测试、示例消费者全部迁移，核实已知工程外兼容要求，并通过相关包测试及server跨包编译后，在独立批次删除。T1不删除这些名称。

## 验证结果

工具链：`C:\Users\zx187\go\go1.27.2\bin\go.exe`，`go1.27.2 windows/amd64`；`go.mod`未修改。

| 检查 | 修改前 | 修改后 |
|---|---|---|
| `go test ./server/common -count=1` | 通过，退出码0 | 通过，退出码0，含新增兼容性测试 |
| 主方案2.5节指定的manager事务测试命令 | 通过，退出码0 | 通过，退出码0 |
| `go test ./server/... -run '^$'` | 本批不要求修改前执行 | 通过，退出码0，50个包完成编译或报告无测试文件 |
| `git diff --check` | — | 通过，退出码0 |
| 三个改动Go文件的`gofmt -l` | — | 无输出 |

新增回归检查先在旧代码上执行，实际失败信息为`common not-started state = 1 (int), want 0 (uint8)`；实现后完整11项兼容检查通过。

本批运行了common全部测试及指定manager测试，server其他包只验证编译，未声称全仓全部测试或外部数据库联调通过。

## 兼容边界和撤回

`common.TRX_STATE_NOT_STARTED`由原无类型整数1改为uint8的0，这是明确的公开声明变化。实施前对工程Go文件搜索仅发现common和manager两处声明，没有旧common值的消费者；工程外未知消费者无法由本仓搜索排除。manager所有现有值和类型保持不变。

全量Markdown与JSON附件继续表示原基线排查快照，不重新生成其历史值、哈希或批次分类。它们的`first_batch_proposal`表示当时的提案分类；当前T1完成情况以本记录为准。

实施前两个目标文件的精确备份、哈希、工作区状态和执行日志保存在本地忽略目录`.superpowers/sdd/2026-10-15-common-constants-unification-plan/`。撤回仅逆向应用本批常量/import/注释差异，并移除本批新增测试；如文件随后发生修改，按差异逐段处理，禁止直接覆盖其他人的修改。本批没有数据迁移、提交、推送或部署。
