# MySQL 8.4 兼容性发布门禁

发布判断由脚本生成，不接受人工修改结果文件：

```powershell
.\scripts\compatibility\release_candidate_gate.ps1
```

脚本按固定顺序执行 clean-data 隔离检查、生产包 build、unit、integration、三次崩溃恢复演练、并发、可观测性 smoke、启动隔离的 xmysql 实例和 JDBC 全量套件，并记录各检查的退出码、测试计数、git revision、时间戳和输出尾部。JDBC 默认必跑；仅本地诊断可以使用 `-SkipJDBC`，但该模式固定为 `NO-GO`。任一检查失败、必需检查未运行或证据文件缺失时均为 `NO-GO`。

JDBC 门禁默认使用 `conf/jdbc-compat-clean.ini` 的 3311 端口和独立数据目录。门禁会构建临时服务二进制、等待端口就绪、注入 `XMYSQL_JDBC_*` 连接参数，并在测试结束后只停止本次启动的同一路径进程；如果端口被其他程序占用，会直接判定为失败，不会复用外部 MySQL。

如需指定 JDBC 端点或服务配置：

```powershell
.\scripts\compatibility\release_candidate_gate.ps1 `
  -JdbcServerConfig conf/jdbc-compat-clean.ini `
  -JdbcUrl 'jdbc:mysql://localhost:3311?useSSL=false&allowPublicKeyRetrieval=true'
```

当前边界：该门禁是发布判定工具，不会把尚未实现的 MySQL 8.4 高级能力标记为通过；全文/空间索引、物理分区、完整 binlog/PITR 等能力仍必须有独立证据后才能纳入发布范围。当前候选门禁已包含隔离 xmysql JDBC 连通性和并发报告，并以 JSON `GO` 作为本轮可发布核心能力的判定。
