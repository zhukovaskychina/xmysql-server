# XMySQL Server

XMySQL Server 是一个使用 Go 编写的实验性单机关系型数据库项目，提供 MySQL 兼容客户端协议，以及受 InnoDB 启发的存储与执行组件。项目面向数据库系统研究和工程实践，**不是 MySQL 的即插即用替代品**。

XMySQL Server is an experimental, single-node relational database written in Go. It provides a MySQL-compatible client protocol and an InnoDB-inspired storage and execution stack. The project is intended for database systems research and engineering; it is **not a drop-in replacement for MySQL**.

> **项目状态：** 项目仍在开发中，尚未完成生产就绪评审。请勿用于生产负载或不可恢复的数据。
>
> **Project status:** Under active development and not yet production-ready. Do not use it for production workloads or data that cannot be recovered.

## 项目概览 | Overview

项目主要包含以下部分：

The project includes the following areas:

- Go 数据库服务与存储引擎 / Go database server and storage engine
- MySQL 网络协议与会话管理 / MySQL wire protocol and session handling
- SQL 解析、计划生成与执行 / SQL parsing, planning, and execution
- 受 InnoDB 启发的页面、索引、事务与恢复组件 / InnoDB-inspired pages, indexes, transactions, and recovery components
- 兼容性测试与运维验证工具 / Compatibility tests and operational verification tools

以上内容介绍项目模块，不代表已完整兼容 MySQL 或 InnoDB。支持范围和已知限制见[能力与兼容性文档](docs/README.md)。

These are project areas, not a claim of complete MySQL or InnoDB compatibility. See the [capability and compatibility documentation](docs/README.md) for scope and known limitations.

## 架构 | Architecture

主要请求路径如下：

The main request path is:

```text
MySQL 客户端 / MySQL client → 网络与协议 / Network and protocol → 会话与分发 / Session and dispatcher
                           → SQL 解析与执行 / SQL parsing and execution → 存储与事务 / Storage and transactions
```

架构图与模块说明见[架构图索引](docs/architecture-diagrams/README.md)和[文档中心](docs/README.md)。

See the [architecture diagram index](docs/architecture-diagrams/README.md) and the [documentation center](docs/README.md) for architecture diagrams and module notes.

## 环境要求 | Requirements

- Go 1.24 或更高版本 / Go 1.24 or later
- Git
- 可选：运行 JDBC 集成测试需要 Java 17 或更高版本及 Maven / Optional: Java 17 or later and Maven for JDBC integration tests

## 构建与测试 | Build and test

在仓库根目录运行：

Run from the repository root:

```bash
go build -o xmysql-server .
go test ./server/dispatcher ./server/innodb/engine
```

仓库 CI 会构建和测试 Go 包，并排除 `cmd/` 与 `client/`。完整命令见 [Go CI 工作流](.github/workflows/go.yml)。

The repository CI workflow builds and tests Go packages while excluding `cmd/` and `client/`. See the [Go CI workflow](.github/workflows/go.yml) for the exact commands.

## 本地运行 | Run locally

`conf/jdbc_local.ini` 配置将服务绑定到 `127.0.0.1:3309`，并将数据库和日志文件写入工作区，供本地开发使用。启动前请检查配置中的路径，尤其是在与该配置说明不同的操作系统上运行时。

The `conf/jdbc_local.ini` profile binds to `127.0.0.1:3309` and writes database and log files under the workspace. It is intended for local development. Check the configured paths before starting the server, especially on operating systems different from the profile's documented environment.

```bash
go run . -configPath=conf/jdbc_local.ini
```

服务会在当前终端前台运行，按 `Ctrl+C` 停止。使用 MySQL 兼容客户端连接 `127.0.0.1:3309`，并提供本地数据目录中已配置的账户。JDBC 测试的默认连接参数及覆盖方式见 [JDBC 测试说明](jdbc_client/TEST_README.md)。

The server runs in the foreground; press `Ctrl+C` to stop it. Connect with a MySQL-compatible client at `127.0.0.1:3309` using an account available in the local data directory. See the [JDBC test guide](jdbc_client/TEST_README.md) for default connection settings and overrides.

### JDBC 连接测试 | JDBC connectivity tests

服务运行在 `3309` 端口时，可执行连接与系统变量专项测试：

With the server running on port `3309`, run the focused connection and system-variable tests:

```bash
cd jdbc_client
mvn test -Pjdbc-connectivity
```

如果服务不可用或未启用口令校验，部分连接测试可能会跳过。判断结果时请检查 Surefire 报告中的执行数和跳过数。更多配置和全量测试说明见 [JDBC 测试指南](jdbc_client/TEST_README.md)。

Some connection tests may be skipped if the server is unavailable or does not enforce password authentication. Check the executed and skipped counts in the Surefire report when interpreting results. See the [JDBC test guide](jdbc_client/TEST_README.md) for configuration and full-suite details.

## 文档 | Documentation

建议从[文档中心](docs/README.md)开始，其中收录能力索引、兼容性边界、架构资料、开发指南和运维文档。

Start with the [documentation center](docs/README.md), which links to capability indexes, compatibility boundaries, architecture references, development guidance, and operations material.

| 主题 / Topic | 文档 / Entry point |
| --- | --- |
| 项目能力与优先级 / Capabilities and priorities | [能力优先级索引 / Capability priority index](docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md) |
| MySQL 兼容性边界 / MySQL compatibility boundaries | [兼容性边界记录 / Compatibility boundary record](docs/planning/COMPATIBILITY_REMAINING_BOUNDARIES_20260928.md) |
| MySQL 8.4 协议与兼容说明 / MySQL 8.4 protocol and compatibility | [兼容性文档 / Compatibility documentation](docs/compatibility/mysql84_compatibility_matrix.md) |
| 开发与运维文档 / Development and operations | [文档中心 / Documentation center](docs/README.md) |

历史计划和报告可能描述早期代码或测试结果。使用其中的状态结论前，请核对当前源码和可访问的测试证据。

Historical plans and reports may describe earlier code or test results. Check the current source and accessible test artifacts before relying on their status claims.

## 参与贡献 | Contributing

欢迎提交问题报告和代码贡献。提交变更前请：

Contributions and issue reports are welcome. Before submitting a change:

1. 对较大的改动，先提交 issue 或 discussion，明确范围和预期行为。 / For larger changes, open an issue or discussion to clarify scope and expected behavior.
2. 保持改动聚焦；行为变更应包含相应测试。 / Keep changes focused and include tests for behavior changes.
3. 运行相关 Go 测试；行为或支持范围变化时同步更新文档。 / Run relevant Go tests and update documentation when behavior or supported scope changes.
4. 在 pull request 中列出执行过的命令及结果。 / List the commands run and their results in the pull request.

请遵循[编码规范](docs/CODING_STANDARDS.md)和[代码评审清单](docs/CODE_REVIEW_CHECKLIST.md)。

Follow the repository's [coding standards](docs/CODING_STANDARDS.md) and [code review checklist](docs/CODE_REVIEW_CHECKLIST.md).

## 许可证 | License

本项目采用 [Apache License 2.0](LICENSE) 发布。

This project is distributed under the [Apache License 2.0](LICENSE).
