# XMySQL Server

XMySQL Server 是一个使用 Go 实现的、面向单机场景的 MySQL 兼容数据库内核项目。  
项目目标不是做演示型 SQL 服务，而是按数据库内核工程标准，逐步补齐存储、事务、索引、执行与可运维能力。

---

## 项目发起

### 背景

- 通过工程化实践验证 InnoDB 风格架构在 Go 语言中的可落地性
- 形成可持续演进的数据库内核代码基座，而非一次性实验代码
- 以真实可验证路径推进：先正确性，再稳定性，再性能与可运维

### 设计原则

- 单机优先，明确边界，避免过早引入分布式复杂度
- 正确性与可扩展性优先于“看起来跑得快”
- 架构分层清晰：Storage / BufferPool / Record / Index / Txn / SQL Pipeline
- 保持最小可用实现，所有简化都应显式且可回退

### 代码主目录

- `server/`: 核心服务与数据库内核代码
- `server/innodb/`: 存储、事务、索引、执行器、优化器核心实现
- `server/protocol/`、`server/net/`: MySQL 协议与网络处理
- `server/dispatcher/`: 请求分发与路由
- `docs/`: 路线图、P0 计划、技术文档与报告

---

## 项目实施

### 当前权威口径（2026-07-15）

- 基础 CRUD 与 JDBC DML 专项测试当前可跑通。
- 这只代表最小集成路径可用，不代表完整 MySQL/InnoDB 生产能力完成。
- 当前缺口与优先级以以下文档为准：
  - `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
  - `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
  - `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
  - `docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`

### 当前实施方式

- 以任务清单驱动交付（P0/P1/P2 + 模块化任务分解）
- 以测试与脚本验证作为完成标准（不是“代码写完”）
- 以文档和报告沉淀为验收证据（恢复、并发、灰度、回滚）

### 关键实施文档

- 当前能力优先级索引：`docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`
- P0 生产阻塞能力 backlog：`docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`
- P1 重要能力 backlog：`docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`
- P* 未来能力 backlog：`docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`
- 开发路线图（导航 + 权威链接）：`docs/planning/DEVELOPMENT_ROADMAP.md`
- 路线图任务版（可执行视角）：`docs/development/DEVELOPMENT_ROADMAP_TASKS.md`
- P0 上线任务分解：`docs/planning/P0_PRODUCTION_TASKS.md`
- P0 上线验收清单：`docs/planning/P0_PRODUCTION_CHECKLIST.md`
- P0 生产部署计划：`docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md`
- 剩余问题分析：`docs/analysis/REMAINING_ISSUES_ANALYSIS.md`

### 主要实施路径（建议顺序）

1. P0 高风险实现整改（引擎关键路径）
2. 崩溃恢复闭环（redo/undo/半提交恢复）
3. 并发正确性验证（冲突写、范围读、长事务）
4. 基础可观测性落地（日志、指标、告警）
5. 灰度与回滚演练（发布与止损能力）

---

## 项目现状（2026-07）

> 说明：本节以 2026-07-15 的 CRUD/JDBC 验证和能力缺口盘点为准；历史 P0 证据文档若与本节冲突，以 `CAPABILITY_PRIORITY_INDEX_20260715.md` 为准。

### 状态结论

- 项目已具备基础单机 CRUD + JDBC DML 能力。
- 当前主要缺口不再是“能否执行简单增删改查”，而是“是否具备生产级 MySQL/InnoDB 正确性、恢复、索引、事务、协议和运维闭环”。
- P0 上线清单需要按新的 P0 能力 backlog 重新验收，不能仅凭历史 evidence suite 或基础测试通过判断生产就绪。

### 已具备能力（与代码对齐的要点）

- MySQL 协议：`COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE` 等预处理路径（`server/net/decoupled_handler.go`、`server/protocol/`）
- 优化器：`CNF` 转换、`optimizer.go` 列裁剪、谓词下推与 `index_pushdown_optimizer.go` 索引条件下推框架
- 执行器：`PhysicalHashJoin` + `HashJoinOperator`（`server/innodb/plan/physical_plan.go`、`server/innodb/engine/volcano_executor.go`）
- 事务：`Savepoint` / `ROLLBACK TO SAVEPOINT`（`server/innodb/manager/transaction_manager.go`）；Gap / Next-Key 锁数据结构与管理（`gap_lock.go`、`lock_manager.go`）
- 存储与索引：B+树合并 `btree_merge.go`；DML 二级索引同步接口与实现路径（`dml_executor.go` 等）
- 恢复：`CrashRecovery` / Redo 重放相关大量测例（`server/innodb/manager/crash_recovery*_test.go` 等）

### 仍需重点补齐

- 完整统一的 InnoDB 风格页/记录格式和 B+Tree 持久化扫描闭环
- 二级索引查询、唯一约束、重建/校验/修复闭环
- JDBC 多连接事务、MVCC 可见性、Undo/Purge、崩溃恢复状态证明
- MySQL/JDBC 核心兼容性矩阵，包括 prepared statement、metadata、错误码和常见 DML 扩展
- 真实运行态可观测性、并发压测、灰度/回滚演练和治理签署

---

## 项目发展（路线图与 P0 目标）

### 近期开发展望（P0 生产就绪）

目标：满足“可进入生产灰度”的最小闭环，而非一次性做完所有长期优化。

核心目标包括：

- 高风险模块整改完成并稳定回归
- 崩溃恢复全场景可复现、可验证、可审计
- 并发正确性通过压测与一致性校验
- 指标/日志/告警具备基础运维可见性
- 具备可执行的灰度与回滚预案，并完成全链路演练

### 中长期发展方向

- 优化器核心规则与统计信息体系增强
- 执行器路径收敛与历史实现清理
- 性能工程化（基准、瓶颈分析、专项优化）
- 持续提升文档、测试与发布流水线成熟度

---

## 生产就绪度入口（建议按此阅读）

1. `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`（当前唯一优先级入口）
2. `docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`（生产阻塞能力）
3. `docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`（重要后续能力）
4. `docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`（P2/P3/future）
5. `docs/planning/P0_PRODUCTION_CHECKLIST.md`（上线证据清单，需按新 P0 重新验收）

---

## 快速开始（开发环境）

### 环境要求

- Go 1.24+
- 推荐 Linux/macOS 开发环境

### 编译与测试

```bash
GO_BIN=${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}

$GO_BIN mod tidy
$GO_BIN build ./...
$GO_BIN test ./server/dispatcher ./server/innodb/engine
```

- 默认未设置 `GO_BIN` 时，脚本与文档将使用 `/Users/zhukovasky/sdk/go1.24.3/bin/go`。

### JDBC 客户端集成测试（可选）

- **连接 + 系统变量门禁**（需服务监听 `localhost:3309`；本地联调配置示例：`conf/jdbc_local.ini`）：
  ```bash
  # 终端 A：启动服务
  $GO_BIN run . -configPath=conf/jdbc_local.ini

  # 终端 B：仅跑连接/变量专项
  cd jdbc_client && mvn test -Pjdbc-connectivity
  ```
- **全量 `mvn test` 当前缺口与修复阶段**：[docs/planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md](docs/planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md)  
- **更多命令与用例说明**：[jdbc_client/TEST_README.md](jdbc_client/TEST_README.md)

### 运行

请按仓库现有配置文件与启动脚本执行（参考 `conf/`、`scripts/` 与相关文档）。

---

## 贡献与协作

- 提交变更前请确保对应测试通过
- 涉及行为变化时同步更新 `docs/` 中相关文档
- 以任务与验收标准为驱动，避免“功能完成但不可上线”的交付

---

## 免责声明

当前仓库处于持续演进阶段。  
在 P0 生产清单全部完成并通过评审前，不建议直接用于关键生产业务负载。
