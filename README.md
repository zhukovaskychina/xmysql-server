# XMySQL Server

<p align="center">
  <img src="./xmysql-logo.png" alt="XMySQL Logo" width="720" />
</p>

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

### 当前实施方式

- 以任务清单驱动交付（P0/P1/P2 + 模块化任务分解）
- 以测试与脚本验证作为完成标准（不是“代码写完”）
- 以文档和报告沉淀为验收证据（恢复、并发、灰度、回滚）

### 关键实施文档

- 开发路线图（阶段与任务）：`docs/planning/DEVELOPMENT_ROADMAP.md`
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

## 项目现状（2026-03）

> 说明：本节以仓库内最新规划文档为准，优先参考 P0 清单与路线图文档。

### 状态结论

- 项目已具备较完整原型能力，但尚未达到“可安全生产灰度”标准
- 路线图显示核心阶段仍有未完成项，尤其是并发、恢复、优化器核心能力
- P0 上线清单中大量条目尚未完成，生产门槛未闭环

### 已具备能力（方向性）

- 基础 MySQL 协议兼容能力（含部分预编译语句）
- InnoDB 风格的存储与索引骨架
- 执行器与查询处理基础能力
- 一定规模的单元与集成测试基础

### 仍需重点补齐

- 事务恢复正确性（Redo/Undo）
- 索引一致性与并发安全
- 锁行为与隔离语义（含范围与冲突场景）
- 可观测性与告警闭环
- 生产发布、灰度、回滚与演练证据

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

1. `docs/planning/P0_PRODUCTION_TASKS.md`（做什么）
2. `docs/planning/P0_PRODUCTION_CHECKLIST.md`（做到什么算完成）
3. `docs/planning/P0_PRODUCTION_DEPLOYMENT_PLAN.md`（如何上线与回退）
4. `docs/planning/DEVELOPMENT_ROADMAP.md`（阶段路线与优先级）
5. `docs/analysis/REMAINING_ISSUES_ANALYSIS.md`（核心风险与剩余差距）

---

## 快速开始（开发环境）

### 环境要求

- Go 1.20+
- 推荐 Linux/macOS 开发环境

### 编译与测试

```bash
go mod tidy
go build ./...
go test ./server/dispatcher ./server/innodb/engine
```

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
