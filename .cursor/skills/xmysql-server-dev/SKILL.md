---
name: xmysql-server-dev
description: Assists development of XMySQL Server, a MySQL-compatible single-node database kernel in Go. Use when working on xmysql-server, XMySQL, InnoDB-style storage, transactions, MVCC, Redo/Undo, B+Tree, query optimizer, executor, MySQL protocol, or when the user refers to task IDs (TXN-xxx, OPT-xxx, LOG-xxx, IDX-xxx, STG-xxx), 开发计划, or 未实现功能梳理.
---

# XMySQL Server 开发辅助

工程级开发助手，限定在 xmysql-server 仓库既有架构、文档与开发计划内进行分析、设计与代码建议。

## 何时启用

仅当满足以下任一条件时使用本 Skill：

- 用户明确提到 **xmysql-server** / **XMySQL**
- 讨论或实现：事务/MVCC、Redo/Undo/日志、B+Tree/索引、查询优化器/执行器、InnoDB 风格存储、MySQL 协议/网络层
- 用户引用或创建 **任务 ID**（如 TXN-001、IDX-004、OPT-006）
- 用户讨论 **开发计划、阶段目标、未实现功能清单**

不满足以上条件时，不套用本 Skill。

## 最高优先级规则

### 1. 架构规则优先

不可违反的约束定义在 **`.cursor/rules/database-engine-design.mdc`**，优先级高于通用最佳实践。

要点：单机、MVCC 按阶段引入、先讲设计再写代码、禁止过度抽象、严格区分存储/缓冲池/记录/索引/事务/SQL Pipeline。本 Skill 只补充任务与工程上下文，不突破该规则。

### 2. 任务 ID 为事实来源

任何设计、实现、修改必须绑定明确任务 ID：明确范围与依赖，遵守阻塞关系与先后顺序。若用户未给出任务 ID，主动确认或映射到已有任务。

## 项目结构（禁止猜测）

| 位置 | 说明 |
|------|------|
| `server/` | 代码根目录 |
| `server/innodb/` | 事务、MVCC、B+Tree、缓冲池、日志、计划器、执行器 |
| `server/protocol/`、`server/net/` | MySQL 协议与网络 |
| `server/dispatcher/` | 请求分发 |
| `server/session/` | 会话管理 |
| `server/auth/` | 认证 |
| `docs/README.md` | 文档入口 |

## 任务 ID → 模块映射

| 前缀 | 模块 | 主要代码位置 |
|------|------|--------------|
| TXN | 事务/MVCC | server/innodb/**/txn, mvcc, manager |
| LOG | Redo/Undo/日志 | server/innodb/**/redo, undo, log |
| IDX | B+Tree/索引 | server/innodb/**/btree, index |
| OPT | 查询优化器 | server/innodb/plan, optimizer |
| EXE/EXEC | 执行器 | server/innodb/**/executor |
| STG | 存储引擎 | server/innodb/storage |
| BUF | 缓冲池 | server/innodb/buffer |
| NET/PROTO | 协议层 | server/protocol, server/net |

禁止随意跨模块实现逻辑。

## 强制工作流

1. **确认任务 ID**，查阅：`docs/development/开发计划.md`、`docs/未实现功能梳理.md`、`docs/development/DEVELOPMENT_ROADMAP_TASKS.md`
2. **阐明设计意图**：为什么要做、当前阶段刻意不解决什么
3. **定位模块与包路径**：精确到 server/ 下包
4. **最小化实现**：不做前瞻性设计、不引入多余抽象
5. **同步更新文档**（若有行为变化），尤其 TXN/LOG/MVCC/崩溃恢复

## 领域参考文档（按需）

- 事务/MVCC：`docs/innodb/MVCC_README.md`、`docs/REMAINING_ISSUES_ANALYSIS.md`
- 存储/页面/行格式：`docs/storage/`、`docs/innodb/`
- B+Tree：`docs/btree/BTREE_DOCUMENTATION_INDEX.md`
- 优化器/执行器：`docs/query-optimizer/`、`docs/volcano/`
- 协议层：`docs/protocol/`，开发计划中的 COM_STMT_*、NET/PROTO 任务

## 明确禁止

- 引入任何分布式设计
- 盲目照搬 MySQL/TiDB/CockroachDB 内部实现
- 为“未来可能需要”而加抽象层
- 忽略任务依赖与顺序
- 不说明设计意图直接给代码
- 改动架构却不更新文档

## 回答标准

严格对齐当前开发路线图、使用统一任务 ID 体系、遵守模块边界、体现数据库内核工程师视角。关键信息缺失时先追问澄清，禁止猜测。
