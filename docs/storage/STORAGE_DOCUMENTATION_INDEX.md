# 存储引擎文档索引

> **2026-04**：`docs/storage/` 与 `docs/storage-reports/` 下有多份「完成报告 / 集成总结」名称相近，易混淆。本页为**唯一推荐入口**；已合并为跳转页的文件仍保留路径，避免外链失效。

---

## 一、状态与总览（优先阅读）

| 文档 | 说明 |
|------|------|
| [STORAGE_ENGINE_FINAL_REPORT.md](./STORAGE_ENGINE_FINAL_REPORT.md) | **权威**：完成度、P0/P1 任务矩阵、代码量与结论 |
| [STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md](./STORAGE_ENGINE_IMPLEMENTATION_SUMMARY.md) | 各 STG 任务的**实现拆解**（长文）；文首注明总览以 FINAL 为准 |
| [../development/PROJECT_STATUS_AND_NEXT_STEPS.md](../development/PROJECT_STATUS_AND_NEXT_STEPS.md) | 存储专项会话状态（与 FINAL 对齐时可对照） |

### 已合并为跳转页

| 文档 | 指向 |
|------|------|
| [STORAGE_ENGINE_COMPLETION_SUMMARY.md](./STORAGE_ENGINE_COMPLETION_SUMMARY.md) | → `STORAGE_ENGINE_FINAL_REPORT.md` |
| [STORAGE_ENGINE_INTEGRATION_SUMMARY.md](./STORAGE_ENGINE_INTEGRATION_SUMMARY.md) | → `STORAGE_ENGINE_INTEGRATION.md` |
| [STG-001-006-015-018-COMPLETION-SUMMARY.md](./STG-001-006-015-018-COMPLETION-SUMMARY.md) | → `STORAGE_ENGINE_FINAL_REPORT.md`（子集任务细节见各 `STG-*.md`） |

---

## 二、集成与持久化

| 文档 | 说明 |
|------|------|
| [STORAGE_ENGINE_INTEGRATION.md](./STORAGE_ENGINE_INTEGRATION.md) | DML 与 B+ 树 / 事务 / 持久化集成（**集成主线**） |
| [PERSISTENCE_IMPLEMENTATION_SUMMARY.md](./PERSISTENCE_IMPLEMENTATION_SUMMARY.md) | 持久化相关实现总结 |

---

## 三、专题实现（STG / 表空间）

| 文档 | 说明 |
|------|------|
| [STG-012_EXTENT_REUSE_IMPLEMENTATION.md](./STG-012_EXTENT_REUSE_IMPLEMENTATION.md) | 区复用 |
| [STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md](./STG-016_DYNAMIC_FORMAT_IMPLEMENTATION.md) | Dynamic 行格式 |
| [system_space_manager_final_architecture.md](./system_space_manager_final_architecture.md) | 系统表空间 |
| [ibdata1_independent_tablespace_mapping.md](./ibdata1_independent_tablespace_mapping.md) | ibdata1 映射 |
| [storage_architecture_clarification.md](./storage_architecture_clarification.md) | 架构澄清说明 |

---

## 四、`storage-reports/`（专项报告与架构）

| 文档 | 说明 |
|------|------|
| [STORAGE_ARCHITECTURE.md](../storage-reports/STORAGE_ARCHITECTURE.md) | InnoDB 风格分层架构（**设计主文档** v2） |
| [STORAGE_MODULE_ARCHITECTURE_ANALYSIS.md](../storage-reports/STORAGE_MODULE_ARCHITECTURE_ANALYSIS.md) | `storage` 目录代码级分析（问题清单、统计） |
| [STORAGE-001_FIX_SUMMARY.md](../storage-reports/STORAGE-001_FIX_SUMMARY.md) | **STORAGE-001**：脏页刷新策略修复 |
| [STORAGE-001_CONCURRENCY_FIX_SUMMARY.md](../storage-reports/STORAGE-001_CONCURRENCY_FIX_SUMMARY.md) | **STORAGE-001**：表空间扩展并发修复（与上同名不同题，见文首说明） |
| [STORAGE-001_DIRTY_PAGE_FLUSH_VERIFICATION.md](../storage-reports/STORAGE-001_DIRTY_PAGE_FLUSH_VERIFICATION.md) | 脏页刷新验证 |
| 其他 | `STORAGE_OPTIMIZATION_TASKS.md`、`STORAGE_MODULE_REFACTORING_PLAN.md` 等见同目录 |

---

## 五、代码入口（速查）

- `server/innodb/storage/`（store + wrapper）
- `server/innodb/manager/`（表空间、缓冲池、页面分配等）
