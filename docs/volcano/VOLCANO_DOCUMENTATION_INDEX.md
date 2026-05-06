# Volcano 火山模型文档索引

> **2026-04**：下列文档按「先读后查」排序；已合并为跳转页的文件仍保留文件名，避免外链失效。

---

## 入门与实现（优先）

| 文档 | 说明 |
|------|------|
| [VOLCANO_MODEL_IMPLEMENTATION.md](./VOLCANO_MODEL_IMPLEMENTATION.md) | **主文档**：`Operator` 接口、算子、执行流程、与物理计划衔接 |
| [VOLCANO_MODEL_ANALYSIS_SUMMARY.md](./VOLCANO_MODEL_ANALYSIS_SUMMARY.md) | 新旧双接口、重复代码与清理建议（分析报告） |

## 清理与发布（历史交付）

| 文档 | 说明 |
|------|------|
| [VOLCANO_MODEL_CLEANUP_PLAN.md](./VOLCANO_MODEL_CLEANUP_PLAN.md) | 清理设计/计划（篇幅较长） |
| [VOLCANO_CLEANUP_FINAL_REPORT.md](./VOLCANO_CLEANUP_FINAL_REPORT.md) | **清理结论主文档**：阶段清单、量化成果、测试与文档交付 |
| [VOLCANO_CLEANUP_CHANGELOG.md](./VOLCANO_CLEANUP_CHANGELOG.md) | 变更日志条目 |
| [VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md](./VOLCANO_CLEANUP_IMPLEMENTATION_SUMMARY.md) | **→ 跳转** 至 FINAL_REPORT |

## 已合并跳转

| 文档 | 指向 |
|------|------|
| [VOLCANO_MODEL_REFACTOR_SUMMARY.md](./VOLCANO_MODEL_REFACTOR_SUMMARY.md) | → `VOLCANO_MODEL_IMPLEMENTATION.md` |

## 代码入口

- `server/innodb/engine/volcano_executor.go`
- `server/innodb/engine/executor.go`（构建算子树、与 dispatcher 兼容部分）

## 相关索引

- [执行器文档索引](../executor-reports/EXECUTOR_DOCUMENTATION_INDEX.md)（EXEC-001 计划与完成报告）
