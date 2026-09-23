# MySQL 8.4 可观测性兼容矩阵

| 能力 | 当前数据源 | 验证方式 | 发布要求 |
|---|---|---|---|
| 查询计数/错误数 | runtime recorder | `PERFORMANCE_SCHEMA.events_statements_summary_by_digest` | 随真实请求变化 |
| 查询耗时 | runtime recorder histogram | Prometheus 文本和 digest 汇总 | 非固定样例 |
| 线程 | handler/session 状态 | `PERFORMANCE_SCHEMA.threads` | 连接生命周期可查询 |
| 慢查询 | slow-query recorder | smoke workload + 脱敏日志 | 阈值和滚动日志证据 |
| 锁等待 | LockManager wait graph | `data_lock_waits`/并发报告 | 等待者、阻塞者和资源一致 |

运行：

```powershell
.\scripts\compatibility\observability_smoke.ps1
```

若某项仍只有静态占位行，报告必须保持 `NO-GO`，不能用 fixture 代替 live recorder 数据。
