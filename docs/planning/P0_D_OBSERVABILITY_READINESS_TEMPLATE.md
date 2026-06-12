# XMySQL Server P0-D 可观测性上线就绪模板

用于 `P0_PRODUCTION_CHECKLIST` 与 `P0_PRODUCTION_GAP_ANALYSIS` 的第一版证据准备。

## 1. 文档约束

- 目标：在生产灰度前形成可复核的日志、指标、告警最小闭环，不再停留在内部结构定义。
- 证据要求：本文件中每项条目需要至少有一条真实可复现路径或截图。
- 交付边界：先闭环“是否能观察到问题”，再逐步优化告警规则精度。

## 2. 统一日志字段规范（T-D-01）

### 2.1 日志字段（至少要求）

| 字段 | 描述 | 是否必填 |
|---|---|---|
| `ts` | UTC/本地时间戳（毫秒） | 是 |
| `level` | `info` / `warn` / `error` / `fatal` | 是 |
| `component` | 模块名，如 `protocol`, `executor`, `checkpoint` | 是 |
| `trace_id` | 跨模块追踪 ID | 是 |
| `request_id` | 请求上下文 ID | 建议 |
| `sql` | SQL 原文摘要或脱敏后 SQL | 是 |
| `rows_affected` | 影响行数 | 是 |
| `duration_ms` | 执行耗时 | 是 |
| `error_code` | 业务错误码 | 否 |
| `mysql_err` | MySQL 异常码 | 否 |
| `client` | 客户端地址 | 否 |
| `db` | 数据库名 | 否 |
| `table` | 表名 | 否 |
| `status` | `ok` / `fail` | 是 |

### 2.2 可观测日志示例

```text
ts=2026-05-16T09:45:30.123Z level=error component=executor trace_id=tr-01 request_id=req-77 sql="INSERT INTO t(...) VALUES(...)" rows_affected=0 duration_ms=1280 error_code=E001 mysql_err=1062 status=fail msg="duplicate key"
```

### 2.3 校验清单

- 关键执行路径是否都输出 `trace_id`、`duration_ms`、`status`。
- 同一 trace 在错误闭环时是否可以定位到具体模块与 SQL。
- 错误日志是否能输出 `mysql_err` 与 `error_code`。

## 3. 慢查询日志（T-D-01）

### 3.1 建议开关

- `slow_query_log=ON`
- `slow_query_time`（示例：`2s`）
- 采样文件：`slow_query.log`

### 3.2 示例行（可复现）

```text
time=2026-05-16T09:45:31.001Z query_time=1.245 user=root db=xx db_table=txn slow_query=YES rows_sent=1 rows_examined=12000 sql="SELECT * FROM ..."
```

### 3.3 样本输出要求

- 同时保留：
  - SQL 文本（脱敏）
  - `query_time`
  - `rows_examined` / `rows_sent`
  - `trace_id`
  - 命中 `slow_query` 标志

## 4. 指标与告警（T-D-02）

### 4.1 指标样例

- `xmysql_qps`
- `xmysql_p50_latency_ms`
- `xmysql_p95_latency_ms`
- `xmysql_p99_latency_ms`
- `xmysql_error_rate`
- `xmysql_active_connections`
- `xmysql_active_transactions`

### 4.2 指标接入清单

- 采集系统：`Prometheus` / 兼容 exporter
- 可视化：`Grafana` 面板
- 最少一条每分钟抓取任务

### 4.3 告警规则样例

```yaml
groups:
- name: xmysql-p0-observability
  rules:
  - alert: XMySQLQpsDrop
    expr: xmysql_qps < 1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "QPS 下降，可能存在服务异常"
  - alert: XMySQLP99LatencyHigh
    expr: xmysql_p99_latency_ms > 1500
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "P99 延迟过高"
  - alert: XMySQLErrorRateHigh
    expr: rate(xmysql_error_rate[5m]) > 0.05
    for: 3m
    labels:
      severity: critical
    annotations:
      summary: "错误率异常"
```

### 4.4 验收输出

- 告警规则文件路径：
- Grafana 面板链接（或导出截图）：
- 手工触发告警与恢复的记录：

## 5. 审核证据输入模板

### 5.1 必填项

- `[ ]` 错误日志字段规范文档已上线
- `[ ]` 错误日志样例日志保存路径：
- `[ ]` 慢查询日志样例路径：
- `[ ]` 指标导出入口与 `scrape` 示例：
- `[ ]` 告警规则文件：
- `[ ]` 告警触发记录（时间、阈值、恢复时间）：

### 5.2 备注

- 这个模板是最小闭环版本，不要求先完整做到自动化告警大盘，只要求能证明“可观测、可追踪、可告警”三件事。
- 代码落地与告警动作在后续阶段按生产规范细化。

