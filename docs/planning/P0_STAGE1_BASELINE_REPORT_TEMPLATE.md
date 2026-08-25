# P0 Stage1 基线验证模板

## 运行命令

- 命令：`STAGE1_REPORT_DIR=... STAGE1_DRY_RUN=0 ./scripts/p0_stage1_baseline.sh`
- 可选：`STAGE1_SKIP_FULL_ENGINE=1` 只运行 focused baseline（不跑 engine 全量）

## 输出位置

- 结果总目录：`reports/stage1_baseline_YYYYMMDD_HHMMSS/`
- 总结文件：`summary.log`
- 每条命令日志：按条目名生成独立 `.log`

## 报告字段（按行）

- `repo`: 仓库绝对路径
- `timestamp`: 执行时间戳
- `go_binary`: go 可执行文件路径
- `go_version_line`: `go version` 原始输出
- `required_min_version`: 最低版本阈值
- `command`: 执行的命令
- `[PASS]/[FAIL]`: 每条命令结果
- `RESULT`: `PASS`/`FAIL`

## 阶段条件（Stage1 门槛）

- [ ] `go test ./server/innodb/engine -run 'TestIndexScanOperator|TestIndexReading|Test.*Duplicate.*'` 有明确结果
- [ ] `go test ./server/dispatcher` 有明确结果
- [ ] `go test ./server/innodb/engine` 在可允许时有明确结果（或说明跳过原因）
