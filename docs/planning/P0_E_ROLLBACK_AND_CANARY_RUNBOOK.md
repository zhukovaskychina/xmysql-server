# XMySQL Server P0-E 灰度、回退与全链路演练手册（执行版）

目标：将 `D` 与 `E` 阶段从计划态推进到可复核态。本文的每一段结论必须落在同一条演练记录中，才能作为生产准入依据。

## 1. 前置条件

- P0-B、P0-C、P0-D 已有可复放结果：  
  - `reports/STAGE1_AUDIT_DEMO/stage1_baseline_20260516_220508/summary.log`  
  - `reports/p0_a2_01_stability_20260517_012720/summary.log`  
  - `reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md`
- 回归构建流程可执行（至少能在变更窗口内运行对应 `go test` 命令）。
- `go run . -configPath=conf/jdbc_local.ini` 服务启动成功（作为可执行前置条件）。
- 生产改动范围已固定：仅包含已审阅组件与配置项。

## 2. 变更发布范围

- 版本与提交：`<git_commit_or_tag>`
- 配置差异清单：`<config_delta_file>`
- 影响实例：`<cluster_or_host_list>`
- 目标库表：`<db>/<table>`（高风险表必须列明）
- 变更窗口与负责人：`<start_end>/<owner>`

## 3. 灰度流程（E-01）

### 3.1 阶段 0：只读灰度（建议 5~10 分钟）

1. 只放开只读流量。  
2. 监控窗口 3 分钟内无严重错误峰值。  
3. 验收：
   - 服务启动稳定
   - 错误率、连接数无异常上冲
   - 关键查询链路可追踪（trace 或日志链路）

### 3.2 阶段 1：低风险写入（建议 10~20 分钟）

1. 按 5% 写流量放开，确认事务可提交。  
2. 每 5 分钟检查一次：  
   - 事务提交成功率（要求不下降）  
   - 慢查询告警可控  
   - 无明显重放冲突扩散

### 3.3 阶段 2：扩量与扩容（可选）

1. 在阶段 1 通过后，按 `5% -> 10% -> 20%` 逐级放量。  
2. 每阶段停留 3 分钟稳定监控窗口。  
3. 任一指标不达标立即进入回退流程。

## 4. 快速回退流程（E-01 + E-03）

### 4.1 回退触发条件

- 1 小时窗口内恢复失败率持续高于预设阈值。  
- 核心 API P99 超阈值且告警未在 5 分钟内回落。  
- 数据一致性或结果核验出现异常。  
- 任意业务主流程出现明显阻断。  

### 4.2 回退动作（顺序固定）

1. 停止新流量入口，切回上一个稳定版本实例。  
2. 恢复关键参数（读写开关、限流阈值、连接池、路由开关）到上一个稳定值。  
3. 统一校验（5 分钟内）：  
   - 配置版本一致  
   - 核心实例连接数与事务数恢复  
   - 错误日志恢复到基线范围
4. 形成回退记录（含开始时间、恢复时间、触发指标、结论）。

### 4.3 回退时长目标

- 触发到低风险读写恢复：`<= 15 分钟`  
- 数据可见性恢复：`<= 30 分钟`

## 5. 数据回滚手册（E-02）

本节配合 `scripts/p0_e_backup_snapshot.sh` 执行。

### 5.1 备份与恢复点管理（可执行）

- 备份目录：`./reports/p0_e_backups`
- 命名规范：
  - 备份文件：`xmysql-backup-<snapshot_tag>-<yyyymmdd_hhmmss>.tar.gz`
  - 清单文件：同名 `.manifest.json`
- 备份命令：
  - `P0E_MODE=backup P0E_DATA_DIR=./data P0E_BACKUP_ROOT=./reports/p0_e_backups P0E_SNAPSHOT_TAG=<tag> bash scripts/p0_e_backup_snapshot.sh`
- 备份列表：
  - `P0E_MODE=list bash scripts/p0_e_backup_snapshot.sh`
- 恢复命令：
  - `P0E_MODE=restore P0E_BACKUP_FILE=./reports/p0_e_backups/<file>.tar.gz P0E_RESTORE_DIR=./data_restored bash scripts/p0_e_backup_snapshot.sh`

### 5.1.1 E-03 全链路演练链路（read/write/restore）

- 一条命令执行：
  - `P0E_MODE=canary [P0E_DSN=<dsn>] [P0E_CANARY_DB=<db>] [P0E_CANARY_TABLE=<table>] bash scripts/p0_e_backup_snapshot.sh`
- 阶段顺序：
  - `read`（ping）
  - `write`（创建库/表/写入、快照与校验）
  - `restore`（回滚到恢复点）
- 阶段日志字段（最小）：`stage / schema / table / error_code / log_path / duration_ms`
- 产物：
  - `reports/p0_e_backups/canary_<timestamp>/stage_events.log`
  - `reports/p0_e_backups/canary_<timestamp>/summary.md`
- 失败定位优先看：
  - `stage_*.log`

### 5.2 回放边界与校验

- 回放时间范围：`<start_time> ~ <end_time>`（按演练问题窗口）
- 回放前记录：
  - 变更对象（库表、SQL 入口）  
  - 事务范围（时间或事务 id）  
  - 关键表范围
- 校验项：
  - 快照比对：行数、关键聚合值  
  - 业务接口核验：关键返回值与状态码一致性  
  - 回放失败：恢复到最近可用恢复点并重试

## 6. 全链路演练清单（E-03）

### 6.1 一次演练必须覆盖

1. 阶段 0 灰度放量  
2. 阶段 1 写入放量  
3. 告警触发与人工判定  
4. 回退执行  
5. 数据一致性核验  
6. 演练复盘与问题闭环

### 6.2 记录模板

按 `docs/planning/P0_E_CANARY_REHEARSAL_20260517.md` 填写：

- 演练开始/结束时间  
- 变更版本与实例范围  
- 灰度阶段结果与证据路径  
- 告警与回退日志路径  
- 数据核验路径  
- 回退耗时（目标）  
- 是否阻断发布与复盘结论

## 7. 交付与归档

- [ ] 灰度发布手册归档路径：`docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- [ ] 快速回退手册归档路径：`docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md#4-快速回退流程e-01--e-03`
- [ ] 数据回滚路径归档路径：`scripts/p0_e_backup_snapshot.sh`、`reports/p0_e_backups/`
- [ ] 全链路演练报告归档路径：`docs/planning/P0_E_CANARY_REHEARSAL_20260517.md`
- [ ] 演练结论已复核，可复查

## 8. 执行约束

- 任一演练必须先完成编译前置检查：
  - `go run . -configPath=conf/jdbc_local.ini`
  - 未通过时禁止进入阶段 0 放量
