# P0-E-02 备份恢复脚本冒烟演练（一次）

- 演练时间：`2026-05-17 06:33:21`
- 演练类型：`backup/list/restore` 冒烟验证
- 触发理由：补齐 T-E-02 数据回滚策略的可执行证据

## 演练命令与结果

1. 备份

- 命令：
  `P0E_MODE=backup P0E_DATA_DIR=./data P0E_BACKUP_ROOT=./reports/p0_e_backups P0E_SNAPSHOT_TAG=canary_dryrun bash scripts/p0_e_backup_snapshot.sh`
- 结果：
  - `backup_file: ./reports/p0_e_backups/xmysql-backup-canary_dryrun-20260517_063321.tar.gz`
  - `manifest: ./reports/p0_e_backups/xmysql-backup-canary_dryrun-20260517_063321.manifest.json`

2. 备份清单

- 命令：
  `P0E_MODE=list bash scripts/p0_e_backup_snapshot.sh`
- 结果：
  - 列出上述备份文件为首条

3. 恢复

- 命令：
  `P0E_MODE=restore P0E_BACKUP_FILE=./reports/p0_e_backups/xmysql-backup-canary_dryrun-20260517_063321.tar.gz P0E_RESTORE_DIR=/tmp/xmysql_data_restored_test bash scripts/p0_e_backup_snapshot.sh`
- 结果：
  - 输出：`restored_to: /tmp/xmysql_data_restored_test`
  - 恢复目录下存在 `data/` 目录

## 关联文件

- manifest：`reports/p0_e_backups/xmysql-backup-canary_dryrun-20260517_063321.manifest.json`
- backup：`reports/p0_e_backups/xmysql-backup-canary_dryrun-20260517_063321.tar.gz`
- 演练记录：`reports/p0_e_backups/p0_e_backup_restore_dryrun_20260517_063321.md`

## 结论

- 结论：`backup/list/restore` 路径通路可用。
- 风险提示：
  - 未覆盖多节点/大数据量场景。
  - 未执行表级一致性回放校验（待全链路演练阶段补齐）。
