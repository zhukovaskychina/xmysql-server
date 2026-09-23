# MySQL 8.4 Compatibility：恢复与备份演练手册

## 当前可执行范围

XMySQL 的 redo/undo/checkpoint 恢复测试、跨目录逻辑备份导入和 binlog point-in-time restore 已有 Go 验证路径。现在还提供 XMySQL 自有格式的物理快照 API：生成带版本清单和 SHA-256 的 `.xmb` gzip/tar 归档，恢复前完整校验并原子落到新目录。它不能被宣称为官方 MySQL/InnoDB 文件格式兼容，也不提供在线 hot backup 语义。

## 恢复演练

PowerShell：

```powershell
.\scripts\compatibility\crash_recovery_matrix.ps1 -Repeat 3
```

Bash：

```bash
bash scripts/compatibility/crash_recovery_matrix.sh reports/compatibility/crash-recovery 3
```

报告必须包含 git revision、UTC 时间、每次测试退出码和输出尾部。任意一次失败都应为 `FAIL`。

## 数据目录备份边界

生产恢复必须使用同一版本和配置。引擎运行时调用 `XMySQLEngine.CreatePhysicalBackup(ctx, archivePath)` 会先尝试 sharp checkpoint，再刷新存储层并生成归档；直接调用 `server/backup.RestorePhysicalBackup` 前必须停止引擎，目标目录必须不存在且由恢复流程新建。恢复后再启动服务并执行 `CHECK TABLE`、行数、索引和元数据查询。

```go
manifest, err := engine.CreatePhysicalBackup(ctx, "./backups/full-20260824.xmb")
if err != nil {
    return err
}
_, err = backup.VerifyPhysicalBackup("./backups/full-20260824.xmb")
if err != nil {
    return err
}
if err := backup.RestorePhysicalBackup(ctx, "./backups/full-20260824.xmb", "./data-restored"); err != nil {
    return err
}
_ = manifest
```

在线复制数据目录、跨版本直接替换 `.ibd`、把 XMySQL 文件交给官方 MySQL 读取均不在兼容范围内。归档中的符号链接、路径穿越、重复/缺失文件和 SHA-256 不匹配会被拒绝。

## 发布前置条件

在 binlog/GTID 接入事务提交顺序、复制重放和位点持久化前，PITR 项必须保持 `NO-GO`；不能以单次恢复测试替代三次重复演练和状态差异报告。
