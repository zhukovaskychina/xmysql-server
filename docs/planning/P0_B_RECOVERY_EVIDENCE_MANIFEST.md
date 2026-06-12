# P0 B 组恢复演练证据清单（草案）

## 目标

- 对照 GAP-02（崩溃恢复）给出可复核证据
- 形成“演练脚本 / 场景结果 / 一致性标记 / 重复回放”的一页式清单

## 标准输入

- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`

## 必须产出

- 演练汇总日志：每轮 `summary.log`
- 客户端观测日志：`client.log`
- 审计清单：`scripts/p0_b_recovery_audit.sh` 输出文件

## 必检项（每项附文件路径）

- [ ] redo 场景：`verify redo` 步骤 PASS 且 `REDO_VERIFY_OK` 出现
- [ ] undo 场景：undo 分组测试 PASS
- [ ] 半提交场景：half-commit 分组测试 PASS
- [ ] `SHOW TABLES WHERE` 路径 PASS
- [ ] 连续多轮一致性：至少 2 次 run 在关键步骤上结果一致（脚本可多目录扫描）
- [ ] 复盘说明含异常恢复动作、重启时间、恢复后检查结果

## 失败时处理

- 阻断 B-02 结项
- 回退到 `crash_recovery_process_drill.sh` 最近通过场景进行小步修复
- 修复后重新生成演练与审计报告
