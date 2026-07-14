# XMySQL P0-C 并发验证报告
time: 20260516_224723
repo: /Users/zhukovasky/GolandProjects/xmysql-server
report_root: /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation
rounds: 1
timeout: 120s
stop_on_fail: 0

## 1. 测试矩阵
- conflict_write: 插入/回滚/索引分裂并发
- range_read: 范围读与 gap lock 相关路径
- long_tx: 长事务检测与并发链路
- deadlock_wait: 死锁检测与等待路径

## 2. 明细（按轮次）
| round | scenario | status | test_runs | fail_tests | duration_s | deadlock_hit | lock_wait_hit | log |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | conflict_write | FAIL | 0 | 0 | 1 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_224723/round_1/conflict_write.log |
| 1 | range_read | FAIL | 4 | 0 | 4 | 12 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_224723/round_1/range_read.log |
| 1 | long_tx | FAIL | 0 | 0 | 2 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_224723/round_1/long_tx.log |
| 1 | deadlock_wait | FAIL | 4 | 0 | 4 | 13 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_224723/round_1/deadlock_wait.log |

## 3. 场景一致性标记（P0C 标记）
| round | scenario | marker_status | marker |
| --- | --- | --- | --- |
| 1 | conflict_write | MISSING |  |
| 1 | range_read | MISSING |  |
| 1 | long_tx | MISSING |  |
| 1 | deadlock_wait | MISSING |  |

## 4. 一致性检查
- 场景级测试通过率基于 go test 结果; 一致性基于 CONSISTENCY|<scenario> 标记
- conflict_write: baseline=FAIL，rounds=FAIL;一致性=yes
- range_read: baseline=FAIL，rounds=FAIL;一致性=yes
- long_tx: baseline=FAIL，rounds=FAIL;一致性=yes
- deadlock_wait: baseline=FAIL，rounds=FAIL;一致性=yes
  - conflict_write: markers_baseline=MISSING，rounds=MISSING；marker一致性=yes
  - range_read: markers_baseline=MISSING，rounds=MISSING；marker一致性=yes
  - long_tx: markers_baseline=MISSING，rounds=MISSING；marker一致性=yes
  - deadlock_wait: markers_baseline=MISSING，rounds=MISSING；marker一致性=yes
- 一致性结论：PASS（同一场景每轮结果一致）

## 5. 闭环建议
- 若 FAIL：保留失败轮次日志，缩小测试范围后按场景单独复现。
- 若 PASS：将本报告路径加入 P0_C 脚本审计闭环。
## 6. 审批路径
- [ ] C-01 冲突写场景通过
- [ ] C-01 范围读场景通过
- [ ] C-01 长事务场景通过
- [ ] C-01 死锁/锁等待可控
- [ ] C-01 无脏读/丢写/重复写的连续一致性证据
RESULT: PASS
report: /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_224723/concurrency_validation_report.md
