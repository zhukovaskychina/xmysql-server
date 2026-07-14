# XMySQL P0-C 并发验证报告
time: 20260516_223550
repo: /Users/zhukovasky/GolandProjects/xmysql-server
report_root: /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation
rounds: 3
timeout: 60s
stop_on_fail: 1

## 1. 测试矩阵
- conflict_write: 插入/回滚/索引分裂并发
- range_read: 范围读与 gap lock 相关路径
- long_tx: 长事务检测与并发链路
- deadlock_wait: 死锁检测与等待路径

## 2. 明细（按轮次）
| round | scenario | status | test_runs | fail_tests | duration_s | deadlock_hit | lock_wait_hit | log |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | conflict_write | PASS | 5 | 0 | 5 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_1/conflict_write.log |
| 1 | range_read | PASS | 9 | 0 | 10 | 12 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_1/range_read.log |
| 1 | long_tx | PASS | 4 | 0 | 8 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_1/long_tx.log |
| 1 | deadlock_wait | PASS | 5 | 0 | 10 | 15 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_1/deadlock_wait.log |
| 2 | conflict_write | PASS | 5 | 0 | 3 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_2/conflict_write.log |
| 2 | range_read | PASS | 9 | 0 | 11 | 12 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_2/range_read.log |
| 2 | long_tx | PASS | 4 | 0 | 6 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_2/long_tx.log |
| 2 | deadlock_wait | PASS | 5 | 0 | 9 | 15 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_2/deadlock_wait.log |
| 3 | conflict_write | PASS | 5 | 0 | 3 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_3/conflict_write.log |
| 3 | range_read | PASS | 9 | 0 | 12 | 12 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_3/range_read.log |
| 3 | long_tx | PASS | 4 | 0 | 8 | 0 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_3/long_tx.log |
| 3 | deadlock_wait | PASS | 5 | 0 | 11 | 15 | 0 | /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/round_3/deadlock_wait.log |

## 3. 一致性检查
- conflict_write: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- range_read: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- long_tx: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- deadlock_wait: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- 一致性结论：PASS（同一场景每轮结果一致）

## 4. 闭环建议
- 若 FAIL：保留失败轮次日志，缩小测试范围后按场景单独复现。
- 若 PASS：将本报告路径加入 P0_C 脚本审计闭环。
## 5. 审批路径
- [ ] C-01 冲突写场景通过
- [ ] C-01 范围读场景通过
- [ ] C-01 长事务场景通过
- [ ] C-01 死锁/锁等待可控
- [ ] C-01 无脏读/丢写/重复写的连续一致性证据
RESULT: PASS
report: /Users/zhukovasky/GolandProjects/xmysql-server/reports/p0_c_validation/p0_concurrency_validation_20260516_223550/concurrency_validation_report.md
