# XMySQL P0-C 并发验证报告
time: 20260517_002358
repo: /Users/zhukovasky/GolandProjects/xmysql-server
report_root: ./reports/p0_c_validation
rounds: 3
timeout: 300s
stop_on_fail: 0

## 1. 测试矩阵
- conflict_write: 插入/回滚/索引分裂并发
- range_read: 范围读与 gap lock 相关路径
- long_tx: 长事务检测与并发链路
- deadlock_wait: 死锁检测与等待路径

## 2. 明细（按轮次）
| round | scenario | status | test_runs | fail_tests | duration_s | deadlock_hit | lock_wait_hit | log |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | conflict_write | PASS | 5 | 0 | 3 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_1/conflict_write.log |
| 1 | range_read | PASS | 9 | 0 | 7 | 12 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_1/range_read.log |
| 1 | long_tx | PASS | 4 | 0 | 5 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_1/long_tx.log |
| 1 | deadlock_wait | PASS | 5 | 0 | 7 | 15 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_1/deadlock_wait.log |
| 2 | conflict_write | PASS | 5 | 0 | 3 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_2/conflict_write.log |
| 2 | range_read | PASS | 9 | 0 | 8 | 12 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_2/range_read.log |
| 2 | long_tx | PASS | 4 | 0 | 5 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_2/long_tx.log |
| 2 | deadlock_wait | PASS | 5 | 0 | 8 | 15 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_2/deadlock_wait.log |
| 3 | conflict_write | PASS | 5 | 0 | 4 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_3/conflict_write.log |
| 3 | range_read | PASS | 9 | 0 | 9 | 12 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_3/range_read.log |
| 3 | long_tx | PASS | 4 | 0 | 7 | 0 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_3/long_tx.log |
| 3 | deadlock_wait | PASS | 5 | 0 | 11 | 15 | 0 | ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/round_3/deadlock_wait.log |

## 3. 场景一致性标记（P0C 标记）
| round | scenario | marker_status | marker |
| --- | --- | --- | --- |
| 1 | conflict_write | PASS |     p0_concurrency_consistency_test.go:100: CONSISTENCY|conflict_write|expected=80|got=80|checksum=c6071daf71a5078a4ffe18dfd960d4a2e924bf35541d47790a91664b127a57d8|status=PASS |
| 1 | range_read | PASS |     p0_concurrency_consistency_test.go:159: CONSISTENCY|range_read|expected=7200|runs=240|checksum=095dfce3f5d66b13a5062fd5e47d567d643895df5a6320ec359423c67c82265a|status=PASS |
| 1 | long_tx | PASS |     p0_concurrency_consistency_test.go:209: CONSISTENCY|long_tx|status=PASS|long=10|critical=10|warnings=0|alerts=10 |
| 1 | deadlock_wait | PASS |     p0_concurrency_consistency_test.go:253: CONSISTENCY|deadlock_wait|deadlock_detected=40|lock_conflict=40|status=PASS |
| 2 | conflict_write | PASS |     p0_concurrency_consistency_test.go:100: CONSISTENCY|conflict_write|expected=80|got=80|checksum=c6071daf71a5078a4ffe18dfd960d4a2e924bf35541d47790a91664b127a57d8|status=PASS |
| 2 | range_read | PASS |     p0_concurrency_consistency_test.go:159: CONSISTENCY|range_read|expected=7200|runs=240|checksum=095dfce3f5d66b13a5062fd5e47d567d643895df5a6320ec359423c67c82265a|status=PASS |
| 2 | long_tx | PASS |     p0_concurrency_consistency_test.go:209: CONSISTENCY|long_tx|status=PASS|long=10|critical=10|warnings=0|alerts=10 |
| 2 | deadlock_wait | PASS |     p0_concurrency_consistency_test.go:253: CONSISTENCY|deadlock_wait|deadlock_detected=40|lock_conflict=40|status=PASS |
| 3 | conflict_write | PASS |     p0_concurrency_consistency_test.go:100: CONSISTENCY|conflict_write|expected=80|got=80|checksum=c6071daf71a5078a4ffe18dfd960d4a2e924bf35541d47790a91664b127a57d8|status=PASS |
| 3 | range_read | PASS |     p0_concurrency_consistency_test.go:159: CONSISTENCY|range_read|expected=7200|runs=240|checksum=095dfce3f5d66b13a5062fd5e47d567d643895df5a6320ec359423c67c82265a|status=PASS |
| 3 | long_tx | PASS |     p0_concurrency_consistency_test.go:209: CONSISTENCY|long_tx|status=PASS|long=10|critical=10|warnings=0|alerts=10 |
| 3 | deadlock_wait | PASS |     p0_concurrency_consistency_test.go:253: CONSISTENCY|deadlock_wait|deadlock_detected=40|lock_conflict=40|status=PASS |

## 4. 一致性检查
- 场景级测试通过率基于 go test 结果; 一致性基于 CONSISTENCY|<scenario> 标记
- conflict_write: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- range_read: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- long_tx: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
- deadlock_wait: baseline=PASS，rounds=PASS PASS PASS;一致性=yes
  - conflict_write: markers_baseline=PASS，rounds=PASS PASS PASS；marker一致性=yes
  - range_read: markers_baseline=PASS，rounds=PASS PASS PASS；marker一致性=yes
  - long_tx: markers_baseline=PASS，rounds=PASS PASS PASS；marker一致性=yes
  - deadlock_wait: markers_baseline=PASS，rounds=PASS PASS PASS；marker一致性=yes
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
report: ./reports/p0_c_validation/p0_concurrency_validation_20260517_002358/concurrency_validation_report.md
