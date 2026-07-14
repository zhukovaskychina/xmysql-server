# P0-E-03 全链路演练补充（阶段 1 失败点）

- 记录版本：`2026-05-17`
- 文件用途：E-03 阶段 1 最小写入路径回归，记录当前阻断。

## 1. 验证动作

- 时间：`2026-05-17 06:56:33`
- 场景：阶段 0 通过后的最小写入校验（`CREATE DATABASE` / `CREATE TABLE` / `INSERT` / `DROP DATABASE`）
- 客户端脚本输出：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065633_client.txt`
- 服务日志：`reports/p0_e_backups/p0_e_canary_rehearsal_20260517_065633_server.log`

## 2. 执行结果

- `CREATE DATABASE IF NOT EXISTS p0e_db`：通过
- `CREATE TABLE IF NOT EXISTS p0e_db.t1 (id INT PRIMARY KEY, c VARCHAR(20))`：通过
- `INSERT INTO p0e_db.t1 VALUES (1, 'a'), (2, 'b')`：失败
  - 错误：`execute storage-integrated INSERT failed: 获取表存储信息失败: table mysql.t1 not found in storage mapping`
- 说明：当前阶段 1 写入验证仍被 DML 引擎映射路径阻断，未进入 DELETE/查询核验。

## 3. 风险影响

- 阶段 1 放量尚不可执行，当前不满足 E-03 全链路演练通过门槛。
- 需优先修复表存储映射链路（`table mysql.t1 not found in storage mapping`）后继续阶段 1/2/回退。
