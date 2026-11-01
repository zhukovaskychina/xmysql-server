# SQL-01 至 SQL-14 验收记录矩阵

生成时间：2026-10-17。此矩阵记录每个原始语法类别对应的任务、代码证据和当前验收状态；“局部通过”不等于最终交付门禁通过。

| 编号 | 覆盖范围 | 当前证据 | 状态 |
|---|---|---|---|
| SQL-01 | SHOW、DESCRIBE、EXPLAIN | SHOW/EXPLAIN AST 分发、SHOW FULL TABLES/CREATE、parser 与 engine 精确矩阵 | 局部通过 |
| SQL-02 | ALTER TABLE | ALTER parser、主键/外键/CHECK、表空间、分区维护综合测试 | 局部通过 |
| SQL-03 | 其他表/索引/表空间/分区 DDL | Table DDL、生成列、分区、tablespace 精确测试 | 局部通过 |
| SQL-04 | 管理命令与复制控制 | admin AST、复制 source/filter、权限与锁精确测试 | 局部通过 |
| SQL-05 | HANDLER、SQL PREPARE、组件 | prepared、handler、component 兼容测试及文本协议 SQL PREPARE/EXECUTE | 局部通过 |
| SQL-06 | 账户、角色、权限、密码 | account/role/proxy/grant/revoke 与权限矩阵 | 局部通过 |
| SQL-07 | 系统变量、用户变量、SET | dispatcher AST 变量、SET NAMES、作用域和元数据测试 | 局部通过 |
| SQL-08 | 视图及依赖/安全属性 | CTE 视图、updatability、列名持久化、锁/重载测试 | 局部通过 |
| SQL-09 | 存储对象定义 | procedure/function/trigger/event 定义、SHOW CREATE、重载和持久化测试 | 局部通过 |
| SQL-10 | 存储程序体与控制流 | 全部 TestProcedure、SIGNAL/HANDLER/GET DIAGNOSTICS、token/AST 替换测试 | 局部通过 |
| SQL-11 | INFORMATION_SCHEMA/PERFORMANCE_SCHEMA/mysql 系统表 | 精确 metadata、权限、Firewall、例程精度测试；完整宽泛门禁仍失败 | 未最终通过 |
| SQL-12 | SELECT 改写、CTE、窗口、全文、空间 | query AST rewrite、CTE/window/fulltext/spatial 与 EXTRACT AST 测试 | 局部通过 |
| SQL-13 | 条件、分区、索引、CHECK/生成列依赖 | predicate AST、partition、generated/check/foreign key 精确测试 | 局部通过 |
| SQL-14 | 对象提取、权限/锁、监控引用 | object references、DDL/session/metadata lock、table IO/metrics 测试 | 局部通过，宽泛锁门禁仍需复核 |

完整服务、Go 1.24 CI、重启、49 条样例和历史 511 站点基线另见升级验证报告，均未被此矩阵隐式标记为通过。
