# 外连接与内连接：SQL 用法与测试说明

本文说明 XMySQL Server 中 **INNER / LEFT / RIGHT / FULL OUTER JOIN** 的 SQL 写法与执行行为，以及对应的单元测试位置。

---

## 一、支持的连接类型

| 类型 | SQL 写法 | 语义 |
|------|----------|------|
| 内连接 | `JOIN` / `INNER JOIN` / `CROSS JOIN` | 只输出左右表都满足 ON 条件的行 |
| 左外连接 | `LEFT JOIN` / `LEFT OUTER JOIN` | 左表每行至少出一行；无匹配时右表列填 NULL |
| 右外连接 | `RIGHT JOIN` / `RIGHT OUTER JOIN` | 右表每行至少出一行；无匹配时左表列填 NULL |
| 全外连接 | `FULL JOIN` / `FULL OUTER JOIN` | 左表、右表“独有”行都会出，无匹配一侧填 NULL |

（注：当前 parser 已支持 LEFT/RIGHT；FULL 若未在语法中显式支持，执行器已实现，可在逻辑计划中直接使用 `JoinType: "FULL"`。）

---

## 二、示例 SQL

### 1. 内连接（INNER JOIN）

```sql
-- 只返回两表都能关联上的行
SELECT a.id, a.name, b.score
FROM users a
INNER JOIN scores b ON a.id = b.user_id;
```

### 2. 左外连接（LEFT JOIN）

```sql
-- 左表 users 每行都保留；右表无匹配时 b 的列为 NULL
SELECT a.id, a.name, b.score
FROM users a
LEFT JOIN scores b ON a.id = b.user_id;
```

典型结果：`users` 中 id=1,2 在 `scores` 有对应 → 正常两表列；id=3 在 `scores` 无对应 → 一行 (3, 'C', NULL)。

### 3. 右外连接（RIGHT JOIN）

```sql
-- 右表 scores 每行都保留；左表无匹配时 a 的列为 NULL
SELECT a.id, a.name, b.score
FROM users a
RIGHT JOIN scores b ON a.id = b.user_id;
```

典型结果：`scores` 中 user_id=3 在 `users` 无对应 → 一行 (NULL, NULL, 30)。

### 4. 全外连接（FULL OUTER JOIN）

```sql
-- 左表、右表“独有”行都会出现，无匹配一侧填 NULL
SELECT a.id, a.name, b.user_id, b.score
FROM users a
FULL OUTER JOIN scores b ON a.id = b.user_id;
```

典型结果：匹配行正常；仅左表有的行 → 右表列 NULL；仅右表有的行 → 左表列 NULL。

---

## 三、执行路径简述

- **逻辑计划**：`buildTableExpr` 遇到 `JoinTableExpr` 时，根据 `Join` 字符串设置 `LogicalJoin.JoinType`（"INNER" / "LEFT" / "RIGHT"；FULL 若语法支持也会设为 "FULL"）。
- **物理计划**：`PhysicalHashJoin` / `PhysicalMergeJoin` 透传 `JoinType`。
- **执行器**：
  - **NestedLoopJoin**：按连接类型区分 `nextInner` / `nextLeft` / `nextRight` / `nextFull`，无匹配时用 `mergeRecordsWithRightNull` 或 `mergeRecordsWithLeftNull` 补 NULL。
  - **HashJoin**：LEFT 时 build=右表、probe=左表；RIGHT 时 build=左表、probe=右表；FULL 时先做 LEFT 阶段再输出未匹配的 build 行。

---

## 四、单元测试位置与运行方式

- **文件**：`server/innodb/engine/outer_join_test.go`
- **用例**：
  - `TestNestedLoopJoin_LeftOuterJoin`：左外连接，验证左表独有行右表为 NULL。
  - `TestNestedLoopJoin_RightOuterJoin`：右外连接，验证右表独有行左表为 NULL。
  - `TestNestedLoopJoin_FullOuterJoin`：全外连接，验证左独有、右独有、匹配行三种情况。
  - `TestHashJoin_LeftOuterJoin`：Hash 左外连接，验证无匹配时右表列 NULL。

在 **engine 包可成功编译** 的前提下，可仅跑外连接相关测试：

```bash
go test ./server/innodb/engine -run "NestedLoopJoin_Left|NestedLoopJoin_Right|NestedLoopJoin_Full|HashJoin_Left" -v -count=1
```

若当前仓库中 `index_lookup_test.go`、`index_reading_test.go` 等仍有编译错误，需先修复或暂时移出后再执行上述命令。

---

## 五、参考

- 未实现功能总览：`docs/未实现功能梳理.md`（EXEC-005 外连接）
- 逻辑计划与 JOIN 类型：`server/innodb/plan/logical_plan.go`（`buildTableExpr`、`joinStrToJoinType`）
- 执行器实现：`server/innodb/engine/volcano_executor.go`（NestedLoopJoin / HashJoin）
