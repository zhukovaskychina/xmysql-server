# XMySQL Server P0 第一周冲刺计划

## 1. 冲刺目标

本周目标：完成 P0 入口阶段中“高风险实现整改 + 测试稳定化”的最小闭环，形成可复用的方法模板，为后续恢复/并发/观测阶段提供稳定基线。

## 2. 本周范围（仅 Week 1）

- A1 实现侧：
  - T-A1-01 去除错误掩盖型 fallback（第一批路径）
  - T-A1-03 DML 唯一键冲突判定可靠化
- A2 测试侧：
  - T-A2-01 flaky 测试去 sleep（第一批用例）
- 基线保障：
  - 回归流水线与证据归档模板落地

## 3. 人天预算与角色分配

### 3.1 总预算

- 总人天：**8 人天**
- 建议配置：2 名后端 + 1 名测试（可兼职）

### 3.2 任务拆分

- S1（2 人天）：fallback 路径盘点与错误分类规范
- S2（2 人天）：storage_adapter / executor 路径改造与单测
- S3（1 人天）：dml_operators 冲突判定结构化改造
- S4（2 人天）：flaky 测试改造（btree_cache_limit + prefetch）
- S5（1 人天）：回归脚本、报告模板、发布证据归档

## 4. 详细任务卡（按依赖顺序）

### [S1] fallback 盘点与错误分类（前置）
- 依赖：无
- 输入：
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/executor.go`
- 输出：
  - fallback 清单（位置、现状、目标行为）
  - 错误类型约定（可定位字段最小集合）
- 完成定义：
  - 明确第一批改造点（不少于 3 处）

### [S2] 关键路径 fallback 移除（实现）
- 依赖：S1
- 输入：
  - `storage_adapter.go`
  - `executor.go`
- 输出：
  - 第一批关键路径改造代码
  - 对应失败路径单测
- 完成定义：
  - 失败时返回显式错误，不 silently fallback
  - `go test ./server/innodb/engine` 通过

### [S3] DML 冲突判定结构化
- 依赖：S1（可并行于 S2）
- 输入：
  - `server/innodb/engine/dml_operators.go`
- 输出：
  - 冲突判定改造代码与测试
- 完成定义：
  - 不再依赖字符串 contains
  - 冲突/非冲突路径测试均通过

### [S4] flaky 测试去 sleep（第一批）
- 依赖：S2/S3（建议后置）
- 输入：
  - `server/innodb/manager/btree_cache_limit_test.go`
  - `server/innodb/buffer_pool/prefetch_test.go`
- 输出：
  - 条件等待或事件驱动断言版本测试
- 完成定义：
  - 目标测试重复执行 10 轮无随机失败

### [S5] 证据归档与周结
- 依赖：S2/S3/S4
- 输入：
  - 本周 PR、测试输出、失败注入记录
- 输出：
  - 周报（完成项/风险项/下周入口）
  - 证据索引（日志、测试、报告）
- 完成定义：
  - Checklist 对应项可打勾并可追溯证据

## 5. 每日节奏（建议）

- Day 1：S1 启动并完成；S2/S3 开始
- Day 2：S2 主体改造；S3 完成
- Day 3：S2 单测与回归；S4 开始
- Day 4：S4 完成并多轮稳定性验证
- Day 5：S5 归档、复盘、下周计划冻结

## 6. 验收标准（Week 1）

- 完成 S1~S5 所有任务卡
- 以下命令通过：
  - `go test ./server/innodb/engine`
  - `go test ./server/dispatcher ./server/innodb/engine`
- flaky 改造目标用例 10 轮稳定通过
- 形成可审计证据包（测试输出 + 改造说明 + 风险清单）

## 7. 风险与应对

- 风险 R1：老路径耦合高，改造影响面超预期
  - 应对：按第一批最小切片推进，保留可回滚提交边界
- 风险 R2：并发测试耗时导致节奏拖延
  - 应对：先保证关键用例稳定，再扩展覆盖面
- 风险 R3：错误分类不统一导致日志不可检索
  - 应对：先冻结最小错误字段规范再开发

## 8. 周结束出口

- 若 Week 1 全部达标：进入 Week 2（继续 A1/A2 + 启动 B 恢复脚本）
- 若未达标：冻结新增需求，优先清理 S2/S4 阻塞项后再进入下一周
