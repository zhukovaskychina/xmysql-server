# XMySQL Server 内核正确性 P0 收口设计

**日期**: 2026-04-17
**状态**: 已确认设计，待用户 review
**子项目**: 内核正确性 P0 收口

---

## 1. 背景

当前 `xmysql-server` 已具备较完整的单机数据库内核原型，但“内核正确性”相关的高风险问题仍未收口，导致测试失败信号与实际实现缺陷混杂，仓库暂时不具备可信的回归能力。

这类问题主要集中在以下几类：

- `engine` 主链路测试基线未恢复
- 关键路径存在 fallback 掩错
- 重复入口与旧/新实现并存
- page / rollback wrapper 关键路径处于半实现状态
- 部分测试依赖固定 `time.Sleep`，存在 flaky 风险

本子项目不追求一次性解决所有内核问题，而是恢复“P0 最小正确性闭环”，让主链路重新具备可验证、可定位、可持续回归的基础。

---

## 2. 目标

本子项目的目标是：

> 恢复内核正确性 P0 最小闭环，使 `engine` 主链路、wrapper 关键路径和目标测试集重新具备可信回归能力。

具体目标包括：

1. 恢复 `engine` 相关测试的最小可信基线
2. 去除关键执行链路中的 fallback 掩错
3. 明确 page / wrapper 主入口，降低双入口带来的行为漂移
4. 将 wrapper 关键路径补到最小可用
5. 去除目标 flaky 测试中的固定 sleep 依赖
6. 避免测试环境因默认/空配置导致非业务性 panic

---

## 3. 非目标

本子项目明确不包含以下内容：

- 不新增 SQL 功能
- 不扩展协议兼容性
- 不做 TLS / 认证增强
- 不补子查询、CTE、窗口函数等执行能力
- 不处理复制、高可用、分布式相关能力
- 不要求一次性恢复 `go test ./...` 全绿
- 不做大规模架构重写或无关重构

---

## 4. 实施方案比较

### 方案 A：测试先行型

先优先把测试和编译错误修到可运行，再回头收口实现问题。

优点：

- 反馈快
- 可以快速形成表面上的测试进展

缺点：

- 容易把“测试适配现状”当成修复
- 很多失败的根因在实现链路，不先收口实现会导致返工

### 方案 B：风险链路先行型

沿 `engine -> storage adapter / wrapper -> flaky test` 这条正确性主链路逐段收口，每完成一段就补测试，恢复可信回归能力。

优点：

- 测试恢复和实现收口是同一条链
- 能避免掩错路径继续污染测试信号
- 更符合当前仓库的主要风险分布

缺点：

- 前期需要更谨慎地选定边界

### 方案 C：分模块并行型

将 `engine`、`wrapper`、`flaky tests` 并行推进。

优点：

- 表面推进速度快

缺点：

- 当前问题强耦合，容易互相踩线
- 在回归基线未恢复前不适合并行改造

### 结论

本子项目采用 **方案 B：风险链路先行型**。

---

## 5. 系统拆分与顺序

本子项目拆为五个工作包，按依赖顺序推进，而不是按目录推进。

### 工作包 A：测试基线定位与冻结

目标：

- 先把当前失败分组
- 明确哪些是接口漂移，哪些是真实现缺口，哪些是初始化和配置问题

预期结果：

- 形成一张“失败簇清单”
- 为后续修复建立明确优先级

### 工作包 B：`engine` 主链路去掩错

目标：

- 清理关键执行路径中的 fallback 掩错
- 把“模糊成功”改为“明确失败并可定位”

预期结果：

- 失败时能明确暴露模块、语句类型和关键参数
- 测试变红时更能反映真实问题

### 工作包 C：重复入口收敛

目标：

- 明确 page / wrapper 的生产主入口
- 降低双入口并存导致的行为漂移

预期结果：

- 主入口唯一
- 次级入口若保留，仅承担兼容壳职责

### 工作包 D：wrapper 最小可用补齐

目标：

- 把 page / rollback wrapper 关键路径补到最小可用
- 阻止空实现穿透到上层逻辑

预期结果：

- 正常输入可解析
- 异常输入可明确报错
- 基础状态和读写行为可用

### 工作包 E：flaky 测试清理与稳定性验证

目标：

- 去除固定 `time.Sleep` 驱动的测试
- 改为条件等待、状态轮询或事件驱动断言

预期结果：

- 目标测试多轮运行稳定
- 测试失败更能代表真实问题

---

## 6. 文件边界与责任

### 6.1 `engine` 主链路

重点文件：

- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/executor_show_routing_test.go`
- `server/innodb/engine/unified_executor_test.go`

责任边界：

- `executor.go`: 只负责语句级执行编排，不负责掩错式兜底
- `unified_executor.go`: 只负责物理执行树构建与运行，不做猜测式 fallback
- `storage_adapter.go`: 只承担桥接，不承担修正逻辑

约束：

- 不再依赖错误文本做隐式决策
- 错误必须具备可定位信息

### 6.2 `storage wrapper` / `page wrapper`

重点文件：

- `server/innodb/storage/wrapper/page/page_wrapper_base.go`
- `server/innodb/storage/wrapper/page/rollback_page_wrapper.go`
- `server/innodb/storage/wrapper/types/base_page.go`
- `server/innodb/storage/store/pages/page.go`

责任边界：

- `wrapper/page/*`: 面向上层的页解析、状态、读写、校验
- `wrapper/types/base_page.go`: 若继续保留，只能是稳定抽象或兼容壳
- `store/pages/page.go`: 若继续存在，应明确是底层页模型，而非并行生产入口

约束：

- 生产主入口只能有一条
- 兼容壳不可承载新逻辑

### 6.3 flaky tests

重点文件：

- `server/innodb/manager/btree_cache_limit_test.go`
- `server/innodb/manager/space_expansion_concurrent_test.go`
- `server/innodb/buffer_pool/prefetch_test.go`

约束：

- 禁止依赖固定 sleep 长度断言行为
- 优先改为条件等待、轮询超时、channel 或状态同步

### 6.4 配置与初始化

重点文件：

- `server/conf/config.go`
- `server/innodb/engine/enginx.go`
- `server/auth/auth_integration_test.go`

约束：

- 测试配置不可因 `Cfg.Raw == nil` 等原因直接 panic
- 初始化失败要返回可诊断错误

---

## 7. 测试策略

本子项目使用“可信回归”作为核心标准，而不是单纯追求测试数量。

### 7.1 基线恢复测试

将失败分为：

- 过时测试假设
- 真实实现缺口
- 初始化/配置缺陷
- flaky 行为

原则：

- 如果测试只是绑定了错误的旧接口，应更新到新主入口
- 不为兼容错误测试而恢复错误接口

### 7.2 主链路回归测试

围绕 `executor -> unified executor -> storage adapter` 建立最小可信回归集。

目标：

- 一旦失败，能较明确指向哪一层行为损坏

### 7.3 wrapper 最小可用测试

重点覆盖：

- 正常解析
- 非法输入报错
- 基础状态管理可用
- 不再出现空实现直通上层

### 7.4 稳定性测试

对被改造的 flaky 测试执行多轮重复运行。

原则：

- 单次通过不算完成
- 多轮稳定才算完成

---

## 8. 验收标准

本子项目完成时，至少满足以下条件：

1. `go test ./server/innodb/engine` 恢复到可持续回归状态，不再被明显接口漂移和掩错路径污染
2. 关键 fallback 掩错路径被移除或显著收紧
3. page / wrapper 主入口明确，重复入口不再共同承载生产逻辑
4. 目标 flaky 测试去除固定 `time.Sleep` 依赖，并能重复稳定运行
5. 测试环境下的默认配置/空配置不会触发非预期 panic

---

## 9. 交付物

本子项目完成后应产出：

1. 对应代码修复
2. 一组恢复后的关键回归测试
3. 一份主入口收敛说明或变更说明
4. 一份“已解决正确性风险 / 暂未覆盖风险”总结
5. 一组可复跑的验证命令

---

## 10. 不追求的结果

本子项目不以以下结果作为本轮完成条件：

- `go test ./...` 一次性全绿
- 所有历史测试立即全部可信
- wrapper 全量能力补齐
- 功能兼容性或 SQL 能力同步增强

---

## 11. 下一步

在用户 review 并确认本 spec 后，进入 implementation plan 编写阶段，按 `engine 主链路 -> wrapper 关键路径 -> flaky 测试` 的顺序生成详细执行计划。

