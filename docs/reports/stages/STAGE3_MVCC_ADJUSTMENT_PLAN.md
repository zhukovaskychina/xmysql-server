# Stage 3: MVCC 调整计划（已执行）

本阶段目标：
- 修正/对齐 MVCC 可见性判定逻辑（RU/RC/RR）
- 衔接 Manager 层与 Store 层 MVCC（ReadView）
- 进行最小化、安全的验证（不引入大规模变更）

## 关键改动

1) TransactionManager 可见性逻辑
- RU：始终可见
- RC：每次可见性判断时构造“语句级快照”ReadView
- RR：使用 Begin 时建立的“事务级快照”ReadView

2) MVCCManager 接入 ReadView
- BeginTransaction：基于当前活跃事务构造 ReadView（与 TransactionManager 逻辑对齐）
- IsVisible：调用 ReadView.IsVisible(version) 返回真实结果

## 涉及文件
- server/innodb/manager/transaction_manager.go
- server/innodb/manager/mvcc_manager.go

## 验证策略
- 运行 store/mvcc 包中 ReadView 的单测（TestReadView），验证可见性判定语义正确
- 避免触发已知的死锁检测单测不稳定问题（同包内其它测试用例）

## 后续建议（非本阶段范围）
- 锁/死锁检测器的健壮性与一致性测试需要单独回合修复和优化
- 统一 Manager 与 Store 层之间的 MVCC 对象/类型（减少重复定义）
- 增加事务生命周期集成测试（含 RC/RR 行为差异校验）

