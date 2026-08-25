# P0 A1-02 单入口收敛迁移说明

## 1. 单入口定义（本轮已落实）

- 主入口（本阶段）: `server/innodb/storage/wrapper/page/page_factory.go`
  - `CreatePage` 默认/非专用分支返回 `types.NewUnifiedPage(spaceID, pageID, pageType)`。
  - `CreatePage` 的 `FIL_PAGE_INODE` 与 `default` 分支已切为统一入口。

## 2. 兼容层边界

- `server/innodb/storage/wrapper/page/page_wrapper_base.go`
  - `NewBasePageWrapper` 标记为 deprecated，保留兼容实现，不再作为新增代码主入口。
- `server/innodb/storage/wrapper/types/base_page.go`
  - `NewBasePage` 标记为 deprecated，保留历史兼容。
- `server/innodb/storage/store/pages/page.go`
  - `IPage`、`AbstractPage` 标记为兼容接口/实现，不再作为新建入口。

## 3. 当前未迁移调用点（兼容保留）

- 仍有大量历史页面包装器在 `server/innodb/storage/wrapper/page/*.go` 直接实例化 `NewBasePageWrapper`。
- 这些路径先保持兼容，不在本轮 A1-02 全量替换范围内。

## 4. 使用约束（新增代码）

- 禁止新增以下调用：
  - `page.NewBasePageWrapper`
  - `types.NewBasePage`
  - `basic.NewBasePageWrapper`
- 新增页面创建统一走 `page.PageFactory.CreatePage` 与 `types.NewUnifiedPage`。

## 5. 下一步迁移清单

1. 统计 `NewBasePageWrapper` 历史调用面，按页面类型逐步迁移到 `types.UnifiedPage` 适配层。
2. 统一 `store/pages` 的序列化接口调用边界，收敛 `IPage`/`AbstractPage` 的新增使用。
3. 关闭兼容导向的构造器注释与 TODO，在兼容范围内补齐调用方迁移后再移除。
