package pages

// SysTrxSysPage 事务系统页面
type SysTrxSysPage struct {
	AbstractPage

	// 事务 ID（最新分配的事务 ID）
	MaxTrxID []byte // 8 bytes

	// Undo 段数量
	NumUndoSlots []byte // 2 bytes

	// Rollback Segment Page No + Offset（第一个事务回滚段 slot 的页面号与偏移）
	TrxUndoRsegHeader []byte // 10 bytes

	// 每个 Undo 段的 slot（每个 slot 10 字节：FSEG header）
	TrxUndoSlots [][]byte // 128 * 10 bytes = 1280 bytes

	// 表空间 Segment Header
	UndoSpaceHeader []byte // 10 bytes

	// 未使用空间
	EmptySpace []byte
}
