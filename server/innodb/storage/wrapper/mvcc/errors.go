package mvcc

import (
	"xmysql-server/server/innodb/basic"
)

// MVCC相关错误的别名，方便使用
var (
	ErrLockConflict         = basic.ErrLockConflict
	ErrRecordNotFound       = basic.ErrRecordNotFound
	ErrPageFull             = basic.ErrPageFull
	ErrInvalidSnapshot      = basic.ErrInvalidSnapshot
	ErrIncompatibleSnapshot = basic.ErrIncompatibleSnapshot
	ErrSnapshotExpired      = basic.ErrSnapshotExpired
	ErrInvalidPageID        = basic.ErrInvalidPageID
	ErrInvalidVersion       = basic.ErrInvalidVersion
)
