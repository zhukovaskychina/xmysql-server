package mvcc

// Deprecated: This file is deprecated and will be removed in a future version.
// Transaction functionality has been migrated to manager/transaction_manager.go
//
// Migration guide:
// - Old: import "github.com/.../storage/store/mvcc"
//        trx := &mvcc.TrxT{...}
//
// - New: import "github.com/.../server/innodb/manager"
//        trx := &manager.Transaction{...}
//
// This file will be removed after all references are updated.

// TrxT 事务结构
// Deprecated: 使用manager.Transaction代替
type TrxT struct {
	ReadViews *[]ReadView //多并发版本快照

	IsolationLevel IsolationLevel //事务隔离级别

	trxStateT TransactionStatus
}
