package mvcc

type TrxT struct {
	ReadViews *[]ReadView //多并发版本快照

	IsolationLevel IsolationLevel //事务隔离级别

	trxStateT trx_state_t
}
