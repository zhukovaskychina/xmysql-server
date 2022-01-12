package txn

import types "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"

type Txn struct {
	types.XMySQLTransaction
}

func NewTxn() types.XMySQLTransaction {

	return &Txn{}
}

func (t Txn) GetCurrentVersion() types.Version {
	panic("implement me")
}

func (t Txn) IsReadOnly() bool {
	return false
}

func (t Txn) Commit() error {
	panic("implement me")
}

func (t Txn) Rollback() error {
	panic("implement me")
}

func (t Txn) String() string {
	panic("implement me")
}

func (t Txn) StartTxn() {

}
