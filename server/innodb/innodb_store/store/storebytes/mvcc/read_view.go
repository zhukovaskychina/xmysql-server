package mvcc

type ReadView struct {
	mIds         []byte //当前系统中活跃的事务ID列表
	minTrxId     TrxId  //当前系统中活跃的最小事务ID
	maxTrxId     TrxId  //系统分配给下一个事务的ID
	creatorTrxId TrxId  //生成该ReadView的事务ID，正在创建事务的事务Id

}

//改变元祖可见性
func (rv ReadView) ChangesVisible(id TrxId, tableName string) bool {

	return false
}
