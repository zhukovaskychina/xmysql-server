package pages

//回滚页面
type RollBackPage struct {
	AbstractPage
	TrxRsegMaxSize     []byte //4 byte   管理所有的Undo页面链表中的Undo页面数之和的最大值，最大值0xFFFFFFFF
	TrxRsegHistorySize []byte //4 byte History链表占用的页面数量
	TrxRsegHistory     []byte //20 byte	History链表的基节点
	TrxRsegFsegHeader  []byte //10byte	对应的段空间header
	TrxRsegUndoSlots   []byte //4096byte 各个undo页面链表的first undo page 的页面号码的集合，也就是undo slot 集合
	EmptySpace         []byte //16384-4096-38-4-4-20-10

}

//构造Rollback回滚页面，理论上是系统表的第5号空间
func NewRollBackPage() *RollBackPage {
	var rollbackPage = new(RollBackPage)
	return rollbackPage
}
