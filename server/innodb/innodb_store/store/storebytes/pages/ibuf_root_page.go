package pages

//IBuf BTree的节点，单独维护了一个空闲IBuf page链表
//存储changeBuffer的根页面
type IBufRootPage struct {
	AbstractPage
}
