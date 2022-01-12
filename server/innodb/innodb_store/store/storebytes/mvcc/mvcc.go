package mvcc

type Mvcc struct {
	ActiveViews []ReadView
	FreeViews   []ReadView
}

//创建一个readview
func (m Mvcc) CreateView() (*ReadView, *TrxT) {

	return nil, nil
}

//关闭一个readview
func (m Mvcc) CloseView(view *ReadView, ownMutex bool) {

}

//是否关闭一个View
func (m Mvcc) IsViewRelease(view *ReadView) bool {
	return false
}

func (m Mvcc) CloneOldestView() {

}

func (m Mvcc) GetActiveReadViewSize() int {
	return 0
}

func (m Mvcc) IsReadViewActive(view ReadView) bool {
	return false
}
