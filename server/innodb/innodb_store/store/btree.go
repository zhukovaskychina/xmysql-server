package store

import (
	"fmt"
	"github.com/pkg/errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
	"xmysql-server/server/innodb/tuple"
	"xmysql-server/util"
)

// BTree is an implementation of a B-Tree.
//
// BTree stores Item instances in an ordered structure, allowing easy insertion,
// removal, and iteration.x
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	basic.Tree
	//	Tree
	spaceId      uint32
	rootPageNo   uint32
	indexName    string
	rootPage     *Index
	indexSegment Segment
	dataSegment  Segment
	pageIndexes  int //页面数量

	blockFile  *blocks.BlockFile
	BufferPool *buffer_pool.BufferPool
	//用于实现一级索引和二级索引
	internalTuple tuple.TableRowTuple //用于区分叶子段和节点段
	leafTuple     tuple.TableRowTuple

	IsInit bool //判断是否用来初始化
}

func (self *BTree) Keys() (basic.RowItemsIterator, error) {
	panic("implement me")
}

func (self *BTree) Backward() (basic.Iterator, error) {
	panic("implement me")
}

func (self *BTree) Remove(key []byte, where func([]byte) bool) error {
	panic("implement me")
}

func (self *BTree) TREESize() int {
	panic("implement me")
}

func (self *BTree) GetDataSegment() basic.XMySQLSegment {
	return self.dataSegment
}

func (self *BTree) GetInternalSegment() basic.XMySQLSegment {
	return self.indexSegment
}

//用于构建
func NewBtreeAtInit(rootPageNo uint32, indexName string,
	indexSegments Segment, dataSegments Segment,
	rootIndex *Index,
	blockFile *blocks.BlockFile, internalTuple tuple.TableRowTuple, leafTuple tuple.TableRowTuple) *BTree {
	return &BTree{
		rootPageNo:    rootPageNo,
		indexName:     indexName,
		rootPage:      rootIndex,
		indexSegment:  indexSegments,
		dataSegment:   dataSegments,
		pageIndexes:   0,
		blockFile:     blockFile,
		internalTuple: internalTuple,
		leafTuple:     leafTuple,
		IsInit:        true,
	}
}
func NewBtreeWithBufferPool(spaceId uint32, rootPageNo uint32, indexName string,
	indexSegments Segment, dataSegments Segment,
	rootIndex *Index,
	bufferPool *buffer_pool.BufferPool, internalTuple tuple.TableRowTuple, leafTuple tuple.TableRowTuple) *BTree {
	return &BTree{
		spaceId:       spaceId,
		rootPageNo:    rootPageNo,
		indexName:     indexName,
		rootPage:      rootIndex,
		indexSegment:  indexSegments,
		dataSegment:   dataSegments,
		pageIndexes:   0,
		BufferPool:    bufferPool,
		internalTuple: internalTuple,
		leafTuple:     leafTuple,
		IsInit:        false,
	}
}

func (self *BTree) do(pageNumber uint32, internalDo func(page *Index) error, leafDo func(page *Index) error) error {
	var leafOrInternal string
	var index *Index

	if self.IsInit {
		return self.blockFile.Do(pageNumber, func(bytes []byte) error {
			infimumSupremum := bytes[38+56 : 38+56+26]
			flags := util.ConvertByte2BitsString(infimumSupremum[0])[3]
			if flags == common.PAGE_INTERNAL {
				index = NewPageIndexByLoadBytesWithTuple(bytes, self.internalTuple).(*Index)
				return internalDo(index)
			} else {
				index = NewPageIndexByLoadBytesWithTuple(bytes, self.leafTuple).(*Index)
				return leafDo(index)
			}
		})
	}

	//反序列化page页面
	//
	bufferBlock := self.BufferPool.GetPageBlock(self.spaceId, pageNumber)
	bytes := *bufferBlock.Frame
	filePageTypeBytes := bytes[24:26]
	filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
	if filePageType == common.FILE_PAGE_INDEX {

		leafOrInternal = self.getCurrentPageType(bytes, leafOrInternal)
		if leafOrInternal == common.PAGE_INTERNAL {
			index = NewPageIndexByLoadBytesWithTuple(bytes, self.internalTuple).(*Index)
			return internalDo(index)
		} else {
			index = NewPageIndexByLoadBytesWithTuple(bytes, self.leafTuple).(*Index)
			return leafDo(index)
		}
	}
	return nil
}

func (self *BTree) firstKey(pageNumber uint32, do func(key basic.Value) error) error {
	return self.do(
		pageNumber,
		func(internal *Index) error {
			firstKey, _ := internal.GetRowByIndex(0)
			return do(firstKey.GetPrimaryKey())
		},
		func(leaf *Index) error {
			firstKey, _ := leaf.GetRowByIndex(0)
			return do(firstKey.GetPrimaryKey())
		},
	)
}

//func (t *BTree) doKeyAt(pageNumber uint32,do func(key innodb.Value) error) error  {
//	return do(pageNumber)
//}
func (self *BTree) doInternal(pageNumber uint32, do func(index *Index) error) error {
	return self.do(pageNumber, do, func(internal *Index) error {
		fmt.Println("=============")
		return nil
	})
}
func (self *BTree) doLeaf(pageNumber uint32, do func(index *Index) error) error {
	return self.do(pageNumber, func(internal *Index) error {
		fmt.Println("=============")
		return errors.Errorf("Unexpected internal node")
	}, do)
}

func (self *BTree) _rangeUnsafe(bi bpt_iterator) (kvi basic.Iterator, err error) {
	kvi = func() (pageNo uint32, key basic.Value, value basic.Row, e error, it basic.Iterator) {
		var a uint32 //页面号码
		var i int    //第几个记录
		a, i, err, bi = bi()
		if err != nil {
			return 0, nil, nil, err, nil
		}
		if bi == nil {
			return 0, nil, nil, nil, nil
		}
		err = self.doKV(a, i, func(k basic.Value, v basic.Row) error {
			key = k
			value = v
			return nil
		})
		if err != nil {
			return 0, nil, nil, err, nil
		}
		return pageNo, key, value, nil, kvi
	}
	return kvi, nil
}

func (self *BTree) _range(bi bpt_iterator) (kvi basic.Iterator, err error) {
	unsafeKvi, err := self._rangeUnsafe(bi)
	if err != nil {
		return nil, err
	}

	kvi = func() (pageNo uint32, key basic.Value, value basic.Row, e error, it basic.Iterator) {
		var k basic.Value
		var v basic.Row
		pageNo, k, v, err, unsafeKvi = unsafeKvi()
		if err != nil {
			return 0, nil, nil, err, nil
		}
		if unsafeKvi == nil {
			return 0, nil, nil, nil, nil
		}

		return pageNo, k, v, nil, kvi
	}
	return kvi, nil
}

func (self *BTree) rangeIterator(from basic.Value, to basic.Value) (bi bpt_iterator, err error) {
	if from != nil && to == nil {
		bi, err = self.forward(from, to)
	}
	if from == nil && to == nil {
		bi, err = self.backward(from, to)
		return bi, err
	}
	compareValue, _ := from.LessThan(to)
	if !compareValue.Raw().(bool) {
		bi, err = self.forward(from, to)
	} else {
		bi, err = self.backward(from, to)
	}
	return bi, err
}

func (self *BTree) doKV(pageNumber uint32, i int, do func(key basic.Value, value basic.Row) error) (err error) {
	return self.doLeaf(pageNumber, func(n *Index) error {
		if i >= int(len(n.SlotRowData.FullRowList())) {
			return errors.New("Index out of range")
		}
		return n.doKeyAt(i, func(key basic.Value) error {
			return n.doValueAt(i, func(value basic.Row) error {
				return do(key, value)
			})
		})
	})
}

//
func (self *BTree) forward(from, to basic.Value) (bi bpt_iterator, err error) {
	a, i, err := self.getStart(from)
	if err != nil {
		return nil, err
	} else if from == nil {
		return self.forwardFrom(a, i, to)
	}
	var less bool = false
	err = self.doLeaf(a, func(n *Index) error {
		var size = n.GetRecordSize()
		if size == 0 {
			return nil
		}
		return n.doKeyAt(i, func(key basic.Value) error {
			compareValue, compareError := key.LessOrEqual(from)
			if compareError != nil {
				return compareError
			}
			less = !compareValue.Raw().(bool)
			return nil
		})
	})
	if err != nil {
		return nil, err
	} else if less {
		bi = func() (uint32, int, error, bpt_iterator) {
			return 0, 0, nil, nil
		}
		return bi, nil
	}
	return self.forwardFrom(a, i, to)
}

//
func (self *BTree) forwardFrom(a uint32, i int, to basic.Value) (bi bpt_iterator, err error) {
	i--
	bi = func() (uint32, int, error, bpt_iterator) {
		var err error
		var end bool
		a, i, end, err = self.nextLoc(a, i)
		if err != nil {
			return 0, 0, err, nil
		}
		if end {
			return 0, 0, nil, nil
		}
		if to == nil {
			return a, i, nil, bi
		}
		var less bool = false
		err = self.doLeaf(a, func(n *Index) error {
			return n.doKeyAt(i, func(key basic.Value) error {
				compareValue, compareError := key.LessThan(to)
				if compareError != nil {
					return compareError
				}
				less = compareValue.Raw().(bool)
				return nil
			})
		})
		if err != nil {
			return 0, 0, err, nil
		}
		if less {
			return 0, 0, nil, nil
		}
		return a, i, nil, bi
	}
	return bi, nil
}

//
func (self *BTree) backward(from, to basic.Value) (bi bpt_iterator, err error) {
	a, i, err := self.getEnd(from)
	if err != nil {
		return nil, err
	} else if from == nil {
		return self.backwardFrom(a, i, to)
	}
	var greater bool = false
	err = self.doLeaf(a, func(n *Index) error {
		var size = n.GetRecordSize()
		if size == 0 {
			return nil
		}
		return n.doKeyAt(i, func(key basic.Value) error {
			compareValue, compareError := key.LessThan(from)
			if compareError != nil {
				return compareError
			}
			greater = compareValue.Raw().(bool)
			return nil
		})
	})
	if err != nil {
		return nil, err
	} else if greater {
		bi = func() (uint32, int, error, bpt_iterator) {
			return 0, 0, nil, nil
		}
		return bi, nil
	}
	return self.backwardFrom(a, i, to)
}

//
func (self *BTree) backwardFrom(a uint32, i int, to basic.Value) (bi bpt_iterator, err error) {
	i++
	bi = func() (uint32, int, error, bpt_iterator) {
		var err error
		var end bool
		a, i, end, err = self.prevLoc(a, i)
		if err != nil {
			return 0, 0, err, nil
		}
		if end {
			return a, i, nil, bi
		}
		if to == nil {
			return a, i, nil, bi
		}
		var more bool = false
		err = self.doLeaf(a, func(n *Index) error {
			//return n.doKeyAt(self.varchar, i, func(k []byte) error {
			//	more = bytes.Compare(to, k) > 0
			//	return nil
			//})
			return n.doKeyAt(i, func(key basic.Value) error {
				moreValue, err := to.LessThan(key)
				if err != nil {
					return err
				}
				more = moreValue.Raw().(bool)
				return nil
			})
		})
		if err != nil {
			return 0, 0, err, nil
		}
		if more {
			return 0, 0, nil, nil
		}
		return a, i, nil, bi
	}
	return bi, nil
}

//获取下一个页面号，理论上是连续的
//后面需要加载LRU内的页面
func (self *BTree) nextLoc(pageNo uint32, i int) (uint32, int, bool, error) {
	j := i + 1
	nextBlk := func(pageNo uint32, j int) (uint32, int, bool, error) {
		changed := false
		err := self.doLeaf(pageNo, func(n *Index) error {
			if j >= n.GetRecordSize()-2 && n.GetNextPageNo() != 0 {
				pageNo = n.GetNextPageNo()
				j = 0
				changed = true
			}
			return nil
		})
		if err != nil {
			return 0, 0, false, err
		}
		return pageNo, j, changed, nil
	}
	var changed bool = true
	var err error = nil
	for changed {
		pageNo, j, changed, err = nextBlk(pageNo, j)
		if err != nil {
			return 0, 0, false, err
		}
	}
	var end bool = false
	err = self.doLeaf(pageNo, func(n *Index) error {
		if j >= n.GetRecordSize()-2 {
			end = true
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, err
	}
	return pageNo, j, end, nil
}

//获取下一个页面号，理论上是连续的
//后面需要加载LRU内的页面
func (self *BTree) prevLoc(pageNo uint32, i int) (uint32, int, bool, error) {
	j := i - 1
	prevBlk := func(pageNo uint32, j int) (uint32, int, bool, error) {
		changed := false
		err := self.doLeaf(pageNo, func(n *Index) error {
			if j < 0 && n.GetPrePageNo() != 0 {
				pageNo = n.GetPrePageNo()
				changed = true
				return self.doLeaf(pageNo, func(m *Index) error {
					j = m.GetRecordSize() - 1
					return nil
				})
			}
			return nil
		})
		if err != nil {
			return 0, 0, false, err
		}
		return pageNo, j, changed, nil
	}
	var changed bool = true
	var err error = nil
	for changed {
		pageNo, j, changed, err = prevBlk(pageNo, j)
		if err != nil {
			return 0, 0, false, err
		}
	}
	var end bool = false
	err = self.doLeaf(pageNo, func(n *Index) error {
		if j < 0 || j > n.GetRecordSize() {
			end = true
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, err
	}
	return pageNo, j, end, nil
}

/* returns the (addr, idx) of the leaf block and the index of the key in
* the block which is either the first key greater than the search key
* or the last key equal to the search key.
 */
func (self *BTree) getEnd(key basic.Value) (pageNo uint32, i int, err error) {
	return self._getEnd(self.rootPageNo, key)
}

func (self *BTree) _getEnd(root uint32, key basic.Value) (pageNo uint32, i int, err error) {
	if key == nil {
		pageNo, i, err = self.lastKey(root)
	} else {
		pageNo, i, err = self._getStart(root, key)
	}
	if err != nil {
		return 0, 0, err
	}
	var equal bool = true
	for equal {
		b, j, end, err := self.nextLoc(pageNo, i)
		if err != nil {
			return 0, 0, err
		}
		if end {
			return pageNo, i, nil
		}
		err = self.doLeaf(b, func(n *Index) (err error) {
			return n.doKeyAt(j, func(keyTwo basic.Value) error {
				equalValue, cmpErr := key.Equal(keyTwo)
				if cmpErr != nil {
					return cmpErr
				}
				equal = equalValue.Raw().(bool)
				return nil
			})
		})
		if err != nil {
			return 0, 0, err
		}
		if equal {
			pageNo, i = b, j
		}
	}
	return pageNo, i, err
}

/* returns the (addr, idx) of the leaf block and the index of the key in
* the block which has a key greater or equal to the search key.
 */
func (self *BTree) getStart(key basic.Value) (pageNo uint32, i int, err error) {
	return self._getStart(self.rootPageNo, key)
}

func (self *BTree) _getStart(n uint32, key basic.Value) (pageNo uint32, i int, err error) {
	var leafOrInternal string

	bufferBlock := self.BufferPool.GetPageBlock(self.spaceId, n)
	bytes := *bufferBlock.Frame
	filePageTypeBytes := bytes[24:26]
	filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
	if filePageType == common.FILE_PAGE_INDEX {
		infimumSupremum := bytes[38+56 : 38+56+26]
		flags := util.ConvertByte2BitsString(infimumSupremum[0])[3]
		if flags == common.PAGE_LEAF {
			leafOrInternal = common.PAGE_LEAF
		} else {
			leafOrInternal = common.PAGE_INTERNAL
		}
	}

	if leafOrInternal != common.PAGE_LEAF {
		return self.internalGetStart(n, key)
	} else {
		return self.leafGetStart(n, key, false, 0)
	}

}

//非叶子节点的查找，返回当前key的下标，查找到key所在的页面号
//@param n pageNo
//@param key 查找key

func (self *BTree) internalGetStart(n uint32, key basic.Value) (pageNo uint32, i int, err error) {
	var kid uint32
	err = self.doInternal(n, func(nIndex *Index) error {
		currentRow, _ := nIndex.FindByKey(key)
		kid = currentRow.GetPageNumber()
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return self._getStart(kid, key)
}

//叶子页面的查找
//
func (self *BTree) leafGetStart(n uint32, key basic.Value, stop bool, end uint32) (pageNo uint32, i int, err error) {
	if key == nil {
		return n, 0, nil
	}
	if stop && n == end {
		return 0, 0, errors.Errorf("hit end %v %v %v", n, end, key)
	}
	var next uint32 = 0
	err = self.doLeaf(n, func(nIndex *Index) (err error) {
		if nIndex.GetRecordSize() == 0 {
			return nil
		}
		nIndex.FindByKey(key)

		//var has bool
		//
		//currentRow,has:=nIndex.FindByKey(key)
		//if has {
		//
		//}
		//i, has, err = self.find(self.varchar, n, key)
		//if err != nil {
		//	return err
		//}
		//if i >= int(n.meta.keyCount) && i > 0 {
		//	i = int(n.meta.keyCount) - 1
		//}
		//return n.doKeyAt(self.varchar, i, func(k []byte) error {
		//	if !has && n.meta.next != 0 && bytes.Compare(k, key) < 0 {
		//		next = n.meta.next
		//	}
		//	return nil
		//})
		//下一个关联页面号码
		next = nIndex.GetNextPageNo()
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if next != 0 {
		return self.leafGetStart(next, key, stop, end)
	}
	return n, i + 1, nil
}

func (self *BTree) lastKey(n uint32) (pageNo uint32, i int, err error) {
	//获取最后一个页面
	var leafOrInternal string

	bufferBlock := self.BufferPool.GetPageBlock(self.spaceId, n)
	bytes := *bufferBlock.Frame
	filePageTypeBytes := bytes[24:26]
	filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
	if filePageType == common.FILE_PAGE_INDEX {
		infimumSupremum := bytes[38+56 : 38+56+26]
		flags := util.ConvertByte2BitsString(infimumSupremum[0])[3]
		if flags == common.PAGE_LEAF {
			leafOrInternal = common.PAGE_LEAF
		} else {
			leafOrInternal = common.PAGE_INTERNAL
		}
	}

	if leafOrInternal == common.PAGE_INTERNAL {
		return self.internalLastKey(n)
	} else {
		return self.leafLastKey(n)
	}

}

//
func (self *BTree) internalLastKey(n uint32) (a uint32, i int, err error) {
	var kid uint32
	err = self.doInternal(n, func(nIndex *Index) error {

		currentRows, found := nIndex.GetRowByIndex(nIndex.GetRecordSize() - 1)

		if found {
			kid = currentRows.GetPageNumber()
		}

		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return self.lastKey(kid)
}

func (self *BTree) leafLastKey(n uint32) (a uint32, i int, err error) {
	var next uint32 = 0
	err = self.doLeaf(n, func(nIndex *Index) (err error) {
		//if n.meta.keyCount == 0 {
		//	// this happens when the tree is empty!
		//	return nil
		//}
		//i = int(n.meta.keyCount) - 1
		//return n.doKeyAt(self.varchar, i, func(k []byte) error {
		//	if n.meta.next != 0 {
		//		next = n.meta.next
		//	}
		//	return nil
		//})
		if nIndex.GetRecordSize() == 0 {
			return nil
		}
		i = nIndex.GetRecordSize()
		if nIndex.GetNextPageNo() != 0 {
			next = nIndex.GetNextPageNo()
		}

		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if next != 0 {
		return self.leafLastKey(next)
	}
	return n, i, nil
}

func (self *BTree) doKey(a uint32, i int, do func(key basic.Value) error) (err error) {
	return self.do(
		a,
		func(n *Index) error {
			if i >= n.GetRecordSize() {
				return errors.Errorf("Index out of range")
			}
			return n.doKeyAt(i, func(key basic.Value) error {
				return do(key)
			})
		},
		func(n *Index) error {
			if i >= n.GetRecordSize() {
				return errors.Errorf("Index out of range")
			}
			return n.doKeyAt(i, func(key basic.Value) error {
				return do(key)
			})
		},
	)
}
