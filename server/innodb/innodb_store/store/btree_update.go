package store

import (
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/util"
)

func (self *BTree) update(n uint32, key basic.Value, value basic.Row) (a, b uint32, err error) {
	var leafOrInternal string
	err = self.blockFile.Do(n, func(content []byte) error {
		//此处需要判断是否index
		if util.ReadUB2Byte2Int(content[24:26]) == common.FILE_PAGE_INDEX {
			//判断是叶子还是非叶子
			currentIndex := NewPageIndexByLoadBytes(content).(*Index)
			if currentIndex.PageLeafOrInternal() == common.PAGE_LEAF {
				leafOrInternal = common.PAGE_LEAF
			} else {
				leafOrInternal = common.PAGE_INTERNAL
			}
			return nil
		} else {
			return errors.New("非Index页面")
		}
	})
	if err != nil {
		return 0, 0, err
	}
	if leafOrInternal == common.PAGE_INTERNAL {
		return self.internalInsert(n, key, value)
	} else {
		return self.leafInsert(n, key, value)
	}
}
