package store

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/tuple"
	"xmysql-server/util"
)

func TestIndexAllFuncs(t *testing.T) {
	t.Parallel()
	t.Run("IndexAddRows", func(t *testing.T) {

		tuple := NewSysTableTuple()
		index := NewPageIndexWithTuple(10, 0, tuple).(*Index)
		currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
		initSysTableRow("test", tuple, currentSysTableRow)
		index.AddRow(currentSysTableRow)
		assert.Equal(t, index.GetRecordSize(), 1)

		content := index.ToByte()

		assert.Equal(t, len(content), 16384)
		//currentContent := NewPageIndexByLoadBytes(content)
		currentContent := NewPageIndexByLoadBytesWithTuple(content, tuple).(*Index)
		assert.Equal(t, currentContent.GetPageNumber(), index.GetPageNumber())

		assert.Equal(t, currentContent.IndexPage.PageDirectory, index.IndexPage.PageDirectory)
	})

	t.Run("增加十条记录", func(t *testing.T) {
		tuple := NewSysTableTuple()
		index := NewPageIndexWithTuple(10, 0, tuple).(*Index)
		currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
		initSysTableRow("test", tuple, currentSysTableRow)

		for i := 0; i < 10; i++ {
			index.AddRow(currentSysTableRow)
		}

		size := index.GetRecordSize()
		assert.Equal(t, size, 10)
	})
	t.Run("增加百条记录", func(t *testing.T) {
		tuple := NewSysTableTuple()
		index := NewPageIndexWithTuple(10, 0, tuple).(*Index)
		currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
		initSysTableRow("test", tuple, currentSysTableRow)

		for i := 0; i < 100; i++ {
			index.AddRow(currentSysTableRow)
		}

		size := index.GetRecordSize()
		assert.Equal(t, size, 100)
		assert.Equal(t, len(index.ToByte()), common.PAGE_SIZE)
	})
	t.Run("增加2百条记录", func(t *testing.T) {
		tuple := NewSysTableTuple()
		index := NewPageIndexWithTuple(10, 0, tuple).(*Index)
		currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
		initSysTableRow("test", tuple, currentSysTableRow)

		for i := 0; i < 200; i++ {
			if !index.IsFull(currentSysTableRow) {
				index.AddRow(currentSysTableRow)
			}
		}

		size := index.GetRecordSize()
		assert.Equal(t, size, 140)
		assert.Equal(t, len(index.ToByte()), common.PAGE_SIZE)
	})
	t.Run("增加2百条记录", func(t *testing.T) {
		tuple := NewSysTableTuple()
		index := NewPageIndexWithTuple(10, 0, tuple).(*Index)

		for i := 0; i < 200; i++ {
			if i == 7 {
				fmt.Println(i)
			}
			//tableName
			currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
			initSysTableRowForRange("test", "IP_PAGE_SIZE"+strconv.Itoa(i), tuple, currentSysTableRow)
			if !index.IsFull(currentSysTableRow) {
				index.AddRow(currentSysTableRow)
			} else {

			}

		}

		size := index.GetRecordSize()
		assert.Equal(t, len(index.ToByte()), common.PAGE_SIZE)
		assert.Equal(t, size, 144)

		//反序列化记录
		row, _ := index.FindByKey(basic.NewVarcharVal([]byte("IP_PAGE_SIZE" + strconv.Itoa(99))))
		fmt.Println(row.ToString())

	})
}

func TestSerializeIndex(t *testing.T) {
	t.Parallel()
	t.Run("序列化行", func(t *testing.T) {
		sysColumnLeafTuple := NewSysColumnsTuple()
		tuple := NewSysSpacesTuple()
		index := NewPageIndexWithTuple(10, 0, sysColumnLeafTuple).(*Index)

		rows := wrapperSysColumns(tuple, 10)
		for _, row := range rows {
			fmt.Println(row.ToString())
		}

		fmt.Println("==========insert before==========")
		index.AddRows(rows)
		assert.Equal(t, index.GetRecordSize(), 11)
		assert.Equal(t, index.GetSlotNDirs(), 3)
		fmt.Println("==========insert after==========")
		serializeBytes := index.ToByte()
		fmt.Println("==========deserialize before==========")
		indexPages := NewPageIndexByLoadBytesWithTuple(serializeBytes, sysColumnLeafTuple).(*Index)
		assert.Equal(t, indexPages.GetRecordSize(), 11)
		assert.Equal(t, indexPages.GetSlotNDirs(), 3)
		assert.Equal(t, indexPages.getSlotDirs(), index.getSlotDirs())
		fmt.Println("==========deserialize after==========")

		for _, row := range indexPages.SlotRowData.GetRowListWithoutInfiuAndSupremum() {
			fmt.Println(row.ToString())
		}

	})
}

func wrapperSysColumns(tuple tuple.TableRowTuple, currentTableId uint64) []basic.Row {
	columnlength := tuple.GetColumnLength()
	sysColumnLeafTuple := NewSysColumnsTuple()
	var resultPrepareInsertRows = make([]basic.Row, 0)
	for i := 0; i < columnlength; i++ {

		currentColumn := tuple.GetColumnInfos(byte(i))

		isHidden := currentColumn.IsHidden
		if isHidden {
			continue
		}

		currentColumnTableRow := NewClusterSysIndexLeafRow(sysColumnLeafTuple, false)

		//
		//rowId
		//currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
		//transaction_id
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(0), 0)
		//rowpointer
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(0), 1)
		//tableId
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(currentTableId), 2)

		//name
		currentColumnTableRow.WriteBytesWithNullWithsPos([]byte(currentColumn.FieldName), 3)

		//pos
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(i)), 4)

		//mtype
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 5)

		//prtype
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 6)

		//len
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 7)

		resultPrepareInsertRows = append(resultPrepareInsertRows, currentColumnTableRow)
	}
	return resultPrepareInsertRows
}

func initSysTableRowForRange(databaseName string, tableName string, tuple tuple.TableRowTuple, currentSysTableRow basic.Row) {
	//rowId
	//currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 0)
	//transaction_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 0)
	//rowpointer
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 1)
	//tableId
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 2)
	//tableName
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte(databaseName+"/"+tableName), 3)
	//flag
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte{0, 0, 0, 0, 0, 0, 0, 0}, 4)
	//N_COLS
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(uint64(uint32(tuple.GetColumnLength()))), 5)

	//space_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 6)

	//FileFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Antelope"), 7)
	//RowFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Redundant"), 8)
	//ZipPageSize
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(0), 9)
	//SpaceType
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("space"), 10)

}

func initSysTableRow(databaseName string, tuple tuple.TableRowTuple, currentSysTableRow basic.Row) {
	//rowId
	//currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 0)
	//transaction_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 0)
	//rowpointer
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 1)
	//tableId
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 2)
	//tableName
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte(databaseName+"/"+tuple.GetTableName()), 3)
	//flag
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte{0, 0, 0, 0, 0, 0, 0, 0}, 4)
	//N_COLS
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(uint64(uint32(tuple.GetColumnLength()))), 5)

	//space_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(1), 6)

	//FileFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Antelope"), 7)
	//RowFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Redundant"), 8)
	//ZipPageSize
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(0), 9)
	//SpaceType
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("space"), 10)

	fmt.Println(currentSysTableRow.ToByte())

}
