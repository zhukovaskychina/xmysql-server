package store

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewClusterLeafRow(t *testing.T) {
	tuple := NewSysTableTuple()
	currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)

	currentSysTableRow.GetNextRowOffset()

	initSysTableRow("test", tuple, currentSysTableRow)

	assert.Equal(t, currentSysTableRow.GetFieldLength(), 12)

}

func TestVarcharContentSize(t *testing.T) {
	tuple := NewSysTableTuple()
	currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)

	currentSysTableRow.GetNextRowOffset()

	initSysTableRow("test", tuple, currentSysTableRow)

	varSize := len(tuple.GetVarColumns())

	assert.Equal(t, varSize, 3)
}

func TestSerializeRow(t *testing.T) {
	tuple := NewSysTableTuple()
	currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)

	initSysTableRow("test", tuple, currentSysTableRow)

	fmt.Println(currentSysTableRow.ToByte())

	currentRow := NewClusterSysIndexLeafRowWithContent(currentSysTableRow.ToByte(), tuple)

	assert.Equal(t, currentRow.GetRowLength(), currentSysTableRow.GetRowLength())
}

func TestVarcharContent(t *testing.T) {
	tuple := NewSysTableTuple()
	currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)

	initSysTableRow("test", tuple, currentSysTableRow)

	varSize := len(tuple.GetVarColumns())

	assert.Equal(t, varSize, 3)

	currentRow := NewClusterSysIndexLeafRowWithContent(currentSysTableRow.ToByte(), tuple)

	assert.Equal(t, currentRow.GetRowLength(), currentSysTableRow.GetRowLength())

}

func TestSetValueLengthByIndex(t *testing.T) {
	tuple := NewSysTableTuple()
	currentSysTableRow := NewClusterSysIndexLeafRow(tuple, false)
	initSysTableRow("test", tuple, currentSysTableRow)

	currentRow := NewClusterSysIndexLeafRowWithContent(currentSysTableRow.ToByte(), tuple)

	row := currentRow.(*ClusterSysIndexLeafRow)

	clr := row.header.(*ClusterLeafRowHeader)
	assert.Equal(t, currentRow.GetRowLength(), currentSysTableRow.GetRowLength())

	assert.Equal(t, len(clr.NullContent), 2)

	//assert.Equal(t, binary.BigEndian.Uint16(clr.NullContent),uint16(0))

}
