package store

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewInfimumRow(t *testing.T) {

	infimumRow := NewInfimumRow()

	assert.Equal(t, infimumRow.GetNOwned(), byte(0), "最小值所属属性为0")

	assert.Equal(t, infimumRow.GetRowLength(), 13, "最小值长度为13")

}

func TestNewSupremumRow(t *testing.T) {
	supremumRow := NewSupremumRow()

	assert.Less(t, supremumRow.GetNOwned(), byte(8), "最小值所属属性为0")

	supremumRow.SetNOwned(10)
	assert.Equal(t, supremumRow.GetRowLength(), 13, "最小值长度为13")
}

func TestSupremumRow_GetNOwned(t *testing.T) {
	supremumRow := NewSupremumRow()
	supremumRow.SetNOwned(8)

	assert.Equal(t, supremumRow.GetNOwned(), 8)
}

func TestSupremumRow_GetFieldLength(t *testing.T) {

	supremumRow := NewSupremumRow()
	assert.Equal(t, supremumRow.GetRowLength(), 13)
}

func TestSupremumRow_GetHeapNo(t *testing.T) {
	supremumRow := NewSupremumRow()

	assert.Equal(t, int(supremumRow.GetHeapNo()), 0)

	supremumRow.SetHeapNo(128)
	fmt.Println(supremumRow.GetHeapNo())
	assert.Equal(t, int(supremumRow.GetHeapNo()), 128)
}

func TestInfimumRow_GetHeapNo(t *testing.T) {
	supremumRow := NewInfimumRow()

	assert.Equal(t, int(supremumRow.GetHeapNo()), 0)

	supremumRow.SetHeapNo(128)
	fmt.Println(supremumRow.GetHeapNo())
	assert.Equal(t, int(supremumRow.GetHeapNo()), 128)
}

func TestInfimumRow_GetNextRowOffset(t *testing.T) {
	infimumRow := NewInfimumRow()

	assert.Equal(t, int(infimumRow.GetNextRowOffset()), 0)

	infimumRow.SetNextRowOffset(1280)
	fmt.Println(infimumRow.GetNextRowOffset())
	assert.Equal(t, int(infimumRow.GetNextRowOffset()), 1280)

}

func TestSupremumRow_GetNextRowOffset(t *testing.T) {
	infimumRow := NewSupremumRow()

	assert.Equal(t, int(infimumRow.GetNextRowOffset()), 0)

	infimumRow.SetNextRowOffset(1280)
	fmt.Println(infimumRow.GetNextRowOffset())
	assert.Equal(t, int(infimumRow.GetNextRowOffset()), 1280)

}
