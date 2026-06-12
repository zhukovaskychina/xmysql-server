package manager

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleRow_SetTransactionId(t *testing.T) {
	row := &SimpleRow{data: []byte{1, 2, 3, 4, 5}}

	row.SetTransactionId(12345)

	assert.Len(t, row.data, 13)
	assert.Equal(t, byte(1), row.data[0])
	assert.Equal(t, byte(2), row.data[1])
	assert.Equal(t, byte(3), row.data[2])
	assert.Equal(t, byte(4), row.data[3])
	assert.Equal(t, byte(5), row.data[4])
	assert.Equal(t, uint64(12345), binary.LittleEndian.Uint64(row.data[5:13]))
}
