package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSpecialRow_SetTransactionIdNoop(t *testing.T) {
	row := NewInfimumRow()
	origin := append([]byte(nil), row.ToByte()...)

	row.SetTransactionId(123)

	assert.Equal(t, origin, row.ToByte())
}
