package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestInsertOperator_IsDuplicateKeyError_StructuredErrors(t *testing.T) {
	op := &InsertOperator{}

	assert.True(t, op.isDuplicateKeyError(basic.ErrDuplicateKey))
	assert.True(t, op.isDuplicateKeyError(fmt.Errorf("%w: %v", basic.ErrDuplicateKey, errors.New("insert failed"))))
	assert.True(t, op.isDuplicateKeyError(common.NewErr(common.ErrDupEntry, "1", "users.PRIMARY")))
}

func TestInsertOperator_IsDuplicateKeyError_AvoidsFalsePositiveByMessage(t *testing.T) {
	op := &InsertOperator{}

	assert.False(t, op.isDuplicateKeyError(errors.New("duplicate job execution in scheduler")))
	assert.False(t, op.isDuplicateKeyError(common.NewErr(common.ErrUnknown, "duplicate job execution")))
}
