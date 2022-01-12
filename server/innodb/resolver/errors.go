package resolver

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/terror"
)

// Optimizer error codes.
const (
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6
)

// Optimizer base errors.
var (
	ErrInvalidGroupFuncUse = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference    = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
)
