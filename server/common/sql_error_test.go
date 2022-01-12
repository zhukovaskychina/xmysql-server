package common

import (
	"fmt"
	"testing"
)

func TestErr(t *testing.T) {
	fmt.Println(NewErr(ErrNoSuchTable, "mysql", "student").Error())
	fmt.Println(NewErr(ErrNotSupportedYet, "子查询", "目前暂时不支持子查询").Error())

	fmt.Println(NewErr(ErrSyntax))
}
