package table

import (
	"fmt"
	"testing"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestDDL(t *testing.T) {
	sql := "CREATE TABLE tb_emp1 " +
		"  (    id INT(11),    name VARCHAR(25),    deptId INT(11),    salary FLOAT    );"

	stmt, _ := sqlparser.Parse(sql)
	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		{
			fmt.Println(stmt.Action)
			fmt.Println(stmt.NewName)
		}

	}
}
