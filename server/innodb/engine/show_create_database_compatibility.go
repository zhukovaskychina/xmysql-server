package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func (e *XMySQLExecutor) executeShowCreateDatabaseCompatibility(ctx *ExecutionContext, query string) (bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false, nil
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (!strings.EqualFold(show.Type, "create database") && !strings.EqualFold(show.Type, "create schema")) || show.OnTable.IsEmpty() {
		return false, nil
	}
	databaseName := show.OnTable.Name.String()
	if err := e.validateDatabaseExists(databaseName); err != nil {
		return true, err
	}
	if !e.schemaMetadataVisibleToSession(ctx.Session, databaseName) {
		return true, fmt.Errorf("access denied: user lacks privilege to inspect database '%s'", databaseName)
	}
	createSQL := fmt.Sprintf("CREATE DATABASE `%s`", strings.ReplaceAll(databaseName, "`", "``"))
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data: map[string]interface{}{
			"columns": []string{"Database", "Create Database"},
			"rows":    [][]interface{}{{databaseName, createSQL}},
		},
		Message: "SHOW CREATE DATABASE executed successfully",
	}
	return true, nil
}
