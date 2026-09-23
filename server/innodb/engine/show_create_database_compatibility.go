package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

var showCreateDatabasePattern = regexp.MustCompile(`(?is)^\s*show\s+create\s+(?:database|schema)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*;?\s*$`)

func (e *XMySQLExecutor) executeShowCreateDatabaseCompatibility(ctx *ExecutionContext, query string) (bool, error) {
	match := showCreateDatabasePattern.FindStringSubmatch(strings.TrimSpace(query))
	if len(match) == 0 {
		return false, nil
	}
	databaseName := match[1]
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
