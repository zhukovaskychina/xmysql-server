package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

var createTableLikePattern = regexp.MustCompile(`(?is)^\s*create\s+table\s+(if\s+not\s+exists\s+)?((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+like\s+((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s*;?\s*$`)

func (e *XMySQLExecutor) executeRawCreateTableLikeCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	match := createTableLikePattern.FindStringSubmatch(strings.TrimSpace(query))
	if len(match) == 0 {
		return false, nil
	}
	targetDB, targetTable := compatibilityQualifiedTable(match[2], databaseName)
	sourceDB, sourceTable := compatibilityQualifiedTable(match[3], databaseName)
	if targetDB == "" || targetTable == "" || sourceDB == "" || sourceTable == "" {
		return true, fmt.Errorf("CREATE TABLE LIKE requires source and target tables")
	}
	if err := e.validateDatabaseExists(targetDB); err != nil {
		return true, err
	}
	if err := e.validateDatabaseExists(sourceDB); err != nil {
		return true, err
	}
	if exists, err := e.checkTableExists(targetDB, targetTable); err != nil {
		return true, err
	} else if exists {
		if strings.TrimSpace(match[1]) != "" {
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' already exists", targetTable)}
			return true, nil
		}
		return true, fmt.Errorf("table '%s.%s' already exists", targetDB, targetTable)
	}
	definition, err := e.loadCreateTableDefinition(sourceDB, sourceTable)
	if err != nil {
		return true, err
	}
	open := strings.Index(definition, "(")
	if open < 0 {
		return true, fmt.Errorf("source table '%s.%s' has no CREATE TABLE definition", sourceDB, sourceTable)
	}
	cloneSQL := "create table `" + strings.ReplaceAll(targetTable, "`", "``") + "` " + strings.TrimSpace(definition[open:])
	stmt, err := sqlparser.Parse(cloneSQL)
	if err != nil {
		return true, fmt.Errorf("parse CREATE TABLE LIKE definition: %w", err)
	}
	createStmt, ok := stmt.(*sqlparser.DDL)
	if !ok {
		return true, fmt.Errorf("CREATE TABLE LIKE definition did not produce CREATE TABLE AST")
	}
	originalQuery := ctx.RawQuery
	originalDatabase := ctx.DatabaseName
	ctx.RawQuery = cloneSQL
	ctx.DatabaseName = targetDB
	e.executeCreateTableStatement(ctx, targetDB, createStmt)
	ctx.RawQuery = originalQuery
	ctx.DatabaseName = originalDatabase
	return true, nil
}

func compatibilityQualifiedTable(raw, defaultDB string) (database, table string) {
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(raw), "`", ""), ".")
	if len(parts) == 1 {
		return defaultDB, strings.TrimSpace(parts[0])
	}
	return strings.TrimSpace(parts[len(parts)-2]), strings.TrimSpace(parts[len(parts)-1])
}
