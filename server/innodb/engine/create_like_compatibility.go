package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func (e *XMySQLExecutor) executeRawCreateTableLikeCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false, nil
	}
	stmt, ok := parsed.AST.(*sqlparser.TableDDLStatement)
	if !ok || !strings.EqualFold(stmt.Action, "create") || stmt.Like.IsEmpty() || len(stmt.Targets) != 1 {
		return false, nil
	}
	target := stmt.Targets[0]
	targetRaw := target.Name.String()
	if !target.Qualifier.IsEmpty() {
		targetRaw = target.Qualifier.String() + "." + target.Name.String()
	}
	sourceRaw := stmt.Like.Name.String()
	if !stmt.Like.Qualifier.IsEmpty() {
		sourceRaw = stmt.Like.Qualifier.String() + "." + stmt.Like.Name.String()
	}
	targetDB, targetTable := compatibilityQualifiedTable(targetRaw, databaseName)
	sourceDB, sourceTable := compatibilityQualifiedTable(sourceRaw, databaseName)
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
		if stmt.IfExists {
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
	parsedDDL, err := sqlparser.Parse(cloneSQL)
	if err != nil {
		return true, fmt.Errorf("parse CREATE TABLE LIKE definition: %w", err)
	}
	createStmt, ok := parsedDDL.(*sqlparser.DDL)
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
