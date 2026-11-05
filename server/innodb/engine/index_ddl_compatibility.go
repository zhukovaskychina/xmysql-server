package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// standaloneCreateIndexPattern preserves the narrow test/compatibility API
// while delegating matching to the IndexDDL AST instead of a regexp.
var standaloneCreateIndexPattern standaloneCreateIndexMatcher

type standaloneCreateIndexMatcher struct{}

func (standaloneCreateIndexMatcher) FindStringSubmatch(query string) []string {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil || parsed == nil {
		return nil
	}
	statement, ok := parsed.AST.(*sqlparser.IndexDDLStatement)
	if !ok || !strings.EqualFold(statement.Action, "create") || statement.FullText {
		return nil
	}
	schema := statement.Table.Qualifier.String()
	unique, ifNotExists := "", ""
	if statement.Unique {
		unique = "unique"
	}
	if statement.IfNotExists {
		ifNotExists = "if not exists"
	}
	return []string{"", unique, ifNotExists, statement.Name, schema, statement.Table.Name.String(), strings.Join(statement.Columns, ","), statement.Visibility}
}

// executeRawStandaloneIndexCompatibility handles CREATE/DROP INDEX before
// the legacy yacc parser, whose DDL AST only models the ALTER TABLE form.
// Both operations reuse the durable ALTER index implementation so metadata,
// row index rebuilding, and foreign-key validation remain identical.
func (e *XMySQLExecutor) executeRawStandaloneIndexCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	parsed, parseErr := sqlparser.ParseForExecution(trimmed)
	if parseErr != nil || parsed == nil {
		return false, nil
	}
	statement, ok := parsed.AST.(*sqlparser.IndexDDLStatement)
	if !ok {
		return false, nil
	}
	schema := trimIndexIdentifier(statement.Table.Qualifier.String())
	if schema == "" {
		schema = strings.TrimSpace(databaseName)
	}
	table := trimIndexIdentifier(statement.Table.Name.String())
	indexName := trimIndexIdentifier(statement.Name)
	columns := strings.Join(statement.Columns, ", ")
	if schema == "" || table == "" || indexName == "" {
		return true, fmt.Errorf("%s INDEX requires database, table, index name, and columns", strings.ToUpper(statement.Action))
	}
	if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
		return true, err
	}
	if strings.EqualFold(statement.Action, "create") {
		if columns == "" {
			return true, fmt.Errorf("CREATE INDEX requires columns")
		}
		kind := "index"
		if statement.Unique {
			kind = "unique index"
		}
		if statement.FullText {
			kind = "fulltext index"
		}
		visibilityClause := ""
		if statement.Visibility != "" {
			visibilityClause = " " + statement.Visibility
		}
		translated := fmt.Sprintf("alter table `%s`.`%s` add %s `%s` (%s)%s", schema, table, kind, indexName, columns, visibilityClause)
		_, err := e.alterTableIndexDDL(schema, table, translated, sessionForeignKeyChecksEnabled(sessionFromExecutionContext(ctx)))
		if statement.IfNotExists && err != nil && strings.Contains(strings.ToLower(err.Error()), "duplicate index") {
			return true, nil
		}
		return true, err
	}
	translated := fmt.Sprintf("alter table `%s`.`%s` drop index `%s`", schema, table, indexName)
	_, err := e.alterTableIndexDDL(schema, table, translated, sessionForeignKeyChecksEnabled(sessionFromExecutionContext(ctx)))
	if statement.IfExists && err != nil && strings.Contains(strings.ToLower(err.Error()), "index '") && strings.Contains(strings.ToLower(err.Error()), "does not exist") {
		return true, nil
	}
	return true, err
	/*
		if match := standaloneCreateFullTextIndexPattern.FindStringSubmatch(trimmed); len(match) == 6 {
			ignoreExisting := strings.TrimSpace(match[1]) != ""
			indexName := trimIndexIdentifier(match[2])
			schema := trimIndexIdentifier(match[3])
			if schema == "" {
				schema = strings.TrimSpace(databaseName)
			}
			table := trimIndexIdentifier(match[4])
			columns := strings.TrimSpace(match[5])
			if schema == "" || table == "" || indexName == "" || columns == "" {
				return true, fmt.Errorf("CREATE FULLTEXT INDEX requires database, table, index name, and columns")
			}
			if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
				return true, err
			}
			translated := fmt.Sprintf("alter table `%s`.`%s` add fulltext index `%s` (%s)", schema, table, indexName, columns)
			_, err := e.alterTableIndexDDL(schema, table, translated, sessionForeignKeyChecksEnabled(sessionFromExecutionContext(ctx)))
			if ignoreExisting && err != nil && strings.Contains(strings.ToLower(err.Error()), "duplicate index") {
				return true, nil
			}
			return true, err
		}
		if match := standaloneCreateIndexPattern.FindStringSubmatch(trimmed); len(match) == 8 {
			unique := strings.TrimSpace(match[1]) != ""
			ignoreExisting := strings.TrimSpace(match[2]) != ""
			indexName := trimIndexIdentifier(match[3])
			schema := trimIndexIdentifier(match[4])
			if schema == "" {
				schema = strings.TrimSpace(databaseName)
			}
			table := trimIndexIdentifier(match[5])
			columns := strings.TrimSpace(match[6])
			visibility := strings.ToLower(strings.TrimSpace(match[7]))
			if schema == "" || table == "" || indexName == "" || columns == "" {
				return true, fmt.Errorf("CREATE INDEX requires database, table, index name, and columns")
			}
			if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
				return true, err
			}
			kind := "index"
			if unique {
				kind = "unique index"
			}
			visibilityClause := ""
			if visibility != "" {
				visibilityClause = " " + visibility
			}
			translated := fmt.Sprintf("alter table `%s`.`%s` add %s `%s` (%s)%s", schema, table, kind, indexName, columns, visibilityClause)
			_, err := e.alterTableIndexDDL(schema, table, translated, sessionForeignKeyChecksEnabled(sessionFromExecutionContext(ctx)))
			if ignoreExisting && err != nil && strings.Contains(strings.ToLower(err.Error()), "duplicate index") {
				return true, nil
			}
			return true, err
		}
		if match := standaloneDropIndexPattern.FindStringSubmatch(trimmed); len(match) == 5 {
			ignoreMissing := strings.TrimSpace(match[1]) != ""
			indexName := trimIndexIdentifier(match[2])
			schema := trimIndexIdentifier(match[3])
			if schema == "" {
				schema = strings.TrimSpace(databaseName)
			}
			table := trimIndexIdentifier(match[4])
			if schema == "" || table == "" || indexName == "" {
				return true, fmt.Errorf("DROP INDEX requires database, table, and index name")
			}
			if err := e.checkTablePrivilege(ctx, schema, table, "INDEX"); err != nil {
				return true, err
			}
			translated := fmt.Sprintf("alter table `%s`.`%s` drop index `%s`", schema, table, indexName)
			_, err := e.alterTableIndexDDL(schema, table, translated, sessionForeignKeyChecksEnabled(sessionFromExecutionContext(ctx)))
			if ignoreMissing && err != nil && strings.Contains(strings.ToLower(err.Error()), "index '") && strings.Contains(strings.ToLower(err.Error()), "does not exist") {
				return true, nil
			}
			return true, err
		}
		return false, nil
	*/
}

func trimIndexIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`")
}

func sessionFromExecutionContext(ctx *ExecutionContext) server.MySQLServerSession {
	if ctx == nil {
		return nil
	}
	return ctx.Session
}
