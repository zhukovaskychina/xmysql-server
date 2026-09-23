package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

const optionalIndexIdentifier = "`?([a-zA-Z0-9_$]+)`?"

var standaloneCreateIndexPattern = regexp.MustCompile(`(?is)^\s*create\s+(unique\s+)?index\s+(if\s+not\s+exists\s+)?` + optionalIndexIdentifier + `\s+on\s+(?:` + optionalIndexIdentifier + `\s*\.\s*)?` + optionalIndexIdentifier + `\s*\(([^)]*)\)(?:\s+(visible|invisible))?\s*$`)
var standaloneDropIndexPattern = regexp.MustCompile(`(?is)^\s*drop\s+index\s+(if\s+exists\s+)?` + optionalIndexIdentifier + `\s+on\s+(?:` + optionalIndexIdentifier + `\s*\.\s*)?` + optionalIndexIdentifier + `(?:\s+.*)?$`)

// executeRawStandaloneIndexCompatibility handles CREATE/DROP INDEX before
// the legacy yacc parser, whose DDL AST only models the ALTER TABLE form.
// Both operations reuse the durable ALTER index implementation so metadata,
// row index rebuilding, and foreign-key validation remain identical.
func (e *XMySQLExecutor) executeRawStandaloneIndexCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
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
