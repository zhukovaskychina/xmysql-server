package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

var createTableInvisibleIndexPattern = regexp.MustCompile(`(?is)^\s*(?:(?:unique)\s+)?(?:index|key)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*\(`)

// executeRawCreateTableIndexVisibility handles the trailing INVISIBLE option
// that the legacy CREATE TABLE parser does not accept. It strips only that
// option, lets the normal table-creation path build all rows/metadata, then
// marks the affected ordinary indexes in the durable table definition.
func (e *XMySQLExecutor) executeRawCreateTableIndexVisibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if !strings.HasPrefix(strings.ToLower(trimmed), "create table") {
		return false, nil
	}
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return false, nil
	}
	close := matchingParenIndex(trimmed, open)
	if close < 0 {
		return true, fmt.Errorf("CREATE TABLE column list is unbalanced")
	}
	definitions := splitTopLevelComma(trimmed[open+1 : close])
	invisible := make(map[string]struct{})
	changed := false
	for index, definition := range definitions {
		match := regexp.MustCompile(`(?is)\s+(visible|invisible)\s*$`).FindStringSubmatch(definition)
		if len(match) == 0 {
			continue
		}
		baseDefinition := strings.TrimSpace(regexp.MustCompile(`(?is)\s+(?:visible|invisible)\s*$`).ReplaceAllString(definition, ""))
		indexMatch := createTableInvisibleIndexPattern.FindStringSubmatch(baseDefinition)
		if len(indexMatch) == 0 {
			return true, fmt.Errorf("INVISIBLE is only supported for ordinary CREATE TABLE indexes")
		}
		if strings.EqualFold(match[1], "invisible") {
			invisible[strings.ToLower(indexMatch[1])] = struct{}{}
		}
		definitions[index] = baseDefinition
		changed = true
	}
	if !changed {
		return false, nil
	}

	baseQuery := trimmed[:open+1] + strings.Join(definitions, ", ") + trimmed[close:]
	stmt, err := sqlparser.Parse(baseQuery)
	if err != nil {
		return true, fmt.Errorf("parse CREATE TABLE with invisible index: %w", err)
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok || !strings.EqualFold(ddl.Action, sqlparser.CreateStr) {
		return true, fmt.Errorf("INVISIBLE index option is only supported on CREATE TABLE")
	}
	targetDB := databaseName
	if qualifier := ddl.NewName.Qualifier.String(); qualifier != "" {
		targetDB = qualifier
	}
	original := ctx.RawQuery
	ctx.RawQuery = baseQuery
	e.executeCreateTableStatement(ctx, targetDB, ddl)
	ctx.RawQuery = original
	if len(invisible) == 0 {
		return true, nil
	}
	tableName := ddl.NewName.Name.String()
	frmPath := filepath.Join(e.getDataDir(), targetDB, tableName+".frm")
	raw, err := os.ReadFile(frmPath)
	if err != nil {
		return true, err
	}
	var tableInfo map[string]interface{}
	if err := json.Unmarshal(raw, &tableInfo); err != nil {
		return true, err
	}
	indexes, _ := tableInfo["indexes"].([]interface{})
	for _, rawIndex := range indexes {
		index, ok := rawIndex.(map[string]interface{})
		if !ok {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fmt.Sprint(index["name"])))
		if _, exists := invisible[name]; exists {
			index["visible"] = false
			index["visibility_set"] = true
		}
	}
	tableInfo["indexes"] = indexes
	updated, err := json.MarshalIndent(tableInfo, "", "  ")
	if err != nil {
		return true, err
	}
	return true, os.WriteFile(frmPath, updated, 0644)
}
