package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeRawCreateTableIndexVisibility handles the trailing INVISIBLE option
// represented by the native CREATE TABLE AST. It lets the normal
// table-creation path build all rows/metadata, then
// marks the affected ordinary indexes in the durable table definition.
func (e *XMySQLExecutor) executeRawCreateTableIndexVisibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false, nil
	}
	statement, ok := parsed.AST.(*sqlparser.TableDDLStatement)
	if !ok || statement.AsSelect != nil {
		return false, nil
	}
	ddl, ok := statement.Primary.(*sqlparser.DDL)
	if !ok || ddl.Action != sqlparser.CreateStr || ddl.TableSpec == nil {
		return false, nil
	}
	invisible := make(map[string]struct{})
	changed := false
	for _, index := range ddl.TableSpec.Indexes {
		if index == nil || index.Info == nil {
			continue
		}
		seen := false
		for _, option := range index.Options {
			if option.Name != "visible" && option.Name != "invisible" {
				continue
			}
			if seen {
				return true, fmt.Errorf("index %s visibility specified more than once", index.Info.Name.String())
			}
			seen = true
			if index.Info.Primary || index.Info.Spatial || strings.Contains(strings.ToLower(index.Info.Type), "fulltext") {
				return true, fmt.Errorf("INVISIBLE is only supported for ordinary CREATE TABLE indexes")
			}
			if option.Name == "invisible" {
				invisible[strings.ToLower(index.Info.Name.String())] = struct{}{}
			}
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	baseQuery := query
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
	return true, writeMetadataFileAtomic(frmPath, updated)
}
