package engine

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// executeInformationSchemaInnoDBForeignSelect exposes the persisted foreign
// key dictionary. The ID, table names, and referential-action bit flags are
// derived from the durable table definition.
func (e *XMySQLExecutor) executeInformationSchemaInnoDBForeignSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE"})
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil || info == nil {
			continue
		}
		for _, foreignKey := range info.ForeignKeys {
			id := innodbForeignID(table.schemaName, table.tableName, foreignKey)
			forName := table.schemaName + "/" + table.tableName
			refSchema := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_schema"])), "`")
			if refSchema == "" || refSchema == "<nil>" {
				refSchema = table.schemaName
			}
			refTable := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`")
			refName := refSchema + "/" + refTable
			columnsForKey := stringListFromForeignValue(foreignKey["columns"])
			if !innodbForeignFilterMatches(query, id, forName, refName) {
				continue
			}
			values := map[string]interface{}{
				"ID": id, "FOR_NAME": forName, "REF_NAME": refName,
				"N_COLS": int64(len(columnsForKey)), "TYPE": innodbForeignType(foreignKey),
			}
			if !performanceSchemaLockValuesMatch(query, values) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0]) })
	return newInformationSchemaSelectResult("information_schema.innodb_foreign", columns, rows)
}

// innodbForeignType mirrors the MySQL INNODB_FOREIGN.TYPE bit flags. The
// persisted metadata stores the normalized referential actions as strings,
// which is sufficient to reconstruct the documented flags without fabricating
// any physical dictionary state.
func innodbForeignType(foreignKey map[string]interface{}) int64 {
	if foreignKey == nil {
		return 0
	}
	typeValue := int64(0)
	switch strings.ToLower(strings.TrimSpace(fmt.Sprint(foreignKey["on_delete"]))) {
	case "cascade":
		typeValue |= 1
	case "set null":
		typeValue |= 2
	case "no action":
		typeValue |= 16
	}
	switch strings.ToLower(strings.TrimSpace(fmt.Sprint(foreignKey["on_update"]))) {
	case "cascade":
		typeValue |= 4
	case "set null":
		typeValue |= 8
	case "no action":
		typeValue |= 32
	}
	return typeValue
}

func (e *XMySQLExecutor) executeInformationSchemaInnoDBForeignColsSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"ID", "FOR_COL_NAME", "REF_COL_NAME", "POS"})
	rows := make([][]interface{}, 0)
	for _, table := range e.scanFrmTables() {
		info, err := e.readPersistedTableInfo(table.schemaName, table.tableName)
		if err != nil || info == nil {
			continue
		}
		for _, foreignKey := range info.ForeignKeys {
			id := innodbForeignID(table.schemaName, table.tableName, foreignKey)
			forName := table.schemaName + "/" + table.tableName
			refSchema := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_schema"])), "`")
			if refSchema == "" || refSchema == "<nil>" {
				refSchema = table.schemaName
			}
			refName := refSchema + "/" + strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["ref_table"])), "`")
			if !innodbForeignFilterMatches(query, id, forName, refName) {
				continue
			}
			foreignColumns := stringListFromForeignValue(foreignKey["columns"])
			referencedColumns := stringListFromForeignValue(foreignKey["ref_columns"])
			for index, foreignColumn := range foreignColumns {
				var referencedColumn interface{}
				if index < len(referencedColumns) {
					referencedColumn = referencedColumns[index]
				}
				values := map[string]interface{}{
					"ID": id, "FOR_COL_NAME": foreignColumn,
					"REF_COL_NAME": referencedColumn, "POS": int64(index),
				}
				if !performanceSchemaLockValuesMatch(query, values) {
					continue
				}
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j])
	})
	return newInformationSchemaSelectResult("information_schema.innodb_foreign_cols", columns, rows)
}

func innodbForeignID(schema, table string, foreignKey map[string]interface{}) string {
	name := strings.Trim(strings.TrimSpace(fmt.Sprint(foreignKey["name"])), "`")
	if name == "" || name == "<nil>" {
		name = "fk_" + table
	}
	return schema + "/" + name
}

func stringListFromForeignValue(value interface{}) []string {
	values := make([]string, 0)
	switch list := value.(type) {
	case []string:
		for _, item := range list {
			values = append(values, strings.Trim(strings.TrimSpace(item), "`"))
		}
	case []interface{}:
		for _, item := range list {
			values = append(values, strings.Trim(strings.TrimSpace(fmt.Sprint(item)), "`"))
		}
	}
	return values
}

func innodbForeignFilterMatches(query, id, forName, refName string) bool {
	for _, filter := range []struct {
		column string
		value  string
	}{
		{"id", id}, {"for_name", forName}, {"ref_name", refName},
	} {
		pattern := regexp.MustCompile(`(?is)\b` + filter.column + `\b\s*=\s*('([^']*)'|"([^"]*)")`)
		match := pattern.FindStringSubmatch(query)
		if len(match) == 0 {
			continue
		}
		expected := match[2]
		if expected == "" {
			expected = match[3]
		}
		if !strings.EqualFold(filter.value, expected) {
			return false
		}
	}
	return true
}
