package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type AlterOperationKind string

const (
	AlterAddColumn            AlterOperationKind = "ADD_COLUMN"
	AlterDropColumn           AlterOperationKind = "DROP_COLUMN"
	AlterModifyColumn         AlterOperationKind = "MODIFY_COLUMN"
	AlterChangeColumn         AlterOperationKind = "CHANGE_COLUMN"
	AlterRenameColumn         AlterOperationKind = "RENAME_COLUMN"
	AlterAddIndex             AlterOperationKind = "ADD_INDEX"
	AlterDropIndex            AlterOperationKind = "DROP_INDEX"
	AlterRenameIndex          AlterOperationKind = "RENAME_INDEX"
	AlterAlterIndexVisibility AlterOperationKind = "ALTER_INDEX_VISIBILITY"
	AlterTableOption          AlterOperationKind = "TABLE_OPTION"
)

type AlterOperation struct {
	Kind          AlterOperationKind
	OldName       string
	NewName       string
	Definition    string
	IndexName     string
	Columns       []string
	Unique        bool
	FullText      bool
	IfExists      bool
	IfNotExists   bool
	Visible       bool
	VisibilitySet bool
	First         bool
	AfterColumn   string
	OptionName    string
	OptionValue   string
	Collation     string
}

func parseAlterOperations(query string) (string, []AlterOperation, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	parsed, parseErr := sqlparser.ParseForExecution(trimmed)
	if parseErr != nil {
		return "", nil, false, nil
	}
	statement, ok := parsed.AST.(*sqlparser.AlterTable)
	if !ok || statement.Table.Name.String() == "" {
		return "", nil, false, nil
	}
	// The first comma-separated fragment contains the table name. Remove it
	// with the same identifier lexer used by the ALTER AST parser so quoted and
	// qualified names keep their exact SQL boundary.
	rest := strings.TrimSpace(trimmed[len("alter table"):])
	_, rest, ok = consumeAlterIdentifier(rest)
	if !ok {
		return "", nil, false, nil
	}
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, ".") {
		_, rest, ok = consumeAlterIdentifier(strings.TrimSpace(rest[1:]))
		if !ok {
			return "", nil, false, nil
		}
	}
	rawParts := splitTopLevelComma(rest)
	if err := validateAlterExecutionOptions(rawParts); err != nil {
		return statement.Table.Name.String(), nil, true, err
	}
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		trimmedPart := strings.TrimSpace(part)
		if strings.EqualFold(trimmedPart, "force") || isAlterExecutionOption(trimmedPart) {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return statement.Table.Name.String(), nil, true, nil
	}
	if len(parts) <= 1 {
		lowerPart := strings.ToLower(parts[0])
		modifier := strings.Contains(lowerPart, "if exists") || strings.Contains(lowerPart, "if not exists")
		columnPosition, indexVisibility := false, false
		if operation, parseErr := parseSingleAlterOperation(parts[0]); parseErr == nil {
			columnPosition = operation.First || operation.AfterColumn != ""
			indexVisibility = operation.Kind == AlterAlterIndexVisibility || (operation.Kind == AlterAddIndex && operation.VisibilitySet)
		}
		if len(parts) == len(rawParts) && !modifier && !columnPosition && !indexVisibility {
			return "", nil, false, nil
		}
	}
	ops := make([]AlterOperation, 0, len(parts))
	for _, raw := range parts {
		operation, err := parseSingleAlterOperation(strings.TrimSpace(raw))
		if err != nil {
			return statement.Table.Name.String(), nil, true, err
		}
		ops = append(ops, operation)
	}
	return statement.Table.Name.String(), ops, true, nil
}

func validateAlterExecutionOptions(parts []string) error {
	seen := map[string]string{}
	for _, raw := range parts {
		part := strings.TrimSpace(raw)
		name, value, ok := alterExecutionOption(part)
		if !ok {
			lower := strings.ToLower(part)
			if strings.HasPrefix(lower, "algorithm") || strings.HasPrefix(lower, "lock") {
				return fmt.Errorf("invalid ALTER TABLE option %q", part)
			}
			continue
		}
		name, value = strings.ToLower(name), strings.ToLower(value)
		if previous, exists := seen[name]; exists {
			return fmt.Errorf("ALTER TABLE option %s specified more than once (%s, %s)", strings.ToUpper(name), previous, value)
		}
		seen[name] = value
		switch name {
		case "algorithm":
			if value != "default" && value != "copy" && value != "inplace" && value != "instant" {
				return fmt.Errorf("invalid ALTER TABLE option ALGORITHM=%s", value)
			}
		case "lock":
			if value != "default" && value != "none" && value != "shared" && value != "exclusive" {
				return fmt.Errorf("invalid ALTER TABLE option LOCK=%s", value)
			}
		case "engine":
			if value != "innodb" {
				return fmt.Errorf("unsupported ALTER TABLE option ENGINE=%s", value)
			}
		}
	}
	return nil
}

func alterExecutionOption(part string) (name, value string, ok bool) {
	trimmed := strings.TrimSpace(part)
	equal := strings.Index(trimmed, "=")
	if equal <= 0 {
		return "", "", false
	}
	name, value = strings.TrimSpace(trimmed[:equal]), strings.TrimSpace(trimmed[equal+1:])
	if !strings.EqualFold(name, "algorithm") && !strings.EqualFold(name, "lock") && !strings.EqualFold(name, "engine") || value == "" || strings.ContainsAny(value, " \t\r\n") {
		return "", "", false
	}
	return name, value, true
}

func isAlterExecutionOption(part string) bool {
	_, _, ok := alterExecutionOption(part)
	return ok
}

func parseSingleAlterOperation(raw string) (AlterOperation, error) {
	parsed, err := sqlparser.ParseForExecution("ALTER TABLE __xmysql_alter_target " + strings.TrimSpace(raw))
	if err != nil {
		return AlterOperation{}, fmt.Errorf("unsupported ALTER operation %q: %w", raw, err)
	}
	statement, ok := parsed.AST.(*sqlparser.AlterTable)
	if !ok || len(statement.Operations) != 1 {
		return AlterOperation{}, fmt.Errorf("unsupported ALTER operation %q", raw)
	}
	source := statement.Operations[0]
	switch source.Kind {
	case sqlparser.AlterOperationAddColumn:
		return AlterOperation{Kind: AlterAddColumn, NewName: source.Name, Definition: alterOperationDefinitionTail(source), First: source.First, AfterColumn: source.After, IfNotExists: source.IfNotExists}, nil
	case sqlparser.AlterOperationDropColumn:
		return AlterOperation{Kind: AlterDropColumn, OldName: source.Name, IfExists: source.IfExists}, nil
	case sqlparser.AlterOperationModifyColumn:
		definition, first, after := parseColumnPosition(alterDefinitionTail(source.Definition, source.Name))
		return AlterOperation{Kind: AlterModifyColumn, OldName: source.Name, NewName: source.Name, Definition: definition, First: first, AfterColumn: after}, nil
	case sqlparser.AlterOperationChangeColumn:
		definition, first, after := parseColumnPosition(alterDefinitionTail(source.Definition, source.Name))
		return AlterOperation{Kind: AlterChangeColumn, OldName: source.Name, NewName: source.NewName, Definition: definition, First: first, AfterColumn: after}, nil
	case sqlparser.AlterOperationRenameColumn:
		return AlterOperation{Kind: AlterRenameColumn, OldName: source.Name, NewName: source.NewName}, nil
	case sqlparser.AlterOperationAddIndex:
		return AlterOperation{Kind: AlterAddIndex, IndexName: source.Name, Columns: append([]string(nil), source.Columns...), Unique: strings.EqualFold(source.ConstraintType, "unique"), FullText: strings.Contains(strings.ToLower(source.Definition), "fulltext"), Visible: !strings.EqualFold(source.Visible, "invisible"), VisibilitySet: source.Visible != "", IfNotExists: source.IfNotExists}, nil
	case sqlparser.AlterOperationDropIndex:
		return AlterOperation{Kind: AlterDropIndex, IndexName: source.Name, IfExists: source.IfExists}, nil
	case sqlparser.AlterOperationRenameIndex:
		return AlterOperation{Kind: AlterRenameIndex, OldName: source.Name, NewName: source.NewName}, nil
	case sqlparser.AlterOperationAlterIndexVisibility:
		return AlterOperation{Kind: AlterAlterIndexVisibility, IndexName: source.Name, Visible: !strings.EqualFold(source.Visible, "invisible"), VisibilitySet: true}, nil
	default:
		return AlterOperation{}, fmt.Errorf("unsupported ALTER operation %q", raw)
	}
}

func alterOperationDefinitionTail(source sqlparser.AlterTableOperation) string {
	definition := alterDefinitionTail(source.Definition, source.Name)
	fields := strings.Fields(definition)
	if source.First && len(fields) > 0 && strings.EqualFold(fields[len(fields)-1], "first") {
		return strings.Join(fields[:len(fields)-1], " ")
	}
	if source.After != "" && len(fields) >= 2 && strings.EqualFold(fields[len(fields)-2], "after") {
		return strings.Join(fields[:len(fields)-2], " ")
	}
	return definition
}

func splitIdentifierList(input string) []string {
	parts := splitTopLevelComma(input)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.Trim(strings.TrimSpace(part), "`")
		if name != "" {
			result = append(result, name)
		}
	}
	return result
}

func (e *XMySQLExecutor) executeAlterOperationsCompatibility(query, databaseName string, foreignKeyChecksEnabled bool) (bool, error) {
	tableName, operations, handled, err := parseAlterOperations(query)
	if !handled {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	original, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	var oldMeta *metadata.TableMeta
	for _, operation := range operations {
		if operation.Kind != AlterDropColumn && operation.Kind != AlterAddColumn && operation.Kind != AlterModifyColumn && operation.Kind != AlterChangeColumn {
			continue
		}
		oldMeta, err = (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), databaseName, tableName)
		if err != nil {
			return true, err
		}
		break
	}
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	for _, operation := range operations {
		if foreignKeyChecksEnabled && operation.Kind == AlterDropColumn {
			if err := e.validateReferencedForeignKeysAfterColumnDrop(databaseName, tableName, operation.OldName); err != nil {
				return true, err
			}
		}
		if foreignKeyChecksEnabled && operation.Kind == AlterDropIndex {
			if err := e.validateReferencedForeignKeyIndexDrop(databaseName, tableName, operation.IndexName, tableInfo); err != nil {
				return true, err
			}
		}
	}
	if err := applyAlterOperationsWithForeignKeyChecks(tableInfo, operations, foreignKeyChecksEnabled); err != nil {
		return true, err
	}
	normalizeTemporaryAlteredColumnTypes(tableInfo, operations)
	if alterUsesInstantAlgorithm(query) {
		markInstantAddedColumns(tableInfo, original, operations)
	}
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return true, err
	}
	if oldMeta != nil {
		aliases := make(map[string]string)
		for _, operation := range operations {
			if operation.Kind == AlterChangeColumn && !strings.EqualFold(operation.OldName, operation.NewName) {
				aliases[operation.OldName] = operation.NewName
			}
		}
		if err := e.rewriteRowsAfterSchemaChange(databaseName, tableName, oldMeta, aliases); err != nil {
			_ = writeTableMetadataMapAtomic(frmPath, original)
			return true, err
		}
		if err := e.refreshAlteredSecondaryIndexes(databaseName, tableName); err != nil {
			_ = writeTableMetadataMapAtomic(frmPath, original)
			return true, err
		}
	}
	return true, nil
}

func alterUsesInstantAlgorithm(query string) bool {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return false
	}
	statement, ok := parsed.AST.(*sqlparser.AlterTable)
	if !ok {
		return false
	}
	for _, operation := range statement.Operations {
		if strings.EqualFold(operation.Name, "algorithm") && strings.EqualFold(operation.Value, "instant") {
			return true
		}
	}
	return false
}

// markInstantAddedColumns preserves the one piece of INNODB_COLUMNS metadata
// that the SQL-level table definition cannot otherwise reconstruct: whether a
// column was introduced by ALTER TABLE ... ALGORITHM=INSTANT.  MySQL exposes
// HAS_DEFAULT as this instant-add indicator, not as a duplicate of the
// ordinary COLUMNS.COLUMN_DEFAULT value.
func markInstantAddedColumns(tableInfo, before map[string]interface{}, operations []AlterOperation) {
	versionChange := false
	for _, operation := range operations {
		if operation.Kind == AlterAddColumn || operation.Kind == AlterDropColumn {
			versionChange = true
			break
		}
	}
	if versionChange {
		current := int(persistedNumber(tableInfo["total_row_versions"]))
		tableInfo["total_row_versions"] = current + 1
	}
	added := make(map[string]struct{})
	for _, operation := range operations {
		if operation.Kind == AlterAddColumn {
			added[strings.ToLower(operation.NewName)] = struct{}{}
		}
	}
	if len(added) == 0 {
		return
	}
	previous := make(map[string]struct{})
	if columns, ok := before["columns"].([]interface{}); ok {
		for _, raw := range columns {
			if column, ok := raw.(map[string]interface{}); ok {
				previous[strings.ToLower(strings.TrimSpace(fmt.Sprint(column["name"])))] = struct{}{}
			}
		}
	}
	columns, ok := tableInfo["columns"].([]interface{})
	if !ok {
		return
	}
	for _, raw := range columns {
		column, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fmt.Sprint(column["name"])))
		if _, isAdded := added[name]; !isAdded {
			continue
		}
		if _, existed := previous[name]; existed {
			continue
		}
		column["instant_added"] = true
	}
}

// normalizeQualifiedAlterTableTarget keeps the raw ALTER compatibility
// handlers focused on one physical table while preserving the SQL-level
// target database. RENAME TABLE-style cross-schema moves are intentionally
// handled by their dedicated path and must not be normalized here.
func normalizeQualifiedAlterTableTarget(query, currentDB string) (string, string, string, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "alter table") || len(trimmed) == len("alter table") || !isSQLSpace(trimmed[len("alter table")]) {
		return query, currentDB, "", false, nil
	}
	rest := strings.TrimSpace(trimmed[len("alter table"):])
	first, remaining, ok := consumeAlterIdentifier(rest)
	if !ok {
		return query, currentDB, "", true, fmt.Errorf("ALTER TABLE requires a database and table name")
	}
	rawTarget := first
	remaining = strings.TrimSpace(remaining)
	if strings.HasPrefix(remaining, ".") {
		second, after, secondOK := consumeAlterIdentifier(strings.TrimSpace(remaining[1:]))
		if !secondOK {
			return query, currentDB, "", true, fmt.Errorf("ALTER TABLE requires a database and table name")
		}
		rawTarget = first + "." + second
		remaining = after
	}
	targetDB, targetTable := compatibilityQualifiedTable(rawTarget, currentDB)
	if targetDB == "" || targetTable == "" {
		return query, currentDB, "", true, fmt.Errorf("ALTER TABLE requires a database and table name")
	}
	rest = strings.TrimSpace(remaining)
	normalized := "alter table " + targetTable + " " + rest
	return normalized, targetDB, targetTable, true, nil
}

func consumeAlterIdentifier(input string) (identifier, remainder string, ok bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", false
	}
	if input[0] == '`' {
		end := strings.Index(input[1:], "`")
		if end < 0 {
			return "", "", false
		}
		end++
		return input[:end+1], input[end+1:], true
	}
	end := 0
	for end < len(input) && input[end] != '.' && !isSQLSpace(input[end]) {
		end++
	}
	if end == 0 {
		return "", "", false
	}
	return input[:end], input[end:], true
}

func isSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

// rewriteRowsAfterSchemaChange rewrites existing clustered records after a
// column-layout or column-definition change. Clustered records are positional,
// so changing only .frm metadata can shift values, truncate records, or lose a
// value renamed by CHANGE COLUMN. The rewrite keeps the primary-key identity
// and rebuilds each row using the post-DDL schema.
func (e *XMySQLExecutor) rewriteRowsAfterSchemaChange(databaseName, tableName string, oldMeta *metadata.TableMeta, aliases map[string]string) error {
	if e == nil || oldMeta == nil || e.tableStorageManager == nil {
		return nil
	}
	newMeta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), databaseName, tableName)
	if err != nil {
		return fmt.Errorf("load altered table metadata: %w", err)
	}
	storageInfo, err := e.tableStorageManager.GetTableStorageInfo(databaseName, tableName)
	if err != nil {
		return fmt.Errorf("load altered table storage: %w", err)
	}
	btreeManager, err := e.tableStorageManager.CreateBTreeManagerForTable(context.Background(), databaseName, tableName)
	if err != nil {
		return fmt.Errorf("load altered table B+Tree: %w", err)
	}
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return err
	}
	dml.schemaName = databaseName
	dml.tableName = tableName
	rows, err := dml.scanRowsForTableConditions(context.Background(), databaseName, tableName, nil, oldMeta, storageInfo, btreeManager)
	if err != nil {
		return fmt.Errorf("scan rows before DROP COLUMN rewrite: %w", err)
	}

	type rowRewrite struct {
		oldKey   interface{}
		newKey   interface{}
		oldBytes []byte
		newBytes []byte
	}
	rewrites := make([]rowRewrite, 0, len(rows))
	for _, rowInfo := range rows {
		if rowInfo == nil {
			continue
		}
		oldRow := &InsertRowData{ColumnValues: cloneRowValues(rowInfo.OldValues), ColumnTypes: make(map[string]metadata.DataType, len(oldMeta.Columns))}
		for _, column := range oldMeta.Columns {
			if column != nil {
				oldRow.ColumnTypes[column.Name] = column.Type
			}
		}
		newRow := &InsertRowData{ColumnValues: make(map[string]interface{}, len(newMeta.Columns)), ColumnTypes: make(map[string]metadata.DataType, len(newMeta.Columns))}
		for _, column := range newMeta.Columns {
			if column == nil {
				continue
			}
			value, exists := rowInfo.OldValues[column.Name]
			if !exists {
				for oldName, newName := range aliases {
					if strings.EqualFold(newName, column.Name) {
						value, exists = rowInfo.OldValues[oldName]
						break
					}
				}
			}
			if !exists {
				value = column.DefaultValue
			}
			newRow.ColumnValues[column.Name] = value
			newRow.ColumnTypes[column.Name] = column.Type
		}
		oldKey, err := dml.clusteredKeyFromRowData(rowInfo.OldValues, oldMeta, rowInfo.StorageKey)
		if err != nil {
			return fmt.Errorf("derive old clustered key during DROP COLUMN rewrite: %w", err)
		}
		newKey, err := dml.generatePrimaryKey(newRow, newMeta)
		if err != nil {
			return fmt.Errorf("derive new clustered key during DROP COLUMN rewrite: %w", err)
		}
		oldBytes, err := dml.serializeRowData(oldRow, oldMeta)
		if err != nil {
			return fmt.Errorf("serialize old row during DROP COLUMN rewrite: %w", err)
		}
		newBytes, err := dml.serializeRowData(newRow, newMeta)
		if err != nil {
			return fmt.Errorf("serialize new row during DROP COLUMN rewrite: %w", err)
		}
		rewrites = append(rewrites, rowRewrite{oldKey: oldKey, newKey: newKey, oldBytes: oldBytes, newBytes: newBytes})
	}

	mutated := make([]rowRewrite, 0, len(rewrites))
	rollback := func() {
		for index := len(mutated) - 1; index >= 0; index-- {
			_ = btreeManager.Delete(context.Background(), mutated[index].newKey)
			_ = btreeManager.Insert(context.Background(), mutated[index].oldKey, mutated[index].oldBytes)
		}
	}
	for _, rewrite := range rewrites {
		if err := dml.waitForCheckpointWritePermit(context.Background()); err != nil {
			rollback()
			return err
		}
		if err := btreeManager.Delete(context.Background(), rewrite.oldKey); err != nil {
			rollback()
			return fmt.Errorf("delete old row during DROP COLUMN rewrite: %w", err)
		}
		if err := btreeManager.Insert(context.Background(), rewrite.newKey, rewrite.newBytes); err != nil {
			_ = btreeManager.Insert(context.Background(), rewrite.oldKey, rewrite.oldBytes)
			rollback()
			return fmt.Errorf("insert rewritten row during DROP COLUMN rewrite: %w", err)
		}
		mutated = append(mutated, rewrite)
	}
	if len(rewrites) > 0 {
		if err := dml.persistTableRootPage(storageInfo); err != nil {
			rollback()
			return fmt.Errorf("persist rewritten table root page: %w", err)
		}
	}
	return nil
}

func cloneRowValues(values map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func applyAlterOperations(tableInfo map[string]interface{}, operations []AlterOperation) error {
	return applyAlterOperationsWithForeignKeyChecks(tableInfo, operations, true)
}

func applyAlterOperationsWithForeignKeyChecks(tableInfo map[string]interface{}, operations []AlterOperation, foreignKeyChecksEnabled bool) error {
	columns, _ := tableInfo["columns"].([]interface{})
	indexes, _ := tableInfo["indexes"].([]interface{})
	// Work on detached metadata so a later failing clause cannot leak an
	// earlier rename or index-column rewrite into the caller's table definition.
	columns = cloneAlterMetadataList(columns)
	indexes = cloneAlterMetadataList(indexes)
	for _, operation := range operations {
		switch operation.Kind {
		case AlterAddColumn:
			if columnExists(columns, operation.NewName) {
				if operation.IfNotExists {
					continue
				}
				return fmt.Errorf("duplicate column name '%s'", operation.NewName)
			}
			parsed := parseCreateTableColumnsFallback("create table alter_target (" + operation.NewName + " " + operation.Definition + ")")
			if len(parsed) != 1 {
				return fmt.Errorf("invalid column definition for '%s'", operation.NewName)
			}
			if operation.First {
				columns = append([]interface{}{parsed[0]}, columns...)
				continue
			}
			if operation.AfterColumn != "" {
				position := -1
				for i, raw := range columns {
					if column, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(column["name"]), operation.AfterColumn) {
						position = i
						break
					}
				}
				if position < 0 {
					return fmt.Errorf("column '%s' does not exist", operation.AfterColumn)
				}
				columns = append(columns, nil)
				copy(columns[position+2:], columns[position+1:])
				columns[position+1] = parsed[0]
				continue
			}
			columns = append(columns, parsed[0])
		case AlterDropColumn:
			var found bool
			filtered := make([]interface{}, 0, len(columns))
			for _, raw := range columns {
				column, ok := raw.(map[string]interface{})
				if ok && strings.EqualFold(fmt.Sprint(column["name"]), operation.OldName) {
					found = true
					continue
				}
				filtered = append(filtered, raw)
			}
			if !found {
				if operation.IfExists {
					continue
				}
				return fmt.Errorf("column '%s' does not exist", operation.OldName)
			}
			columns = filtered
			indexes = removeIndexesUsingColumn(indexes, operation.OldName)
			if err := reconcileChecksAfterColumnDrop(tableInfo, operation.OldName, columns); err != nil {
				return err
			}
			if err := reconcileForeignKeysAfterColumnDrop(tableInfo, operation.OldName); err != nil {
				return err
			}
		case AlterModifyColumn, AlterChangeColumn:
			if operation.Kind == AlterChangeColumn && !strings.EqualFold(operation.OldName, operation.NewName) && columnExists(columns, operation.NewName) {
				return fmt.Errorf("duplicate column name '%s'", operation.NewName)
			}
			parsed := parseCreateTableColumnsFallback("create table alter_target (" + operation.NewName + " " + operation.Definition + ")")
			if len(parsed) != 1 {
				return fmt.Errorf("invalid column definition for '%s'", operation.NewName)
			}
			found := false
			for i, raw := range columns {
				column, ok := raw.(map[string]interface{})
				if !ok || !strings.EqualFold(fmt.Sprint(column["name"]), operation.OldName) {
					continue
				}
				updated := parsed[0]
				updated["name"] = operation.NewName
				for _, key := range []string{"primary", "unique", "auto_increment", "generated", "generated_expression", "default"} {
					if _, exists := updated[key]; !exists {
						updated[key] = column[key]
					}
				}
				columns[i] = updated
				found = true
				if !strings.EqualFold(operation.OldName, operation.NewName) {
					for _, index := range indexes {
						if indexMap, ok := index.(map[string]interface{}); ok {
							indexColumns, _ := indexMap["columns"].([]interface{})
							for j, indexColumn := range indexColumns {
								if strings.EqualFold(fmt.Sprint(indexColumn), operation.OldName) {
									indexColumns[j] = operation.NewName
								}
							}
						}
					}
				}
				break
			}
			if !found {
				return fmt.Errorf("column '%s' does not exist", operation.OldName)
			}
			if operation.First || operation.AfterColumn != "" {
				// Remove the updated column from its old position, then insert it
				// according to FIRST/AFTER while still operating on the detached
				// ALTER working copy.
				var updated interface{}
				for i, raw := range columns {
					column, ok := raw.(map[string]interface{})
					if ok && strings.EqualFold(fmt.Sprint(column["name"]), operation.NewName) {
						updated = raw
						columns = append(columns[:i], columns[i+1:]...)
						break
					}
				}
				if operation.First {
					columns = append([]interface{}{updated}, columns...)
				} else {
					position := -1
					for i, raw := range columns {
						column, ok := raw.(map[string]interface{})
						if ok && strings.EqualFold(fmt.Sprint(column["name"]), operation.AfterColumn) {
							position = i
							break
						}
					}
					if position < 0 {
						return fmt.Errorf("column '%s' does not exist", operation.AfterColumn)
					}
					columns = append(columns, nil)
					copy(columns[position+2:], columns[position+1:])
					columns[position+1] = updated
				}
			}
		case AlterRenameColumn:
			if !strings.EqualFold(operation.OldName, operation.NewName) && columnExists(columns, operation.NewName) {
				return fmt.Errorf("duplicate column name '%s'", operation.NewName)
			}
			found := false
			for _, raw := range columns {
				column, ok := raw.(map[string]interface{})
				if ok && strings.EqualFold(fmt.Sprint(column["name"]), operation.OldName) {
					column["name"] = operation.NewName
					found = true
				}
			}
			if !found {
				return fmt.Errorf("column '%s' does not exist", operation.OldName)
			}
			for _, raw := range indexes {
				if index, ok := raw.(map[string]interface{}); ok {
					indexColumns, _ := index["columns"].([]interface{})
					for i, column := range indexColumns {
						if strings.EqualFold(fmt.Sprint(column), operation.OldName) {
							indexColumns[i] = operation.NewName
						}
					}
				}
			}
		case AlterAddIndex:
			if indexExists(indexes, operation.IndexName) {
				if operation.IfNotExists {
					continue
				}
				return fmt.Errorf("duplicate key name '%s'", operation.IndexName)
			}
			indexColumns := make([]interface{}, 0, len(operation.Columns))
			for _, column := range operation.Columns {
				indexColumns = append(indexColumns, column)
			}
			indexType := "INDEX"
			if operation.FullText {
				indexType = "FULLTEXT"
			}
			index := map[string]interface{}{"name": operation.IndexName, "columns": indexColumns, "unique": operation.Unique, "primary": false, "type": indexType}
			if operation.FullText {
				index["advanced"] = true
			}
			if operation.VisibilitySet {
				index["visible"] = operation.Visible
				index["visibility_set"] = true
			}
			indexes = append(indexes, index)
		case AlterDropIndex:
			if foreignKeyChecksEnabled {
				if err := validateForeignKeyIndexDrop(tableInfo, operation.IndexName); err != nil {
					return err
				}
			}
			filtered := make([]interface{}, 0, len(indexes))
			found := false
			for _, raw := range indexes {
				index, ok := raw.(map[string]interface{})
				if ok && strings.EqualFold(fmt.Sprint(index["name"]), operation.IndexName) {
					found = true
					continue
				}
				filtered = append(filtered, raw)
			}
			if !found {
				if operation.IfExists {
					continue
				}
				return fmt.Errorf("index '%s' does not exist", operation.IndexName)
			}
			indexes = filtered
			delete(tableInfo, "fulltext_segments_"+operation.IndexName)
			delete(tableInfo, "spatial_entries_"+operation.IndexName)
		case AlterRenameIndex:
			if !strings.EqualFold(operation.OldName, operation.NewName) && indexExists(indexes, operation.NewName) {
				return fmt.Errorf("duplicate key name '%s'", operation.NewName)
			}
			found := false
			for _, raw := range indexes {
				if index, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(index["name"]), operation.OldName) {
					index["name"] = operation.NewName
					found = true
				}
			}
			if !found {
				return fmt.Errorf("index '%s' does not exist", operation.OldName)
			}
			if state, exists := tableInfo["fulltext_segments_"+operation.OldName]; exists {
				delete(tableInfo, "fulltext_segments_"+operation.OldName)
				tableInfo["fulltext_segments_"+operation.NewName] = state
			}
			if state, exists := tableInfo["spatial_entries_"+operation.OldName]; exists {
				delete(tableInfo, "spatial_entries_"+operation.OldName)
				tableInfo["spatial_entries_"+operation.NewName] = state
			}
		case AlterAlterIndexVisibility:
			found := false
			for _, raw := range indexes {
				if index, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(index["name"]), operation.IndexName) {
					if boolValue, exists := index["primary"].(bool); exists && boolValue {
						return fmt.Errorf("cannot alter visibility of primary key '%s'", operation.IndexName)
					}
					index["visible"] = operation.Visible
					index["visibility_set"] = true
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("index '%s' does not exist", operation.IndexName)
			}
		}
	}
	tableInfo["columns"] = columns
	tableInfo["indexes"] = indexes
	return nil
}

func cloneAlterMetadataList(items []interface{}) []interface{} {
	if items == nil {
		return nil
	}
	cloned := make([]interface{}, len(items))
	for i, raw := range items {
		metadata, ok := raw.(map[string]interface{})
		if !ok {
			cloned[i] = raw
			continue
		}
		copyMetadata := make(map[string]interface{}, len(metadata))
		for key, value := range metadata {
			if nested, ok := value.([]interface{}); ok {
				copyMetadata[key] = append([]interface{}(nil), nested...)
				continue
			}
			copyMetadata[key] = value
		}
		cloned[i] = copyMetadata
	}
	return cloned
}

func columnExists(columns []interface{}, name string) bool {
	for _, raw := range columns {
		if column, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(column["name"]), name) {
			return true
		}
	}
	return false
}

func indexExists(indexes []interface{}, name string) bool {
	for _, raw := range indexes {
		if index, ok := raw.(map[string]interface{}); ok && strings.EqualFold(fmt.Sprint(index["name"]), name) {
			return true
		}
	}
	return false
}

func parseColumnPosition(definition string) (string, bool, string) {
	definition = strings.TrimSpace(definition)
	fields := strings.Fields(definition)
	if len(fields) > 0 && strings.EqualFold(fields[len(fields)-1], "first") {
		return strings.TrimSpace(strings.Join(fields[:len(fields)-1], " ")), true, ""
	}
	if len(fields) >= 2 && strings.EqualFold(fields[len(fields)-2], "after") {
		return strings.TrimSpace(strings.Join(fields[:len(fields)-2], " ")), false, strings.Trim(fields[len(fields)-1], "` ")
	}
	return definition, false, ""
}

func removeIndexesUsingColumn(indexes []interface{}, columnName string) []interface{} {
	filtered := make([]interface{}, 0, len(indexes))
	for _, raw := range indexes {
		index, ok := raw.(map[string]interface{})
		if !ok {
			filtered = append(filtered, raw)
			continue
		}
		columns, _ := index["columns"].([]interface{})
		uses := false
		for _, column := range columns {
			if strings.EqualFold(fmt.Sprint(column), columnName) {
				uses = true
				break
			}
		}
		if !uses {
			filtered = append(filtered, raw)
		}
	}
	return filtered
}

func reconcileChecksAfterColumnDrop(tableInfo map[string]interface{}, droppedColumn string, remainingColumns []interface{}) error {
	checks, _ := tableInfo["checks"].([]interface{})
	if len(checks) == 0 {
		return nil
	}
	names, _ := tableInfo["check_names"].(map[string]interface{})
	enforced, _ := tableInfo["check_enforced"].(map[string]interface{})
	keptChecks := make([]interface{}, 0, len(checks))
	removedExpressions := make(map[string]struct{})
	for _, rawCheck := range checks {
		expression := fmt.Sprint(rawCheck)
		if !checkExpressionReferencesColumn(expression, droppedColumn) {
			keptChecks = append(keptChecks, rawCheck)
			continue
		}
		for _, rawColumn := range remainingColumns {
			column, ok := rawColumn.(map[string]interface{})
			if ok && checkExpressionReferencesColumn(expression, fmt.Sprint(column["name"])) {
				return fmt.Errorf("cannot drop column '%s': check constraint references column '%s'", droppedColumn, column["name"])
			}
		}
		removedExpressions[strings.ToLower(expression)] = struct{}{}
	}
	if len(removedExpressions) == 0 {
		return nil
	}
	if names != nil {
		for name, rawExpression := range names {
			if _, removed := removedExpressions[strings.ToLower(fmt.Sprint(rawExpression))]; removed {
				delete(names, name)
				if enforced != nil {
					delete(enforced, name)
				}
			}
		}
	}
	tableInfo["checks"] = keptChecks
	tableInfo["check_names"] = names
	tableInfo["check_enforced"] = enforced
	return nil
}

func checkExpressionReferencesColumn(expression, columnName string) bool {
	columnName = strings.Trim(strings.TrimSpace(columnName), "`")
	if columnName == "" {
		return false
	}
	parsed, err := sqlparser.Parse("select " + strings.TrimSpace(expression))
	if err != nil || parsed == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if column, ok := node.(*sqlparser.ColName); ok && strings.EqualFold(strings.Trim(column.Name.String(), "`"), columnName) {
			found = true
			return false, nil
		}
		return true, nil
	}, parsed)
	return found
}

func reconcileForeignKeysAfterColumnDrop(tableInfo map[string]interface{}, droppedColumn string) error {
	foreignKeys, _ := tableInfo["foreign_keys"].([]interface{})
	for _, rawForeignKey := range foreignKeys {
		foreignKey, ok := rawForeignKey.(map[string]interface{})
		if !ok {
			continue
		}
		columns, _ := foreignKey["columns"].([]interface{})
		for _, rawColumn := range columns {
			if strings.EqualFold(fmt.Sprint(rawColumn), droppedColumn) {
				return fmt.Errorf("cannot drop column '%s': foreign key constraint '%s' references it", droppedColumn, foreignKey["name"])
			}
		}
	}
	return nil
}

func validateForeignKeyIndexDrop(tableInfo map[string]interface{}, indexName string) error {
	indexes, _ := tableInfo["indexes"].([]interface{})
	var targetColumns []string
	for _, rawIndex := range indexes {
		index, ok := rawIndex.(map[string]interface{})
		if ok && strings.EqualFold(fmt.Sprint(index["name"]), indexName) {
			targetColumns = metadataIdentifierList(index["columns"])
			break
		}
	}
	if len(targetColumns) == 0 {
		return nil
	}
	foreignKeys, _ := tableInfo["foreign_keys"].([]interface{})
	for _, rawForeignKey := range foreignKeys {
		foreignKey, ok := rawForeignKey.(map[string]interface{})
		if !ok {
			continue
		}
		foreignKeyColumns := metadataIdentifierList(foreignKey["columns"])
		if !identifierPrefixMatches(targetColumns, foreignKeyColumns) {
			continue
		}
		coveredByOtherIndex := false
		for _, rawIndex := range indexes {
			index, ok := rawIndex.(map[string]interface{})
			if !ok || strings.EqualFold(fmt.Sprint(index["name"]), indexName) {
				continue
			}
			if identifierPrefixMatches(metadataIdentifierList(index["columns"]), foreignKeyColumns) {
				coveredByOtherIndex = true
				break
			}
		}
		if !coveredByOtherIndex {
			return fmt.Errorf("cannot drop index '%s': foreign key constraint '%s' requires an index on (%s)", indexName, foreignKey["name"], strings.Join(foreignKeyColumns, ", "))
		}
	}
	return nil
}

func metadataIdentifierList(value interface{}) []string {
	switch typed := value.(type) {
	case []interface{}:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, strings.Trim(strings.TrimSpace(fmt.Sprint(item)), "`"))
		}
		return result
	case []string:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, strings.Trim(strings.TrimSpace(item), "`"))
		}
		return result
	default:
		return nil
	}
}

func identifierPrefixMatches(indexColumns, requiredColumns []string) bool {
	if len(requiredColumns) == 0 || len(indexColumns) < len(requiredColumns) {
		return false
	}
	for index, required := range requiredColumns {
		if !strings.EqualFold(indexColumns[index], required) {
			return false
		}
	}
	return true
}
