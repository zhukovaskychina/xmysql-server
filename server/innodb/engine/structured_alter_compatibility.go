package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func structuredAlterOperations(stmt *sqlparser.AlterTable) ([]AlterOperation, error) {
	if stmt == nil || len(stmt.Operations) == 0 {
		return nil, fmt.Errorf("%w: ALTER operation missing", sqlparser.ErrInvalidStatement)
	}
	operations := make([]AlterOperation, 0, len(stmt.Operations))
	for _, source := range stmt.Operations {
		op := AlterOperation{Definition: source.Definition, First: source.First, AfterColumn: source.After}
		switch source.Kind {
		case sqlparser.AlterOperationAddColumn:
			op.Kind, op.NewName = AlterAddColumn, source.Name
			op.Definition = alterDefinitionTail(source.Definition, source.Name)
		case sqlparser.AlterOperationDropColumn:
			op.Kind, op.OldName = AlterDropColumn, source.Name
		case sqlparser.AlterOperationModifyColumn:
			op.Kind, op.OldName, op.NewName = AlterModifyColumn, source.Name, source.Name
			op.Definition = alterDefinitionTail(source.Definition, source.Name)
		case sqlparser.AlterOperationChangeColumn:
			op.Kind, op.OldName, op.NewName = AlterChangeColumn, source.Name, source.NewName
			op.Definition = alterDefinitionTail(source.Definition, source.NewName)
		case sqlparser.AlterOperationRenameColumn:
			op.Kind, op.OldName, op.NewName = AlterRenameColumn, source.Name, source.NewName
		case sqlparser.AlterOperationAddIndex:
			if constraintType := strings.ToLower(strings.TrimSpace(source.ConstraintType)); constraintType != "" && constraintType != "unique" {
				// PRIMARY/FOREIGN/CHECK definitions have metadata semantics that
				// the compact index adapter does not represent. Let the existing
				// constraint-aware compatibility owner handle them instead of
				// silently turning a constraint into an ordinary index.
				return nil, fmt.Errorf("%w: ALTER constraint %q", sqlparser.ErrUnsupportedStatement, source.ConstraintType)
			}
			op.Kind, op.IndexName, op.Columns = AlterAddIndex, source.Name, append([]string(nil), source.Columns...)
			if source.Visible != "" {
				op.Visible, op.VisibilitySet = !strings.EqualFold(source.Visible, "invisible"), true
			}
		case sqlparser.AlterOperationAlterIndexVisibility:
			op.Kind, op.IndexName, op.Visible, op.VisibilitySet = AlterAlterIndexVisibility, source.Name, !strings.EqualFold(source.Visible, "invisible"), true
		case sqlparser.AlterOperationAddConstraint:
			if strings.EqualFold(source.ConstraintType, "unique") {
				op.Kind, op.IndexName, op.Columns = AlterAddIndex, source.Name, append([]string(nil), source.Columns...)
				op.Unique = true
			} else {
				return nil, fmt.Errorf("%w: ALTER constraint %q", sqlparser.ErrUnsupportedStatement, source.ConstraintType)
			}
		case sqlparser.AlterOperationOption:
			op.Kind, op.OptionName, op.OptionValue, op.Collation = AlterTableOption, source.Name, source.Value, source.Collation
		case sqlparser.AlterOperationDropIndex:
			op.Kind, op.IndexName = AlterDropIndex, source.Name
		default:
			return nil, fmt.Errorf("%w: ALTER operation %q", sqlparser.ErrUnsupportedStatement, source.Kind)
		}
		operations = append(operations, op)
	}
	return operations, nil
}

func alterDefinitionTail(definition, name string) string {
	needle := strings.ToLower(strings.Trim(name, "`"))
	fields := strings.Fields(definition)
	for index, field := range fields {
		if strings.EqualFold(strings.Trim(field, "`"), needle) {
			return strings.TrimSpace(strings.Join(fields[index+1:], " "))
		}
	}
	return strings.TrimSpace(definition)
}

func (e *XMySQLExecutor) executeStructuredAlterCompatibility(ctx *ExecutionContext, stmt *sqlparser.AlterTable, currentDB string) (bool, error) {
	if stmt != nil && len(stmt.Operations) == 1 {
		source := stmt.Operations[0]
		schema := strings.TrimSpace(stmt.Table.Qualifier.String())
		if schema == "" {
			schema = strings.TrimSpace(currentDB)
		}
		table := strings.TrimSpace(stmt.Table.Name.String())
		foreignKeyChecks := sessionForeignKeyChecksEnabled(ctx.Session)
		if source.Kind == sqlparser.AlterOperationDropConstraint && source.ConstraintType == "" {
			checkErr := e.alterCheckMetadata(schema, table, source.Name, "", true, false)
			if checkErr == nil {
				return true, nil
			}
			foreignErr := e.alterForeignKeyMetadata(schema, table, source.Name, nil, "", "", nil, "", false, foreignKeyChecks)
			if foreignErr == nil {
				return true, nil
			}
			return true, checkErr
		}
		if strings.EqualFold(source.ConstraintType, "check") {
			if source.Kind == sqlparser.AlterOperationAlterCheck {
				return true, e.alterCheckEnforcement(schema, table, source.Name, strings.EqualFold(strings.TrimSpace(source.Visible), "enforced"))
			}
			enforced := !strings.EqualFold(strings.TrimSpace(source.Visible), "not enforced")
			return true, e.alterCheckMetadata(schema, table, source.Name, source.Value, enforced, source.Kind == sqlparser.AlterOperationAddConstraint)
		}
		if source.Kind == sqlparser.AlterOperationSetDefault || source.Kind == sqlparser.AlterOperationDropDefault {
			schema := strings.TrimSpace(stmt.Table.Qualifier.String())
			if schema == "" {
				schema = strings.TrimSpace(currentDB)
			}
			return true, e.alterColumnDefaultMetadata(schema, strings.TrimSpace(stmt.Table.Name.String()), source.Name, source.Value, source.Kind == sqlparser.AlterOperationSetDefault)
		}
		if strings.EqualFold(source.ConstraintType, "primary") {
			add := source.Kind == sqlparser.AlterOperationAddIndex
			return true, e.alterPrimaryKeyMetadata(schema, table, source.Columns, add, foreignKeyChecks)
		}
		if strings.EqualFold(source.ConstraintType, "foreign") && (source.Kind == sqlparser.AlterOperationAddIndex || source.Kind == sqlparser.AlterOperationAddConstraint) {
			tail := ""
			if source.OnDelete != "" {
				tail += " ON DELETE " + source.OnDelete
			}
			if source.OnUpdate != "" {
				tail += " ON UPDATE " + source.OnUpdate
			}
			return true, e.alterForeignKeyMetadata(schema, table, source.Name, source.Columns, source.ReferencedSchema, source.ReferencedTable, source.ReferencedColumns, tail, true, foreignKeyChecks)
		}
		if strings.EqualFold(source.ConstraintType, "foreign") && source.Kind == sqlparser.AlterOperationDropConstraint {
			return true, e.alterForeignKeyMetadata(schema, table, source.Name, nil, "", "", nil, "", false, foreignKeyChecks)
		}
		if source.Kind == sqlparser.AlterOperationPartition && (source.PartitionAction == "drop" || source.PartitionAction == "truncate") {
			return e.executeTypedPartitionDropTruncate(ctx, schema, table, source.PartitionAction, source.PartitionNames)
		}
		if source.Kind == sqlparser.AlterOperationPartition && source.PartitionAction == "add" && source.PartitionDefinition != "" {
			return e.executeRawPartitionAdd(ctx, []string{"", schema + "." + table, source.PartitionDefinition}, currentDB)
		}
		if source.Kind == sqlparser.AlterOperationPartition && source.PartitionAction == "exchange" && len(source.PartitionNames) == 1 && source.PartitionExchangeTable != "" {
			without := ""
			if source.PartitionWithoutValidation {
				without = "without validation"
			}
			return e.executeRawPartitionExchange(ctx, []string{"", schema + "." + table, source.PartitionNames[0], source.PartitionExchangeTable, without}, currentDB)
		}
		if source.Kind == sqlparser.AlterOperationPartition && source.PartitionAction == "reorganize" && len(source.PartitionNames) > 0 && source.PartitionDefinition != "" {
			return e.executeRawPartitionReorganize(ctx, []string{"", schema + "." + table, strings.Join(source.PartitionNames, ","), source.PartitionDefinition}, currentDB)
		}
	}
	if stmt != nil && len(stmt.Operations) == 1 && stmt.Operations[0].Kind == sqlparser.AlterOperationTablespace {
		schema := strings.TrimSpace(stmt.Table.Qualifier.String())
		if schema == "" {
			schema = strings.TrimSpace(currentDB)
		}
		table := strings.TrimSpace(stmt.Table.Name.String())
		target := strings.Trim(strings.TrimSpace(stmt.Operations[0].Name), "` ")
		return e.executeTableTablespaceCompatibility(ctx, schema, table, target)
	}
	operations, err := structuredAlterOperations(stmt)
	if err != nil {
		if errors.Is(err, sqlparser.ErrUnsupportedStatement) {
			return false, nil
		}
		return true, err
	}
	databaseName := strings.TrimSpace(currentDB)
	if stmt.Table.Qualifier.String() != "" {
		databaseName = stmt.Table.Qualifier.String()
	}
	tableName := stmt.Table.Name.String()
	if databaseName == "" || tableName == "" {
		return true, fmt.Errorf("ALTER TABLE requires a database and table name")
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		return true, err
	}
	if err := e.checkAlterTablePrivileges(ctx, databaseName, tableName); err != nil {
		return true, err
	}
	lockKey := strings.ToLower(databaseName) + "." + strings.ToLower(tableName)
	if !sessionHoldsTableLock(ctx.Session, lockKey) {
		release, lockErr := e.acquireDDLWriteLock(ctx, lockKey, false)
		if lockErr != nil {
			return true, tableLockWaitError(lockErr)
		}
		defer release()
	}
	allOptions := len(operations) > 0
	for _, operation := range operations {
		if operation.Kind != AlterTableOption {
			allOptions = false
			break
		}
	}
	if allOptions {
		for _, operation := range operations {
			switch strings.ToLower(operation.OptionName) {
			case "comment":
				path := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
				info, readErr := readTableMetadataMap(path)
				if readErr != nil {
					return true, readErr
				}
				info["table_comment"] = operation.OptionValue
				if writeErr := writeTableMetadataMapAtomic(path, info); writeErr != nil {
					return true, writeErr
				}
			case "charset", "character", "convert":
				if err := e.persistAlterTableCharacterOptions(databaseName, tableName, operation.OptionValue, operation.Collation, operation.Definition); err != nil {
					return true, err
				}
			case "collate":
				if err := e.persistAlterTableCharacterOptions(databaseName, tableName, "", operation.OptionValue, operation.Definition); err != nil {
					return true, err
				}
			case "row_format":
				if err := e.persistAlterTableRowFormat(databaseName, tableName, operation.OptionValue); err != nil {
					return true, err
				}
			default:
				return false, nil
			}
		}
		return true, nil
	}
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	original, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	working := cloneAlterMetadataMap(original)
	if err := applyAlterOperationsWithForeignKeyChecks(working, operations, sessionForeignKeyChecksEnabled(ctx.Session)); err != nil {
		return true, err
	}
	if _, temporary := temporaryPhysicalTableName(ctx.Session, databaseName, tableName); temporary {
		normalizeTemporaryAlteredColumnTypes(working, operations)
	}
	if err := writeTableMetadataMapAtomic(frmPath, working); err != nil {
		return true, err
	}
	if err := e.refreshAlteredSecondaryIndexes(databaseName, tableName); err != nil {
		_ = writeTableMetadataMapAtomic(frmPath, original)
		return true, err
	}
	return true, nil
}

func normalizeTemporaryAlteredColumnTypes(tableInfo map[string]interface{}, operations []AlterOperation) {
	changed := make(map[string]struct{})
	for _, operation := range operations {
		switch operation.Kind {
		case AlterAddColumn, AlterModifyColumn, AlterChangeColumn:
			changed[strings.ToLower(operation.NewName)] = struct{}{}
		}
	}
	columns, _ := tableInfo["columns"].([]interface{})
	for _, raw := range columns {
		column, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name := strings.ToLower(fmt.Sprint(column["name"]))
		if _, ok := changed[name]; !ok {
			continue
		}
		if typeName, ok := column["type"].(string); ok {
			if open := strings.Index(typeName, "("); open > 0 {
				column["type"] = strings.TrimSpace(typeName[:open])
			}
		}
	}
}

func cloneAlterMetadataMap(source map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	if columns, ok := source["columns"].([]interface{}); ok {
		cloned["columns"] = cloneAlterMetadataList(columns)
	}
	if indexes, ok := source["indexes"].([]interface{}); ok {
		cloned["indexes"] = cloneAlterMetadataList(indexes)
	}
	return cloned
}
