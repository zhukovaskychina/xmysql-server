package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// partitionDescriptor is the durable, logical partition dictionary used by
// the compatibility layer. Physical row routing is intentionally kept behind
// this descriptor so metadata remains restart-safe while routing is expanded.
type partitionDescriptor struct {
	Method     string                   `json:"method"`
	Expression string                   `json:"expression"`
	Partitions []map[string]interface{} `json:"partitions"`
}

// executeRawCreatePartitionCompatibility handles CREATE TABLE partition
// clauses that are not represented by the legacy yacc create-table AST.
func (e *XMySQLExecutor) executeRawCreatePartitionCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	descriptor, baseQuery, ok, err := parsePartitionedCreate(query)
	if !ok {
		return false, err
	}
	if err != nil {
		return true, err
	}
	stmt, err := sqlparser.Parse(baseQuery)
	if err != nil {
		return true, fmt.Errorf("parse partitioned CREATE TABLE base statement: %w", err)
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok || !strings.EqualFold(ddl.Action, sqlparser.CreateStr) {
		return true, fmt.Errorf("partition clause is only supported on CREATE TABLE")
	}
	e.executeCreateTableStatement(ctx, databaseName, ddl)
	frmPath := filepath.Join(e.getDataDir(), databaseName, ddl.NewName.Name.String()+".frm")
	if _, statErr := os.Stat(frmPath); statErr != nil {
		return true, nil
	}
	return true, e.persistPartitionMetadata(databaseName, ddl.NewName.Name.String(), descriptor)
}

// executeRawPartitionMaintenance handles data-safe partition maintenance for
// the logical compatibility descriptor.
func (e *XMySQLExecutor) executeRawPartitionMaintenance(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	qualifiedTable := `((?:` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)?)`
	removeMatch := regexp.MustCompile(`(?is)^alter\s+table\s+` + qualifiedTable + `\s+remove\s+partitioning\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(removeMatch) == 2 {
		return e.executeRawPartitionRemove(ctx, removeMatch, databaseName)
	}
	exchangeMatch := regexp.MustCompile(`(?is)^alter\s+table\s+` + qualifiedTable + `\s+exchange\s+partition\s+(` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+with\s+table\s+` + qualifiedTable + `(?:\s+(without\s+validation))?\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(exchangeMatch) == 5 {
		return e.executeRawPartitionExchange(ctx, exchangeMatch, databaseName)
	}
	reorganizeMatch := regexp.MustCompile(`(?is)^alter\s+table\s+((?:` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)?)\s+reorganize\s+partition\s+((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*,\s*)*` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+into\s+(.+?)\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(reorganizeMatch) == 4 {
		return e.executeRawPartitionReorganize(ctx, reorganizeMatch, databaseName)
	}
	addMatch := regexp.MustCompile(`(?is)^alter\s+table\s+((?:` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)?)\s+add\s+partition\s+(.+?)\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(addMatch) == 3 {
		return e.executeRawPartitionAdd(ctx, addMatch, databaseName)
	}
	match := regexp.MustCompile(`(?is)^alter\s+table\s+((?:` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.` + "`?" + `]+` + "`?" + `)?)\s+(drop|truncate)\s+partition\s+(.+?)\s*;?$`).FindStringSubmatch(strings.TrimSpace(query))
	if len(match) != 4 {
		return false, nil
	}
	operation := strings.ToLower(match[2])
	parts := strings.Split(match[1], ".")
	schema, table := databaseName, strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 2 {
		schema = strings.Trim(parts[0], "` ")
	}
	frmPath := filepath.Join(e.getDataDir(), schema, table+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	partitioning, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("table %s is not partitioned", table)
	}
	definitions, ok := partitioning["partitions"].([]interface{})
	if !ok {
		return true, fmt.Errorf("partition metadata for %s is invalid", table)
	}
	dropNames := map[string]struct{}{}
	for _, name := range strings.Split(match[3], ",") {
		name = strings.ToLower(strings.Trim(strings.TrimSpace(name), "`"))
		if name != "" {
			dropNames[name] = struct{}{}
		}
	}
	if len(dropNames) == 0 {
		return true, fmt.Errorf("%s PARTITION requires a partition name", strings.ToUpper(operation))
	}
	remaining := make([]interface{}, 0, len(definitions))
	droppedDefinitions := make([]interface{}, 0, len(definitions))
	removed := 0
	for _, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := definition["name"].(string)
		if _, drop := dropNames[strings.ToLower(name)]; drop {
			droppedDefinitions = append(droppedDefinitions, raw)
			removed++
			continue
		}
		definition["ordinal"] = len(remaining) + 1
		remaining = append(remaining, definition)
	}
	if removed == 0 {
		return true, fmt.Errorf("unknown partition in %s PARTITION", strings.ToUpper(operation))
	}
	if operation == "truncate" {
		if err := e.deleteRowsFromDroppedPartitions(ctx, schema, table, partitioning, droppedDefinitions); err != nil {
			return true, err
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partition data truncated"}
		return true, nil
	}
	if len(remaining) == 0 {
		return true, fmt.Errorf("cannot drop all partitions")
	}
	if err := e.deleteRowsFromDroppedPartitions(ctx, schema, table, partitioning, droppedDefinitions); err != nil {
		return true, err
	}
	partitioning["partitions"] = remaining
	tableInfo["partitioning"] = partitioning
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return true, err
	}
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.SetPartitioning(schema, table, partitioning); err != nil {
			return true, err
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partition metadata updated"}
	return true, nil
}

func (e *XMySQLExecutor) executeRawPartitionRemove(ctx *ExecutionContext, match []string, databaseName string) (bool, error) {
	schema, table := partitionQualifiedTable(match[1], databaseName)
	frmPath := filepath.Join(e.getDataDir(), schema, table+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	_, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("table %s is not partitioned", table)
	}
	rows, err := e.readPartitionExchangeRows(ctx, schema, table)
	if err != nil {
		return true, err
	}
	if err := e.deletePartitionExchangeRows(ctx, schema, table, rows); err != nil {
		return true, err
	}
	delete(tableInfo, "partitioning")
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return true, err
	}
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.SetPartitioning(schema, table, nil); err != nil {
			return true, err
		}
	}
	if err := e.restorePartitionReorganizationRows(ctx, schema, table, rows); err != nil {
		return true, err
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partitioning removed"}
	return true, nil
}

func (e *XMySQLExecutor) executeRawPartitionExchange(ctx *ExecutionContext, match []string, databaseName string) (bool, error) {
	sourceSchema, sourceTable := partitionQualifiedTable(match[1], databaseName)
	partitionName := strings.ToLower(strings.Trim(strings.TrimSpace(match[2]), "`"))
	targetSchema, targetTable := partitionQualifiedTable(match[3], databaseName)
	sourceFrmPath := filepath.Join(e.getDataDir(), sourceSchema, sourceTable+".frm")
	targetFrmPath := filepath.Join(e.getDataDir(), targetSchema, targetTable+".frm")
	sourceInfo, err := readTableMetadataMap(sourceFrmPath)
	if err != nil {
		return true, err
	}
	targetInfo, err := readTableMetadataMap(targetFrmPath)
	if err != nil {
		return true, err
	}
	partitioning, ok := sourceInfo["partitioning"].(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("table %s is not partitioned", sourceTable)
	}
	if _, targetPartitioned := targetInfo["partitioning"].(map[string]interface{}); targetPartitioned {
		return true, fmt.Errorf("exchange table %s must not be partitioned", targetTable)
	}
	definitions, ok := partitioning["partitions"].([]interface{})
	if !ok || len(definitions) == 0 {
		return true, fmt.Errorf("partition metadata for %s is invalid", sourceTable)
	}
	partitionIndex := -1
	for index, raw := range definitions {
		definition, definitionOK := raw.(map[string]interface{})
		if !definitionOK {
			return true, fmt.Errorf("partition metadata for %s is invalid", sourceTable)
		}
		name, _ := definition["name"].(string)
		if strings.EqualFold(name, partitionName) {
			partitionIndex = index
			break
		}
	}
	if partitionIndex < 0 {
		return true, fmt.Errorf("unknown partition %s", partitionName)
	}
	sourceMeta, err := e.getShowColumnsTableMetadata(sourceSchema, sourceTable)
	if err != nil {
		return true, err
	}
	targetMeta, err := e.getShowColumnsTableMetadata(targetSchema, targetTable)
	if err != nil {
		return true, err
	}
	if err := validateExchangeTableMetadata(sourceMeta, targetMeta, sourceTable, targetTable); err != nil {
		return true, err
	}
	sourceRows, err := e.readPartitionExchangeRows(ctx, sourceSchema, sourceTable)
	if err != nil {
		return true, err
	}
	targetRows, err := e.readPartitionExchangeRows(ctx, targetSchema, targetTable)
	if err != nil {
		return true, err
	}
	method, _ := partitioning["method"].(string)
	expression, _ := partitioning["expression"].(string)
	selectedRows := make([]partitionReorganizationRow, 0, len(sourceRows))
	for _, row := range sourceRows {
		values, valueErr := partitionExpressionValuesFromRow(row.columns, row.values, expression)
		if valueErr != nil {
			continue
		}
		index, routeErr := partitionForValues(strings.ToUpper(method), values, definitions)
		if routeErr != nil {
			return true, routeErr
		}
		if index == partitionIndex {
			selectedRows = append(selectedRows, row)
		}
	}
	withoutValidation := strings.TrimSpace(match[4]) != ""
	if !withoutValidation {
		for _, row := range targetRows {
			values, valueErr := partitionExpressionValuesFromRow(row.columns, row.values, expression)
			if valueErr != nil {
				return true, fmt.Errorf("exchange table row cannot be mapped to partition %s: %w", partitionName, valueErr)
			}
			index, routeErr := partitionForValues(strings.ToUpper(method), values, definitions)
			if routeErr != nil {
				return true, fmt.Errorf("exchange table row cannot be mapped to partition %s: %w", partitionName, routeErr)
			}
			if index != partitionIndex {
				return true, fmt.Errorf("exchange table row does not belong to partition %s", partitionName)
			}
		}
	}
	if err := e.deletePartitionExchangeRows(ctx, sourceSchema, sourceTable, selectedRows); err != nil {
		return true, err
	}
	if err := e.deletePartitionExchangeRows(ctx, targetSchema, targetTable, targetRows); err != nil {
		return true, err
	}
	if err := e.restorePartitionReorganizationRows(ctx, targetSchema, targetTable, selectedRows); err != nil {
		return true, err
	}
	if err := e.restorePartitionReorganizationRows(ctx, sourceSchema, sourceTable, targetRows); err != nil {
		return true, err
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partition data exchanged"}
	return true, nil
}

func partitionQualifiedTable(value, databaseName string) (string, string) {
	parts := strings.Split(value, ".")
	schema := databaseName
	table := strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 2 {
		schema = strings.Trim(parts[0], "` ")
	}
	return schema, table
}

func validateExchangeTableMetadata(source, target *metadata.TableMeta, sourceTable, targetTable string) error {
	if source == nil || target == nil || len(source.Columns) != len(target.Columns) {
		return fmt.Errorf("exchange tables %s and %s have incompatible columns", sourceTable, targetTable)
	}
	for index, sourceColumn := range source.Columns {
		targetColumn := target.Columns[index]
		if sourceColumn == nil || targetColumn == nil || !strings.EqualFold(sourceColumn.Name, targetColumn.Name) || sourceColumn.Type != targetColumn.Type || sourceColumn.IsUnsigned != targetColumn.IsUnsigned {
			return fmt.Errorf("exchange tables %s and %s have incompatible columns", sourceTable, targetTable)
		}
	}
	return nil
}

func (e *XMySQLExecutor) readPartitionExchangeRows(ctx *ExecutionContext, schema, table string) ([]partitionReorganizationRow, error) {
	stmt, err := sqlparser.Parse("select * from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table))
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("partition exchange source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, schema)
	if err != nil {
		return nil, err
	}
	rows := make([]partitionReorganizationRow, 0, len(result.Records))
	for _, record := range result.Records {
		values := record.GetValues()
		rowValues := make([]interface{}, len(result.Columns))
		for index := range result.Columns {
			if index < len(values) && !values[index].IsNull() {
				rowValues[index] = values[index].Raw()
			}
		}
		rows = append(rows, partitionReorganizationRow{columns: append([]string(nil), result.Columns...), values: rowValues})
	}
	return rows, nil
}

func (e *XMySQLExecutor) deletePartitionExchangeRows(ctx *ExecutionContext, schema, table string, rows []partitionReorganizationRow) error {
	if len(rows) == 0 {
		return nil
	}
	meta, err := e.getShowColumnsTableMetadata(schema, table)
	if err != nil {
		return err
	}
	keyColumns := append([]string(nil), meta.PrimaryKey...)
	if len(keyColumns) == 0 {
		for _, column := range meta.Columns {
			if column != nil {
				keyColumns = append(keyColumns, column.Name)
			}
		}
	}
	for _, row := range rows {
		conditions := make([]string, 0, len(keyColumns))
		for _, column := range keyColumns {
			index := -1
			for candidate, name := range row.columns {
				if strings.EqualFold(strings.Trim(name, "` "), strings.Trim(column, "` ")) {
					index = candidate
					break
				}
			}
			if index < 0 || index >= len(row.values) {
				return fmt.Errorf("partition exchange key column %s is not present", column)
			}
			if row.values[index] == nil {
				conditions = append(conditions, quotePartitionIdentifier(column)+" IS NULL")
			} else {
				conditions = append(conditions, quotePartitionIdentifier(column)+" = "+partitionSQLLiteral(row.values[index]))
			}
		}
		deleteStmt, parseErr := sqlparser.Parse("delete from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table) + " where " + strings.Join(conditions, " and ") + " limit 1")
		if parseErr != nil {
			return parseErr
		}
		deleteQuery, ok := deleteStmt.(*sqlparser.Delete)
		if !ok {
			return fmt.Errorf("partition exchange cleanup is not DELETE")
		}
		if _, deleteErr := e.executeDeleteStatement(ctx, deleteQuery, schema, ctx.Session); deleteErr != nil {
			return deleteErr
		}
	}
	return nil
}

func (e *XMySQLExecutor) executeRawPartitionReorganize(ctx *ExecutionContext, match []string, databaseName string) (bool, error) {
	parts := strings.Split(match[1], ".")
	schema, table := databaseName, strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 2 {
		schema = strings.Trim(parts[0], "` ")
	}
	frmPath := filepath.Join(e.getDataDir(), schema, table+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	partitioning, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("table %s is not partitioned", table)
	}
	method, _ := partitioning["method"].(string)
	expression, _ := partitioning["expression"].(string)
	definitions, ok := partitioning["partitions"].([]interface{})
	if !ok || len(definitions) == 0 {
		return true, fmt.Errorf("partition metadata for %s is invalid", table)
	}
	oldNames := make(map[string]struct{})
	for _, rawName := range strings.Split(match[2], ",") {
		name := strings.ToLower(strings.Trim(strings.TrimSpace(rawName), "`"))
		if name == "" {
			return true, fmt.Errorf("REORGANIZE PARTITION requires a partition name")
		}
		oldNames[name] = struct{}{}
	}
	oldIndex := -1
	existingNames := make(map[string]struct{}, len(definitions))
	for index, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			return true, fmt.Errorf("partition metadata for %s is invalid", table)
		}
		name, _ := definition["name"].(string)
		nameKey := strings.ToLower(name)
		if _, duplicate := existingNames[nameKey]; duplicate {
			return true, fmt.Errorf("duplicate partition name %s", name)
		}
		existingNames[nameKey] = struct{}{}
		if _, selected := oldNames[nameKey]; selected && oldIndex < 0 {
			oldIndex = index
		}
	}
	if oldIndex < 0 || len(oldNames) == 0 {
		return true, fmt.Errorf("unknown partition %s", strings.Trim(match[2], "`"))
	}
	for oldName := range oldNames {
		if _, exists := existingNames[oldName]; !exists {
			return true, fmt.Errorf("unknown partition %s", oldName)
		}
	}

	clause := strings.TrimSpace(match[3])
	open := strings.Index(clause, "(")
	if open < 0 {
		return true, fmt.Errorf("REORGANIZE PARTITION requires replacement definitions")
	}
	close := matchingParenIndex(clause, open)
	if close < 0 || strings.TrimSpace(clause[close+1:]) != "" {
		return true, fmt.Errorf("REORGANIZE PARTITION has unbalanced parentheses")
	}
	replacements, err := parsePartitionDefinitions(clause[open+1:close], strings.ToUpper(method))
	if err != nil {
		return true, err
	}
	if len(replacements) == 0 {
		return true, fmt.Errorf("REORGANIZE PARTITION requires replacement definitions")
	}
	for _, replacement := range replacements {
		name, _ := replacement["name"].(string)
		nameKey := strings.ToLower(name)
		if _, replacingSameName := oldNames[nameKey]; !replacingSameName {
			if _, duplicate := existingNames[nameKey]; duplicate {
				return true, fmt.Errorf("duplicate partition name %s", name)
			}
		}
	}

	candidate := make([]interface{}, 0, len(definitions)-1+len(replacements))
	for index, raw := range definitions {
		definition, _ := raw.(map[string]interface{})
		name, _ := definition["name"].(string)
		_, selected := oldNames[strings.ToLower(name)]
		if index == oldIndex {
			for _, replacement := range replacements {
				candidate = append(candidate, replacement)
			}
			continue
		}
		if selected {
			continue
		}
		candidate = append(candidate, raw)
	}
	if strings.EqualFold(method, "RANGE") {
		if err := validateRangePartitionDefinitions(candidate); err != nil {
			return true, err
		}
	} else if strings.EqualFold(method, "LIST") {
		if err := validateListPartitionDefinitions(candidate); err != nil {
			return true, err
		}
	}
	for index, raw := range candidate {
		if definition, ok := raw.(map[string]interface{}); ok {
			definition["ordinal"] = index + 1
		}
	}
	candidatePartitioning := make(map[string]interface{}, len(partitioning))
	for key, value := range partitioning {
		candidatePartitioning[key] = value
	}
	candidatePartitioning["partitions"] = candidate
	if err := e.validateExistingRowsForPartitioning(ctx, schema, table, definitions, candidatePartitioning, oldNames, oldIndex, len(replacements)); err != nil {
		return true, err
	}
	rowsToMove, err := e.preparePartitionReorganization(ctx, schema, table, method, expression, definitions, oldNames)
	if err != nil {
		return true, err
	}
	partitioning["partitions"] = candidate
	tableInfo["partitioning"] = partitioning
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return true, err
	}
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.SetPartitioning(schema, table, partitioning); err != nil {
			return true, err
		}
	}
	if err := e.restorePartitionReorganizationRows(ctx, schema, table, rowsToMove); err != nil {
		return true, err
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partition metadata reorganized"}
	return true, nil
}

type partitionReorganizationRow struct {
	columns []string
	values  []interface{}
}

// preparePartitionReorganization makes the physical move explicit.  The
// source rows are deleted while the old partition mapping is still active;
// restorePartitionReorganizationRows inserts them after the new spaces and
// routing descriptor are installed.
func (e *XMySQLExecutor) preparePartitionReorganization(ctx *ExecutionContext, schema, table, method, expression string, definitions []interface{}, selectedNames map[string]struct{}) ([]partitionReorganizationRow, error) {
	selectStmt, err := sqlparser.Parse("select * from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table))
	if err != nil {
		return nil, err
	}
	selectQuery, ok := selectStmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("partition reorganization source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectQuery, schema)
	if err != nil {
		return nil, err
	}
	columnNames := append([]string(nil), result.Columns...)
	rows := make([]partitionReorganizationRow, 0)
	for _, record := range result.Records {
		values := record.GetValues()
		partitionValues, valueErr := partitionExpressionValuesFromRecord(columnNames, values, expression)
		if valueErr != nil {
			continue
		}
		partitionIndex, routeErr := partitionForValues(strings.ToUpper(method), partitionValues, definitions)
		if routeErr != nil {
			return nil, routeErr
		}
		if partitionIndex < 0 || partitionIndex >= len(definitions) {
			continue
		}
		definition, ok := definitions[partitionIndex].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("partition definition %d is invalid", partitionIndex+1)
		}
		name, _ := definition["name"].(string)
		if _, selected := selectedNames[strings.ToLower(name)]; !selected {
			continue
		}
		rowValues := make([]interface{}, len(columnNames))
		for index := range columnNames {
			if index < len(values) && !values[index].IsNull() {
				rowValues[index] = values[index].Raw()
			}
		}
		rows = append(rows, partitionReorganizationRow{columns: append([]string(nil), columnNames...), values: rowValues})
	}
	for _, row := range rows {
		conditions := make([]string, 0, len(row.columns))
		for index, column := range row.columns {
			value := row.values[index]
			if value == nil {
				conditions = append(conditions, quotePartitionIdentifier(column)+" IS NULL")
			} else {
				conditions = append(conditions, quotePartitionIdentifier(column)+" = "+partitionSQLLiteral(value))
			}
		}
		deleteSQL := "delete from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table) + " where " + strings.Join(conditions, " and ")
		deleteStmt, parseErr := sqlparser.Parse(deleteSQL)
		if parseErr != nil {
			return nil, parseErr
		}
		deleteQuery, ok := deleteStmt.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("partition reorganization delete is not DELETE")
		}
		if _, deleteErr := e.executeDeleteStatement(ctx, deleteQuery, schema, ctx.Session); deleteErr != nil {
			return nil, deleteErr
		}
	}
	return rows, nil
}

func (e *XMySQLExecutor) restorePartitionReorganizationRows(ctx *ExecutionContext, schema, table string, rows []partitionReorganizationRow) error {
	for _, row := range rows {
		literals := make([]string, len(row.values))
		for index, value := range row.values {
			if value == nil {
				literals[index] = "NULL"
			} else {
				literals[index] = partitionSQLLiteral(value)
			}
		}
		quotedColumns := make([]string, len(row.columns))
		for index, column := range row.columns {
			quotedColumns[index] = quotePartitionIdentifier(column)
		}
		insertSQL := "insert into " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table) + " (" + strings.Join(quotedColumns, ", ") + ") values (" + strings.Join(literals, ", ") + ")"
		insertStmt, err := sqlparser.Parse(insertSQL)
		if err != nil {
			return err
		}
		insertQuery, ok := insertStmt.(*sqlparser.Insert)
		if !ok {
			return fmt.Errorf("partition reorganization restore is not INSERT")
		}
		if _, err := e.executeInsertStatement(ctx, insertQuery, schema, ctx.Session); err != nil {
			return err
		}
	}
	return nil
}

func (e *XMySQLExecutor) validateExistingRowsForPartitioning(ctx *ExecutionContext, schema, table string, originalDefinitions []interface{}, partitioning map[string]interface{}, selectedNames map[string]struct{}, replacementStart, replacementCount int) error {
	if e == nil || ctx == nil {
		return nil
	}
	method, _ := partitioning["method"].(string)
	expression, _ := partitioning["expression"].(string)
	expression = strings.ToLower(strings.Trim(strings.TrimSpace(expression), "` "))
	definitions, _ := partitioning["partitions"].([]interface{})
	if expression == "" || len(definitions) == 0 {
		return fmt.Errorf("partition metadata for %s.%s is invalid", schema, table)
	}
	stmt, err := sqlparser.Parse("select * from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table))
	if err != nil {
		return err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("partition validation source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectStmt, schema)
	if err != nil {
		return err
	}
	for _, record := range result.Records {
		values := record.GetValues()
		partitionValues, valueErr := partitionExpressionValuesFromRecord(result.Columns, values, expression)
		if valueErr != nil {
			return fmt.Errorf("existing row cannot be mapped to partition expression %s: %w", expression, valueErr)
		}
		candidatePartition, err := partitionForValues(strings.ToUpper(method), partitionValues, definitions)
		if err != nil {
			return fmt.Errorf("existing row cannot be mapped after partition reorganization: %w", err)
		}
		originalPartition, err := partitionForValues(strings.ToUpper(method), partitionValues, originalDefinitions)
		if err != nil {
			return fmt.Errorf("existing row cannot be mapped before partition reorganization: %w", err)
		}
		if originalPartition >= 0 && originalPartition < len(originalDefinitions) {
			if originalDefinition, ok := originalDefinitions[originalPartition].(map[string]interface{}); ok {
				name, _ := originalDefinition["name"].(string)
				if _, selected := selectedNames[strings.ToLower(name)]; selected && (candidatePartition < replacementStart || candidatePartition >= replacementStart+replacementCount) {
					return fmt.Errorf("existing row from partition %s falls outside replacement partitions", name)
				}
			}
		}
	}
	return nil
}

func validateRangePartitionDefinitions(definitions []interface{}) error {
	var previous int64
	hasPrevious := false
	var previousTuple []interface{}
	seenMaxValue := false
	for _, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("partition metadata is invalid")
		}
		name, _ := definition["name"].(string)
		values, _ := definition["values"].(string)
		lowerValues := strings.ToLower(values)
		if seenMaxValue {
			return fmt.Errorf("MAXVALUE partition %s must be the last partition", name)
		}
		if strings.Contains(lowerValues, "maxvalue") {
			seenMaxValue = true
			continue
		}
		if bound, ok := firstPartitionNumber(values); ok {
			if len(previousTuple) > 0 {
				return fmt.Errorf("RANGE partition %s mixes scalar and tuple bounds", name)
			}
			if hasPrevious && bound <= previous {
				return fmt.Errorf("RANGE partition %s must have ascending bounds", name)
			}
			previous, hasPrevious = bound, true
			continue
		}
		bound, ok := partitionTupleBound(values)
		if !ok {
			return fmt.Errorf("RANGE partition %s has an invalid bound", name)
		}
		if hasPrevious {
			return fmt.Errorf("RANGE partition %s mixes scalar and tuple bounds", name)
		}
		if len(previousTuple) > 0 {
			comparison, err := comparePartitionTuples(previousTuple, bound)
			if err != nil || comparison >= 0 {
				return fmt.Errorf("RANGE partition %s must have ascending bounds", name)
			}
		}
		previousTuple = bound
	}
	return nil
}

func validateListPartitionDefinitions(definitions []interface{}) error {
	var tupleArity int
	hasTuple := false
	hasScalar := false
	seenTuples := make([][]interface{}, 0)
	seenScalars := make([]interface{}, 0)
	for _, raw := range definitions {
		definition, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("partition metadata is invalid")
		}
		name, _ := definition["name"].(string)
		values, _ := definition["values"].(string)
		lowerValues := strings.ToLower(values)
		at := strings.Index(lowerValues, "in")
		if at < 0 {
			return fmt.Errorf("LIST partition %s has an invalid definition", name)
		}
		if tuples := partitionListTupleBounds(values); len(tuples) > 0 {
			if hasScalar {
				return fmt.Errorf("LIST partition %s mixes scalar and tuple values", name)
			}
			hasTuple = true
			for _, tuple := range tuples {
				if tupleArity == 0 {
					tupleArity = len(tuple)
				} else if len(tuple) != tupleArity {
					return fmt.Errorf("LIST partition %s has inconsistent tuple arity", name)
				}
				for _, previous := range seenTuples {
					comparison, err := comparePartitionTuples(previous, tuple)
					if err != nil {
						return fmt.Errorf("LIST partition %s has incompatible tuple values: %w", name, err)
					}
					if comparison == 0 {
						return fmt.Errorf("LIST partition %s duplicates an existing tuple value", name)
					}
				}
				seenTuples = append(seenTuples, tuple)
			}
			continue
		}
		if hasTuple {
			return fmt.Errorf("LIST partition %s mixes scalar and tuple values", name)
		}
		hasScalar = true
		listText := strings.Trim(strings.TrimSpace(values[at+2:]), "()")
		for _, item := range splitTopLevelComma(listText) {
			item = strings.TrimSpace(item)
			value, ok := partitionPredicateLiteral(item)
			if !ok {
				return fmt.Errorf("LIST partition %s has an invalid value %s", name, item)
			}
			for _, previous := range seenScalars {
				comparison, err := comparePartitionTuples([]interface{}{previous}, []interface{}{value})
				if err != nil {
					return fmt.Errorf("LIST partition %s has incompatible values: %w", name, err)
				}
				if comparison == 0 {
					return fmt.Errorf("LIST partition %s duplicates an existing value", name)
				}
			}
			seenScalars = append(seenScalars, value)
		}
	}
	return nil
}

func (e *XMySQLExecutor) executeRawPartitionAdd(ctx *ExecutionContext, match []string, databaseName string) (bool, error) {
	parts := strings.Split(match[1], ".")
	schema, table := databaseName, strings.Trim(parts[len(parts)-1], "` ")
	if len(parts) == 2 {
		schema = strings.Trim(parts[0], "` ")
	}
	frmPath := filepath.Join(e.getDataDir(), schema, table+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return true, err
	}
	partitioning, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("table %s is not partitioned", table)
	}
	method, _ := partitioning["method"].(string)
	definitions, ok := partitioning["partitions"].([]interface{})
	if !ok || len(definitions) == 0 {
		return true, fmt.Errorf("partition metadata for %s is invalid", table)
	}
	clause := strings.TrimSpace(match[2])
	open := strings.Index(clause, "(")
	if open < 0 {
		return true, fmt.Errorf("ADD PARTITION requires partition definitions")
	}
	close := matchingParenIndex(clause, open)
	if close < 0 {
		return true, fmt.Errorf("ADD PARTITION has unbalanced parentheses")
	}
	added, err := parsePartitionDefinitions(clause[open+1:close], strings.ToUpper(method))
	if err != nil {
		return true, err
	}
	existingNames := make(map[string]struct{}, len(definitions))
	var previousRangeBound int64
	hasPreviousRangeBound := false
	for _, raw := range definitions {
		if definition, ok := raw.(map[string]interface{}); ok {
			name, _ := definition["name"].(string)
			existingNames[strings.ToLower(name)] = struct{}{}
			if strings.EqualFold(method, "RANGE") {
				values, _ := definition["values"].(string)
				if strings.Contains(strings.ToLower(values), "maxvalue") {
					return true, fmt.Errorf("cannot add a partition after MAXVALUE; use REORGANIZE PARTITION")
				}
				if bound, ok := firstPartitionNumber(values); ok {
					previousRangeBound, hasPreviousRangeBound = bound, true
				}
			}
		}
	}
	for _, definition := range added {
		name, _ := definition["name"].(string)
		if _, exists := existingNames[strings.ToLower(name)]; exists {
			return true, fmt.Errorf("duplicate partition name %s", name)
		}
		if strings.EqualFold(method, "RANGE") {
			values, _ := definition["values"].(string)
			if strings.Contains(strings.ToLower(values), "maxvalue") {
				return true, fmt.Errorf("cannot add a partition after MAXVALUE; use REORGANIZE PARTITION")
			}
			if bound, ok := firstPartitionNumber(values); ok {
				if hasPreviousRangeBound && bound <= previousRangeBound {
					return true, fmt.Errorf("RANGE partition %s must extend bounds in ascending order", name)
				}
				previousRangeBound, hasPreviousRangeBound = bound, true
			}
		}
		definition["ordinal"] = len(definitions) + 1
		definitions = append(definitions, definition)
		existingNames[strings.ToLower(name)] = struct{}{}
	}
	if strings.EqualFold(method, "RANGE") {
		candidate := make([]interface{}, len(definitions))
		copy(candidate, definitions)
		if err := validateRangePartitionDefinitions(candidate); err != nil {
			return true, err
		}
	} else if strings.EqualFold(method, "LIST") {
		candidate := make([]interface{}, len(definitions))
		copy(candidate, definitions)
		if err := validateListPartitionDefinitions(candidate); err != nil {
			return true, err
		}
	}
	partitioning["partitions"] = definitions
	tableInfo["partitioning"] = partitioning
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return true, err
	}
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.SetPartitioning(schema, table, partitioning); err != nil {
			return true, err
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: "partition metadata updated"}
	return true, nil
}

// deleteRowsFromDroppedPartitions makes DROP PARTITION match MySQL's data
// lifecycle contract: rows belonging to a removed partition must disappear,
// not merely become unreachable through metadata. The current storage layout
// is table-level, so this is implemented as a key-preserving delete pass over
// the table before the descriptor is committed.
func (e *XMySQLExecutor) deleteRowsFromDroppedPartitions(ctx *ExecutionContext, schema, table string, partitioning map[string]interface{}, dropped []interface{}) error {
	if len(dropped) == 0 || e == nil {
		return nil
	}
	method, _ := partitioning["method"].(string)
	expression, _ := partitioning["expression"].(string)
	definitions, _ := partitioning["partitions"].([]interface{})
	if strings.TrimSpace(expression) == "" || len(definitions) == 0 {
		return fmt.Errorf("partition metadata for %s.%s is invalid", schema, table)
	}
	meta, err := e.getShowColumnsTableMetadata(schema, table)
	if err != nil {
		return err
	}
	selectStmt, err := sqlparser.Parse("select * from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table))
	if err != nil {
		return err
	}
	selectQuery, ok := selectStmt.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("partition cleanup source is not SELECT")
	}
	result, err := e.executeSelectStatement(ctx, selectQuery, schema)
	if err != nil {
		return err
	}
	droppedIndexes := make(map[int]struct{}, len(dropped))
	for _, raw := range dropped {
		for index, definition := range definitions {
			if fmt.Sprint(definition) == fmt.Sprint(raw) {
				droppedIndexes[index] = struct{}{}
				break
			}
		}
	}
	columnIndex := make(map[string]int, len(result.Columns))
	for index, name := range result.Columns {
		columnIndex[strings.ToLower(strings.Trim(name, "` "))] = index
	}
	keyColumns := append([]string(nil), meta.PrimaryKey...)
	if len(keyColumns) == 0 {
		for _, column := range meta.Columns {
			if column != nil {
				keyColumns = append(keyColumns, column.Name)
			}
		}
	}
	for _, record := range result.Records {
		values := record.GetValues()
		partitionValues, valueErr := partitionExpressionValuesFromRecord(result.Columns, values, expression)
		if valueErr != nil {
			continue
		}
		partitionIndex, err := partitionForValues(strings.ToUpper(method), partitionValues, definitions)
		if err != nil {
			return err
		}
		if _, drop := droppedIndexes[partitionIndex]; !drop {
			continue
		}
		conditions := make([]string, 0, len(keyColumns))
		for _, column := range keyColumns {
			index, exists := columnIndex[strings.ToLower(column)]
			if !exists || index >= len(values) {
				return fmt.Errorf("partition cleanup key column %s is not present", column)
			}
			value := values[index].Raw()
			if value == nil {
				conditions = append(conditions, quotePartitionIdentifier(column)+" IS NULL")
			} else {
				conditions = append(conditions, quotePartitionIdentifier(column)+" = "+partitionSQLLiteral(value))
			}
		}
		deleteSQL := "delete from " + quotePartitionIdentifier(schema) + "." + quotePartitionIdentifier(table) + " where " + strings.Join(conditions, " and ")
		deleteStmt, err := sqlparser.Parse(deleteSQL)
		if err != nil {
			return err
		}
		deleteQuery, ok := deleteStmt.(*sqlparser.Delete)
		if !ok {
			return fmt.Errorf("partition cleanup statement is not DELETE")
		}
		if _, err := e.executeDeleteStatement(ctx, deleteQuery, schema, ctx.Session); err != nil {
			return err
		}
	}
	return nil
}

func quotePartitionIdentifier(value string) string {
	return "`" + strings.ReplaceAll(strings.Trim(value, "` "), "`", "``") + "`"
}

func partitionSQLLiteral(value interface{}) string {
	switch typed := value.(type) {
	case []byte:
		return partitionSQLLiteral(string(typed))
	case string:
		if _, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
			return strings.TrimSpace(typed)
		}
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func (e *XMySQLExecutor) persistPartitionMetadata(databaseName, tableName string, descriptor map[string]interface{}) error {
	frmPath := filepath.Join(e.getDataDir(), databaseName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return err
	}
	tableInfo["partitioning"] = descriptor
	if err := writeTableMetadataMapAtomic(frmPath, tableInfo); err != nil {
		return err
	}
	if e.tableStorageManager != nil {
		if err := e.tableStorageManager.SetPartitioning(databaseName, tableName, descriptor); err != nil {
			return err
		}
	}
	return nil
}

func parsePartitionedCreate(query string) (map[string]interface{}, string, bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "create table") {
		return nil, "", false, nil
	}
	partitionAt := strings.Index(lower, "partition by")
	if partitionAt < 0 {
		return nil, "", false, nil
	}
	baseQuery := strings.TrimSpace(trimmed[:partitionAt])
	clause := strings.TrimSpace(trimmed[partitionAt+len("partition by"):])
	openExpr := strings.Index(clause, "(")
	if openExpr < 0 {
		return nil, "", true, fmt.Errorf("partition expression is missing")
	}
	closeExpr := matchingParenIndex(clause, openExpr)
	if closeExpr < 0 {
		return nil, "", true, fmt.Errorf("partition expression has unbalanced parentheses")
	}
	methodAndExpression := strings.TrimSpace(clause[:openExpr])
	method := strings.Fields(methodAndExpression)
	if len(method) == 0 {
		return nil, "", true, fmt.Errorf("partition method is missing")
	}
	methodName := strings.ToUpper(method[0])
	if methodName != "RANGE" && methodName != "LIST" && methodName != "HASH" && methodName != "KEY" {
		return nil, "", true, fmt.Errorf("unsupported partition method %q", methodName)
	}
	expression := strings.TrimSpace(clause[openExpr+1 : closeExpr])
	definitionText := strings.TrimSpace(clause[closeExpr+1:])
	openDefs := strings.Index(definitionText, "(")
	if openDefs < 0 {
		return nil, "", true, fmt.Errorf("partition definitions are missing")
	}
	closeDefs := matchingParenIndex(definitionText, openDefs)
	if closeDefs < 0 {
		return nil, "", true, fmt.Errorf("partition definitions have unbalanced parentheses")
	}
	partitions, err := parsePartitionDefinitions(definitionText[openDefs+1:closeDefs], methodName)
	if err != nil {
		return nil, "", true, err
	}
	if methodName == "LIST" {
		definitions := make([]interface{}, len(partitions))
		for index := range partitions {
			definitions[index] = partitions[index]
		}
		if err := validateListPartitionDefinitions(definitions); err != nil {
			return nil, "", true, err
		}
	}
	descriptor := map[string]interface{}{
		"method":     methodName,
		"expression": expression,
		"partitions": partitions,
	}
	return descriptor, baseQuery, true, nil
}

func parsePartitionDefinitions(input, method string) ([]map[string]interface{}, error) {
	parts := splitTopLevelComma(input)
	if len(parts) == 0 {
		return nil, fmt.Errorf("partition definitions are empty")
	}
	definitionPattern := regexp.MustCompile(`(?is)^partition\s+([a-zA-Z0-9_$]+)\s+values\s+(.*)$`)
	result := make([]map[string]interface{}, 0, len(parts))
	for ordinal, raw := range parts {
		match := definitionPattern.FindStringSubmatch(strings.TrimSpace(raw))
		if len(match) != 3 {
			return nil, fmt.Errorf("invalid partition definition %q", strings.TrimSpace(raw))
		}
		values := strings.TrimSpace(match[2])
		entry := map[string]interface{}{
			"name":    match[1],
			"ordinal": ordinal + 1,
			"values":  values,
		}
		if method == "RANGE" && !strings.Contains(strings.ToLower(values), "less than") {
			return nil, fmt.Errorf("RANGE partition %q must use VALUES LESS THAN", match[1])
		}
		result = append(result, entry)
	}
	return result, nil
}

func (e *XMySQLExecutor) partitionRowsForTable(schemaName, tableName string) []map[string]interface{} {
	frmPath := filepath.Join(e.getDataDir(), schemaName, tableName+".frm")
	tableInfo, err := readTableMetadataMap(frmPath)
	if err != nil {
		return nil
	}
	partitioning, ok := tableInfo["partitioning"].(map[string]interface{})
	if !ok {
		return nil
	}
	method, _ := partitioning["method"].(string)
	expression, _ := partitioning["expression"].(string)
	definitions, _ := partitioning["partitions"].([]interface{})
	rows := make([]map[string]interface{}, 0, len(definitions))
	for ordinal, value := range definitions {
		definition, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := definition["name"].(string)
		values, _ := definition["values"].(string)
		position := ordinal + 1
		if raw, ok := definition["ordinal"].(float64); ok {
			position = int(raw)
		}
		rows = append(rows, map[string]interface{}{
			"PARTITION_NAME": name, "PARTITION_ORDINAL_POSITION": int64(position),
			"PARTITION_METHOD": method, "PARTITION_EXPRESSION": expression,
			"PARTITION_DESCRIPTION": partitionDescription(values),
		})
	}
	return rows
}

func partitionDescription(values string) interface{} {
	if strings.Contains(strings.ToLower(values), "maxvalue") {
		return "MAXVALUE"
	}
	open := strings.Index(values, "(")
	close := strings.LastIndex(values, ")")
	if open >= 0 && close > open {
		return strings.TrimSpace(values[open+1 : close])
	}
	return values
}
