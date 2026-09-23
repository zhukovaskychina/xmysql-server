package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const commonCTASTableOptions = `(?:(?:engine\s*=\s*innodb|default\s+(?:character\s+set|charset)\s*=\s*[a-zA-Z0-9_]+|(?:character\s+set|charset)\s*=\s*[a-zA-Z0-9_]+|collate\s*=\s*[a-zA-Z0-9_]+|row_format\s*=\s*(?:dynamic|compact|compressed|redundant|default))\s+)*`

var createTableAsSelectPattern = regexp.MustCompile(`(?is)^\s*create\s+table\s+(if\s+not\s+exists\s+)?((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+(` + commonCTASTableOptions + `)(?:(ignore|replace)\s+)?(?:as\s+)?((?:select|with)\b.*)$`)
var createTableAsSelectExplicitPattern = regexp.MustCompile(`(?is)^\s*create\s+table\s+(if\s+not\s+exists\s+)?((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+(\(.*\))\s+(` + commonCTASTableOptions + `)(?:(ignore|replace)\s+)?(?:as\s+)?((?:select|with)\b.*)$`)
var createTableAsSelectDefinitionPattern = regexp.MustCompile(`(?is)^\s*` + commonCTASTableOptions + `(?:(?:ignore|replace)\s+)?(?:as\s+)?(?:select|with)\b`)

// executeRawCreateTableAsSelectCompatibility implements the common CTAS path
// that the legacy parser does not represent in its DDL AST. The source SELECT
// is executed first, its result schema is converted into an ordinary InnoDB
// table definition, and the materialized rows are inserted through the normal
// storage-integrated DML path.
func (e *XMySQLExecutor) executeRawCreateTableAsSelectCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	match := createTableAsSelectPattern.FindStringSubmatch(trimmed)
	explicitMatch := createTableAsSelectExplicitPattern.FindStringSubmatch(trimmed)
	if len(match) != 6 && len(explicitMatch) != 7 {
		return false, nil
	}
	tableOptions := ""
	mode := ""
	if len(explicitMatch) == 7 {
		match = []string{explicitMatch[0], explicitMatch[1], explicitMatch[2], explicitMatch[6]}
		tableOptions = strings.TrimSpace(explicitMatch[4])
		mode = strings.ToLower(strings.TrimSpace(explicitMatch[5]))
	} else {
		tableOptions = strings.TrimSpace(match[3])
		mode = strings.ToLower(strings.TrimSpace(match[4]))
		match = []string{match[0], match[1], match[2], match[5]}
	}
	explicitDefinitions := ""
	if len(explicitMatch) == 7 {
		explicitDefinitions = explicitMatch[3]
	}
	targetDB, targetTable := compatibilityQualifiedTable(match[2], databaseName)
	if targetDB == "" || targetTable == "" {
		return true, fmt.Errorf("CREATE TABLE AS SELECT requires a target table and database")
	}
	if err := e.validateDatabaseExists(targetDB); err != nil {
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

	sourceQuery := strings.TrimSpace(match[3])
	sourceCtx := *ctx
	var sourceResult *SelectResult
	var err error
	lowerSourceQuery := strings.ToLower(sourceQuery)
	if strings.HasPrefix(lowerSourceQuery, "with recursive") {
		sourceResults := make(chan *Result, 1)
		sourceCtx.Results = sourceResults
		sourceCtx.DatabaseName = databaseName
		sourceCtx.RawQuery = sourceQuery
		handled, sourceErr := e.executeCTECompatibility(&sourceCtx, sourceQuery, databaseName)
		if sourceErr != nil {
			return true, fmt.Errorf("execute CREATE TABLE AS SELECT recursive CTE source: %w", sourceErr)
		}
		if !handled {
			return true, fmt.Errorf("unsupported recursive CTE in CREATE TABLE AS SELECT source")
		}
		result := <-sourceResults
		if result == nil {
			return true, fmt.Errorf("recursive CTE source returned no result")
		}
		if result.Err != nil {
			return true, fmt.Errorf("execute recursive CTE source: %w", result.Err)
		}
		var ok bool
		sourceResult, ok = result.Data.(*SelectResult)
		if !ok {
			return true, fmt.Errorf("recursive CTE source did not return a SELECT result")
		}
	} else {
		if strings.HasPrefix(lowerSourceQuery, "with") {
			rewritten, rewriteErr := e.rewriteSimpleCTEQuery(sourceQuery)
			if rewriteErr != nil {
				return true, fmt.Errorf("rewrite CREATE TABLE AS SELECT CTE source: %w", rewriteErr)
			}
			if strings.TrimSpace(rewritten) == "" {
				return true, fmt.Errorf("unsupported non-recursive CTE in CREATE TABLE AS SELECT source")
			}
			sourceQuery = strings.TrimSpace(rewritten)
		}
		sourceResults := make(chan *Result, 1)
		sourceCtx.Results = sourceResults
		derivedHandled, derivedErr := e.executeDerivedTableCompatibility(&sourceCtx, sourceQuery, databaseName)
		if derivedErr != nil {
			return true, fmt.Errorf("execute CREATE TABLE AS SELECT derived source: %w", derivedErr)
		}
		if derivedHandled {
			result := <-sourceResults
			if result == nil {
				return true, fmt.Errorf("derived source returned no result")
			}
			if result.Err != nil {
				return true, fmt.Errorf("execute derived source: %w", result.Err)
			}
			var ok bool
			sourceResult, ok = result.Data.(*SelectResult)
			if !ok {
				return true, fmt.Errorf("derived source did not return a SELECT result")
			}
		} else {
			selectStmt, parseErr := sqlparser.Parse(sourceQuery)
			if parseErr != nil {
				return true, fmt.Errorf("parse CREATE TABLE AS SELECT source: %w", parseErr)
			}
			switch statement := selectStmt.(type) {
			case *sqlparser.Select:
				sourceResult, err = e.executeSelectStatement(&sourceCtx, statement, databaseName)
			case *sqlparser.Union:
				branches, unionAll, unionOK := splitUnionQuery(sourceQuery)
				if !unionOK {
					return true, fmt.Errorf("unsupported CREATE TABLE AS SELECT set operation")
				}
				sourceResult, err = e.executeUnionQuery(&sourceCtx, branches, unionAll, databaseName)
			default:
				if branches, operators, setOK := splitSetOperationQuery(sourceQuery); setOK && hasNonUnionSetOperator(operators) {
					sourceResult, err = e.executeMixedSetOperationQuery(&sourceCtx, branches, operators, databaseName)
				} else {
					return true, fmt.Errorf("CREATE TABLE AS SELECT source must be a SELECT")
				}
			}
		}
	}
	if err != nil {
		return true, fmt.Errorf("execute CREATE TABLE AS SELECT source: %w", err)
	}
	if sourceResult == nil || len(sourceResult.Columns) == 0 {
		return true, fmt.Errorf("CREATE TABLE AS SELECT source returned no columns")
	}

	columnNames := make([]string, len(sourceResult.Columns))
	columnTypes := make([]string, len(sourceResult.Columns))
	seenNames := make(map[string]int, len(columnNames))
	definitions := make([]string, len(columnNames))
	if explicitDefinitions != "" {
		fallbackColumns := parseCreateTableColumnsFallback("create table `" + strings.ReplaceAll(targetTable, "`", "``") + "` " + explicitDefinitions)
		if len(fallbackColumns) != len(columnNames) {
			return true, fmt.Errorf("CREATE TABLE AS SELECT target column count %d does not match source column count %d", len(fallbackColumns), len(columnNames))
		}
		for index, rawColumn := range fallbackColumns {
			name, _ := rawColumn["name"].(string)
			columnNames[index] = name
		}
		definitions = nil
	}
	for index, rawName := range sourceResult.Columns {
		if explicitDefinitions != "" {
			continue
		}
		name := strings.Trim(strings.TrimSpace(rawName), "`")
		if name == "" {
			name = fmt.Sprintf("column_%d", index+1)
		}
		key := strings.ToLower(name)
		if seen := seenNames[key]; seen > 0 {
			seen++
			name = fmt.Sprintf("%s_%d", name, seen)
			key = strings.ToLower(name)
		}
		seenNames[key]++
		columnNames[index] = name
		typeName := ""
		if index < len(sourceResult.ColumnTypes) {
			typeName = sourceResult.ColumnTypes[index]
		}
		columnTypes[index] = ctasSQLType(typeName, sourceResult.Records, index)
		definitions[index] = fmt.Sprintf("`%s` %s NULL", strings.ReplaceAll(name, "`", "``"), columnTypes[index])
	}

	createSQL := fmt.Sprintf("create table `%s` %s %s", strings.ReplaceAll(targetTable, "`", "``"), explicitDefinitions, tableOptions)
	if explicitDefinitions == "" {
		createSQL = fmt.Sprintf("create table `%s` (%s) %s", strings.ReplaceAll(targetTable, "`", "``"), strings.Join(definitions, ", "), tableOptions)
	}
	createStmt, err := sqlparser.Parse(createSQL)
	if err != nil {
		return true, fmt.Errorf("parse generated CREATE TABLE definition: %w", err)
	}
	createDDL, ok := createStmt.(*sqlparser.DDL)
	if !ok {
		return true, fmt.Errorf("generated CTAS definition did not produce CREATE TABLE AST")
	}
	createResults := make(chan *Result, 1)
	createCtx := *ctx
	createCtx.Results = createResults
	createCtx.DatabaseName = targetDB
	createCtx.RawQuery = createSQL
	e.executeCreateTableStatement(&createCtx, targetDB, createDDL)
	createResult := <-createResults
	if createResult.Err != nil {
		return true, createResult.Err
	}

	if len(sourceResult.Records) > 0 {
		values := make([]string, 0, len(sourceResult.Records))
		for _, record := range sourceResult.Records {
			rowValues := record.GetValues()
			literals := make([]string, len(columnNames))
			for index := range columnNames {
				if index >= len(rowValues) {
					literals[index] = "NULL"
					continue
				}
				literals[index] = correlatedValueSQLLiteral(rowValues[index])
			}
			values = append(values, "("+strings.Join(literals, ", ")+")")
		}
		insertVerb := "insert into"
		if mode == "replace" {
			insertVerb = "replace into"
		} else if mode == "ignore" {
			insertVerb = "insert ignore into"
		}
		insertSQL := fmt.Sprintf("%s `%s` (%s) values %s", insertVerb, strings.ReplaceAll(targetTable, "`", "``"), joinQuotedIdentifiers(columnNames), strings.Join(values, ", "))
		insertStmt, parseErr := sqlparser.Parse(insertSQL)
		if parseErr != nil {
			_ = e.dropTableImpl(targetDB, targetTable)
			return true, fmt.Errorf("parse CTAS materialization INSERT: %w", parseErr)
		}
		insert, insertOK := insertStmt.(*sqlparser.Insert)
		if !insertOK {
			_ = e.dropTableImpl(targetDB, targetTable)
			return true, fmt.Errorf("CTAS materialization did not produce INSERT AST")
		}
		if err := e.prepareTransactionalDML(ctx.Session); err != nil {
			_ = e.dropTableImpl(targetDB, targetTable)
			return true, err
		}
		if _, err := e.executeInsertStatement(ctx, insert, targetDB, ctx.Session); err != nil {
			_ = e.dropTableImpl(targetDB, targetTable)
			return true, fmt.Errorf("materialize CREATE TABLE AS SELECT rows: %w", err)
		}
	}

	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' created successfully", targetTable)}
	return true, nil
}

func joinQuotedIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return strings.Join(quoted, ", ")
}

func ctasSQLType(typeName string, records []Record, columnIndex int) string {
	upper := strings.ToUpper(strings.TrimSpace(typeName))
	if upper != "" {
		switch {
		case strings.Contains(upper, "INT") || upper == "YEAR" || upper == "BIT":
			return upper
		case strings.Contains(upper, "DECIMAL") || strings.Contains(upper, "NUMERIC"):
			return "DECIMAL(30, 10)"
		case strings.Contains(upper, "DOUBLE") || strings.Contains(upper, "FLOAT"):
			return "DOUBLE"
		case strings.Contains(upper, "DATE") || strings.Contains(upper, "TIME") || strings.Contains(upper, "TIMESTAMP"):
			return upper
		case strings.Contains(upper, "BLOB") || strings.Contains(upper, "BINARY"):
			return "BLOB"
		case strings.Contains(upper, "JSON"):
			return "JSON"
		default:
			return "VARCHAR(255)"
		}
	}
	for _, record := range records {
		values := record.GetValues()
		if columnIndex >= len(values) || values[columnIndex].IsNull() {
			continue
		}
		switch values[columnIndex].Type() {
		case 0, 1, 2, 3, 4:
			return "BIGINT"
		case 5, 6, 7:
			return "DOUBLE"
		default:
			return "VARCHAR(255)"
		}
	}
	return "VARCHAR(255)"
}
