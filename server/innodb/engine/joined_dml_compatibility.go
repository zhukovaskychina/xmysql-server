package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const joinedDMLInternalUpdateParam = "__xmysql_joined_dml_internal_update__"

func internalJoinedDMLUpdate(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	value, _ := session.GetParamByName(joinedDMLInternalUpdateParam).(bool)
	return value
}

// executeJoinedUpdateCompatibility materializes the target primary keys from
// the join result, then sends a single-target UPDATE through the normal
// storage-integrated transaction path. This keeps duplicate matches from
// updating a target row more than once and preserves the existing constraint,
// index, trigger, cascade, and replication handling.
func (e *XMySQLExecutor) executeJoinedUpdateCompatibility(ctx *ExecutionContext, stmt *sqlparser.Update, schemaName string, session server.MySQLServerSession) (*DMLResult, error) {
	target, err := joinedDMLTarget(stmt.TableExprs)
	if err != nil {
		return nil, err
	}
	needsSourceValues := joinedDMLHasSourceAssignments(stmt.Exprs, target)

	targetSchema := strings.TrimSpace(schemaName)
	targetTable := joinedDMLTableName(target)
	if qualifier := strings.TrimSpace(targetTable.Qualifier.String()); qualifier != "" {
		targetSchema = qualifier
	}
	targetSchema = e.resolveDmlSchema(ctx, schemaName, targetSchema)
	targetName := strings.TrimSpace(targetTable.Name.String())
	if err := e.checkTableOrColumnPrivileges(ctx, targetSchema, targetName, "UPDATE", updatePrivilegeColumns(stmt.Exprs)); err != nil {
		return nil, err
	}
	meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), targetSchema, targetName)
	if err != nil {
		return nil, fmt.Errorf("load UPDATE JOIN target metadata: %w", err)
	}
	identityColumns := joinedDMLIdentityColumns(meta)
	if len(identityColumns) == 0 {
		return nil, fmt.Errorf("UPDATE JOIN target table %s has no columns", targetName)
	}

	keyProjection := make([]string, 0, len(identityColumns))
	for index, column := range identityColumns {
		keyProjection = append(keyProjection, fmt.Sprintf("%s.%s AS __xmysql_join_key_%d", joinedDMLTargetReference(target), column, index))
	}
	if needsSourceValues {
		for index, assignment := range stmt.Exprs {
			keyProjection = append(keyProjection, fmt.Sprintf("%s AS __xmysql_join_value_%d", sqlparser.String(assignment.Expr), index))
		}
	}
	selectSQL := joinedDMLSelectSQL(keyProjection, stmt.TableExprs, stmt.Where, stmt.OrderBy, stmt.Limit)
	parsedSelect, err := parseJoinedDMLSelect(selectSQL)
	if err != nil {
		return nil, fmt.Errorf("build UPDATE JOIN source SELECT: %w", err)
	}
	selected, err := e.executeSelectStatement(&ExecutionContext{
		Context:      contextOrBackground(ctx),
		Session:      session,
		DatabaseName: targetSchema,
		RawQuery:     selectSQL,
	}, parsedSelect, targetSchema)
	if err != nil {
		return nil, fmt.Errorf("materialize UPDATE JOIN target rows: %w", err)
	}
	var updateSQL string
	if needsSourceValues {
		updateSQL, err = joinedDMLSourceValueUpdateSQL(target, stmt.Exprs, selected, identityColumns)
	} else {
		var predicate string
		predicate, err = joinedDMLKeyPredicate(selected, identityColumns)
		if err == nil {
			updateSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s", sqlparser.String(target), sqlparser.String(stmt.Exprs), predicate)
		}
	}
	if err != nil {
		return nil, err
	}
	parsed, err := sqlparser.Parse(updateSQL)
	if err != nil {
		return nil, fmt.Errorf("build single-target UPDATE JOIN plan: %w", err)
	}
	plainUpdate, ok := parsed.(*sqlparser.Update)
	if !ok {
		return nil, fmt.Errorf("build single-target UPDATE JOIN plan returned %T", parsed)
	}
	if session != nil {
		previousInternal, _ := session.GetParamByName(joinedDMLInternalUpdateParam).(bool)
		session.SetParamByName(joinedDMLInternalUpdateParam, true)
		defer session.SetParamByName(joinedDMLInternalUpdateParam, previousInternal)
	}
	return e.executeUpdateStatement(ctx, plainUpdate, targetSchema, session)
}

// executeJoinedDeleteCompatibility applies the same key materialization
// strategy to single-target DELETE JOIN/USING statements.
func (e *XMySQLExecutor) executeJoinedDeleteCompatibility(ctx *ExecutionContext, stmt *sqlparser.Delete, schemaName string, session server.MySQLServerSession) (*DMLResult, error) {
	if len(stmt.Targets) > 1 {
		return nil, fmt.Errorf("multi-target DELETE JOIN is not supported")
	}
	target, err := joinedDMLTarget(stmt.TableExprs)
	if err != nil {
		return nil, err
	}
	if len(stmt.Targets) == 1 && !joinedDMLTargetMatches(stmt.Targets[0], target) {
		return nil, fmt.Errorf("DELETE JOIN target %s is not the first joined table", stmt.Targets[0].Name.String())
	}

	targetSchema := strings.TrimSpace(schemaName)
	targetTable := joinedDMLTableName(target)
	if qualifier := strings.TrimSpace(targetTable.Qualifier.String()); qualifier != "" {
		targetSchema = qualifier
	}
	targetSchema = e.resolveDmlSchema(ctx, schemaName, targetSchema)
	targetName := strings.TrimSpace(targetTable.Name.String())
	if err := e.checkTablePrivilege(ctx, targetSchema, targetName, "DELETE"); err != nil {
		return nil, err
	}
	meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), targetSchema, targetName)
	if err != nil {
		return nil, fmt.Errorf("load DELETE JOIN target metadata: %w", err)
	}
	identityColumns := joinedDMLIdentityColumns(meta)
	if len(identityColumns) == 0 {
		return nil, fmt.Errorf("DELETE JOIN target table %s has no columns", targetName)
	}

	keyProjection := make([]string, 0, len(identityColumns))
	for index, column := range identityColumns {
		keyProjection = append(keyProjection, fmt.Sprintf("%s.%s AS __xmysql_join_key_%d", joinedDMLTargetReference(target), column, index))
	}
	selectSQL := joinedDMLSelectSQL(keyProjection, stmt.TableExprs, stmt.Where, stmt.OrderBy, stmt.Limit)
	parsedSelect, err := parseJoinedDMLSelect(selectSQL)
	if err != nil {
		return nil, fmt.Errorf("build DELETE JOIN source SELECT: %w", err)
	}
	selected, err := e.executeSelectStatement(&ExecutionContext{
		Context:      contextOrBackground(ctx),
		Session:      session,
		DatabaseName: targetSchema,
		RawQuery:     selectSQL,
	}, parsedSelect, targetSchema)
	if err != nil {
		return nil, fmt.Errorf("materialize DELETE JOIN target rows: %w", err)
	}
	predicate, err := joinedDMLKeyPredicate(selected, identityColumns)
	if err != nil {
		return nil, err
	}
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", sqlparser.String(targetTable), predicate)
	parsed, err := sqlparser.Parse(deleteSQL)
	if err != nil {
		return nil, fmt.Errorf("build single-target DELETE JOIN plan: %w", err)
	}
	plainDelete, ok := parsed.(*sqlparser.Delete)
	if !ok {
		return nil, fmt.Errorf("build single-target DELETE JOIN plan returned %T", parsed)
	}
	return e.executeDeleteStatement(ctx, plainDelete, targetSchema, session)
}

func joinedDMLTarget(tableExprs sqlparser.TableExprs) (*sqlparser.AliasedTableExpr, error) {
	if len(tableExprs) == 0 {
		return nil, fmt.Errorf("joined DML statement has no target table")
	}
	var walk func(sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, error)
	walk = func(expr sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, error) {
		switch value := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			if _, ok := value.Expr.(sqlparser.TableName); !ok {
				return nil, fmt.Errorf("joined DML target must be a base table, got %T", value.Expr)
			}
			return value, nil
		case *sqlparser.JoinTableExpr:
			return walk(value.LeftExpr)
		case *sqlparser.ParenTableExpr:
			if len(value.Exprs) == 0 {
				return nil, fmt.Errorf("joined DML target parentheses are empty")
			}
			return walk(value.Exprs[0])
		default:
			return nil, fmt.Errorf("joined DML target has unsupported table expression %T", expr)
		}
	}
	return walk(tableExprs[0])
}

func joinedDMLHasJoin(tableExprs sqlparser.TableExprs) bool {
	if len(tableExprs) > 1 {
		return true
	}
	var hasJoin func(sqlparser.TableExpr) bool
	hasJoin = func(expr sqlparser.TableExpr) bool {
		switch value := expr.(type) {
		case *sqlparser.JoinTableExpr:
			return true
		case *sqlparser.ParenTableExpr:
			for _, child := range value.Exprs {
				if hasJoin(child) {
					return true
				}
			}
		}
		return false
	}
	return len(tableExprs) == 1 && hasJoin(tableExprs[0])
}

func joinedDMLTargetReference(target *sqlparser.AliasedTableExpr) string {
	if alias := strings.TrimSpace(target.As.String()); alias != "" {
		return alias
	}
	return joinedDMLTableName(target).Name.String()
}

func joinedDMLTableName(target *sqlparser.AliasedTableExpr) sqlparser.TableName {
	if target == nil {
		return sqlparser.TableName{}
	}
	if table, ok := target.Expr.(sqlparser.TableName); ok {
		return table
	}
	return sqlparser.TableName{}
}

func joinedDMLTargetMatches(target sqlparser.TableName, expr *sqlparser.AliasedTableExpr) bool {
	return strings.EqualFold(strings.TrimSpace(target.Name.String()), strings.TrimSpace(joinedDMLTableName(expr).Name.String())) ||
		strings.EqualFold(strings.TrimSpace(target.Name.String()), strings.TrimSpace(expr.As.String()))
}

// joinedDMLIdentityColumns returns the narrowest logical row identity that can
// be reconstructed by the compatibility SELECT. Tables without an explicit
// primary key use every visible column and NULL-safe predicates. The storage
// layer still owns the hidden clustered key, but it is intentionally not part
// of the SQL-visible schema, so exposing it here would change MySQL semantics.
func joinedDMLIdentityColumns(meta *metadata.TableMeta) []string {
	if meta == nil {
		return nil
	}
	if primaryKeys := correlatedDMLPrimaryKeys(meta.PrimaryKey); len(primaryKeys) > 0 {
		return primaryKeys
	}
	columns := make([]string, 0, len(meta.Columns))
	for _, column := range meta.Columns {
		if column != nil && strings.TrimSpace(column.Name) != "" {
			columns = append(columns, column.Name)
		}
	}
	return columns
}

func joinedDMLHasSourceAssignments(exprs sqlparser.UpdateExprs, target *sqlparser.AliasedTableExpr) bool {
	allowed := strings.ToLower(joinedDMLTargetReference(target))
	tableName := strings.ToLower(joinedDMLTableName(target).Name.String())
	for _, assignment := range exprs {
		if assignment == nil || assignment.Expr == nil {
			continue
		}
		found := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			column, ok := node.(*sqlparser.ColName)
			if !ok || column == nil || column.Qualifier.IsEmpty() {
				return true, nil
			}
			qualifier := strings.ToLower(strings.TrimSpace(sqlparser.String(column.Qualifier)))
			if qualifier != allowed && qualifier != tableName {
				found = true
				return false, nil
			}
			return true, nil
		}, assignment.Expr)
		if found {
			return true
		}
	}
	return false
}

func joinedDMLSelectSQL(projection []string, tableExprs sqlparser.TableExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit) string {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(projection, ", "), sqlparser.String(tableExprs))
	if where != nil {
		query += " " + sqlparser.String(where)
	}
	if len(orderBy) > 0 {
		query += " " + sqlparser.String(orderBy)
	}
	if limit != nil {
		query += " " + sqlparser.String(limit)
	}
	return query
}

func parseJoinedDMLSelect(query string) (*sqlparser.Select, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("expected SELECT, got %T", statement)
	}
	return selectStmt, nil
}

func joinedDMLKeyPredicate(result *SelectResult, primaryKeys []string) (string, error) {
	keyCount := len(primaryKeys)
	if keyCount == 0 {
		return "", fmt.Errorf("joined DML key predicate has no primary key columns")
	}
	if result == nil || len(result.Records) == 0 {
		return "1 = 0", nil
	}
	rows, err := joinedDMLMaterializedRows(result, len(primaryKeys), 0)
	if err != nil {
		return "", err
	}
	keys := make([]string, 0, len(rows))
	seen := make(map[string]struct{})
	for _, row := range rows {
		terms := make([]string, 0, keyCount)
		identity := make([]string, 0, keyCount)
		for index := 0; index < keyCount; index++ {
			literal := joinedDMLValueLiteral(row.keys[index])
			terms = append(terms, fmt.Sprintf("%s <=> %s", primaryKeys[index], literal))
			identity = append(identity, literal)
		}
		key := strings.Join(identity, "|")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, "("+strings.Join(terms, " AND ")+")")
	}
	if len(keys) == 0 {
		return "1 = 0", nil
	}
	return strings.Join(keys, " OR "), nil
}

type joinedDMLMaterializedRow struct {
	keys   []basic.Value
	values []basic.Value
}

func joinedDMLMaterializedRows(result *SelectResult, keyCount, valueCount int) ([]joinedDMLMaterializedRow, error) {
	if result == nil || len(result.Records) == 0 {
		return nil, nil
	}
	rows := make([]joinedDMLMaterializedRow, 0, len(result.Records))
	for _, record := range result.Records {
		if record == nil {
			continue
		}
		values := record.GetValues()
		if len(values) < keyCount+valueCount {
			return nil, fmt.Errorf("joined DML projection returned %d values, expected at least %d", len(values), keyCount+valueCount)
		}
		rows = append(rows, joinedDMLMaterializedRow{
			keys:   append([]basic.Value(nil), values[:keyCount]...),
			values: append([]basic.Value(nil), values[keyCount:keyCount+valueCount]...),
		})
	}
	return rows, nil
}

func joinedDMLSourceValueUpdateSQL(target *sqlparser.AliasedTableExpr, exprs sqlparser.UpdateExprs, result *SelectResult, primaryKeys []string) (string, error) {
	rows, err := joinedDMLMaterializedRows(result, len(primaryKeys), len(exprs))
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return fmt.Sprintf("UPDATE %s SET %s WHERE 1 = 0", sqlparser.String(target), sqlparser.String(exprs)), nil
	}
	seen := make(map[string]struct{})
	unique := rows[:0]
	for _, row := range rows {
		identity := make([]string, 0, len(row.keys))
		for _, key := range row.keys {
			identity = append(identity, joinedDMLValueLiteral(key))
		}
		key := strings.Join(identity, "|")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
	}
	assignments := make([]string, 0, len(exprs))
	for assignmentIndex, assignment := range exprs {
		column := assignment.Name.Name.String()
		when := make([]string, 0, len(unique))
		for _, row := range unique {
			terms := make([]string, 0, len(primaryKeys))
			for keyIndex, primaryKey := range primaryKeys {
				terms = append(terms, fmt.Sprintf("%s <=> %s", primaryKey, joinedDMLValueLiteral(row.keys[keyIndex])))
			}
			when = append(when, fmt.Sprintf("WHEN %s THEN %s", strings.Join(terms, " AND "), joinedDMLValueLiteral(row.values[assignmentIndex])))
		}
		assignments = append(assignments, fmt.Sprintf("%s = CASE %s ELSE %s END", column, strings.Join(when, " "), column))
	}
	where, err := joinedDMLKeyPredicate(result, primaryKeys)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", sqlparser.String(target), strings.Join(assignments, ", "), where), nil
}

func joinedDMLValueLiteral(value basic.Value) string {
	if value == nil || value.IsNull() {
		return "NULL"
	}
	raw := value.Raw()
	switch typed := raw.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'"
	case bool:
		if typed {
			return "1"
		}
		return "0"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(typed)
	default:
		return strconv.Quote(fmt.Sprint(typed))
	}
}
