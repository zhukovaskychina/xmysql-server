package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const temporaryTableSessionStateParam = "__xmysql_temporary_tables__"
const temporaryTableCreatePrivilegeBypassParam = "__xmysql_creating_temporary_table__"

var (
	temporaryTableSequence atomic.Uint64
)

type temporaryTableBinding struct {
	Database string
	Logical  string
	Physical string
	TableID  uint64
}

type temporaryTableSessionState struct {
	mu     sync.RWMutex
	tables map[string]temporaryTableBinding
}

func getTemporaryTableSessionState(session server.MySQLServerSession, create bool) *temporaryTableSessionState {
	if session == nil {
		return nil
	}
	if state, ok := session.GetParamByName(temporaryTableSessionStateParam).(*temporaryTableSessionState); ok && state != nil {
		return state
	}
	if !create {
		return nil
	}
	state := &temporaryTableSessionState{tables: make(map[string]temporaryTableBinding)}
	session.SetParamByName(temporaryTableSessionStateParam, state)
	return state
}

func temporaryTableKey(database, table string) string {
	return strings.ToLower(strings.TrimSpace(database) + "\x00" + strings.TrimSpace(table))
}

func (e *XMySQLExecutor) renameTemporaryTable(ctx *ExecutionContext, databaseName, sourceRaw, targetRaw string) (bool, error) {
	if ctx == nil || ctx.Session == nil {
		return false, nil
	}
	databaseName = temporaryTableDatabaseName(ctx, databaseName)
	sourceDB, sourceTable := compatibilityQualifiedTable(sourceRaw, databaseName)
	targetDB, targetTable := compatibilityQualifiedTable(targetRaw, databaseName)
	if sourceDB == "" || sourceTable == "" || targetDB == "" || targetTable == "" {
		return true, fmt.Errorf("RENAME TABLE requires valid source and target names")
	}
	state := getTemporaryTableSessionState(ctx.Session, false)
	if state == nil {
		return false, nil
	}
	sourceKey, targetKey := temporaryTableKey(sourceDB, sourceTable), temporaryTableKey(targetDB, targetTable)
	state.mu.Lock()
	binding, exists := state.tables[sourceKey]
	_, targetExists := state.tables[targetKey]
	if !exists {
		state.mu.Unlock()
		return false, nil
	}
	if !strings.EqualFold(sourceDB, targetDB) {
		state.mu.Unlock()
		return true, fmt.Errorf("temporary table rename across databases is unsupported")
	}
	if targetExists {
		state.mu.Unlock()
		return true, fmt.Errorf("temporary table '%s.%s' already exists", targetDB, targetTable)
	}
	delete(state.tables, sourceKey)
	binding.Database, binding.Logical = targetDB, targetTable
	state.tables[targetKey] = binding
	state.mu.Unlock()
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Temporary table '%s' renamed successfully", targetTable)}
	return true, nil
}

func parseTemporaryDropTargets(query, currentDB string, session server.MySQLServerSession) ([]string, bool, bool, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return nil, false, false, nil
	}
	statement, ok := parsed.AST.(*sqlparser.TableDDLStatement)
	if !ok || !strings.EqualFold(statement.Action, "drop") || len(statement.Targets) == 0 {
		return nil, false, false, nil
	}
	if !statement.Temporary {
		if session == nil {
			return nil, false, false, nil
		}
		for _, target := range statement.Targets {
			databaseName, tableName := target.Qualifier.String(), target.Name.String()
			if databaseName == "" {
				databaseName = currentDB
			}
			if databaseName == "" || tableName == "" || !isSessionTemporaryTable(session, databaseName, tableName) {
				return nil, false, false, nil
			}
		}
	}
	targets := make([]string, 0, len(statement.Targets))
	for _, target := range statement.Targets {
		name := target.Name.String()
		if target.Qualifier.String() != "" {
			name = target.Qualifier.String() + "." + name
		}
		if name == "" {
			return nil, false, true, fmt.Errorf("DROP TEMPORARY TABLE requires valid table names")
		}
		targets = append(targets, name)
	}
	return targets, statement.IfExists, true, nil
}

func (e *XMySQLExecutor) executeRawTemporaryTableCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	type temporaryCreateSpec struct {
		target      string
		ifNotExists bool
		definition  string
	}
	var createSpec *temporaryCreateSpec
	if parsed, err := sqlparser.ParseForExecution(query); err == nil {
		if statement, ok := parsed.AST.(*sqlparser.TableDDLStatement); ok && statement.Temporary && len(statement.Targets) == 1 {
			definition := statement.Definition
			if statement.AsSelect != nil {
				definition = ""
				if statement.Definition != "" {
					definition = "(" + statement.Definition + ") "
				}
				if statement.TableOptions != "" {
					definition += statement.TableOptions + " "
				}
				if statement.SelectMode != "" {
					definition += statement.SelectMode + " "
				}
				definition += "as " + statement.AsSelectSQL
			} else if statement.Like.Name.String() != "" {
				definition = "like " + sqlparser.String(statement.Like)
			} else if definition != "" {
				definition = "(" + definition + ")"
				if statement.TableOptions != "" {
					definition += " " + statement.TableOptions
				}
			}
			createSpec = &temporaryCreateSpec{target: sqlparser.String(statement.Targets[0]), ifNotExists: statement.IfNotExists, definition: strings.TrimSpace(definition)}
		}
	}
	if createSpec != nil {
		if ctx == nil || ctx.Session == nil {
			return true, fmt.Errorf("CREATE TEMPORARY TABLE requires a session")
		}
		databaseName = temporaryTableDatabaseName(ctx, databaseName)
		targetDB, targetTable := compatibilityQualifiedTable(createSpec.target, databaseName)
		if targetDB == "" || targetTable == "" {
			return true, fmt.Errorf("CREATE TEMPORARY TABLE requires a database and table name")
		}
		if err := e.checkDatabasePrivilege(ctx, targetDB, "CREATE TEMPORARY TABLES"); err != nil {
			return true, err
		}
		if err := e.validateDatabaseExists(targetDB); err != nil {
			return true, err
		}
		state := getTemporaryTableSessionState(ctx.Session, true)
		key := temporaryTableKey(targetDB, targetTable)
		state.mu.RLock()
		existing, exists := state.tables[key]
		state.mu.RUnlock()
		if exists {
			if createSpec.ifNotExists {
				ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' already exists", targetTable)}
				return true, nil
			}
			return true, fmt.Errorf("temporary table '%s.%s' already exists", targetDB, existing.Logical)
		}

		tableID := temporaryTableSequence.Add(1)
		physical := fmt.Sprintf("__xmysql_tmp_%d", tableID)
		createSQL := fmt.Sprintf("create table `%s` %s", physical, createSpec.definition)
		createSQL = e.rewriteTemporaryTableReferences(ctx.Session, createSQL)
		childResults := make(chan *Result, 1)
		child := *ctx
		child.Results = childResults
		child.DatabaseName = targetDB
		child.RawQuery = createSQL
		previousTemporaryCreate, _ := ctx.Session.GetParamByName(temporaryTableCreatePrivilegeBypassParam).(bool)
		ctx.Session.SetParamByName(temporaryTableCreatePrivilegeBypassParam, true)
		defer ctx.Session.SetParamByName(temporaryTableCreatePrivilegeBypassParam, previousTemporaryCreate)
		var handled bool
		var err error
		definition := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(createSpec.definition), ";"))
		if strings.HasPrefix(strings.ToLower(definition), "like ") {
			handled, err = e.executeRawCreateTableLikeCompatibility(&child, createSQL, targetDB)
		} else if createTableAsSelectAST(createSQL) != nil {
			handled, err = e.executeRawCreateTableAsSelectCompatibility(&child, createSQL, targetDB)
		} else {
			var stmt interface{}
			stmt, err = sqlparser.Parse(createSQL)
			var createDDL *sqlparser.DDL
			if err == nil {
				createDDL, _ = stmt.(*sqlparser.DDL)
				if createDDL == nil {
					err = fmt.Errorf("CREATE TEMPORARY TABLE definition did not produce CREATE TABLE AST")
				}
			}
			if err == nil {
				e.executeCreateTableStatement(&child, targetDB, createDDL)
				handled = true
			}
		}
		if !handled && err == nil {
			err = fmt.Errorf("unsupported CREATE TEMPORARY TABLE definition")
		}
		if err != nil {
			return true, err
		}
		created := <-childResults
		if created == nil || created.Err != nil {
			if created == nil {
				return true, fmt.Errorf("CREATE TEMPORARY TABLE returned no result")
			}
			return true, created.Err
		}
		state.mu.Lock()
		state.tables[key] = temporaryTableBinding{Database: targetDB, Logical: targetTable, Physical: physical, TableID: tableID}
		state.mu.Unlock()
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Temporary table '%s' created successfully", targetTable)}
		return true, nil
	}

	if parsed, err := sqlparser.ParseForExecution(query); err == nil {
		if statement, ok := parsed.AST.(*sqlparser.AlterTable); ok && len(statement.Operations) == 1 &&
			statement.Operations[0].Kind == sqlparser.AlterOperationRenameTable && statement.Operations[0].RenameTableTo.Name.String() != "" {
			return e.renameTemporaryTable(ctx, databaseName, sqlparser.String(statement.Table), sqlparser.String(statement.Operations[0].RenameTableTo))
		}
		if statement, ok := parsed.AST.(*sqlparser.TableDDLStatement); ok &&
			((strings.EqualFold(statement.Action, "rename") && len(statement.Targets) == 1 && len(statement.RenameTo) == 1) ||
				(strings.EqualFold(statement.Action, "alter") && len(statement.Targets) == 1 && len(statement.RenameTo) == 1)) {
			return e.renameTemporaryTable(ctx, databaseName, sqlparser.String(statement.Targets[0]), sqlparser.String(statement.RenameTo[0]))
		}
	}

	if targets, ifExists, matched, parseErr := parseTemporaryDropTargets(query, databaseName, sessionFromExecutionContext(ctx)); matched {
		if parseErr != nil {
			return true, parseErr
		}
		if ctx == nil || ctx.Session == nil {
			return true, fmt.Errorf("DROP TEMPORARY TABLE requires a session")
		}
		databaseName = temporaryTableDatabaseName(ctx, databaseName)
		state := getTemporaryTableSessionState(ctx.Session, false)
		if len(targets) == 0 {
			return true, fmt.Errorf("DROP TABLE requires at least one table name")
		}
		type temporaryDrop struct {
			key     string
			logical string
			binding temporaryTableBinding
		}
		drops := make([]temporaryDrop, 0, len(targets))
		missing := false
		for _, target := range targets {
			dropDB, dropTable := compatibilityQualifiedTable(target, databaseName)
			if dropDB == "" || dropTable == "" {
				return true, fmt.Errorf("DROP TABLE requires valid table names")
			}
			key := temporaryTableKey(dropDB, dropTable)
			var binding temporaryTableBinding
			var ok bool
			if state != nil {
				state.mu.RLock()
				binding, ok = state.tables[key]
				state.mu.RUnlock()
			}
			if !ok {
				missing = true
				if ifExists == false {
					return true, fmt.Errorf("temporary table '%s.%s' does not exist", dropDB, dropTable)
				}
				continue
			}
			drops = append(drops, temporaryDrop{key: key, logical: dropTable, binding: binding})
		}
		if !ifExists && missing {
			// A mixed ordinary DROP TABLE must remain available to the normal
			// DDL path; consuming it here could drop only the temporary subset.
			return false, nil
		}
		for _, item := range drops {
			dropSQL := fmt.Sprintf("drop table `%s`", item.binding.Physical)
			stmt, err := sqlparser.Parse(dropSQL)
			if err != nil {
				return true, fmt.Errorf("parse DROP TEMPORARY TABLE: %w", err)
			}
			dropDDL, ok := stmt.(*sqlparser.DDL)
			if !ok {
				return true, fmt.Errorf("DROP TEMPORARY TABLE did not produce DROP TABLE AST")
			}
			childResults := make(chan *Result, 1)
			child := *ctx
			child.Results = childResults
			child.DatabaseName = item.binding.Database
			child.RawQuery = dropSQL
			e.executeDropTableStatement(&child, item.binding.Database, dropDDL, ctx.Session)
			dropped := <-childResults
			if dropped == nil || dropped.Err != nil {
				if dropped == nil {
					return true, fmt.Errorf("DROP TEMPORARY TABLE returned no result")
				}
				return true, dropped.Err
			}
			state.mu.Lock()
			delete(state.tables, item.key)
			state.mu.Unlock()
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("%d temporary table(s) dropped successfully", len(drops))}
		return true, nil
	}

	return false, nil
}

func temporaryTableDatabaseName(ctx *ExecutionContext, databaseName string) string {
	if strings.TrimSpace(databaseName) != "" || ctx == nil || ctx.Session == nil {
		return databaseName
	}
	if current, ok := ctx.Session.GetParamByName("database").(string); ok {
		return strings.TrimSpace(current)
	}
	return databaseName
}

func (e *XMySQLExecutor) hasTemporaryTableFilesInDirectory(databaseName string) bool {
	entries, err := os.ReadDir(filepath.Join(e.getDataDir(), databaseName))
	if err != nil {
		return false
	}
	for _, entry := range entries {
		name := strings.ToLower(entry.Name())
		if strings.HasPrefix(name, "__xmysql_tmp_") && (strings.HasSuffix(name, ".frm") || strings.HasSuffix(name, ".ibd")) {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) visibleTemporaryTableNames(session server.MySQLServerSession, databaseName string, tables []string) []string {
	logicalTemporaryTables := make(map[string]string)
	state := getTemporaryTableSessionState(session, false)
	if state != nil {
		state.mu.RLock()
		for _, binding := range state.tables {
			if strings.EqualFold(binding.Database, databaseName) {
				logicalTemporaryTables[strings.ToLower(binding.Logical)] = binding.Logical
			}
		}
		state.mu.RUnlock()
	}
	visible := make([]string, 0, len(tables)+1)
	for _, table := range tables {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(table)), "__xmysql_tmp_") {
			continue
		}
		if _, shadowed := logicalTemporaryTables[strings.ToLower(strings.TrimSpace(table))]; shadowed {
			continue
		}
		visible = append(visible, table)
	}
	for _, logical := range logicalTemporaryTables {
		visible = append(visible, logical)
	}
	sort.Slice(visible, func(i, j int) bool { return strings.ToLower(visible[i]) < strings.ToLower(visible[j]) })
	return visible
}

func temporaryMetadataTableName(session server.MySQLServerSession, databaseName, tableName string) (string, bool) {
	state := getTemporaryTableSessionState(session, false)
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "__xmysql_tmp_") {
		if state == nil {
			return "", false
		}
		state.mu.RLock()
		defer state.mu.RUnlock()
		for _, binding := range state.tables {
			if strings.EqualFold(binding.Database, databaseName) && strings.EqualFold(binding.Physical, tableName) {
				return binding.Logical, true
			}
		}
		return "", false
	}
	if state != nil {
		state.mu.RLock()
		defer state.mu.RUnlock()
		for _, binding := range state.tables {
			if strings.EqualFold(binding.Database, databaseName) && strings.EqualFold(binding.Logical, tableName) {
				return "", false
			}
		}
	}
	return tableName, true
}

func temporaryPhysicalTableName(session server.MySQLServerSession, databaseName, tableName string) (string, bool) {
	state := getTemporaryTableSessionState(session, false)
	if state == nil {
		return tableName, false
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, binding := range state.tables {
		if strings.EqualFold(binding.Database, databaseName) && strings.EqualFold(binding.Logical, tableName) {
			return binding.Physical, true
		}
	}
	return tableName, false
}

func (e *XMySQLExecutor) rewriteTemporaryTableReferences(session server.MySQLServerSession, query string) string {
	state := getTemporaryTableSessionState(session, false)
	if state == nil || strings.TrimSpace(query) == "" {
		return query
	}
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	if strings.HasPrefix(lowerQuery, "show create table ") ||
		strings.HasPrefix(lowerQuery, "show index ") ||
		strings.HasPrefix(lowerQuery, "show indexes ") ||
		strings.HasPrefix(lowerQuery, "show keys ") {
		return query
	}
	state.mu.RLock()
	bindings := make([]temporaryTableBinding, 0, len(state.tables))
	for _, binding := range state.tables {
		bindings = append(bindings, binding)
	}
	state.mu.RUnlock()
	if len(bindings) == 0 {
		return query
	}
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil || parsed == nil {
		return query
	}
	switch parsed.Family {
	case sqlparser.StatementFamilySelect, sqlparser.StatementFamilyInsert,
		sqlparser.StatementFamilyUpdate, sqlparser.StatementFamilyDelete,
		sqlparser.StatementFamilyDDL, sqlparser.StatementFamilyAlter:
	default:
		return query
	}
	changed := false
	for _, binding := range bindings {
		changed = rewriteTemporaryTableAST(parsed.AST, binding) || changed
	}
	if !changed {
		return query
	}
	return sqlparser.String(parsed.AST)
}

func rewriteTemporaryTableAST(statement sqlparser.Statement, binding temporaryTableBinding) bool {
	if statement == nil || binding.Logical == "" || binding.Physical == "" {
		return false
	}
	changed := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliased, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok {
			return true, nil
		}
		table, ok := aliased.Expr.(sqlparser.TableName)
		if !ok || !temporaryTableNameMatches(table, binding) {
			return true, nil
		}
		table.Name = sqlparser.NewTableIdent(binding.Physical)
		aliased.Expr = table
		changed = true
		return true, nil
	}, statement)
	if insert, ok := statement.(*sqlparser.Insert); ok && temporaryTableNameMatches(insert.Table, binding) {
		insert.Table.Name = sqlparser.NewTableIdent(binding.Physical)
		changed = true
	}
	if ddl, ok := statement.(*sqlparser.DDL); ok {
		if temporaryTableNameMatches(ddl.Table, binding) {
			ddl.Table.Name = sqlparser.NewTableIdent(binding.Physical)
			changed = true
		}
		if temporaryTableNameMatches(ddl.NewName, binding) {
			ddl.NewName.Name = sqlparser.NewTableIdent(binding.Physical)
			changed = true
		}
	}
	if alter, ok := statement.(*sqlparser.AlterTable); ok && temporaryTableNameMatches(alter.Table, binding) {
		alter.Table.Name = sqlparser.NewTableIdent(binding.Physical)
		changed = true
	}
	if indexDDL, ok := statement.(*sqlparser.IndexDDLStatement); ok && temporaryTableNameMatches(indexDDL.Table, binding) {
		indexDDL.Table.Name = sqlparser.NewTableIdent(binding.Physical)
		changed = true
	}
	if tableDDL, ok := statement.(*sqlparser.TableDDLStatement); ok {
		for index := range tableDDL.Targets {
			if temporaryTableNameMatches(tableDDL.Targets[index], binding) {
				tableDDL.Targets[index].Name = sqlparser.NewTableIdent(binding.Physical)
				changed = true
			}
		}
		for index := range tableDDL.RenameTo {
			if temporaryTableNameMatches(tableDDL.RenameTo[index], binding) {
				tableDDL.RenameTo[index].Name = sqlparser.NewTableIdent(binding.Physical)
				changed = true
			}
		}
		if primary, ok := tableDDL.Primary.(*sqlparser.DDL); ok && temporaryTableNameMatches(primary.Table, binding) {
			primary.Table.Name = sqlparser.NewTableIdent(binding.Physical)
			changed = true
		}
		if primary, ok := tableDDL.Primary.(*sqlparser.AlterTable); ok && temporaryTableNameMatches(primary.Table, binding) {
			primary.Table.Name = sqlparser.NewTableIdent(binding.Physical)
			changed = true
		}
	}
	return changed
}

func temporaryTableNameMatches(table sqlparser.TableName, binding temporaryTableBinding) bool {
	if !strings.EqualFold(table.Name.String(), binding.Logical) {
		return false
	}
	if table.Qualifier.IsEmpty() {
		return true
	}
	return strings.EqualFold(table.Qualifier.String(), binding.Database)
}

// CleanupTemporaryTables removes the physical tables owned by a connection.
// It is used by COM_RESET_CONNECTION and by the network handlers on disconnect
// so session-local tables do not leak into the persistent catalog.
func (e *XMySQLExecutor) CleanupTemporaryTables(session server.MySQLServerSession) error {
	// Connection teardown and COM_RESET_CONNECTION both release session-owned
	// table locks.  FLUSH TABLES WITH READ LOCK is also session-owned, so it
	// must be released on the same cleanup path or a disconnected owner could
	// leave every subsequent writer blocked indefinitely.
	e.releaseGlobalReadLock(session)
	state := getTemporaryTableSessionState(session, false)
	if state == nil {
		return nil
	}
	state.mu.RLock()
	bindings := make(map[string]temporaryTableBinding, len(state.tables))
	for key, binding := range state.tables {
		bindings[key] = binding
	}
	state.mu.RUnlock()
	for key, binding := range bindings {
		if err := e.dropTableImpl(binding.Database, binding.Physical); err != nil {
			return fmt.Errorf("drop temporary table %s.%s: %w", binding.Database, binding.Logical, err)
		}
		state.mu.Lock()
		delete(state.tables, key)
		state.mu.Unlock()
	}
	session.SetParamByName(temporaryTableSessionStateParam, nil)
	return nil
}

// CleanupOrphanedTemporaryTables removes temporary-table files left by a
// process that terminated before its connection cleanup callback ran. MySQL
// temporary tables are connection-scoped and must not survive an engine
// restart, so only the reserved physical-name prefix is eligible here.
func (e *XMySQLExecutor) CleanupOrphanedTemporaryTables() error {
	for _, table := range e.scanFrmTables() {
		if !strings.HasPrefix(strings.ToLower(table.tableName), "__xmysql_tmp_") {
			continue
		}
		if err := e.dropTableImpl(table.schemaName, table.tableName); err != nil {
			return fmt.Errorf("drop orphaned temporary table %s.%s: %w", table.schemaName, table.tableName, err)
		}
	}
	return nil
}
