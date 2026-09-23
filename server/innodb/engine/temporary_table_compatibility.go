package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	temporaryTableCreatePattern = regexp.MustCompile(`(?is)^\s*create\s+temporary\s+table\s+(if\s+not\s+exists\s+)?((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)\s+(.+?)\s*;?\s*$`)
	temporaryTableRenamePattern = regexp.MustCompile(`(?is)^\s*(?:alter\s+table\s+(.+?)\s+rename\s+to\s+(.+?)|rename\s+table\s+(.+?)\s+to\s+(.+?))\s*;?\s*$`)
	temporaryTableDropPattern   = regexp.MustCompile(`(?is)^\s*drop\s+(temporary\s+)?table\s+(if\s+exists\s+)?(.+?)\s*;?\s*$`)

	temporaryTableSequence atomic.Uint64
)

type temporaryTableBinding struct {
	Database string
	Logical  string
	Physical string
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

func (e *XMySQLExecutor) executeRawTemporaryTableCompatibility(ctx *ExecutionContext, query, databaseName string) (bool, error) {
	if match := temporaryTableCreatePattern.FindStringSubmatch(strings.TrimSpace(query)); len(match) == 4 {
		if ctx == nil || ctx.Session == nil {
			return true, fmt.Errorf("CREATE TEMPORARY TABLE requires a session")
		}
		databaseName = temporaryTableDatabaseName(ctx, databaseName)
		targetDB, targetTable := compatibilityQualifiedTable(match[2], databaseName)
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
			if strings.TrimSpace(match[1]) != "" {
				ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Table '%s' already exists", targetTable)}
				return true, nil
			}
			return true, fmt.Errorf("temporary table '%s.%s' already exists", targetDB, existing.Logical)
		}

		physical := fmt.Sprintf("__xmysql_tmp_%d", temporaryTableSequence.Add(1))
		createSQL := fmt.Sprintf("create table `%s` %s", physical, strings.TrimSpace(match[3]))
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
		definition := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(match[3]), ";"))
		if strings.HasPrefix(strings.ToLower(definition), "like ") {
			handled, err = e.executeRawCreateTableLikeCompatibility(&child, createSQL, targetDB)
		} else if createTableAsSelectDefinitionPattern.MatchString(definition) || strings.Contains(strings.ToLower(definition), ") as select ") || strings.Contains(strings.ToLower(definition), ") select ") || strings.Contains(strings.ToLower(definition), ") ignore as select ") || strings.Contains(strings.ToLower(definition), ") replace as select ") {
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
		state.tables[key] = temporaryTableBinding{Database: targetDB, Logical: targetTable, Physical: physical}
		state.mu.Unlock()
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Temporary table '%s' created successfully", targetTable)}
		return true, nil
	}

	if match := temporaryTableRenamePattern.FindStringSubmatch(strings.TrimSpace(query)); len(match) == 5 {
		if ctx == nil || ctx.Session == nil {
			return false, nil
		}
		sourceRaw, targetRaw := match[1], match[2]
		if sourceRaw == "" {
			sourceRaw, targetRaw = match[3], match[4]
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
		sourceKey := temporaryTableKey(sourceDB, sourceTable)
		targetKey := temporaryTableKey(targetDB, targetTable)
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
		binding.Database = targetDB
		binding.Logical = targetTable
		state.tables[targetKey] = binding
		state.mu.Unlock()
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: fmt.Sprintf("Temporary table '%s' renamed successfully", targetTable)}
		return true, nil
	}

	if match := temporaryTableDropPattern.FindStringSubmatch(strings.TrimSpace(query)); len(match) == 4 {
		if ctx == nil || ctx.Session == nil {
			if strings.TrimSpace(match[1]) != "" {
				return true, fmt.Errorf("DROP TEMPORARY TABLE requires a session")
			}
			return false, nil
		}
		databaseName = temporaryTableDatabaseName(ctx, databaseName)
		state := getTemporaryTableSessionState(ctx.Session, false)
		targets := splitTopLevelComma(match[3])
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
				if strings.TrimSpace(match[1]) != "" && strings.TrimSpace(match[2]) == "" {
					return true, fmt.Errorf("temporary table '%s.%s' does not exist", dropDB, dropTable)
				}
				continue
			}
			drops = append(drops, temporaryDrop{key: key, logical: dropTable, binding: binding})
		}
		if strings.TrimSpace(match[1]) == "" && missing {
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
	rewritten := query
	for _, binding := range bindings {
		logical := regexp.QuoteMeta(binding.Logical)
		database := regexp.QuoteMeta(binding.Database)
		pattern := regexp.MustCompile(`(?is)(\b(?:from|join|update|into|table)\s+)(?:(?:` + "`?" + database + "`?" + `)\s*\.\s*)?(?:` + "`?" + logical + "`?" + `)(\s|,|\)|;|$)`)
		rewritten = pattern.ReplaceAllString(rewritten, `${1}`+binding.Physical+`${2}`)
		qualifierPattern := regexp.MustCompile(`(?is)(^|[^a-zA-Z0-9_$])(?:` + "`?" + logical + "`?" + `)\s*\.`)
		rewritten = qualifierPattern.ReplaceAllString(rewritten, `${1}`+binding.Physical+`. `)
	}
	return rewritten
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
