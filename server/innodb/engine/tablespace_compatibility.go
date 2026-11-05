package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// MySQL exposes general tablespace DDL separately from CREATE TABLE. The
// yacc parser intentionally does not own these variants, so keep a narrow raw
// compatibility path around the storage manager's real tablespace lifecycle.
// DATAFILE and ENGINE clauses are accepted for Connector/J and common schema
// tooling; the embedded engine chooses its configured data directory and
// native .ibd naming.
func (e *XMySQLExecutor) executeTablespaceCompatibility(ctx *ExecutionContext, query string) (bool, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if trimmed == "" {
		return false, nil
	}
	var tablespace *sqlparser.TablespaceStatement
	if parsed, parseErr := sqlparser.ParseForExecution(trimmed); parseErr == nil && parsed != nil {
		tablespace, _ = parsed.AST.(*sqlparser.TablespaceStatement)
	}
	// Tablespace operations are DDL in MySQL and therefore end the current
	// transaction before changing tablespace metadata or ownership. Temporary
	// tables do not enter this raw path, so their special non-commit behavior is
	// preserved by the temporary-table compatibility handler.
	if tablespace != nil || tableTablespaceTransferStatement(trimmed) != nil || isAlterTableTablespaceQuery(trimmed) {
		if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
			return true, err
		}
	}
	var releaseTableLock func()
	if tableTablespaceTransferStatement(trimmed) != nil || isAlterTableTablespaceQuery(trimmed) {
		if table, ok := alterTableAccess(trimmed, ctxDatabaseName(ctx)); ok && !sessionHoldsTableLock(ctx.Session, table) {
			var err error
			releaseTableLock, err = e.acquireDDLWriteLock(ctx, table, false)
			if err != nil {
				return true, tableLockWaitError(err)
			}
			defer releaseTableLock()
		}
	}
	if handled, err := e.executeRawTableTablespaceTransfer(ctx, query, ctxDatabaseName(ctx)); handled {
		return true, err
	}
	if handled, err := e.executeRawTableTablespaceCompatibility(ctx, query, ctxDatabaseName(ctx)); handled {
		return true, err
	}

	if tablespace != nil && strings.EqualFold(tablespace.Action, "create") {
		ifNotExists := tablespace.IfNotExists
		name := unquoteTablespaceIdentifier(tablespace.Name)
		if name == "" {
			return true, fmt.Errorf("invalid tablespace name")
		}
		options := strings.TrimSpace(tablespace.Options)
		if options != "" && !validTablespaceCreateOptions(options) {
			return true, fmt.Errorf("unsupported CREATE TABLESPACE options: %s", options)
		}
		if e.storageManager == nil {
			return true, fmt.Errorf("storage manager is not available")
		}
		if _, err := e.storageManager.GetTablespace(name); err == nil && !ifNotExists {
			return true, fmt.Errorf("tablespace %s already exists", name)
		}
		if _, err := e.storageManager.CreateTablespace(name); err != nil {
			return true, err
		}
		return true, writeTablespaceResult(ctx, fmt.Sprintf("Tablespace '%s' created successfully", name))
	}

	if tablespace != nil && strings.EqualFold(tablespace.Action, "alter") {
		oldName := unquoteTablespaceIdentifier(tablespace.Name)
		newName := unquoteTablespaceIdentifier(tablespace.RenameTo)
		if oldName == "" || newName == "" {
			return true, fmt.Errorf("invalid tablespace name")
		}
		if e.storageManager == nil {
			return true, fmt.Errorf("storage manager is not available")
		}
		if err := e.storageManager.RenameTablespace(oldName, newName); err != nil {
			return true, err
		}
		return true, writeTablespaceResult(ctx, fmt.Sprintf("Tablespace '%s' renamed to '%s'", oldName, newName))
	}

	if tablespace != nil && strings.EqualFold(tablespace.Action, "drop") {
		name := unquoteTablespaceIdentifier(tablespace.Name)
		if name == "" {
			return true, fmt.Errorf("invalid tablespace name")
		}
		if e.storageManager == nil {
			return true, fmt.Errorf("storage manager is not available")
		}
		if err := e.storageManager.DropTablespace(name); err != nil {
			return true, err
		}
		return true, writeTablespaceResult(ctx, fmt.Sprintf("Tablespace '%s' dropped successfully", name))
	}

	return false, nil
}

func tableOptionTablespace(query string) string {
	return unquoteTablespaceIdentifier(scanTablespaceName(query))
}

func validTablespaceCreateOptions(options string) bool {
	// A native datafile path cannot be honored safely on every host. Accept the
	// clause as metadata-compatible syntax, while rejecting unrelated options
	// instead of silently changing their meaning.
	return validTablespaceOptionText(options)
}

func unquoteTablespaceIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`")
}

func writeTablespaceResult(ctx *ExecutionContext, message string) error {
	if ctx == nil || ctx.Results == nil {
		return nil
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_DDL, Message: message}
	return nil
}
