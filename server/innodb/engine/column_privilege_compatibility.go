package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type selectPrivilegeSource struct {
	schema    string
	table     string
	aliases   map[string]struct{}
	available map[string]struct{}
	required  map[string]struct{}
}

// checkSelectColumnPrivileges closes the gap between persisted column grants
// and query execution. A table-level SELECT grant still permits every column;
// when it is absent, every physical column referenced by the SELECT (including
// WHERE/ON/ORDER BY expressions and stars) must be covered by a column grant.
// Derived sources are deliberately left to their child SELECT, which keeps
// this check aligned with the executor's existing materialization boundaries.
func (e *XMySQLExecutor) checkSelectColumnPrivileges(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) error {
	return e.checkSelectColumnPrivilegesMode(ctx, stmt, databaseName, ctx != nil && ctx.ViewSecurityRequired)
}

func (e *XMySQLExecutor) checkSelectColumnPrivilegesForView(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) error {
	return e.checkSelectColumnPrivilegesMode(ctx, stmt, databaseName, true)
}

func (e *XMySQLExecutor) checkSelectColumnPrivilegesMode(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string, requireTableGrant bool) error {
	if ctx == nil || ctx.Session == nil || stmt == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	if strings.TrimSpace(user) == "" || strings.EqualFold(strings.TrimSpace(user), "root") {
		return nil
	}

	sources := make([]*selectPrivilegeSource, 0)
	for _, expression := range stmt.From {
		collectSelectPrivilegeSources(expression, databaseName, &sources)
	}
	if len(sources) == 0 {
		return nil
	}

	for _, source := range sources {
		if _, temporary := temporaryPhysicalTableName(ctx.Session, source.schema, source.table); temporary {
			continue
		}
		meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), source.schema, source.table)
		if err != nil || meta == nil {
			// Views, CTEs, and derived sources are checked by the path that
			// materializes their underlying SELECT. A missing physical table
			// will be reported by normal SELECT execution afterward.
			continue
		}
		for _, column := range meta.Columns {
			if column != nil {
				source.available[strings.ToLower(column.Name)] = struct{}{}
			}
		}
	}

	if err := collectSelectPrivilegeReferences(stmt, sources); err != nil {
		return err
	}
	for _, source := range sources {
		if len(source.required) == 0 {
			continue
		}
		if err := e.checkTablePrivilege(ctx, source.schema, source.table, "SELECT"); err == nil {
			continue
		}
		if !e.hasColumnSelectGrantForTable(ctx, source.schema, source.table) {
			if requireTableGrant {
				return fmt.Errorf("access denied: user lacks SELECT privilege on table '%s.%s'", source.schema, source.table)
			}
			continue
		}
		for column := range source.required {
			if err := e.checkColumnPrivilege(ctx, source.schema, source.table, column, "SELECT"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *XMySQLExecutor) hasColumnSelectGrantForTable(ctx *ExecutionContext, schema, table string) bool {
	if ctx == nil || ctx.Session == nil {
		return false
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return false
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return false
	}
	requestedSchema := strings.ToLower(strings.TrimSpace(schema))
	requestedTable := strings.ToLower(strings.TrimSpace(table))
	for scope, privileges := range effectiveAccountColumnGrants(file, *account, ctx.Session) {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(scope)), ".")
		if len(parts) != 3 || (parts[0] != "*" && parts[0] != requestedSchema) || (parts[1] != "*" && parts[1] != requestedTable) {
			continue
		}
		for _, privilege := range privileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "SELECT") || strings.EqualFold(strings.TrimSpace(privilege), "ALL") || strings.EqualFold(strings.TrimSpace(privilege), "ALL PRIVILEGES") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) checkTableOrColumnPrivileges(ctx *ExecutionContext, schema, table, privilege string, columns []string) error {
	tableErr := e.checkTablePrivilege(ctx, schema, table, privilege)
	if tableErr == nil || !e.hasColumnGrantForTable(ctx, schema, table, privilege) {
		return tableErr
	}
	if len(columns) == 0 {
		columns = e.tableColumnNames(schema, table)
	}
	if len(columns) == 0 {
		return tableErr
	}
	for _, column := range columns {
		if err := e.checkColumnPrivilege(ctx, schema, table, column, privilege); err != nil {
			return err
		}
	}
	return nil
}

func (e *XMySQLExecutor) hasColumnGrantForTable(ctx *ExecutionContext, schema, table, privilege string) bool {
	if ctx == nil || ctx.Session == nil {
		return false
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return false
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return false
	}
	requestedSchema := strings.ToLower(strings.TrimSpace(schema))
	requestedTable := strings.ToLower(strings.TrimSpace(table))
	for scope, privileges := range effectiveAccountColumnGrants(file, *account, ctx.Session) {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(scope)), ".")
		if len(parts) != 3 || (parts[0] != "*" && parts[0] != requestedSchema) || (parts[1] != "*" && parts[1] != requestedTable) {
			continue
		}
		for _, granted := range privileges {
			if strings.EqualFold(strings.TrimSpace(granted), privilege) || strings.EqualFold(strings.TrimSpace(granted), "ALL") || strings.EqualFold(strings.TrimSpace(granted), "ALL PRIVILEGES") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) tableColumnNames(schema, table string) []string {
	meta, err := (&SelectExecutor{}).loadTableMetaFromFrm(e.getDataDir(), schema, table)
	if err != nil || meta == nil {
		return nil
	}
	columns := make([]string, 0, len(meta.Columns))
	for _, column := range meta.Columns {
		if column != nil && strings.TrimSpace(column.Name) != "" {
			columns = append(columns, column.Name)
		}
	}
	return columns
}

func insertPrivilegeColumns(e *XMySQLExecutor, schema, table string, stmt *sqlparser.Insert) []string {
	if stmt != nil && len(stmt.Columns) > 0 {
		columns := make([]string, 0, len(stmt.Columns))
		for _, column := range stmt.Columns {
			columns = append(columns, column.String())
		}
		return columns
	}
	return e.tableColumnNames(schema, table)
}

func updatePrivilegeColumns(exprs sqlparser.UpdateExprs) []string {
	columns := make([]string, 0, len(exprs))
	for _, expression := range exprs {
		if expression != nil && expression.Name != nil {
			columns = append(columns, expression.Name.Name.String())
		}
	}
	return columns
}

func (e *XMySQLExecutor) checkUpdateReadColumnPrivileges(ctx *ExecutionContext, stmt *sqlparser.Update, schema, table string) error {
	if ctx == nil || ctx.Session == nil || stmt == nil || internalJoinedDMLUpdate(ctx.Session) || !e.hasColumnGrantForTable(ctx, schema, table, "UPDATE") {
		return nil
	}
	if e.checkTablePrivilege(ctx, schema, table, "SELECT") == nil {
		return nil
	}
	available := make(map[string]struct{})
	for _, column := range e.tableColumnNames(schema, table) {
		available[strings.ToLower(column)] = struct{}{}
	}
	aliases := map[string]struct{}{strings.ToLower(table): {}}
	if len(stmt.TableExprs) > 0 {
		if aliased, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr); ok && !aliased.As.IsEmpty() {
			aliases[strings.ToLower(aliased.As.String())] = struct{}{}
		}
	}
	required := map[string]struct{}{}
	collect := func(node sqlparser.SQLNode) error {
		if node == nil {
			return nil
		}
		return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			column, ok := node.(*sqlparser.ColName)
			if !ok || column == nil {
				return true, nil
			}
			if !column.Qualifier.IsEmpty() {
				if _, ok := aliases[strings.ToLower(column.Qualifier.Name.String())]; !ok {
					return true, nil
				}
			}
			name := strings.ToLower(strings.TrimSpace(column.Name.String()))
			if _, ok := available[name]; ok {
				required[name] = struct{}{}
			}
			return true, nil
		}, node)
	}
	for _, expression := range stmt.Exprs {
		if expression != nil {
			if err := collect(expression.Expr); err != nil {
				return err
			}
		}
	}
	if stmt.Where != nil {
		if err := collect(stmt.Where); err != nil {
			return err
		}
	}
	if stmt.OrderBy != nil {
		if err := collect(stmt.OrderBy); err != nil {
			return err
		}
	}
	for column := range required {
		if err := e.checkColumnPrivilege(ctx, schema, table, column, "SELECT"); err != nil {
			return err
		}
	}
	return nil
}

func collectSelectPrivilegeSources(expression sqlparser.TableExpr, databaseName string, sources *[]*selectPrivilegeSource) {
	switch value := expression.(type) {
	case *sqlparser.AliasedTableExpr:
		table, ok := value.Expr.(sqlparser.TableName)
		if !ok || table.IsEmpty() {
			return
		}
		schema := table.Qualifier.String()
		if schema == "" {
			schema = databaseName
		}
		source := &selectPrivilegeSource{
			schema:    strings.TrimSpace(schema),
			table:     strings.TrimSpace(table.Name.String()),
			aliases:   map[string]struct{}{},
			available: map[string]struct{}{},
			required:  map[string]struct{}{},
		}
		source.aliases[strings.ToLower(source.table)] = struct{}{}
		if !value.As.IsEmpty() {
			source.aliases[strings.ToLower(value.As.String())] = struct{}{}
		}
		*sources = append(*sources, source)
	case *sqlparser.JoinTableExpr:
		collectSelectPrivilegeSources(value.LeftExpr, databaseName, sources)
		collectSelectPrivilegeSources(value.RightExpr, databaseName, sources)
	case *sqlparser.ParenTableExpr:
		for _, nested := range value.Exprs {
			collectSelectPrivilegeSources(nested, databaseName, sources)
		}
	}
}

func collectSelectPrivilegeReferences(stmt *sqlparser.Select, sources []*selectPrivilegeSource) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch value := node.(type) {
		case *sqlparser.StarExpr:
			if value.TableName.IsEmpty() {
				for _, source := range sources {
					addAllSelectPrivilegeColumns(source)
				}
				return true, nil
			}
			for _, source := range sources {
				if selectPrivilegeSourceMatches(source, value.TableName) {
					addAllSelectPrivilegeColumns(source)
				}
			}
		case *sqlparser.ColName:
			column := strings.ToLower(strings.TrimSpace(value.Name.String()))
			if column == "" {
				return true, nil
			}
			matched := false
			if !value.Qualifier.IsEmpty() {
				for _, source := range sources {
					if selectPrivilegeSourceMatches(source, value.Qualifier) {
						source.required[column] = struct{}{}
						matched = true
					}
				}
			} else {
				for _, source := range sources {
					if _, exists := source.available[column]; exists {
						source.required[column] = struct{}{}
						matched = true
					}
				}
				if !matched && len(sources) == 1 {
					sources[0].required[column] = struct{}{}
				}
			}
		}
		return true, nil
	}, stmt)
}

func addAllSelectPrivilegeColumns(source *selectPrivilegeSource) {
	// The metadata loader populates source.columns before references are
	// collected. Keeping this helper a no-op for an empty map avoids treating
	// derived sources as physical tables.
	for column := range source.available {
		source.required[column] = struct{}{}
	}
}

func selectPrivilegeSourceMatches(source *selectPrivilegeSource, qualifier sqlparser.TableName) bool {
	if source == nil {
		return false
	}
	if !qualifier.Qualifier.IsEmpty() && !strings.EqualFold(source.schema, qualifier.Qualifier.String()) {
		return false
	}
	name := qualifier.Name.String()
	if name == "" {
		return false
	}
	_, ok := source.aliases[strings.ToLower(name)]
	return ok
}

func (e *XMySQLExecutor) checkColumnPrivilege(ctx *ExecutionContext, schema, table, column, privilege string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if _, temporary := temporaryPhysicalTableName(ctx.Session, schema, table); temporary {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountColumnGrants(file, *account, ctx.Session)
	requested := strings.TrimSpace(schema) + "." + strings.TrimSpace(table) + "." + strings.TrimSpace(column)
	for grantedScope, privileges := range grants {
		if !columnScopeCovers(requested, grantedScope) {
			continue
		}
		for _, granted := range privileges {
			if strings.EqualFold(strings.TrimSpace(granted), privilege) || strings.EqualFold(strings.TrimSpace(granted), "ALL") || strings.EqualFold(strings.TrimSpace(granted), "ALL PRIVILEGES") {
				return nil
			}
		}
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege on column '%s'", user, host, privilege, requested)
}

func effectiveAccountColumnGrants(file persistedAccountFile, account persistedAccount, session server.MySQLServerSession) map[string][]string {
	grants := make(map[string][]string, len(account.ColumnGrants))
	for scope, privileges := range account.ColumnGrants {
		grants[scope] = append([]string(nil), privileges...)
	}
	roles := append([]string(nil), account.Roles...)
	if session != nil {
		if active, ok := session.GetParamByName("active_roles").([]string); ok {
			roles = active
		}
	}
	visited := map[string]bool{}
	var mergeRole func(string, int)
	mergeRole = func(roleName string, depth int) {
		if depth > 16 || visited[strings.ToLower(roleName)] {
			return
		}
		visited[strings.ToLower(roleName)] = true
		parts := strings.SplitN(roleName, "@", 2)
		if len(parts) != 2 {
			return
		}
		for _, role := range file.Accounts {
			if !strings.EqualFold(role.User, parts[0]) || !strings.EqualFold(role.Host, parts[1]) {
				continue
			}
			for scope, privileges := range role.ColumnGrants {
				grants[scope] = appendUniqueStrings(grants[scope], privileges...)
			}
			for _, nested := range role.Roles {
				mergeRole(nested, depth+1)
			}
			break
		}
	}
	for _, role := range roles {
		mergeRole(role, 1)
	}
	return grants
}

func columnScopeCovers(requested, granted string) bool {
	requestedParts := strings.Split(strings.ToLower(strings.TrimSpace(requested)), ".")
	grantedParts := strings.Split(strings.ToLower(strings.TrimSpace(granted)), ".")
	if len(requestedParts) != 3 || len(grantedParts) != 3 {
		return strings.EqualFold(strings.TrimSpace(requested), strings.TrimSpace(granted))
	}
	for index := range requestedParts {
		if grantedParts[index] != "*" && grantedParts[index] != requestedParts[index] {
			return false
		}
	}
	return true
}
