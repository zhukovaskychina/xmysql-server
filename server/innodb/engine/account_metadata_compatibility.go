package engine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func (e *XMySQLExecutor) executeInformationSchemaColumnPrivilegesSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err == nil {
		for _, account := range file.Accounts {
			if !informationSchemaPrivilegeAccountVisible(file, session, account) {
				continue
			}
			for scope, privileges := range account.ColumnGrants {
				parts := strings.Split(scope, ".")
				if len(parts) != 3 || !metadataPatternMatches(parts[0], filters["table_schema"]) || !metadataPatternMatches(parts[1], filters["table_name"]) || !metadataPatternMatches(parts[2], filters["column_name"]) {
					continue
				}
				for _, privilege := range privileges {
					if strings.EqualFold(privilege, "GRANT OPTION") {
						continue
					}
					values := map[string]interface{}{"GRANTEE": fmt.Sprintf("'%s'@'%s'", account.User, account.Host), "TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1], "COLUMN_NAME": parts[2], "PRIVILEGE_TYPE": strings.ToUpper(privilege), "IS_GRANTABLE": informationSchemaGrantable(account.Grants[parts[0]+"."+parts[1]])}
					if !informationSchemaPrivilegeQueryMatches(query, values) {
						continue
					}
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_column_privileges", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaTablePrivilegesSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err == nil {
		for _, account := range file.Accounts {
			if !informationSchemaPrivilegeAccountVisible(file, session, account) {
				continue
			}
			for scope, privileges := range account.Grants {
				parts := strings.Split(scope, ".")
				if len(parts) != 2 || parts[1] == "*" || !metadataPatternMatches(parts[0], filters["table_schema"]) || !metadataPatternMatches(parts[1], filters["table_name"]) {
					continue
				}
				for _, privilege := range privileges {
					if strings.EqualFold(privilege, "GRANT OPTION") {
						continue
					}
					values := map[string]interface{}{"GRANTEE": fmt.Sprintf("'%s'@'%s'", account.User, account.Host), "TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1], "PRIVILEGE_TYPE": strings.ToUpper(privilege), "IS_GRANTABLE": informationSchemaGrantable(privileges)}
					if !informationSchemaPrivilegeQueryMatches(query, values) {
						continue
					}
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_table_privileges", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaSchemaPrivilegesSelect(query string, session server.MySQLServerSession) *SelectResult {
	defaults := []string{"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := informationSchemaMetadataFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err == nil {
		for _, account := range file.Accounts {
			if !informationSchemaPrivilegeAccountVisible(file, session, account) {
				continue
			}
			for scope, privileges := range account.Grants {
				parts := strings.Split(scope, ".")
				if len(parts) != 2 || parts[1] != "*" || !metadataPatternMatches(parts[0], filters["table_schema"]) {
					continue
				}
				for _, privilege := range privileges {
					if strings.EqualFold(privilege, "GRANT OPTION") {
						continue
					}
					values := map[string]interface{}{"GRANTEE": fmt.Sprintf("'%s'@'%s'", account.User, account.Host), "TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "PRIVILEGE_TYPE": strings.ToUpper(privilege), "IS_GRANTABLE": informationSchemaGrantable(privileges)}
					if !informationSchemaPrivilegeQueryMatches(query, values) {
						continue
					}
					rows = append(rows, projectInformationSchemaRow(columns, values))
				}
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_schema_privileges", columns, rows)
}

func (e *XMySQLExecutor) executeInformationSchemaUserPrivilegesSelect(query string, session server.MySQLServerSession) *SelectResult {
	columns := requestedInformationSchemaColumns(query, []string{"GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"})
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err == nil {
		for _, account := range file.Accounts {
			if !informationSchemaPrivilegeAccountVisible(file, session, account) {
				continue
			}
			privileges := append([]string(nil), account.Grants["*.*"]...)
			privileges = appendUniqueStrings(privileges, account.GlobalGrants...)
			for _, privilege := range privileges {
				if strings.EqualFold(privilege, "GRANT OPTION") {
					continue
				}
				values := map[string]interface{}{"GRANTEE": fmt.Sprintf("'%s'@'%s'", account.User, account.Host), "TABLE_CATALOG": "def", "PRIVILEGE_TYPE": strings.ToUpper(privilege), "IS_GRANTABLE": informationSchemaGrantable(privileges)}
				if !informationSchemaPrivilegeQueryMatches(query, values) {
					continue
				}
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
		}
	}
	return newInformationSchemaSelectResult("information_schema_user_privileges", columns, rows)
}

var informationSchemaPrivilegeFilterPattern = regexp.MustCompile(`(?i)\b(grantee|table_catalog|table_schema|table_name|column_name|privilege_type|is_grantable)\b\s*(?:=|like)\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`)

// informationSchemaPrivilegeQueryMatches applies the column predicates that
// are common to the privilege views.  Keeping this separate from the object
// metadata filters matters because USER_PRIVILEGES has no table or schema
// columns, while callers still expect predicates such as PRIVILEGE_TYPE and
// GRANTEE to be evaluated before rows are projected.
func informationSchemaPrivilegeQueryMatches(query string, values map[string]interface{}) bool {
	for _, match := range informationSchemaPrivilegeFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		pattern := match[2]
		if pattern == "" {
			pattern = match[3]
			pattern = strings.ReplaceAll(pattern, `""`, `"`)
		} else {
			pattern = strings.ReplaceAll(pattern, "''", "'")
		}
		if !metadataPatternMatches(fmt.Sprint(values[field]), pattern) {
			return false
		}
	}
	return true
}

// informationSchemaPrivilegeAccountVisible applies the account-level visibility
// boundary shared by the INFORMATION_SCHEMA privilege views. A server-side
// or replication projection has no user session and may inspect all accounts.
// An ordinary session may inspect its own account, while a session with
// SELECT/UPDATE on mysql.user (including through an enabled role), or the
// CREATE USER + SYSTEM_USER administrator combination, may inspect all grant
// rows. This mirrors the account metadata visibility boundary without
// treating a role's object grants as direct grants of the current account.
func informationSchemaPrivilegeAccountVisible(file persistedAccountFile, session server.MySQLServerSession, target persistedAccount) bool {
	if session == nil || sessionBoolParam(session, "replication_replay") {
		return true
	}
	user, _ := session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" {
		return false
	}
	if strings.EqualFold(user, "root") {
		return true
	}
	current := sessionAccount(file, session)
	if current == nil {
		return false
	}
	if strings.EqualFold(current.User, target.User) && strings.EqualFold(current.Host, target.Host) {
		return true
	}
	effective := effectiveAccountGrants(file, *current, session)
	if grantsContainExactScope(effective, "mysql.user", "SELECT") || grantsContainExactScope(effective, "mysql.user", "UPDATE") {
		return true
	}
	return grantsContain(effective, "*.*", "CREATE USER") && grantsContain(effective, "*.*", "SYSTEM_USER")
}

func grantsContainExactScope(grants map[string][]string, scope, wanted string) bool {
	if partialRevokeDenied(grants, scope, wanted) {
		return false
	}
	for grantedScope, privileges := range grants {
		if strings.HasPrefix(grantedScope, partialRevokeScopePrefix) || !strings.EqualFold(grantedScope, scope) {
			continue
		}
		for _, privilege := range privileges {
			if strings.EqualFold(privilege, wanted) || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) executeMySQLProcsPrivSelect(query string) *SelectResult {
	columns := requestedInformationSchemaColumns(query, jdbcMySQLProcsPrivMetadataColumns())
	filters := map[string]string{}
	filterPattern := regexp.MustCompile(`(?i)\b(db|routine_name|host|user)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
	for _, match := range filterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToLower(match[1])] = value
	}
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql_procs_priv", columns, nil)
	}
	rows := make([][]interface{}, 0)
	for _, account := range file.Accounts {
		if !metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) {
			continue
		}
		for scope, privileges := range account.Grants {
			parts := strings.Split(scope, ".")
			if len(parts) != 2 || parts[0] == "*" || parts[1] == "*" ||
				!metadataFilterMatches(parts[0], filters["db"]) || !metadataFilterMatches(parts[1], filters["routine_name"]) {
				continue
			}
			values := map[string]interface{}{
				"HOST": account.Host, "USER": account.User, "DB": parts[0], "ROUTINE_NAME": parts[1],
				"PROC_PRIV": strings.Join(privileges, ","), "IS_PROC": "1",
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql_procs_priv", columns, rows)
}

func (e *XMySQLExecutor) executeMySQLProxiesPrivSelect(query string) *SelectResult {
	defaults := []string{"Host", "User", "Proxied_host", "Proxied_user", "With_grant", "Grantor", "Timestamp"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql_proxies_priv", columns, nil)
	}
	proxyTargetPattern := regexp.MustCompile(`(?is)^'([^']*)'\s*@\s*'([^']*)'$`)
	for _, account := range file.Accounts {
		if !metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) {
			continue
		}
		for scope, privileges := range account.Grants {
			if !containsGrantPrivilege(privileges, "PROXY") {
				continue
			}
			match := proxyTargetPattern.FindStringSubmatch(strings.TrimSpace(scope))
			if len(match) != 3 {
				continue
			}
			values := map[string]interface{}{
				"HOST": account.Host, "USER": account.User,
				"PROXIED_HOST": match[2], "PROXIED_USER": match[1],
				"WITH_GRANT": mysqlBoolFlag(hasGrantOption(privileges)),
				"GRANTOR":    "root@localhost", "TIMESTAMP": nil,
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql_proxies_priv", columns, rows)
}

func containsGrantPrivilege(privileges []string, wanted string) bool {
	for _, privilege := range privileges {
		if strings.EqualFold(strings.TrimSpace(privilege), wanted) {
			return true
		}
	}
	return false
}

var mysqlPrivilegeFilterPattern = regexp.MustCompile(`(?i)\b(host|db|user|table_name|column_name)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)

func mysqlSystemTableFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range mysqlPrivilegeFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToLower(match[1])] = value
	}
	return filters
}

func mysqlPrivilegeFlag(privileges []string, wanted string) string {
	for _, privilege := range privileges {
		if strings.EqualFold(privilege, wanted) || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
			return "Y"
		}
	}
	return "N"
}

func mysqlBoolFlag(value bool) string {
	if value {
		return "Y"
	}
	return "N"
}

func informationSchemaGrantable(privileges []string) string {
	if hasGrantOption(privileges) {
		return "YES"
	}
	return "NO"
}

func (e *XMySQLExecutor) executeMySQLUserSelect(query string) *SelectResult {
	defaults := []string{"Host", "User", "plugin", "authentication_string", "ssl_type", "account_locked", "password_expired", "Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv", "Grant_priv", "Index_priv", "Alter_priv", "Create_user_priv", "Create_role_priv", "Drop_role_priv", "user_attributes"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.user", columns, nil)
	}
	globalPrivilegeColumns := map[string]string{
		"SELECT_PRIV": "SELECT", "INSERT_PRIV": "INSERT", "UPDATE_PRIV": "UPDATE", "DELETE_PRIV": "DELETE",
		"CREATE_PRIV": "CREATE", "DROP_PRIV": "DROP", "GRANT_PRIV": "GRANT OPTION", "INDEX_PRIV": "INDEX", "ALTER_PRIV": "ALTER",
	}
	for _, account := range file.Accounts {
		if !metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) {
			continue
		}
		sslType := ""
		if account.X509Required {
			sslType = "X509"
		} else if account.TLSRequired {
			sslType = "SSL"
		}
		values := map[string]interface{}{
			"HOST": account.Host, "USER": account.User, "PLUGIN": account.Plugin, "AUTHENTICATION_STRING": account.Password,
			"SSL_TYPE": sslType, "ACCOUNT_LOCKED": mysqlBoolFlag(account.AccountLocked), "PASSWORD_EXPIRED": mysqlBoolFlag(account.PasswordExpired),
			"USER_ATTRIBUTES": nullableInformationSchemaString(account.UserAttributes),
		}
		globalPrivileges := account.GlobalGrants
		if len(globalPrivileges) == 0 && account.Grants != nil {
			globalPrivileges = account.Grants["*.*"]
		}
		for column, privilege := range globalPrivilegeColumns {
			values[column] = mysqlPrivilegeFlag(globalPrivileges, privilege)
		}
		values["CREATE_USER_PRIV"] = mysqlPrivilegeFlag(globalPrivileges, "CREATE USER")
		values["CREATE_ROLE_PRIV"] = mysqlPrivilegeFlag(globalPrivileges, "CREATE ROLE")
		values["DROP_ROLE_PRIV"] = mysqlPrivilegeFlag(globalPrivileges, "DROP ROLE")
		rows = append(rows, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("mysql.user", columns, rows)
}

// executeMySQLGlobalGrantsSelect exposes the dynamic global privileges stored
// in the durable account grant map. Standard static mysql.user flags are
// intentionally excluded; this view represents the rows MySQL keeps in
// mysql.global_grants for privileges such as BACKUP_ADMIN and SYSTEM_USER.
func (e *XMySQLExecutor) executeMySQLGlobalGrantsSelect(query string) *SelectResult {
	defaults := []string{"USER", "HOST", "PRIV", "WITH_GRANT_OPTION"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.global_grants", columns, nil)
	}
	for _, account := range file.Accounts {
		if !metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) {
			continue
		}
		privileges := account.GlobalGrants
		if len(privileges) == 0 && account.Grants != nil {
			privileges = account.Grants["*.*"]
		}
		for _, privilege := range privileges {
			privilege = strings.ToUpper(strings.TrimSpace(privilege))
			if !isDynamicGlobalPrivilege(privilege) {
				continue
			}
			values := map[string]interface{}{
				"USER": account.User, "HOST": account.Host, "PRIV": privilege,
				"WITH_GRANT_OPTION": mysqlBoolFlag(hasGrantOption(privileges)),
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql.global_grants", columns, rows)
}

func isDynamicGlobalPrivilege(privilege string) bool {
	return common.IsRegisteredDynamicPrivilege(privilege)
}

func (e *XMySQLExecutor) executeMySQLDBSelect(query string) *SelectResult {
	defaults := []string{"Host", "Db", "User", "Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv", "Grant_priv", "References_priv", "Index_priv", "Alter_priv", "Create_tmp_table_priv", "Lock_tables_priv", "Create_view_priv", "Show_view_priv", "Create_routine_priv", "Alter_routine_priv", "Execute_priv", "Event_priv", "Trigger_priv"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.db", columns, nil)
	}
	privilegeColumns := map[string]string{
		"SELECT_PRIV": "SELECT", "INSERT_PRIV": "INSERT", "UPDATE_PRIV": "UPDATE", "DELETE_PRIV": "DELETE",
		"CREATE_PRIV": "CREATE", "DROP_PRIV": "DROP", "GRANT_PRIV": "GRANT OPTION", "REFERENCES_PRIV": "REFERENCES",
		"INDEX_PRIV": "INDEX", "ALTER_PRIV": "ALTER", "CREATE_TMP_TABLE_PRIV": "CREATE TEMPORARY TABLES",
		"LOCK_TABLES_PRIV": "LOCK TABLES", "CREATE_VIEW_PRIV": "CREATE VIEW", "SHOW_VIEW_PRIV": "SHOW VIEW",
		"CREATE_ROUTINE_PRIV": "CREATE ROUTINE", "ALTER_ROUTINE_PRIV": "ALTER ROUTINE", "EXECUTE_PRIV": "EXECUTE",
		"EVENT_PRIV": "EVENT", "TRIGGER_PRIV": "TRIGGER",
	}
	for _, account := range file.Accounts {
		for scope, privileges := range account.Grants {
			if len(privileges) == 0 {
				continue
			}
			parts := strings.Split(scope, ".")
			if len(parts) != 2 || parts[1] != "*" || parts[0] == "*" ||
				!metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) ||
				!metadataFilterMatches(parts[0], filters["db"]) {
				continue
			}
			values := map[string]interface{}{"HOST": account.Host, "DB": parts[0], "USER": account.User}
			for column, privilege := range privilegeColumns {
				values[column] = mysqlPrivilegeFlag(privileges, privilege)
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql.db", columns, rows)
}

func (e *XMySQLExecutor) executeMySQLTablesPrivSelect(query string) *SelectResult {
	defaults := []string{"Host", "Db", "User", "Table_name", "Grantor", "Timestamp", "Table_priv", "Column_priv", "Grant_priv"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.tables_priv", columns, nil)
	}
	for _, account := range file.Accounts {
		for scope, privileges := range account.Grants {
			parts := strings.Split(scope, ".")
			if len(parts) != 2 || parts[0] == "*" || parts[1] == "*" ||
				!metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) ||
				!metadataFilterMatches(parts[0], filters["db"]) || !metadataFilterMatches(parts[1], filters["table_name"]) {
				continue
			}
			values := map[string]interface{}{
				"HOST": account.Host, "DB": parts[0], "USER": account.User, "TABLE_NAME": parts[1],
				"GRANTOR": "root@localhost", "TIMESTAMP": nil, "TABLE_PRIV": strings.Join(displayGrantPrivileges(privileges), ","), "COLUMN_PRIV": "",
				"GRANT_PRIV": mysqlBoolFlag(hasGrantOption(privileges)),
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql.tables_priv", columns, rows)
}

func (e *XMySQLExecutor) executeMySQLColumnsPrivSelect(query string) *SelectResult {
	defaults := []string{"Host", "Db", "User", "Table_name", "Column_name", "Timestamp", "Column_priv"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlSystemTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.columns_priv", columns, nil)
	}
	for _, account := range file.Accounts {
		for scope, privileges := range account.ColumnGrants {
			parts := strings.Split(scope, ".")
			if len(parts) != 3 || !metadataFilterMatches(account.Host, filters["host"]) || !metadataFilterMatches(account.User, filters["user"]) ||
				!metadataFilterMatches(parts[0], filters["db"]) || !metadataFilterMatches(parts[1], filters["table_name"]) ||
				!metadataFilterMatches(parts[2], filters["column_name"]) {
				continue
			}
			values := map[string]interface{}{
				"HOST": account.Host, "DB": parts[0], "USER": account.User, "TABLE_NAME": parts[1],
				"COLUMN_NAME": parts[2], "TIMESTAMP": nil, "COLUMN_PRIV": strings.Join(privileges, ","),
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql.columns_priv", columns, rows)
}
