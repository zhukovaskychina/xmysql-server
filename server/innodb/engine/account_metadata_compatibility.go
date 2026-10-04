package engine

import (
	"fmt"
	"regexp"
	"sort"
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
				for _, privilege := range informationSchemaPrivilegeRows(privileges, "column") {
					values := map[string]interface{}{"GRANTEE": fmt.Sprintf("'%s'@'%s'", account.User, account.Host), "TABLE_CATALOG": "def", "TABLE_SCHEMA": parts[0], "TABLE_NAME": parts[1], "COLUMN_NAME": parts[2], "PRIVILEGE_TYPE": strings.ToUpper(privilege), "IS_GRANTABLE": informationSchemaGrantable(privileges)}
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
				for _, privilege := range informationSchemaPrivilegeRows(privileges, "table") {
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
				for _, privilege := range informationSchemaPrivilegeRows(privileges, "schema") {
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
			expandedPrivileges := informationSchemaPrivilegeRows(privileges, "global")
			// MySQL exposes the implicit no-privilege grant as one USAGE row.
			// SHOW GRANTS renders the same state as GRANT USAGE ON *.*; an
			// empty projection here would incorrectly make a newly created
			// account disappear from USER_PRIVILEGES.
			if len(expandedPrivileges) == 0 {
				expandedPrivileges = []string{"USAGE"}
			}
			for _, privilege := range expandedPrivileges {
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

// executeInformationSchemaPrivilegesUnionAll evaluates the UNION ALL shape
// emitted by MySQL metadata clients when they combine column and table
// privileges. The two branches already have authoritative visibility and row
// expansion rules, so the union must compose those producers instead of
// returning the old compatibility-only empty result.
func (e *XMySQLExecutor) executeInformationSchemaPrivilegesUnionAll(query string, session server.MySQLServerSession) *SelectResult {
	return e.executeInformationSchemaPrivilegesUnion(query, session, "union all", false)
}

func (e *XMySQLExecutor) executeInformationSchemaPrivilegesUnionDistinct(query string, session server.MySQLServerSession) *SelectResult {
	operator := "union"
	if strings.Contains(strings.ToLower(query), " union distinct ") {
		operator = "union distinct"
	}
	return e.executeInformationSchemaPrivilegesUnion(query, session, operator, true)
}

// executeInformationSchemaPrivilegesIntersectExcept evaluates set operations
// over privilege views through the same producers used by the individual
// INFORMATION_SCHEMA tables. The generic SQL set-operation path cannot be
// used here because this metadata handler must retain its account-visibility
// and grant-expansion semantics for every branch.
func (e *XMySQLExecutor) executeInformationSchemaPrivilegesIntersectExcept(query string, session server.MySQLServerSession) *SelectResult {
	branches, operators, ok := splitIntersectExceptQuery(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if !ok || len(branches) < 2 || len(operators) != len(branches)-1 {
		return newInformationSchemaSelectResult("information_schema_privileges_set", nil, nil)
	}
	results := make([]*SelectResult, 0, len(branches))
	for _, branch := range branches {
		lower := strings.ToLower(branch)
		switch {
		case strings.Contains(lower, "information_schema.column_privileges"):
			results = append(results, e.executeInformationSchemaColumnPrivilegesSelect(branch, session))
		case strings.Contains(lower, "information_schema.table_privileges"):
			results = append(results, e.executeInformationSchemaTablePrivilegesSelect(branch, session))
		default:
			return newInformationSchemaSelectResult("information_schema_privileges_set", nil, nil)
		}
	}

	columns := append([]string(nil), results[0].Columns...)
	rows := informationSchemaPrivilegeResultRows(results[0])
	for index, operator := range operators {
		if len(results[index+1].Columns) != len(columns) {
			return newInformationSchemaSelectResult("information_schema_privileges_set", columns, nil)
		}
		right := informationSchemaPrivilegeResultRows(results[index+1])
		leftCounts := informationSchemaPrivilegeRowCounts(rows)
		rightCounts := informationSchemaPrivilegeRowCounts(right)
		out := make([][]interface{}, 0, len(rows))
		seen := make(map[string]struct{}, len(leftCounts))
		for _, row := range rows {
			key := fmt.Sprintf("%#v", row)
			if operator.kind == "INTERSECT" {
				if rightCounts[key] == 0 {
					continue
				}
				if operator.all {
					if leftCounts[key] == 0 || rightCounts[key] == 0 {
						continue
					}
					leftCounts[key]--
					rightCounts[key]--
					out = append(out, row)
					continue
				}
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, row)
				continue
			}
			if operator.kind == "EXCEPT" {
				if operator.all {
					if rightCounts[key] > 0 {
						rightCounts[key]--
						continue
					}
					out = append(out, row)
					continue
				}
				if rightCounts[key] > 0 {
					continue
				}
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, row)
			}
		}
		rows = out
	}
	return newInformationSchemaSelectResult("information_schema_privileges_set", columns, rows)
}

func informationSchemaPrivilegeResultRows(result *SelectResult) [][]interface{} {
	rows := make([][]interface{}, 0, len(result.Records))
	for _, record := range result.Records {
		values := record.GetValues()
		row := make([]interface{}, len(values))
		for index, value := range values {
			if value.IsNull() {
				row[index] = nil
				continue
			}
			row[index] = value.Raw()
		}
		rows = append(rows, row)
	}
	return rows
}

func informationSchemaPrivilegeRowCounts(rows [][]interface{}) map[string]int {
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[fmt.Sprintf("%#v", row)]++
	}
	return counts
}

func (e *XMySQLExecutor) executeInformationSchemaPrivilegesUnion(query string, session server.MySQLServerSession, operator string, distinct bool) *SelectResult {
	parts := splitTopLevelKeywordAll(strings.TrimSpace(strings.TrimSuffix(query, ";")), operator)
	if len(parts) < 2 {
		return newInformationSchemaSelectResult("information_schema_privileges_union", nil, nil)
	}
	results := make([]*SelectResult, 0, len(parts))
	for _, part := range parts {
		lower := strings.ToLower(part)
		switch {
		case strings.Contains(lower, "information_schema.column_privileges"):
			results = append(results, e.executeInformationSchemaColumnPrivilegesSelect(part, session))
		case strings.Contains(lower, "information_schema.table_privileges"):
			results = append(results, e.executeInformationSchemaTablePrivilegesSelect(part, session))
		default:
			return newInformationSchemaSelectResult("information_schema_privileges_union", nil, nil)
		}
	}
	columns := append([]string(nil), results[0].Columns...)
	rows := make([][]interface{}, 0)
	for _, result := range results {
		if result == nil || len(result.Columns) != len(columns) {
			continue
		}
		for _, record := range result.Records {
			values := record.GetValues()
			row := make([]interface{}, len(values))
			for index, value := range values {
				if value.IsNull() {
					row[index] = nil
					continue
				}
				row[index] = value.Raw()
			}
			rows = append(rows, row)
		}
	}
	if distinct {
		unique := make([][]interface{}, 0, len(rows))
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			key := fmt.Sprintf("%#v", row)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, row)
		}
		rows = unique
	}
	return newInformationSchemaSelectResult("information_schema_privileges_union", columns, rows)
}

func splitTopLevelKeywordAll(input, keyword string) []string {
	remaining := strings.TrimSpace(input)
	parts := make([]string, 0, 2)
	for remaining != "" {
		split := splitTopLevelKeyword(remaining, keyword)
		if len(split) != 2 {
			parts = append(parts, remaining)
			break
		}
		if strings.TrimSpace(split[0]) != "" {
			parts = append(parts, strings.TrimSpace(split[0]))
		}
		remaining = strings.TrimSpace(split[1])
	}
	return parts
}

var informationSchemaPrivilegeFilterPattern = regexp.MustCompile(`(?i)\b(grantee|table_catalog|table_schema|table_name|column_name|privilege_type|is_grantable)\b\s*(?:=|like)\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`)
var informationSchemaPrivilegeNullFilterPattern = regexp.MustCompile(`(?i)\b(grantee|table_catalog|table_schema|table_name|column_name|privilege_type|is_grantable)\b\s+is\s+(not\s+)?null`)
var informationSchemaPrivilegeInFilterPattern = regexp.MustCompile(`(?is)\b(grantee|table_catalog|table_schema|table_name|column_name|privilege_type|is_grantable)\b\s+(not\s+)?in\s*\(([^)]*)\)`)

// informationSchemaPrivilegeQueryMatches applies the column predicates that
// are common to the privilege views.  Keeping this separate from the object
// metadata filters matters because USER_PRIVILEGES has no table or schema
// columns, while callers still expect predicates such as PRIVILEGE_TYPE and
// GRANTEE to be evaluated before rows are projected.
func informationSchemaPrivilegeQueryMatches(query string, values map[string]interface{}) bool {
	for _, match := range informationSchemaPrivilegeNullFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		wantNull := strings.TrimSpace(match[2]) == ""
		value, exists := values[field]
		isNull := !exists || value == nil
		if isNull != wantNull {
			return false
		}
	}
	for _, match := range informationSchemaPrivilegeFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		pattern := match[2]
		if pattern == "" {
			pattern = match[3]
			pattern = strings.ReplaceAll(pattern, `""`, `"`)
		} else {
			pattern = strings.ReplaceAll(pattern, "''", "'")
		}
		if !metadataPatternMatchesAllowEmpty(fmt.Sprint(values[field]), pattern) {
			return false
		}
	}
	for _, match := range informationSchemaPrivilegeInFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		value := fmt.Sprint(values[field])
		contains := false
		for _, raw := range splitTopLevelComma(match[3]) {
			candidate := strings.Trim(strings.TrimSpace(raw), "'\"")
			candidate = strings.ReplaceAll(candidate, "''", "'")
			candidate = strings.ReplaceAll(candidate, `""`, `"`)
			if strings.EqualFold(candidate, value) {
				contains = true
				break
			}
		}
		notIn := strings.TrimSpace(match[2]) != ""
		if notIn == contains {
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
	if grantsContain(effective, "mysql.user", "SELECT") || grantsContain(effective, "mysql.user", "UPDATE") {
		return true
	}
	return grantsContain(effective, "*.*", "CREATE USER") && grantsContain(effective, "*.*", "SYSTEM_USER")
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
		if !mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) {
			continue
		}
		for scope, privileges := range account.Grants {
			parts := strings.Split(scope, ".")
			if len(parts) != 2 || parts[0] == "*" || parts[1] == "*" ||
				!mysqlSystemTableFieldMatches(query, "db", parts[0], filters) || !mysqlSystemTableFieldMatches(query, "routine_name", parts[1], filters) {
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
		if !mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) {
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
			if !mysqlSystemTableFieldMatches(query, "proxied_host", match[2], filters) ||
				!mysqlSystemTableFieldMatches(query, "proxied_user", match[1], filters) ||
				!mysqlSystemTableFieldMatches(query, "with_grant", mysqlBoolFlag(hasGrantOption(privileges)), filters) ||
				!mysqlSystemTableFieldMatches(query, "grantor", "root@localhost", filters) {
				continue
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

var mysqlPrivilegeFilterPattern = regexp.MustCompile(`(?i)\b(host|db|user|table_name|column_name|routine_name|proxied_host|proxied_user|with_grant|grantor|plugin|ssl_type|ssl_cipher|x509_issuer|x509_subject|max_questions|max_updates|max_connections|max_user_connections|password_expired|password_lifetime|account_locked|password_reuse_history|password_reuse_time|password_require_current|failed_login_attempts|password_lock_time)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
var mysqlPrivilegeInFilterPattern = regexp.MustCompile(`(?is)\b(host|db|user|table_name|column_name|routine_name|proxied_host|proxied_user|with_grant|grantor|plugin|ssl_type|ssl_cipher|x509_issuer|x509_subject|max_questions|max_updates|max_connections|max_user_connections|password_expired|password_lifetime|account_locked|password_reuse_history|password_reuse_time|password_require_current|failed_login_attempts|password_lock_time)\b\s+(not\s+)?in\s*\(([^)]*)\)`)
var mysqlPrivilegeNullFilterPattern = regexp.MustCompile(`(?is)\b(host|db|user|table_name|column_name|routine_name|proxied_host|proxied_user|with_grant|grantor|plugin|ssl_type|ssl_cipher|x509_issuer|x509_subject|max_questions|max_updates|max_connections|max_user_connections|password_expired|password_lifetime|account_locked|password_reuse_history|password_reuse_time|password_require_current|failed_login_attempts|password_lock_time)\b\s+is\s+(not\s+)?null`)

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

func mysqlSystemTableFieldMatches(query, field, value string, filters map[string]string) bool {
	if pattern, exists := filters[field]; exists && !metadataPatternMatchesAllowEmpty(value, pattern) {
		return false
	}
	for _, match := range mysqlPrivilegeNullFilterPattern.FindAllStringSubmatch(query, -1) {
		if !strings.EqualFold(match[1], field) {
			continue
		}
		if strings.TrimSpace(match[2]) == "" {
			return false
		}
	}
	for _, match := range mysqlPrivilegeInFilterPattern.FindAllStringSubmatch(query, -1) {
		if !strings.EqualFold(match[1], field) {
			continue
		}
		matched := false
		for _, raw := range splitTopLevelComma(match[3]) {
			candidate := strings.Trim(strings.TrimSpace(raw), "'\"")
			candidate = strings.ReplaceAll(candidate, "''", "'")
			candidate = strings.ReplaceAll(candidate, `""`, `"`)
			if strings.EqualFold(candidate, value) {
				matched = true
				break
			}
		}
		notIn := strings.TrimSpace(match[2]) != ""
		if notIn == matched {
			return false
		}
	}
	return true
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

func accountResourceValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func accountPasswordLockValue(account persistedAccount) string {
	if account.PasswordLockUnbounded {
		return "-1"
	}
	return fmt.Sprintf("%d", accountResourceValue(account.PasswordLockTime))
}

func informationSchemaGrantable(privileges []string) string {
	if hasGrantOption(privileges) {
		return "YES"
	}
	return "NO"
}

// informationSchemaPrivilegeRows expands an ALL grant into the individual
// privileges represented by MySQL's INFORMATION_SCHEMA privilege views. The
// views expose one row per privilege and never expose GRANT OPTION as its own
// PRIVILEGE_TYPE row. Keeping the expansion at projection time preserves the
// account file's compact grant representation and its independent grant-option
// bit.
func informationSchemaPrivilegeRows(privileges []string, scope string) []string {
	var all []string
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "column":
		all = []string{"SELECT", "INSERT", "UPDATE", "REFERENCES"}
	case "table":
		all = []string{
			"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER",
			"CREATE VIEW", "SHOW VIEW", "TRIGGER",
		}
	case "schema":
		all = []string{
			"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER",
			"CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE",
			"ALTER ROUTINE", "EVENT", "TRIGGER",
		}
	case "routine":
		all = []string{"EXECUTE", "ALTER ROUTINE"}
	case "global":
		all = []string{
			"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD", "SHUTDOWN", "PROCESS", "FILE",
			"REFERENCES", "INDEX", "ALTER", "SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES",
			"EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW", "SHOW VIEW", "CREATE ROLE",
			"DROP ROLE", "CREATE USER", "CREATE TABLESPACE", "TRIGGER", "EVENT", "CREATE ROUTINE", "ALTER ROUTINE",
			"PROXY",
		}
	}
	rows := make([]string, 0, len(privileges))
	for _, raw := range privileges {
		privilege := strings.ToUpper(strings.TrimSpace(raw))
		switch privilege {
		case "", "GRANT OPTION":
			continue
		case "ALL", "ALL PRIVILEGES":
			rows = appendUniqueStrings(rows, all...)
		default:
			rows = appendUniqueStrings(rows, privilege)
		}
	}
	return rows
}

// informationSchemaColumnPrivileges projects the privileges that the current
// account actually has on one column.  COLUMNS.PRIVILEGES is not a schema
// shape constant: table/global grants, column grants, active roles, and
// partial revokes all affect the value visible to the session.
func (e *XMySQLExecutor) informationSchemaColumnPrivileges(session server.MySQLServerSession, schema, table, column string) string {
	privilegeNames := []struct {
		name string
		priv string
	}{
		{name: "select", priv: "SELECT"},
		{name: "insert", priv: "INSERT"},
		{name: "update", priv: "UPDATE"},
		{name: "references", priv: "REFERENCES"},
	}
	if session == nil {
		return "select,insert,update,references"
	}
	user, _ := session.GetParamByName("user").(string)
	if strings.TrimSpace(user) == "" || strings.EqualFold(strings.TrimSpace(user), "root") {
		return "select,insert,update,references"
	}
	file, err := e.accountFileForSession(&ExecutionContext{Session: session})
	if err != nil {
		return ""
	}
	account := sessionAccount(file, session)
	if account == nil {
		return ""
	}
	tableScope := strings.TrimSpace(schema) + "." + strings.TrimSpace(table)
	columnScope := tableScope + "." + strings.TrimSpace(column)
	tableGrants := effectiveAccountGrants(file, *account, session)
	columnGrants := effectiveAccountColumnGrants(file, *account, session)
	result := make([]string, 0, len(privilegeNames))
	for _, candidate := range privilegeNames {
		granted := grantsContain(tableGrants, tableScope, candidate.priv)
		if !granted {
			for scope, privileges := range columnGrants {
				if !columnScopeCovers(columnScope, scope) {
					continue
				}
				for _, privilege := range privileges {
					if strings.EqualFold(strings.TrimSpace(privilege), candidate.priv) || strings.EqualFold(strings.TrimSpace(privilege), "ALL") || strings.EqualFold(strings.TrimSpace(privilege), "ALL PRIVILEGES") {
						granted = true
						break
					}
				}
				if granted {
					break
				}
			}
		}
		if granted {
			result = append(result, candidate.name)
		}
	}
	return strings.Join(result, ",")
}

func (e *XMySQLExecutor) executeMySQLUserSelect(query string) *SelectResult {
	defaults := []string{"Host", "User", "Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv", "Reload_priv", "Shutdown_priv", "Process_priv", "File_priv", "Grant_priv", "References_priv", "Index_priv", "Alter_priv", "Show_db_priv", "Super_priv", "Create_tmp_table_priv", "Lock_tables_priv", "Execute_priv", "Repl_slave_priv", "Repl_client_priv", "Create_view_priv", "Show_view_priv", "Create_routine_priv", "Alter_routine_priv", "Create_user_priv", "Event_priv", "Trigger_priv", "Create_tablespace_priv", "ssl_type", "ssl_cipher", "x509_issuer", "x509_subject", "max_questions", "max_updates", "max_connections", "max_user_connections", "plugin", "authentication_string", "password_expired", "password_last_changed", "password_lifetime", "account_locked", "Create_role_priv", "Drop_role_priv", "password_reuse_history", "password_reuse_time", "password_require_current", "failed_login_attempts", "password_lock_time", "user_attributes"}
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
		"RELOAD_PRIV": "RELOAD", "SHUTDOWN_PRIV": "SHUTDOWN", "PROCESS_PRIV": "PROCESS", "FILE_PRIV": "FILE",
		"REFERENCES_PRIV": "REFERENCES", "SHOW_DB_PRIV": "SHOW DATABASES", "SUPER_PRIV": "SUPER",
		"CREATE_TMP_TABLE_PRIV": "CREATE TEMPORARY TABLES", "LOCK_TABLES_PRIV": "LOCK TABLES", "EXECUTE_PRIV": "EXECUTE",
		"REPL_SLAVE_PRIV": "REPLICATION SLAVE", "REPL_CLIENT_PRIV": "REPLICATION CLIENT", "CREATE_VIEW_PRIV": "CREATE VIEW",
		"SHOW_VIEW_PRIV": "SHOW VIEW", "CREATE_ROUTINE_PRIV": "CREATE ROUTINE", "ALTER_ROUTINE_PRIV": "ALTER ROUTINE",
		"EVENT_PRIV": "EVENT", "TRIGGER_PRIV": "TRIGGER", "CREATE_TABLESPACE_PRIV": "CREATE TABLESPACE",
	}
	for _, account := range file.Accounts {
		if !mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) {
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
			"SSL_TYPE": sslType, "SSL_CIPHER": account.SSLCipher, "X509_ISSUER": account.X509Issuer, "X509_SUBJECT": account.X509Subject,
			"MAX_QUESTIONS": fmt.Sprintf("%d", accountResourceValue(account.MaxQuestions)), "MAX_UPDATES": fmt.Sprintf("%d", accountResourceValue(account.MaxUpdates)),
			"MAX_CONNECTIONS": fmt.Sprintf("%d", accountResourceValue(account.MaxConnections)), "MAX_USER_CONNECTIONS": fmt.Sprintf("%d", accountResourceValue(account.MaxUserConnections)),
			"ACCOUNT_LOCKED": mysqlBoolFlag(account.AccountLocked), "PASSWORD_EXPIRED": mysqlBoolFlag(account.PasswordExpired),
			"PASSWORD_LIFETIME": nullableInformationSchemaInt64(account.PasswordLifetime), "PASSWORD_LAST_CHANGED": nullableInformationSchemaStringPtr(account.PasswordLastChanged),
			"PASSWORD_REUSE_HISTORY": nullableInformationSchemaInt64(account.PasswordReuseHistory), "PASSWORD_REUSE_TIME": nullableInformationSchemaInt64(account.PasswordReuseTime),
			"PASSWORD_REQUIRE_CURRENT": nullableInformationSchemaBoolFlag(account.PasswordRequireCurrent),
			"FAILED_LOGIN_ATTEMPTS":    fmt.Sprintf("%d", accountResourceValue(account.FailedLoginAttempts)),
			"PASSWORD_LOCK_TIME":       accountPasswordLockValue(account),
			"USER_ATTRIBUTES":          nullableInformationSchemaString(account.UserAttributes),
		}
		accountMatches := true
		for _, field := range []string{"plugin", "ssl_type", "ssl_cipher", "x509_issuer", "x509_subject", "max_questions", "max_updates", "max_connections", "max_user_connections", "password_expired", "password_lifetime", "account_locked", "password_reuse_history", "password_reuse_time", "password_require_current", "failed_login_attempts", "password_lock_time"} {
			value := values[strings.ToUpper(field)]
			if value == nil {
				value = ""
			}
			if !mysqlSystemTableFieldMatches(query, field, fmt.Sprint(value), filters) {
				accountMatches = false
				break
			}
		}
		if !accountMatches {
			continue
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
		if !mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) {
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

// executeMySQLRoleEdgesSelect projects the durable role graph maintained by
// the account compatibility layer into MySQL's mysql.role_edges grant table.
// The account file stores the graph on the grantee account, while MySQL's
// mysql.role_edges table exposes each edge from the granted role to the
// grantee account (FROM_* is the role, TO_* is the account receiving it).
func (e *XMySQLExecutor) executeMySQLRoleEdgesSelect(query string) *SelectResult {
	defaults := []string{"FROM_HOST", "FROM_USER", "TO_HOST", "TO_USER", "WITH_ADMIN_OPTION"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlRoleTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.role_edges", columns, nil)
	}
	for _, account := range file.Accounts {
		for _, role := range account.Roles {
			roleUser, roleHost := splitPersistedAccountReference(role)
			withAdmin := "N"
			if containsRole(account.RoleAdminOptions, role) {
				withAdmin = "Y"
			}
			values := map[string]interface{}{
				"FROM_HOST": roleHost, "FROM_USER": roleUser,
				"TO_HOST": account.Host, "TO_USER": account.User,
				"WITH_ADMIN_OPTION": withAdmin,
			}
			if !mysqlRoleTableValuesMatch(query, values, filters) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("mysql.role_edges", columns, rows)
}

// executeMySQLDefaultRolesSelect projects the durable default-role lists into
// MySQL's mysql.default_roles grant table. A separate row is exposed for each
// default role assigned to an account.
func (e *XMySQLExecutor) executeMySQLDefaultRolesSelect(query string) *SelectResult {
	defaults := []string{"HOST", "USER", "DEFAULT_ROLE_HOST", "DEFAULT_ROLE_USER"}
	columns := requestedInformationSchemaColumns(query, defaults)
	filters := mysqlRoleTableFilters(query)
	rows := make([][]interface{}, 0)
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return newInformationSchemaSelectResult("mysql.default_roles", columns, nil)
	}
	for _, account := range file.Accounts {
		for _, role := range account.DefaultRoles {
			roleUser, roleHost := splitPersistedAccountReference(role)
			values := map[string]interface{}{
				"HOST": account.Host, "USER": account.User,
				"DEFAULT_ROLE_HOST": roleHost, "DEFAULT_ROLE_USER": roleUser,
			}
			if !mysqlRoleTableValuesMatch(query, values, filters) {
				continue
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := fmt.Sprint(rows[i][column]), fmt.Sprint(rows[j][column])
			if left == right {
				continue
			}
			return left < right
		}
		return false
	})
	return newInformationSchemaSelectResult("mysql.default_roles", columns, rows)
}

var mysqlRoleTableFilterPattern = regexp.MustCompile(`(?i)\b(from_host|from_user|to_host|to_user|with_admin_option|host|user|default_role_host|default_role_user)\b\s*(?:=|like)\s*(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`)
var mysqlRoleTableInFilterPattern = regexp.MustCompile(`(?is)\b(from_host|from_user|to_host|to_user|with_admin_option|host|user|default_role_host|default_role_user)\b\s+(not\s+)?in\s*\(([^)]*)\)`)
var mysqlRoleTableNullFilterPattern = regexp.MustCompile(`(?is)\b(from_host|from_user|to_host|to_user|with_admin_option|host|user|default_role_host|default_role_user)\b\s+is\s+(not\s+)?null`)

func mysqlRoleTableFilters(query string) map[string]string {
	filters := make(map[string]string)
	for _, match := range mysqlRoleTableFilterPattern.FindAllStringSubmatch(query, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		filters[strings.ToUpper(match[1])] = value
	}
	return filters
}

func mysqlRoleTableValuesMatch(query string, values map[string]interface{}, filters map[string]string) bool {
	for key, pattern := range filters {
		if !metadataPatternMatchesAllowEmpty(fmt.Sprint(values[key]), pattern) {
			return false
		}
	}
	for _, match := range mysqlRoleTableNullFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		if _, exists := values[field]; !exists {
			continue
		}
		if strings.TrimSpace(match[2]) == "" {
			return false
		}
	}
	for _, match := range mysqlRoleTableInFilterPattern.FindAllStringSubmatch(query, -1) {
		field := strings.ToUpper(match[1])
		value := fmt.Sprint(values[field])
		matched := false
		for _, raw := range splitTopLevelComma(match[3]) {
			candidate := strings.Trim(strings.TrimSpace(raw), "'\"")
			candidate = strings.ReplaceAll(candidate, "''", "'")
			candidate = strings.ReplaceAll(candidate, `""`, `"`)
			if strings.EqualFold(candidate, value) {
				matched = true
				break
			}
		}
		notIn := strings.TrimSpace(match[2]) != ""
		if notIn == matched {
			return false
		}
	}
	return true
}

func splitPersistedAccountReference(reference string) (user, host string) {
	parts := strings.SplitN(strings.TrimSpace(reference), "@", 2)
	if len(parts) != 2 {
		return strings.TrimSpace(reference), "%"
	}
	return parts[0], parts[1]
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
				!mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) ||
				!mysqlSystemTableFieldMatches(query, "db", parts[0], filters) {
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
				!mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) ||
				!mysqlSystemTableFieldMatches(query, "db", parts[0], filters) || !mysqlSystemTableFieldMatches(query, "table_name", parts[1], filters) {
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
			if len(parts) != 3 || !mysqlSystemTableFieldMatches(query, "host", account.Host, filters) || !mysqlSystemTableFieldMatches(query, "user", account.User, filters) ||
				!mysqlSystemTableFieldMatches(query, "db", parts[0], filters) || !mysqlSystemTableFieldMatches(query, "table_name", parts[1], filters) ||
				!mysqlSystemTableFieldMatches(query, "column_name", parts[2], filters) {
				continue
			}
			values := map[string]interface{}{
				"HOST": account.Host, "DB": parts[0], "USER": account.User, "TABLE_NAME": parts[1],
				"COLUMN_NAME": parts[2], "TIMESTAMP": nil, "COLUMN_PRIV": strings.Join(displayGrantPrivileges(privileges), ","),
			}
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("mysql.columns_priv", columns, rows)
}
