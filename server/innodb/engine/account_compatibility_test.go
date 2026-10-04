package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func TestAccountPasswordHashFollowsAuthenticationPlugin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'cache_hash'@'localhost' identified with caching_sha2_password by 'secret'")
	mustExecSQL(t, executor, "", "create user 'sha_hash'@'localhost' identified with sha256_password by 'secret'")

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	cacheAccount := findPersistedAccount(file, "cache_hash", "localhost")
	shaAccount := findPersistedAccount(file, "sha_hash", "localhost")
	require.Equal(t, "caching_sha2_password", cacheAccount.Plugin)
	require.Equal(t, "sha256_password", shaAccount.Plugin)
	first := sha256.Sum256([]byte("secret"))
	double := sha256.Sum256(first[:])
	require.Equal(t, hex.EncodeToString(double[:]), cacheAccount.Password)
	require.Equal(t, hex.EncodeToString(first[:]), shaAccount.Password)

	mustExecSQL(t, executor, "", "set password for 'sha_hash'@'localhost' = 'changed'")
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	changed := findPersistedAccount(file, "sha_hash", "localhost")
	changedDigest := sha256.Sum256([]byte("changed"))
	require.Equal(t, hex.EncodeToString(changedDigest[:]), changed.Password)
}

func TestMySQLUserSelectRequiresTablePrivilegeForNonRootSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'mysql_user_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'mysql_user_other'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "mysql_user_reader")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "select User from mysql.user", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "lacks SELECT privilege on table 'mysql.user'")

	mustExecSQL(t, executor, "", "grant select on mysql.user to 'mysql_user_reader'@'localhost'")
	rows := mustQuerySessionSQL(t, executor, session, "", "select User from mysql.user")
	require.ElementsMatch(t, [][]interface{}{{"mysql_user_reader"}, {"mysql_user_other"}}, rows)
}

func TestMySQLGrantTableMetadataDispatchRequiresTablePrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'grant_table_reader'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "grant_table_reader")
	session.SetParamByName("host", "localhost")

	tables := []struct {
		table string
		query string
	}{
		{"user", "select User from mysql.user"},
		{"db", "select User from mysql.db"},
		{"tables_priv", "select User from mysql.tables_priv"},
		{"columns_priv", "select User from mysql.columns_priv"},
		{"procs_priv", "select User from mysql.procs_priv"},
		{"proxies_priv", "select User from mysql.proxies_priv"},
		{"global_grants", "select User from mysql.global_grants"},
		{"role_edges", "select To_user from mysql.role_edges"},
		{"default_roles", "select User from mysql.default_roles"},
	}
	for _, table := range tables {
		denied := <-executor.ExecuteQuery(session, table.query, "")
		require.Error(t, denied.Err, table.table)
		require.Contains(t, denied.Err.Error(), "lacks SELECT privilege", table.table)
		mustExecSQL(t, executor, "", fmt.Sprintf("grant select on mysql.%s to 'grant_table_reader'@'localhost'", table.table))
		require.NoError(t, (<-executor.ExecuteQuery(session, table.query, "")).Err, table.table)
	}
}

func TestAccountManagementDDLImplicitlyCommitsActiveTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table account_commit (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into account_commit values (1)")

	mustExecSessionSQL(t, executor, session, "app", "create user 'account_commit_user'@'localhost' identified by 'secret'")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, mustQuerySessionSQL(t, executor, otherSession, "app", "select id from account_commit"), 1)
}

func TestAccountStatementsPersistAndExposeGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'alice'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, insert on app.* to 'alice'@'localhost'")
	result := mustQuerySQL(t, executor, "", "show grants for 'alice'@'localhost'")
	require.Len(t, result, 1)
	require.Contains(t, result[0][0], "SELECT, INSERT")

	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "mysql", "accounts.json"))
	require.NoError(t, err)
	var file persistedAccountFile
	require.NoError(t, json.Unmarshal(raw, &file))
	require.Len(t, file.Accounts, 1)
	require.NotEmpty(t, file.Accounts[0].Password)

	mustExecSQL(t, executor, "", "alter user 'alice'@'localhost' account lock")
	mustExecSQL(t, executor, "", "drop user 'alice'@'localhost'")
	_, err = os.Stat(filepath.Join(executor.GetDataDir(), "mysql", "accounts.json"))
	require.NoError(t, err)
}

func TestObjectGrantsSupportMultipleAccountsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	for _, account := range []string{"grant_a", "grant_b", "proxy_a", "proxy_b", "proxied"} {
		mustExecSQL(t, executor, "", fmt.Sprintf("create user '%s'@'localhost' identified by 'secret'", account))
	}

	mustExecSQL(t, executor, "", "grant select, insert on app.* to 'grant_a'@'localhost', 'grant_b'@'localhost'")
	for _, account := range []string{"grant_a", "grant_b"} {
		grants := mustQuerySQL(t, executor, "", fmt.Sprintf("show grants for '%s'@'localhost'", account))
		require.Len(t, grants, 1)
		require.Contains(t, grants[0][0], "SELECT, INSERT")
	}

	missing := <-executor.ExecuteQuery(nil, "grant update on app.* to 'grant_a'@'localhost', 'missing_grantee'@'localhost'", "")
	require.Error(t, missing.Err)
	require.NotContains(t, mustQuerySQL(t, executor, "", "show grants for 'grant_a'@'localhost'")[0][0], "UPDATE")

	mustExecSQL(t, executor, "", "revoke insert on app.* from 'grant_a'@'localhost', 'grant_b'@'localhost'")
	for _, account := range []string{"grant_a", "grant_b"} {
		grants := mustQuerySQL(t, executor, "", fmt.Sprintf("show grants for '%s'@'localhost'", account))
		require.Len(t, grants, 1)
		require.Contains(t, grants[0][0], "SELECT")
		require.NotContains(t, grants[0][0], "INSERT")
	}

	mustExecSQL(t, executor, "", "grant proxy on 'proxied'@'localhost' to 'proxy_a'@'localhost', 'proxy_b'@'localhost'")
	proxyRows := mustQuerySQL(t, executor, "", "select User from mysql.proxies_priv where Proxied_user = 'proxied'")
	require.ElementsMatch(t, [][]interface{}{{"proxy_a"}, {"proxy_b"}}, proxyRows)
}

func TestCreateUserAndRoleSupportMultipleAccountsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'create_a'@'localhost' identified by 'secret', 'create_b'@'localhost' identified by 'secret'")
	require.Len(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'create_a'"), 1)
	require.Len(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'create_b'"), 1)

	missing := <-executor.ExecuteQuery(nil, "create user 'create_c'@'localhost' identified by 'secret', 'create_missing'@'localhost' identified by 'secret', 'create_a'@'localhost' identified by 'secret'", "")
	require.Error(t, missing.Err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'create_c'"))

	mustExecSQL(t, executor, "", "create role 'create_role_a'@'localhost', 'create_role_b'@'localhost'")
	require.Len(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'create_role_a'"), 1)
	require.Len(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'create_role_b'"), 1)
}

func TestAlterUserSupportsMultipleAccountsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'alter_a'@'localhost' identified by 'old', 'alter_b'@'localhost' identified by 'old'")
	mustExecSQL(t, executor, "", "alter user 'alter_a'@'localhost', 'alter_b'@'localhost' identified by 'new' require x509 account lock password expire")

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	for _, accountName := range []string{"alter_a", "alter_b"} {
		account := findPersistedAccount(file, accountName, "localhost")
		require.Equal(t, nativePasswordHash("new"), account.Password)
		require.Equal(t, "mysql_native_password", account.Plugin)
		require.True(t, account.TLSRequired)
		require.True(t, account.X509Required)
		require.True(t, account.AccountLocked)
		require.True(t, account.PasswordExpired)
	}

	missing := <-executor.ExecuteQuery(nil, "alter user 'alter_a'@'localhost', 'missing_alter_user'@'localhost' identified by 'should_not_apply'", "")
	require.Error(t, missing.Err)
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Equal(t, nativePasswordHash("new"), findPersistedAccount(file, "alter_a", "localhost").Password)
}

func TestDropUserRemovesMultipleAccountsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'drop_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'drop_b'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "drop user 'drop_a'@'localhost', 'drop_b'@'localhost'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User in ('drop_a', 'drop_b')"))

	mustExecSQL(t, executor, "", "create user 'keep_a'@'localhost' identified by 'secret'")
	missing := <-executor.ExecuteQuery(nil, "drop user 'keep_a'@'localhost', 'missing_user'@'localhost'", "")
	require.Error(t, missing.Err)
	require.Equal(t, [][]interface{}{{"keep_a"}}, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'keep_a'"))
}

func TestMySQLUserPrivilegeMetadataSupportsInAndNotInFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'system_filter_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'system_filter_b'@'localhost' identified by 'secret'")

	inRows := mustQuerySQL(t, executor, "", "select User from mysql.user where User in ('system_filter_a')")
	require.Equal(t, [][]interface{}{{"system_filter_a"}}, inRows)

	notInRows := mustQuerySQL(t, executor, "", "select User from mysql.user where User in ('system_filter_a', 'system_filter_b') and User not in ('system_filter_b')")
	require.Equal(t, [][]interface{}{{"system_filter_a"}}, notInRows)

	mustExecSQL(t, executor, "", "create database system_filter_db")
	mustExecSQL(t, executor, "system_filter_db", "create table system_filter_table (id int primary key, label varchar(16))")
	mustExecSQL(t, executor, "", "grant select on system_filter_db.* to 'system_filter_a'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on system_filter_db.system_filter_table to 'system_filter_a'@'localhost'")
	mustExecSQL(t, executor, "", "grant select (id) on system_filter_db.system_filter_table to 'system_filter_a'@'localhost'")

	require.Equal(t, [][]interface{}{{"system_filter_a", "system_filter_db"}}, mustQuerySQL(t, executor, "", "select User, Db from mysql.db where User in ('system_filter_a') and Db in ('system_filter_db')"))
	require.Equal(t, [][]interface{}{{"system_filter_a", "system_filter_db", "system_filter_table"}}, mustQuerySQL(t, executor, "", "select User, Db, Table_name from mysql.tables_priv where User in ('system_filter_a') and Table_name not in ('other_table')"))
	require.Equal(t, [][]interface{}{{"system_filter_a", "system_filter_db", "system_filter_table", "id"}}, mustQuerySQL(t, executor, "", "select User, Db, Table_name, Column_name from mysql.columns_priv where User in ('system_filter_a') and Column_name in ('id')"))
}

func TestDropRoleRemovesMultipleRolesAndBindingsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'drop_role_a'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'drop_role_b'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_holder'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'drop_role_a'@'localhost', 'drop_role_b'@'localhost' to 'role_holder'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "role_holder")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set default role 'drop_role_a'@'localhost', 'drop_role_b'@'localhost' to 'role_holder'@'localhost'")
	mustExecSQL(t, executor, "", "drop role 'drop_role_a'@'localhost', 'drop_role_b'@'localhost'")

	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'drop_role_a'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'drop_role_b'"))
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	holder := findPersistedAccount(file, "role_holder", "localhost")
	require.NotNil(t, holder)
	require.Empty(t, holder.Roles)
	require.Empty(t, holder.DefaultRoles)
	grants := mustQuerySQL(t, executor, "", "show grants for 'role_holder'@'localhost'")
	require.Len(t, grants, 1)
	require.Contains(t, grants[0][0], "USAGE")
}

func TestAccountColumnGrantsRolesAndPluginAreDurable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'role_reader'@'localhost' identified with caching_sha2_password by 'role-secret'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'role_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'role_reader'@'localhost' to 'bob'@'localhost'")

	rows := mustQuerySQL(t, executor, "", "show grants for 'bob'@'localhost'")
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], "role_reader")
	roleRows := mustQuerySQL(t, executor, "", "show grants for 'role_reader'@'localhost'")
	require.Contains(t, roleRows[0][0], "SELECT (email)")

	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "mysql", "accounts.json"))
	require.NoError(t, err)
	var file persistedAccountFile
	require.NoError(t, json.Unmarshal(raw, &file))
	var role, bob persistedAccount
	for _, account := range file.Accounts {
		if account.User == "role_reader" {
			role = account
		}
		if account.User == "bob" {
			bob = account
		}
	}
	require.Equal(t, "caching_sha2_password", role.Plugin)
	require.Contains(t, role.ColumnGrants["app.users.email"], "SELECT")
	require.Contains(t, bob.Roles, "role_reader@localhost")
	privilegeRows := mustQuerySQL(t, executor, "", "select grantee, table_schema, table_name, column_name, privilege_type from information_schema.column_privileges where table_schema = 'app'")
	require.Len(t, privilegeRows, 1)
	require.Equal(t, "email", fmt.Sprint(privilegeRows[0][3]))
}

func TestInformationSchemaPrivilegeViewsHonorAccountVisibilityAndEffectiveRolePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "", "create table app.users (id int primary key, email varchar(64))")
	mustExecSQL(t, executor, "", "create user 'metadata_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'target_grantee'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create role 'metadata_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'metadata_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'target_grantee'@'localhost' with grant option")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'target_grantee'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on app.* to 'target_grantee'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'target_grantee'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on mysql.user to 'metadata_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'metadata_admin'@'localhost' to 'metadata_reader'@'localhost'")

	ordinary := newTestMySQLSession()
	ordinary.SetParamByName("user", "metadata_reader")
	ordinary.SetParamByName("host", "localhost")
	ordinary.SetParamByName("active_roles", []string{})
	ordinaryTable := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'app' and table_name = 'users'")
	require.Equal(t, [][]interface{}{{"'metadata_reader'@'localhost'", "SELECT", "NO"}}, ordinaryTable)
	ordinaryColumn := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee from information_schema.column_privileges where table_schema = 'app'")
	require.Empty(t, ordinaryColumn)

	mustExecSessionSQL(t, executor, ordinary, "", "set role 'metadata_admin'@'localhost'")
	roleVisibleTable := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'app' and table_name = 'users'")
	require.ElementsMatch(t, [][]interface{}{{"'metadata_reader'@'localhost'", "SELECT", "NO"}, {"'target_grantee'@'localhost'", "SELECT", "YES"}}, roleVisibleTable)
	roleFilteredTable := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee from information_schema.table_privileges where grantee = '''target_grantee''@''localhost'''")
	require.Equal(t, [][]interface{}{{"'target_grantee'@'localhost'"}}, roleFilteredTable)
	roleVisibleColumn := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee, column_name, privilege_type from information_schema.column_privileges where table_schema = 'app' and table_name = 'users'")
	require.Equal(t, [][]interface{}{{"'target_grantee'@'localhost'", "email", "SELECT"}}, roleVisibleColumn)
	roleVisibleSchema := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee, privilege_type from information_schema.schema_privileges where table_schema = 'app' and grantee = '''target_grantee''@''localhost'''")
	require.Equal(t, [][]interface{}{{"'target_grantee'@'localhost'", "SELECT"}}, roleVisibleSchema)
	roleVisibleUser := mustQuerySessionSQL(t, executor, ordinary, "", "select grantee, privilege_type from information_schema.user_privileges where grantee = '''target_grantee''@''localhost'''")
	require.Equal(t, [][]interface{}{{"'target_grantee'@'localhost'", "SELECT"}}, roleVisibleUser)

	root := newTestMySQLSession()
	root.SetParamByName("user", "root")
	root.SetParamByName("host", "localhost")
	rootRows := mustQuerySessionSQL(t, executor, root, "", "select grantee from information_schema.user_privileges")
	require.Contains(t, rootRows, []interface{}{"'target_grantee'@'localhost'"})
}

func TestInformationSchemaColumnPrivilegesReflectColumnGrantOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database column_grant_option")
	mustExecSQL(t, executor, "", "create table column_grant_option.users (id int primary key, email varchar(64))")
	mustExecSQL(t, executor, "", "create user 'column_grant_reader'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on column_grant_option.users to 'column_grant_reader'@'localhost' with grant option")

	rows := mustQuerySQL(t, executor, "", "select grantee, column_name, privilege_type, is_grantable from information_schema.column_privileges where grantee = '''column_grant_reader''@''localhost''' and table_schema = 'column_grant_option'")
	require.Equal(t, [][]interface{}{{"'column_grant_reader'@'localhost'", "email", "SELECT", "YES"}}, rows)
}

func TestApplicableRolesReflectPersistedRoleGrant(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, role_name, role_host, is_grantable, default_role from information_schema.applicable_roles")
	require.Equal(t, [][]interface{}{{"'bob'@'localhost'", "report_reader", "localhost", "NO", "NO"}}, rows)
}

func TestMySQLRoleGrantTablesProjectDurableRoleEdgesAndDefaultRoles(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost' with admin option")
	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set default role 'report_reader'@'localhost' to 'bob'@'localhost'")

	roleEdges := mustQuerySQL(t, executor, "", "select from_host, from_user, to_host, to_user, with_admin_option from mysql.role_edges")
	require.Equal(t, [][]interface{}{{"localhost", "report_reader", "localhost", "bob", "Y"}}, roleEdges)

	defaultRoles := mustQuerySQL(t, executor, "", "select host, user, default_role_host, default_role_user from mysql.default_roles")
	require.Equal(t, [][]interface{}{{"localhost", "bob", "localhost", "report_reader"}}, defaultRoles)

	filtered := mustQuerySQL(t, executor, "", "select from_user from mysql.role_edges where to_user = 'bob' and with_admin_option = 'Y'")
	require.Equal(t, [][]interface{}{{"report_reader"}}, filtered)

	mustExecSQL(t, executor, "", "create role 'audit_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'audit_reader'@'localhost' to 'bob'@'localhost' with admin option")
	roleIn := mustQuerySQL(t, executor, "", "select from_user from mysql.role_edges where from_user in ('report_reader')")
	require.Equal(t, [][]interface{}{{"report_reader"}}, roleIn)
	roleNotIn := mustQuerySQL(t, executor, "", "select from_user from mysql.role_edges where from_user in ('report_reader', 'audit_reader') and from_user not in ('audit_reader')")
	require.Equal(t, [][]interface{}{{"report_reader"}}, roleNotIn)
	emptyRole := mustQuerySQL(t, executor, "", "select from_user from mysql.role_edges where from_user = ''")
	require.Empty(t, emptyRole)
}

func TestRoleMetadataResolvesWildcardHostAccountLikePrivilegeChecks(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'wildcard_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'wildcard_bob'@'%' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'wildcard_reader'@'localhost' to 'wildcard_bob'@'%' with admin option")

	session := newTestMySQLSession()
	session.SetParamByName("user", "wildcard_bob")
	session.SetParamByName("host", "10.0.0.8")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.NotNil(t, sessionAccount(file, session))

	applicable := mustQuerySessionSQL(t, executor, session, "", "select user, host, role_name, role_host, is_grantable from information_schema.applicable_roles")
	require.Equal(t, [][]interface{}{{"wildcard_bob", "%", "wildcard_reader", "localhost", "YES"}}, applicable)

	admin := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host, is_grantable from information_schema.administrable_role_authorizations")
	require.Equal(t, [][]interface{}{{"wildcard_reader", "localhost", "YES"}}, admin)
}

func TestRoleMetadataHonorsProjectionAndFilters(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'audit_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost', 'audit_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"report_reader@localhost", "audit_reader@localhost"})

	applicable := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host from information_schema.applicable_roles where role_name = 'report_reader'")
	require.Equal(t, [][]interface{}{{"report_reader", "localhost"}}, applicable)
	applicableIn := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host from information_schema.applicable_roles where role_name in ('report_reader')")
	require.Equal(t, [][]interface{}{{"report_reader", "localhost"}}, applicableIn)
	applicableNull := mustQuerySessionSQL(t, executor, session, "", "select role_name from information_schema.applicable_roles where role_name is null")
	require.Empty(t, applicableNull)
	applicableNotNull := mustQuerySessionSQL(t, executor, session, "", "select role_name from information_schema.applicable_roles where role_name is not null")
	require.ElementsMatch(t, [][]interface{}{{"report_reader"}, {"audit_reader"}}, applicableNotNull)

	tableGrants := mustQuerySessionSQL(t, executor, session, "", "select table_name, privilege_type from information_schema.role_table_grants where table_schema = 'app' and table_name = 'users'")
	require.Equal(t, [][]interface{}{{"users", "SELECT"}}, tableGrants)
	catalogGrants := mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.role_table_grants where table_catalog = 'def'")
	require.Equal(t, [][]interface{}{{"users"}}, catalogGrants)
	privilegeIn := mustQuerySessionSQL(t, executor, session, "", "select privilege_type from information_schema.role_table_grants where privilege_type in ('SELECT')")
	require.Equal(t, [][]interface{}{{"SELECT"}}, privilegeIn)
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.role_table_grants where table_catalog = 'wrong'"))
	require.Equal(t, [][]interface{}{{"users"}}, mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.role_table_grants where grantor = 'root' and grantor_host = 'localhost'"))
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.role_table_grants where grantor = 'other'"))

	active := newTestMySQLSession()
	active.SetParamByName("active_roles", []string{"report_reader@localhost", "audit_reader@localhost"})
	enabled := mustQuerySessionSQL(t, executor, active, "", "select role_name from information_schema.enabled_roles where role_name = 'audit_reader'")
	require.Equal(t, [][]interface{}{{"audit_reader"}}, enabled)
}

func TestRoleTableGrantsExcludeGrantedButInactiveRoles(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database inactive_role_metadata")
	mustExecSQL(t, executor, "inactive_role_metadata", "create table users (id int primary key)")
	mustExecSQL(t, executor, "", "create role 'inactive_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'inactive_bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on inactive_role_metadata.users to 'inactive_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'inactive_reader'@'localhost' to 'inactive_bob'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "inactive_bob")
	session.SetParamByName("host", "localhost")

	rows := mustQuerySessionSQL(t, executor, session, "", "select table_name, privilege_type from information_schema.role_table_grants where table_schema = 'inactive_role_metadata'")
	require.Empty(t, rows, "a granted role without a default or active role must not expose role table grants")
}

func TestEnabledRolesUsePersistedDefaultRolesWhenSessionOmitsActiveRoles(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'default_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'default_bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'default_reader'@'localhost' to 'default_bob'@'localhost'")
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, admin, "", "set default role 'default_reader'@'localhost' to 'default_bob'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "default_bob")
	session.SetParamByName("host", "localhost")

	rows := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host, is_default, is_mandatory from information_schema.enabled_roles")
	require.Equal(t, [][]interface{}{{"default_reader", "localhost", "YES", "NO"}}, rows)
}

func TestApplicableRolesExposeMandatoryRoleAndMySQL84Shape(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'mandatory_applicable'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'applicable_user'@'localhost' identified by 'secret'")
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN", "ROLE_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global mandatory_roles = 'mandatory_applicable@localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "applicable_user")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select user, host, grantee, grantee_host, role_name, role_host, is_grantable, is_default, is_mandatory from information_schema.applicable_roles")
	require.Equal(t, [][]interface{}{{"applicable_user", "localhost", "'applicable_user'@'localhost'", "localhost", "mandatory_applicable", "localhost", "NO", "NO", "YES"}}, rows)
}

func TestMandatoryRolesParticipateInRoleActivationAndEnabledRolesMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'mandatory_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'mandatory_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'mandatory_reader'@'localhost'")
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN", "ROLE_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global mandatory_roles = 'mandatory_reader@localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "mandatory_user")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set role all")

	roles, ok := session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	require.Equal(t, []string{"mandatory_reader@localhost"}, roles)

	rows := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host, is_default, is_mandatory from information_schema.enabled_roles")
	require.Equal(t, [][]interface{}{{"mandatory_reader", "localhost", "NO", "YES"}}, rows)
}

func TestMandatoryRolesRemainActiveAfterSetRoleNone(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'mandatory_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'mandatory_none_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'mandatory_reader'@'localhost'")

	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN", "ROLE_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global mandatory_roles = 'mandatory_reader@localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "mandatory_none_user")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set role none")

	roles, ok := session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	require.Equal(t, []string{"mandatory_reader@localhost"}, roles)
	rows := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host, is_mandatory from information_schema.enabled_roles")
	require.Equal(t, [][]interface{}{{"mandatory_reader", "localhost", "YES"}}, rows)
}

func TestMandatoryRolesCannotBeRevokedOrDropped(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'protected_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'protected_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'protected_reader'@'localhost' to 'protected_user'@'localhost'")
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN", "ROLE_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global mandatory_roles = 'protected_reader@localhost'")

	revoke := <-executor.ExecuteQuery(admin, "revoke 'protected_reader'@'localhost' from 'protected_user'@'localhost'", "")
	require.Error(t, revoke.Err)
	dropRole := <-executor.ExecuteQuery(admin, "drop role 'protected_reader'@'localhost'", "")
	require.Error(t, dropRole.Err)
	dropUser := <-executor.ExecuteQuery(admin, "drop user 'protected_reader'@'localhost'", "")
	require.Error(t, dropUser.Err)
}

func TestSettingMandatoryRolesRequiresRoleAdmin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	result := <-executor.ExecuteQuery(session, "set global mandatory_roles = 'some_role@localhost'", "")
	require.Error(t, result.Err)
	value, err := executor.GetStorageManager().GetSystemVariablesManager().GetVariable("", "mandatory_roles", manager.GlobalScope)
	require.NoError(t, err)
	require.Equal(t, "", value)
}

func TestSetDefaultRoleSupportsMultipleAccountsAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'default_reader'@'localhost'")
	for _, account := range []string{"default_user_a", "default_user_b"} {
		mustExecSQL(t, executor, "", fmt.Sprintf("create user '%s'@'localhost' identified by 'secret'", account))
		mustExecSQL(t, executor, "", fmt.Sprintf("grant 'default_reader'@'localhost' to '%s'@'localhost'", account))
	}

	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set default role 'default_reader'@'localhost' to 'default_user_a'@'localhost', 'default_user_b'@'localhost'")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	for _, account := range []string{"default_user_a", "default_user_b"} {
		require.Equal(t, []string{"default_reader@localhost"}, findPersistedAccount(file, account, "localhost").DefaultRoles)
	}

	missing := <-executor.ExecuteQuery(session, "set default role 'default_reader'@'localhost' to 'default_user_a'@'localhost', 'missing_default_user'@'localhost'", "")
	require.Error(t, missing.Err)
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Equal(t, []string{"default_reader@localhost"}, findPersistedAccount(file, "default_user_a", "localhost").DefaultRoles)
}

func TestAdministrableRoleAuthorizationsReflectAdminOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_admin'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'report_admin'@'localhost' to 'bob'@'localhost' with admin option")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, role_name, role_host, is_grantable from information_schema.administrable_role_authorizations")
	require.Equal(t, [][]interface{}{{"'bob'@'localhost'", "report_admin", "localhost", "YES"}}, rows)
}

func TestRoleAdminMakesApplicableRoleGrantable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'role_admin_target'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_admin_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'role_admin_target'@'localhost' to 'role_admin_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "role_admin_user")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("dynamic_privileges", []string{"ROLE_ADMIN"})

	applicable := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.applicable_roles where role_name = 'role_admin_target'")
	require.Equal(t, [][]interface{}{{"role_admin_target", "YES"}}, applicable)
	admin := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.administrable_role_authorizations where role_name = 'role_admin_target'")
	require.Equal(t, [][]interface{}{{"role_admin_target", "YES"}}, admin)
}

func TestPersistedRoleAdminGrantMakesApplicableRoleGrantable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'persisted_role_admin_target'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'persisted_role_admin_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'persisted_role_admin_target'@'localhost' to 'persisted_role_admin_user'@'localhost'")
	mustExecSQL(t, executor, "", "grant role_admin on *.* to 'persisted_role_admin_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "persisted_role_admin_user")
	session.SetParamByName("host", "localhost")

	applicable := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.applicable_roles where role_name = 'persisted_role_admin_target'")
	require.Equal(t, [][]interface{}{{"persisted_role_admin_target", "YES"}}, applicable)
	admin := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.administrable_role_authorizations where role_name = 'persisted_role_admin_target'")
	require.Equal(t, [][]interface{}{{"persisted_role_admin_target", "YES"}}, admin)
}

func TestRoleMetadataFiltersApplyToAccountAndDefaultRoleFields(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'role_metadata_filter_target'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_metadata_filter_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'role_metadata_filter_target'@'localhost' to 'role_metadata_filter_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "role_metadata_filter_user")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set default role 'role_metadata_filter_target'@'localhost' to 'role_metadata_filter_user'@'localhost'")

	wrongUser := mustQuerySessionSQL(t, executor, session, "", "select user, host, role_name, default_role from information_schema.applicable_roles where user = 'someone_else'")
	require.Empty(t, wrongUser)
	wrongHost := mustQuerySessionSQL(t, executor, session, "", "select user, host, role_name, default_role from information_schema.applicable_roles where host = 'remote'")
	require.Empty(t, wrongHost)
	wrongGranteeHost := mustQuerySessionSQL(t, executor, session, "", "select grantee, grantee_host, role_name from information_schema.applicable_roles where grantee_host = 'remote'")
	require.Empty(t, wrongGranteeHost)
	defaultRole := mustQuerySessionSQL(t, executor, session, "", "select role_name, default_role from information_schema.applicable_roles where default_role = 'YES'")
	require.Equal(t, [][]interface{}{{"role_metadata_filter_target", "YES"}}, defaultRole)
}

func TestNestedRoleAdminOptionDoesNotOvergrantInheritedRole(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'nested_role_admin_parent'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'nested_role_admin_child'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'nested_role_admin_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'nested_role_admin_child'@'localhost' to 'nested_role_admin_parent'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'nested_role_admin_parent'@'localhost' to 'nested_role_admin_user'@'localhost' with admin option")

	session := newTestMySQLSession()
	session.SetParamByName("user", "nested_role_admin_user")
	session.SetParamByName("host", "localhost")

	applicable := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.applicable_roles where role_name in ('nested_role_admin_parent', 'nested_role_admin_child') order by role_name")
	require.Equal(t, [][]interface{}{{"nested_role_admin_parent", "YES"}, {"nested_role_admin_child", "NO"}}, applicable)
	admin := mustQuerySessionSQL(t, executor, session, "", "select role_name, is_grantable from information_schema.administrable_role_authorizations where role_name in ('nested_role_admin_parent', 'nested_role_admin_child') order by role_name")
	require.Equal(t, [][]interface{}{{"nested_role_admin_parent", "YES"}}, admin)
}

func TestNestedRoleActivationSeparatesEnabledRoleFromInheritedPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'nested_activation_parent'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'nested_activation_child'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'nested_activation_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'nested_activation_child'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'nested_activation_child'@'localhost' to 'nested_activation_parent'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'nested_activation_parent'@'localhost' to 'nested_activation_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "nested_activation_user")
	session.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, session, "", "set role all")

	// MySQL exposes the directly activated role in ENABLED_ROLES while its
	// nested role contributes privileges through the active role graph.
	roles := mustQuerySessionSQL(t, executor, session, "", "select role_name, role_host from information_schema.enabled_roles")
	require.Equal(t, [][]interface{}{{"nested_activation_parent", "localhost"}}, roles)
	privileges := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_name, privilege_type from information_schema.role_table_grants where table_schema = 'app'")
	require.Equal(t, [][]interface{}{{"'nested_activation_child'@'localhost'", "users", "SELECT"}}, privileges)
}

func TestRolesGraphMLHidesRoleGraphWithoutRoleAdmin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'graph_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'graph_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'graph_reader'@'localhost' to 'graph_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "graph_user")
	session.SetParamByName("host", "localhost")
	rows := mustQuerySessionSQL(t, executor, session, "", "select roles_graphml()")
	require.Equal(t, [][]interface{}{{"<?xml version=\"1.0\" encoding=\"UTF-8\"?><graphml />"}}, rows)
}

func TestRolesGraphMLShowsDirectRoleEdgesToRoleAdmin(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'graph_admin_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'graph_admin_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'graph_admin_reader'@'localhost' to 'graph_admin_user'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "graph_admin_user")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("dynamic_privileges", []string{"ROLE_ADMIN"})
	rows := mustQuerySessionSQL(t, executor, session, "", "select roles_graphml()")
	require.Len(t, rows, 1)
	graph := rows[0][0].(string)
	require.Contains(t, graph, "<node")
	require.Contains(t, graph, "graph_admin_reader@localhost")
	require.Contains(t, graph, "graph_admin_user@localhost")
	require.Contains(t, graph, "<edge")
}

func TestRoleTableGrantsReflectRoleAccountPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"report_reader@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_schema, table_name, privilege_type, is_grantable from information_schema.role_table_grants")
	require.Equal(t, [][]interface{}{{"'report_reader'@'localhost'", "app", "users", "SELECT", "NO"}}, rows)
}

func TestRolePrivilegeViewsExpandAllGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'all_privilege_role'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'all_privilege_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant all privileges on app.users to 'all_privilege_role'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'all_privilege_role'@'localhost' to 'all_privilege_user'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "all_privilege_user")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"all_privilege_role@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select privilege_type, is_grantable from information_schema.role_table_grants where table_schema = 'app' and table_name = 'users'")
	require.Len(t, rows, 12)
	for _, row := range rows {
		require.NotEqual(t, "ALL", row[0])
		require.NotEqual(t, "ALL PRIVILEGES", row[0])
		require.NotEqual(t, "GRANT OPTION", row[0])
	}
}

func TestRoleTableGrantsPreserveNonRootGrantor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'grant_admin'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant all privileges on *.* to 'grant_admin'@'localhost' with grant option")

	grantAdmin := newTestMySQLSession()
	grantAdmin.SetParamByName("user", "grant_admin")
	grantAdmin.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, grantAdmin, "", "grant select on app.users to 'report_reader'@'localhost'")

	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	bob := newTestMySQLSession()
	bob.SetParamByName("user", "bob")
	bob.SetParamByName("host", "localhost")
	bob.SetParamByName("active_roles", []string{"report_reader@localhost"})
	rows := mustQuerySessionSQL(t, executor, bob, "", "select grantor, grantor_host, table_name from information_schema.role_table_grants")
	require.Equal(t, [][]interface{}{{"grant_admin", "localhost", "users"}}, rows)
}

func TestRoleColumnGrantsReflectRoleAccountPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"report_reader@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_schema, table_name, column_name, privilege_type, is_grantable from information_schema.role_column_grants")
	require.Equal(t, [][]interface{}{{"'report_reader'@'localhost'", "app", "users", "email", "SELECT", "NO"}}, rows)
}

func TestRoleTableGrantsIncludeNestedRolePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'report_admin'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'report_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_admin'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"report_admin@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, table_schema, table_name, privilege_type, is_grantable from information_schema.role_table_grants")
	require.Equal(t, [][]interface{}{{"'report_reader'@'localhost'", "app", "users", "SELECT", "NO"}}, rows)
}

func TestRoleRoutineGrantsReflectProcedureAndFunctionPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create procedure app.report() begin select 1; end")
	mustExecSQL(t, executor, "", "create function app.score() returns int deterministic return 1")
	mustExecSQL(t, executor, "", "grant execute on procedure app.report to 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant alter routine on function app.score to 'routine_reader'@'localhost' with grant option")
	mustExecSQL(t, executor, "", "grant 'routine_reader'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"routine_reader@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, grantee_host, specific_schema, specific_name, routine_name, privilege_type, is_grantable from information_schema.role_routine_grants")
	require.Equal(t, [][]interface{}{
		{"routine_reader", "localhost", "app", "report", "report", "EXECUTE", "NO"},
		{"routine_reader", "localhost", "app", "score", "score", "ALTER ROUTINE", "YES"},
	}, rows)
	catalogRows := mustQuerySessionSQL(t, executor, session, "", "select specific_name from information_schema.role_routine_grants where specific_catalog = 'def' and routine_catalog = 'def'")
	require.Equal(t, [][]interface{}{{"report"}, {"score"}}, catalogRows)
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "", "select specific_name from information_schema.role_routine_grants where specific_catalog = 'wrong'"))
	require.Equal(t, [][]interface{}{{"report"}, {"score"}}, mustQuerySessionSQL(t, executor, session, "", "select specific_name from information_schema.role_routine_grants where grantor = 'root'"))
	require.Empty(t, mustQuerySessionSQL(t, executor, session, "", "select specific_name from information_schema.role_routine_grants where grantor = 'other'"))
}

func TestRoleRoutineGrantsIncludeNestedRolePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'routine_admin'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create procedure app.report() begin select 1; end")
	mustExecSQL(t, executor, "", "grant execute on procedure app.report to 'routine_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'routine_reader'@'localhost' to 'routine_admin'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'routine_admin'@'localhost' to 'bob'@'localhost'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "bob")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("active_roles", []string{"routine_admin@localhost"})

	rows := mustQuerySessionSQL(t, executor, session, "", "select grantee, specific_name, privilege_type from information_schema.role_routine_grants")
	require.Equal(t, [][]interface{}{{"routine_reader", "report", "EXECUTE"}}, rows)
}

func TestMySQLPrivilegeSystemTablesReflectPersistedGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'meta_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, insert on app.* to 'meta_user'@'localhost'")
	mustExecSQL(t, executor, "", "grant update on app.users to 'meta_user'@'localhost'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'meta_user'@'localhost'")

	dbRows := mustQuerySQL(t, executor, "", "select Host, Db, User, Select_priv, Insert_priv, Update_priv from mysql.db where User = 'meta_user' and Db = 'app'")
	require.Equal(t, [][]interface{}{{"localhost", "app", "meta_user", "Y", "Y", "N"}}, dbRows)
	tableRows := mustQuerySQL(t, executor, "", "select Host, Db, User, Table_name, Table_priv from mysql.tables_priv where User = 'meta_user' and Db = 'app' and Table_name = 'users'")
	require.Equal(t, [][]interface{}{{"localhost", "app", "meta_user", "users", "UPDATE"}}, tableRows)
	columnRows := mustQuerySQL(t, executor, "", "select Host, Db, User, Table_name, Column_name, Column_priv from mysql.columns_priv where User = 'meta_user' and Column_name = 'email'")
	require.Equal(t, [][]interface{}{{"localhost", "app", "meta_user", "users", "email", "SELECT"}}, columnRows)
}

func TestMySQLUserReflectsPersistedAuthenticationAndGlobalGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'global_user'@'localhost' identified with caching_sha2_password by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'global_user'@'localhost'")
	rows := mustQuerySQL(t, executor, "", "select Host, User, plugin, authentication_string, account_locked, password_expired, Select_priv from mysql.user where User = 'global_user' and Host = 'localhost'")
	require.Len(t, rows, 1)
	require.Equal(t, "localhost", rows[0][0])
	require.Equal(t, "global_user", rows[0][1])
	require.Equal(t, "caching_sha2_password", rows[0][2])
	require.NotEmpty(t, rows[0][3])
	require.Equal(t, "N", rows[0][4])
	require.Equal(t, "N", rows[0][5])
	require.Equal(t, "Y", rows[0][6])
}

func TestGrantOptionPersistsAndAppearsInPrivilegeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'reporter'@'localhost' with grant option")
	rows := mustQuerySQL(t, executor, "mysql", "select Table_priv, Grant_priv from mysql.tables_priv where User = 'reporter' and Db = 'app' and Table_name = 'users'")
	require.Equal(t, [][]interface{}{{"SELECT", "Y"}}, rows)
	grants := mustQuerySQL(t, executor, "", "show grants for 'reporter'@'localhost'")
	require.Len(t, grants, 1)
	require.Contains(t, grants[0][0], "GRANT OPTION")
	tablePrivileges := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.table_privileges where grantee = \"'reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, tablePrivileges)
	tableCatalog := mustQuerySQL(t, executor, "", "select table_catalog from information_schema.table_privileges where grantee = \"'reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"def"}}, tableCatalog)

	mustExecSQL(t, executor, "", "create user 'schema_reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.* to 'schema_reporter'@'localhost' with grant option")
	schemaPrivileges := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.schema_privileges where grantee = \"'schema_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, schemaPrivileges)
	schemaCatalog := mustQuerySQL(t, executor, "", "select table_catalog from information_schema.schema_privileges where grantee = \"'schema_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"def"}}, schemaCatalog)
	schemaAll := <-executor.ExecuteQuery(nil, "select * from information_schema.schema_privileges where grantee = \"'schema_reporter'@'localhost'\"", "")
	require.NoError(t, schemaAll.Err)
	schemaResult, ok := schemaAll.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"}, schemaResult.Columns)
	require.Equal(t, [][]interface{}{{"'schema_reporter'@'localhost'", "def", "app", "SELECT", "YES"}}, selectResultRows(schemaResult))

	mustExecSQL(t, executor, "", "create user 'column_reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'column_reporter'@'localhost' with grant option")
	columnPrivileges := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.column_privileges where grantee = \"'column_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, columnPrivileges)
	columnCatalog := mustQuerySQL(t, executor, "", "select table_catalog from information_schema.column_privileges where grantee = \"'column_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"def"}}, columnCatalog)

	mustExecSQL(t, executor, "", "create user 'global_reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'global_reporter'@'localhost' with grant option")
	userPrivileges := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.user_privileges where grantee = \"'global_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, userPrivileges)
	userCatalog := mustQuerySQL(t, executor, "", "select table_catalog from information_schema.user_privileges where grantee = \"'global_reporter'@'localhost'\"")
	require.Equal(t, [][]interface{}{{"def"}}, userCatalog)
}

func TestInformationSchemaPrivilegeViewsExpandAllGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'all_table_privileges'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'all_schema_privileges'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'all_global_privileges'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant all privileges on app.users to 'all_table_privileges'@'localhost'")
	mustExecSQL(t, executor, "", "grant all privileges on app.* to 'all_schema_privileges'@'localhost'")
	mustExecSQL(t, executor, "", "grant all privileges on *.* to 'all_global_privileges'@'localhost'")

	tableRows := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.table_privileges where grantee = \"'all_table_privileges'@'localhost'\" order by privilege_type")
	require.ElementsMatch(t, [][]interface{}{
		{"ALTER", "NO"}, {"CREATE", "NO"}, {"CREATE VIEW", "NO"}, {"DELETE", "NO"}, {"DROP", "NO"}, {"INDEX", "NO"},
		{"INSERT", "NO"}, {"REFERENCES", "NO"}, {"SELECT", "NO"}, {"SHOW VIEW", "NO"}, {"TRIGGER", "NO"}, {"UPDATE", "NO"},
	}, tableRows)

	schemaRows := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.schema_privileges where grantee = \"'all_schema_privileges'@'localhost'\"")
	require.Len(t, schemaRows, 18)
	require.Contains(t, schemaRows, []interface{}{"CREATE ROUTINE", "NO"})
	require.Contains(t, schemaRows, []interface{}{"LOCK TABLES", "NO"})

	globalRows := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.user_privileges where grantee = \"'all_global_privileges'@'localhost'\"")
	require.NotEmpty(t, globalRows)
	for _, row := range globalRows {
		require.NotEqual(t, "ALL", row[0])
		require.NotEqual(t, "ALL PRIVILEGES", row[0])
		require.NotEqual(t, "GRANT OPTION", row[0])
	}
}

func TestRoleAdminOptionPersistsAndAppearsInShowGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'analyst'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'delegator'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'analyst'@'localhost' to 'delegator'@'localhost' with admin option")
	rows := mustQuerySQL(t, executor, "", "show grants for 'delegator'@'localhost'")
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], "WITH ADMIN OPTION")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "delegator", "localhost")
	require.NotNil(t, account)
	require.Contains(t, account.RoleAdminOptions, "analyst@localhost")
}

func TestColumnGrantOptionRemainsColumnScopedInPrivilegeMetadata(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'column_scope_reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'column_scope_reporter'@'localhost' with grant option")
	showGrants := mustQuerySQL(t, executor, "", "show grants for 'column_scope_reporter'@'localhost'")
	require.Len(t, showGrants, 1)
	require.Equal(t, "GRANT SELECT (email) ON app.users TO 'column_scope_reporter'@'localhost' WITH GRANT OPTION", showGrants[0][0])

	// A column-only grant must not create a table-level GRANT OPTION row.
	tableRows := mustQuerySQL(t, executor, "mysql", "select Table_priv, Grant_priv from mysql.tables_priv where User = 'column_scope_reporter' and Db = 'app' and Table_name = 'users'")
	require.Empty(t, tableRows)

	columnRows := mustQuerySQL(t, executor, "mysql", "select Column_priv from mysql.columns_priv where User = 'column_scope_reporter' and Db = 'app' and Table_name = 'users' and Column_name = 'email'")
	require.Equal(t, [][]interface{}{{"SELECT"}}, columnRows)

	metadataRows := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.column_privileges where grantee = \"'column_scope_reporter'@'localhost'\" and table_schema = 'app' and table_name = 'users' and column_name = 'email'")
	require.Equal(t, [][]interface{}{{"SELECT", "YES"}}, metadataRows)
	tableMetadata := mustQuerySQL(t, executor, "", "select privilege_type from information_schema.table_privileges where grantee = \"'column_scope_reporter'@'localhost'\" and table_schema = 'app' and table_name = 'users'")
	require.Empty(t, tableMetadata)
}

func TestRevokeGrantOptionForKeepsPrivilegeAndRemovesDelegation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'reporter'@'localhost' with grant option")
	mustExecSQL(t, executor, "", "revoke grant option for select on app.users from 'reporter'@'localhost'")
	rows := mustQuerySQL(t, executor, "mysql", "select Table_priv, Grant_priv from mysql.tables_priv where User = 'reporter' and Db = 'app' and Table_name = 'users'")
	require.Equal(t, [][]interface{}{{"SELECT", "N"}}, rows)
}

func TestRevokeColumnGrantOptionForKeepsColumnPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'column_reporter'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'column_reporter'@'localhost' with grant option")
	mustExecSQL(t, executor, "", "revoke grant option for select (email) on app.users from 'column_reporter'@'localhost'")

	rows := mustQuerySQL(t, executor, "mysql", "select Column_priv from mysql.columns_priv where User = 'column_reporter' and Db = 'app' and Table_name = 'users' and Column_name = 'email'")
	require.Equal(t, [][]interface{}{{"SELECT"}}, rows)
	metadata := mustQuerySQL(t, executor, "", "select privilege_type, is_grantable from information_schema.column_privileges where grantee = \"'column_reporter'@'localhost'\" and table_schema = 'app' and table_name = 'users' and column_name = 'email'")
	require.Equal(t, [][]interface{}{{"SELECT", "NO"}}, metadata)
}

func TestRoleGrantAndRevokeSupportMultipleRoles(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'reader'@'localhost'")
	mustExecSQL(t, executor, "", "create role 'writer'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'operator'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'reader'@'localhost', 'writer'@'localhost' to 'operator'@'localhost'")
	rows := mustQuerySQL(t, executor, "", "show grants for 'operator'@'localhost'")
	require.Len(t, rows, 2)
	require.Contains(t, rows[0][0], "reader")
	require.Contains(t, rows[1][0], "writer")
	mustExecSQL(t, executor, "", "revoke 'reader'@'localhost', 'writer'@'localhost' from 'operator'@'localhost'")
	require.Contains(t, mustQuerySQL(t, executor, "", "show grants for 'operator'@'localhost'")[0][0], "USAGE")
}

func TestRoleGrantAndRevokeSupportMultipleGranteesAtomically(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'batch_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_user_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'role_user_b'@'localhost' identified by 'secret'")

	mustExecSQL(t, executor, "", "grant 'batch_reader'@'localhost' to 'role_user_a'@'localhost', 'role_user_b'@'localhost'")
	for _, account := range []string{"role_user_a", "role_user_b"} {
		grants := mustQuerySQL(t, executor, "", fmt.Sprintf("show grants for '%s'@'localhost'", account))
		require.Len(t, grants, 1)
		require.Contains(t, grants[0][0], "batch_reader")
	}

	missing := <-executor.ExecuteQuery(nil, "grant 'batch_reader'@'localhost' to 'role_user_a'@'localhost', 'missing_role_user'@'localhost'", "")
	require.Error(t, missing.Err)
	require.Contains(t, mustQuerySQL(t, executor, "", "show grants for 'role_user_a'@'localhost'")[0][0], "batch_reader")

	mustExecSQL(t, executor, "", "revoke 'batch_reader'@'localhost' from 'role_user_a'@'localhost', 'role_user_b'@'localhost'")
	for _, account := range []string{"role_user_a", "role_user_b"} {
		grants := mustQuerySQL(t, executor, "", fmt.Sprintf("show grants for '%s'@'localhost'", account))
		require.Len(t, grants, 1)
		require.Contains(t, grants[0][0], "USAGE")
	}
}

func TestRevokeAllPrivilegesClearsScopeAndColumnGrants(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'cleanup'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, update on app.users to 'cleanup'@'localhost'")
	mustExecSQL(t, executor, "", "grant select (email) on app.users to 'cleanup'@'localhost'")
	mustExecSQL(t, executor, "", "revoke all privileges on app.users from 'cleanup'@'localhost'")
	require.Empty(t, mustQuerySQL(t, executor, "mysql", "select User from mysql.tables_priv where User = 'cleanup' and Db = 'app' and Table_name = 'users'"))
	require.Empty(t, mustQuerySQL(t, executor, "mysql", "select User from mysql.columns_priv where User = 'cleanup' and Db = 'app'"))
}

func TestAlterUserPasswordExpiryPersistsAndCanBeCleared(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'expiring'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "alter user 'expiring'@'localhost' password expire")
	expired := mustQuerySQL(t, executor, "", "select User, password_expired from mysql.user where User = 'expiring'")
	require.Equal(t, [][]interface{}{{"expiring", "Y"}}, expired)
	mustExecSQL(t, executor, "", "alter user 'expiring'@'localhost' password expire never")
	active := mustQuerySQL(t, executor, "", "select User, password_expired from mysql.user where User = 'expiring'")
	require.Equal(t, [][]interface{}{{"expiring", "N"}}, active)
}

func TestCreateUserPasswordExpiryPersists(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'expired_on_create'@'localhost' identified by 'secret' password expire")
	require.Equal(t, [][]interface{}{{"expired_on_create", "Y"}}, mustQuerySQL(t, executor, "", "select User, password_expired from mysql.user where User = 'expired_on_create'"))
}

func TestGrantAuthorizationRequiresPrivilegeAndGrantOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'grantor'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'grantee'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.* to 'grantor'@'localhost'")
	grantor := newTestMySQLSession()
	grantor.SetParamByName("user", "grantor")
	grantor.SetParamByName("host", "localhost")
	denied := <-executor.ExecuteQuery(grantor, "grant select on app.users to 'grantee'@'localhost'", "")
	require.Error(t, denied.Err)
	mustExecSQL(t, executor, "", "grant select on app.* to 'grantor'@'localhost' with grant option")
	allowed := <-executor.ExecuteQuery(grantor, "grant select on app.users to 'grantee'@'localhost'", "")
	require.NoError(t, allowed.Err)
}

func TestDynamicPrivilegesPersistExposeAndRevoke(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'backup_operator'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant backup_admin on *.* to 'backup_operator'@'localhost' with grant option")

	grants := mustQuerySQL(t, executor, "", "select user, host, priv, with_grant_option from mysql.global_grants where user='backup_operator'")
	require.Equal(t, [][]interface{}{{"backup_operator", "localhost", "BACKUP_ADMIN", "Y"}}, grants)
	show := mustQuerySQL(t, executor, "", "show grants for 'backup_operator'@'localhost'")
	require.Len(t, show, 1)
	require.Contains(t, show[0][0], "BACKUP_ADMIN")
	require.Contains(t, show[0][0], "WITH GRANT OPTION")
	privileges := mustQuerySQL(t, executor, "", "select grantee, privilege_type, is_grantable from information_schema.user_privileges where grantee=\"'backup_operator'@'localhost'\"")
	require.Contains(t, privileges, []interface{}{"'backup_operator'@'localhost'", "BACKUP_ADMIN", "YES"})

	mustExecSQL(t, executor, "", "revoke grant option for backup_admin on *.* from 'backup_operator'@'localhost'")
	grants = mustQuerySQL(t, executor, "", "select user, host, priv, with_grant_option from mysql.global_grants where user='backup_operator'")
	require.Equal(t, [][]interface{}{{"backup_operator", "localhost", "BACKUP_ADMIN", "N"}}, grants)
	mustExecSQL(t, executor, "", "revoke backup_admin on *.* from 'backup_operator'@'localhost'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select user, host, priv from mysql.global_grants where user='backup_operator'"))
}

func TestDynamicPrivilegeRegistryRejectsUnknownGrantAndShowsRegisteredPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'registry_user'@'localhost' identified by 'secret'")
	unknown := <-executor.ExecuteQuery(nil, "grant not_a_mysql_dynamic_privilege on *.* to 'registry_user'@'localhost'", "")
	require.Error(t, unknown.Err)
	require.Contains(t, unknown.Err.Error(), "unknown dynamic privilege")

	show := mustQuerySQL(t, executor, "", "show privileges")
	var foundBackupAdmin, foundSystemUser bool
	staticNames := map[string]bool{
		"Alter routine": false, "Create role": false, "Create routine": false,
		"Create tablespace": false, "Create temporary tables": false, "Create view": false,
		"Drop role": false, "Event": false, "Execute": false, "Replication client": false,
		"Replication slave": false, "Trigger": false, "Usage": false,
	}
	for _, row := range show {
		if len(row) < 1 {
			continue
		}
		switch row[0] {
		case "BACKUP_ADMIN":
			foundBackupAdmin = true
		case "SYSTEM_USER":
			foundSystemUser = true
		}
		if name, ok := row[0].(string); ok {
			if _, exists := staticNames[name]; exists {
				staticNames[name] = true
			}
		}
	}
	require.True(t, foundBackupAdmin, "SHOW PRIVILEGES omitted BACKUP_ADMIN")
	require.True(t, foundSystemUser, "SHOW PRIVILEGES omitted SYSTEM_USER")
	for name, found := range staticNames {
		require.True(t, found, "SHOW PRIVILEGES omitted %s", name)
	}
}

func TestGrantAllIncludesRegisteredDynamicPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'all_dynamic'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant all on *.* to 'all_dynamic'@'localhost'")
	grants := mustQuerySQL(t, executor, "", "select priv from mysql.global_grants where user='all_dynamic'")
	require.Len(t, grants, 46)
	require.Contains(t, grants, []interface{}{"SYSTEM_USER"})
	mustExecSQL(t, executor, "", "revoke all privileges on *.* from 'all_dynamic'@'localhost'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select priv from mysql.global_grants where user='all_dynamic'"))
}

func TestProxyPrivilegeIsAcceptedAsStaticGrant(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'proxied'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxy_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant proxy on 'proxied'@'localhost' to 'proxy_user'@'localhost'")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "proxy_user", "localhost")
	require.NotNil(t, account)
	require.NotEmpty(t, account.Grants)
	require.Contains(t, account.Grants, "'proxied'@'localhost'")
	require.Contains(t, account.Grants["'proxied'@'localhost'"], "PROXY")

	grants := mustQuerySQL(t, executor, "", "show grants for 'proxy_user'@'localhost'")
	require.Len(t, grants, 1)
	require.Contains(t, grants[0][0], "PROXY")
	proxyRows := mustQuerySQL(t, executor, "", "select Host, User, Proxied_host, Proxied_user, With_grant from mysql.proxies_priv where User = 'proxy_user'")
	require.Equal(t, [][]interface{}{{"localhost", "proxy_user", "localhost", "proxied", "N"}}, proxyRows)
}

func TestMySQLProxiesPrivFiltersApplyToProxyTargetColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'proxied_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxied_b'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxy_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxy_b'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant proxy on 'proxied_a'@'localhost' to 'proxy_a'@'localhost'")
	mustExecSQL(t, executor, "", "grant proxy on 'proxied_b'@'localhost' to 'proxy_b'@'localhost'")

	rows := mustQuerySQL(t, executor, "", "select User, Proxied_user from mysql.proxies_priv where Proxied_user in ('proxied_b')")
	require.Equal(t, [][]interface{}{{"proxy_b", "proxied_b"}}, rows)
	rows = mustQuerySQL(t, executor, "", "select User, Proxied_user from mysql.proxies_priv where Proxied_user not in ('proxied_b')")
	require.Equal(t, [][]interface{}{{"proxy_a", "proxied_a"}}, rows)
	rows = mustQuerySQL(t, executor, "", "select User, Proxied_user from mysql.proxies_priv where Proxied_host is not null")
	require.Len(t, rows, 2)
}

func TestDropUserCleansRoleAndProxyReferences(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_role'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_holder'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxied'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxy_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'report_role'@'localhost' to 'role_holder'@'localhost'")
	mustExecSQL(t, executor, "", "grant proxy on 'proxied'@'localhost' to 'proxy_user'@'localhost'")

	mustExecSQL(t, executor, "", "drop user 'proxied'@'localhost'")
	mustExecSQL(t, executor, "", "drop role 'report_role'@'localhost'")

	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.proxies_priv where Proxied_user = 'proxied'"))
	roleGrants := mustQuerySQL(t, executor, "", "show grants for 'role_holder'@'localhost'")
	require.Len(t, roleGrants, 1)
	require.NotContains(t, roleGrants[0][0], "report_role")
}

func TestRenameUserUpdatesProxyTargets(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'proxied_old'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'proxy_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant proxy on 'proxied_old'@'localhost' to 'proxy_user'@'localhost'")
	mustExecSQL(t, executor, "", "rename user 'proxied_old'@'localhost' to 'proxied_new'@'localhost'")

	rows := mustQuerySQL(t, executor, "", "select Proxied_user from mysql.proxies_priv where User = 'proxy_user'")
	require.Equal(t, [][]interface{}{{"proxied_new"}}, rows)
}

func TestShowCreateUserReflectsPersistedAuthenticationState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'inspectable'@'localhost' identified with mysql_native_password by 'secret' require x509 password expire")
	rows := mustQuerySQL(t, executor, "", "show create user 'inspectable'@'localhost'")
	require.Len(t, rows, 1)
	require.Equal(t, "'inspectable'@'localhost'", rows[0][0])
	require.Contains(t, rows[0][1], "CREATE USER 'inspectable'@'localhost'")
	require.Contains(t, rows[0][1], "IDENTIFIED WITH 'mysql_native_password'")
	require.Contains(t, rows[0][1], "REQUIRE X509")
	require.Contains(t, rows[0][1], "PASSWORD EXPIRE")

	mustExecSQL(t, executor, "", "create user 'default_options'@'localhost' identified by 'secret'")
	defaults := mustQuerySQL(t, executor, "", "show create user 'default_options'@'localhost'")
	createSQL := fmt.Sprint(defaults[0][1])
	require.Contains(t, createSQL, "REQUIRE NONE")
	require.Contains(t, createSQL, "ACCOUNT UNLOCK")
	require.Contains(t, createSQL, "PASSWORD EXPIRE DEFAULT")
	require.Contains(t, createSQL, "PASSWORD HISTORY DEFAULT")
	require.Contains(t, createSQL, "PASSWORD REUSE INTERVAL DEFAULT")
	require.Contains(t, createSQL, "PASSWORD REQUIRE CURRENT DEFAULT")
}

func TestShowCreateUserEnforcesSystemSchemaAndHashVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'show_operator'@'localhost' identified by 'operator-secret'")
	mustExecSQL(t, executor, "", "create user 'show_target'@'localhost' identified by 'target-secret'")

	operator := newTestMySQLSession()
	operator.SetParamByName("user", "show_operator")
	operator.SetParamByName("host", "localhost")

	// A user can inspect its own definition, but the authentication hash is
	// masked until SELECT on mysql.user is granted.
	self := <-executor.ExecuteQuery(operator, "show create user 'show_operator'@'localhost'", "")
	require.NoError(t, self.Err)
	selfRows := self.Data.(*SelectResult).Records
	require.Contains(t, selfRows[0].GetValues()[1].String(), "AS '<secret>'")
	currentUser := <-executor.ExecuteQuery(operator, "show create user current_user()", "")
	require.NoError(t, currentUser.Err)
	require.Contains(t, currentUser.Data.(*SelectResult).Records[0].GetValues()[0].String(), "'show_operator'@'localhost'")

	denied := <-executor.ExecuteQuery(operator, "show create user 'show_target'@'localhost'", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "SELECT")

	mustExecSQL(t, executor, "", "grant select on mysql.* to 'show_operator'@'localhost'")
	allowed := <-executor.ExecuteQuery(operator, "show create user 'show_target'@'localhost'", "")
	require.NoError(t, allowed.Err)
	allowedRows := allowed.Data.(*SelectResult).Records
	require.NotContains(t, allowedRows[0].GetValues()[1].String(), "<secret>")

	mustExecSQL(t, executor, "", "revoke select on mysql.* from 'show_operator'@'localhost'")
	mustExecSQL(t, executor, "", "grant select on mysql.user to 'show_operator'@'localhost'")
	selfWithTableSelect := <-executor.ExecuteQuery(operator, "show create user 'show_operator'@'localhost'", "")
	require.NoError(t, selfWithTableSelect.Err)
	selfWithTableRows := selfWithTableSelect.Data.(*SelectResult).Records
	require.NotContains(t, selfWithTableRows[0].GetValues()[1].String(), "<secret>")
}

func TestRoleAuthorizationRequiresAdminOption(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'analyst'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'operator'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'target'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'analyst'@'localhost' to 'operator'@'localhost'")
	operator := newTestMySQLSession()
	operator.SetParamByName("user", "operator")
	operator.SetParamByName("host", "localhost")
	denied := <-executor.ExecuteQuery(operator, "grant 'analyst'@'localhost' to 'target'@'localhost'", "")
	require.Error(t, denied.Err)
	mustExecSQL(t, executor, "", "grant 'analyst'@'localhost' to 'operator'@'localhost' with admin option")
	allowed := <-executor.ExecuteQuery(operator, "grant 'analyst'@'localhost' to 'target'@'localhost'", "")
	require.NoError(t, allowed.Err)
}

func TestAccountManagementRequiresDedicatedPrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'operator'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'victim'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create role 'reporter'@'localhost'")
	operator := newTestMySQLSession()
	operator.SetParamByName("user", "operator")
	operator.SetParamByName("host", "localhost")

	for _, query := range []string{
		"create user 'new_user'@'localhost' identified by 'secret'",
		"create role 'new_role'@'localhost'",
		"drop user 'victim'@'localhost'",
		"drop role 'reporter'@'localhost'",
	} {
		result := <-executor.ExecuteQuery(operator, query, "")
		require.Error(t, result.Err, query)
	}

	// An account may change its own password, but cannot alter another account.
	self := <-executor.ExecuteQuery(operator, "alter user 'operator'@'localhost' identified by 'new-secret'", "")
	require.NoError(t, self.Err)
	other := <-executor.ExecuteQuery(operator, "alter user 'victim'@'localhost' identified by 'new-secret'", "")
	require.Error(t, other.Err)

	mustExecSQL(t, executor, "", "grant create user on *.* to 'operator'@'localhost'")
	created := <-executor.ExecuteQuery(operator, "create user 'new_user'@'localhost' identified by 'secret'", "")
	require.NoError(t, created.Err)
}

func TestSetPasswordAndRenameUserPersistAccountState(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'operator'@'localhost' identified by 'old-secret'")
	mustExecSQL(t, executor, "", "create user 'target'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "set password for 'operator'@'localhost' = 'new-secret'")
	password := mustQuerySQL(t, executor, "", "select authentication_string from mysql.user where User = 'operator'")
	require.Len(t, password, 1)
	require.NotEqual(t, "", password[0][0])

	mustExecSQL(t, executor, "", "rename user 'operator'@'localhost' to 'renamed'@'localhost'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'operator'"))
	require.Equal(t, [][]interface{}{{"renamed"}}, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'renamed'"))
}

func TestRenameUserValidatesAllPairsBeforeMutation(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'rename_a'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "create user 'rename_b'@'localhost' identified by 'secret'")

	missing := <-executor.ExecuteQuery(nil, "rename user 'rename_a'@'localhost' to 'renamed_a'@'localhost', 'missing_rename'@'localhost' to 'renamed_missing'@'localhost'", "")
	require.Error(t, missing.Err)
	require.Equal(t, [][]interface{}{{"rename_a"}}, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'rename_a'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'renamed_a'"))

	duplicate := <-executor.ExecuteQuery(nil, "rename user 'rename_a'@'localhost' to 'renamed_same'@'localhost', 'rename_b'@'localhost' to 'renamed_same'@'localhost'", "")
	require.Error(t, duplicate.Err)
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where User = 'renamed_same'"))
}

func TestAccountRequireX509PersistsTransportRequirement(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'cert_user'@'localhost' identified by 'secret' require x509")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "cert_user", "localhost")
	require.NotNil(t, account)
	require.True(t, account.TLSRequired)
	require.True(t, account.X509Required)
}

func TestAlterUserRequireNoneClearsPreviousTransportRequirement(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'transport_user'@'localhost' identified by 'secret' require x509")
	require.Equal(t, [][]interface{}{{"X509"}}, mustQuerySQL(t, executor, "", "select ssl_type from mysql.user where User = 'transport_user'"))
	mustExecSQL(t, executor, "", "alter user 'transport_user'@'localhost' require none")

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "transport_user", "localhost")
	require.NotNil(t, account)
	require.False(t, account.TLSRequired)
	require.False(t, account.X509Required)
	require.Equal(t, [][]interface{}{{""}}, mustQuerySQL(t, executor, "", "select ssl_type from mysql.user where User = 'transport_user'"))

	rows := mustQuerySQL(t, executor, "", "show create user 'transport_user'@'localhost'")
	require.Len(t, rows, 1)
	require.NotContains(t, rows[0][1], "REQUIRE SSL")
	require.NotContains(t, rows[0][1], "REQUIRE X509")
}

func TestAccountTLSAttributesAndResourceLimitsPersistAndProject(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'bounded_user'@'localhost' identified by 'secret' require x509 and cipher 'TLS_AES_256_GCM_SHA384' and issuer 'CN=Test CA' and subject 'CN=bounded' with max_queries_per_hour 60 max_updates_per_hour 7 max_connections_per_hour 8 max_user_connections 2")

	rows := mustQuerySQL(t, executor, "", "select ssl_type, ssl_cipher, x509_issuer, x509_subject, max_questions, max_updates, max_connections, max_user_connections from mysql.user where User = 'bounded_user'")
	require.Equal(t, [][]interface{}{{"X509", "TLS_AES_256_GCM_SHA384", "CN=Test CA", "CN=bounded", "60", "7", "8", "2"}}, rows)
	require.Len(t, mustQuerySQL(t, executor, "", "select User from mysql.user where max_user_connections = '2'"), 1)
	require.Empty(t, mustQuerySQL(t, executor, "", "select User from mysql.user where max_user_connections = '3'"))

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "bounded_user", "localhost")
	require.NotNil(t, account)
	require.Equal(t, int64(60), *account.MaxQuestions)
	require.Equal(t, int64(2), *account.MaxUserConnections)

	show := mustQuerySQL(t, executor, "", "show create user 'bounded_user'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), "CIPHER 'TLS_AES_256_GCM_SHA384'")
	require.Contains(t, fmt.Sprint(show[0][1]), "MAX_QUERIES_PER_HOUR 60")
	require.Contains(t, fmt.Sprint(show[0][1]), "MAX_USER_CONNECTIONS 2")

	mustExecSQL(t, executor, "", "alter user 'bounded_user'@'localhost' require none with max_user_connections 0")
	rows = mustQuerySQL(t, executor, "", "select ssl_type, ssl_cipher, x509_issuer, x509_subject, max_user_connections from mysql.user where User = 'bounded_user'")
	require.Equal(t, [][]interface{}{{"", "", "", "", "0"}}, rows)
}

func TestAccountFailedLoginPolicyPersistsAndProjects(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'lock_policy'@'localhost' identified by 'secret' failed_login_attempts 2 password_lock_time 3")

	rows := mustQuerySQL(t, executor, "", "select failed_login_attempts, password_lock_time from mysql.user where User = 'lock_policy'")
	require.Equal(t, [][]interface{}{{"2", "3"}}, rows)
	show := mustQuerySQL(t, executor, "", "show create user 'lock_policy'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), "FAILED_LOGIN_ATTEMPTS 2")
	require.Contains(t, fmt.Sprint(show[0][1]), "PASSWORD_LOCK_TIME 3")

	mustExecSQL(t, executor, "", "alter user 'lock_policy'@'localhost' failed_login_attempts 4 password_lock_time unbounded")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "lock_policy", "localhost")
	require.NotNil(t, account)
	require.Equal(t, int64(4), *account.FailedLoginAttempts)
	require.True(t, account.PasswordLockUnbounded)
	show = mustQuerySQL(t, executor, "", "show create user 'lock_policy'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), "PASSWORD_LOCK_TIME UNBOUNDED")
}

func TestConcurrentGrantUpdatesAreSerializedAndDurable(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'alice'@'localhost' identified by 'secret'")
	var wait sync.WaitGroup
	for _, statement := range []string{
		"grant select on app.* to 'alice'@'localhost'",
		"grant insert on app.* to 'alice'@'localhost'",
	} {
		wait.Add(1)
		go func(query string) {
			defer wait.Done()
			result := <-executor.ExecuteQuery(nil, query, "")
			require.NoError(t, result.Err)
		}(statement)
	}
	wait.Wait()
	rows := mustQuerySQL(t, executor, "", "show grants for 'alice'@'localhost'")
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0], "SELECT")
	require.Contains(t, rows[0][0], "INSERT")
}

func TestAccountGrantChangesCommitAndRollbackWithSessionTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create user 'txn_user'@'localhost' identified by 'secret'")

	mustExecSessionSQL(t, executor, session, "", "begin")
	mustExecSessionSQL(t, executor, session, "", "grant select on app.* to 'txn_user'@'localhost'")

	fileBeforeRollback, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	accountBeforeRollback := findPersistedAccount(fileBeforeRollback, "txn_user", "localhost")
	require.NotNil(t, accountBeforeRollback)
	require.Empty(t, accountBeforeRollback.Grants["app.*"], "grant must remain staged until commit")

	mustExecSessionSQL(t, executor, session, "", "rollback")
	rolledBack, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Empty(t, findPersistedAccount(rolledBack, "txn_user", "localhost").Grants["app.*"])

	mustExecSessionSQL(t, executor, session, "", "begin")
	mustExecSessionSQL(t, executor, session, "", "grant select on app.* to 'txn_user'@'localhost'")
	mustExecSessionSQL(t, executor, session, "", "commit")
	committed, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Contains(t, findPersistedAccount(committed, "txn_user", "localhost").Grants["app.*"], "SELECT")
}

func findPersistedAccount(file persistedAccountFile, user, host string) *persistedAccount {
	for i := range file.Accounts {
		if file.Accounts[i].User == user && file.Accounts[i].Host == host {
			return &file.Accounts[i]
		}
	}
	return nil
}

func TestCreateAndDropRolePersistAsGrantableAccounts(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create role 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'bob'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on app.users to 'report_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	grants := mustQuerySQL(t, executor, "", "show grants for 'bob'@'localhost'")
	require.Contains(t, grants[0][0], "report_reader")
	mustExecSQL(t, executor, "", "drop role 'report_reader'@'localhost'")
	raw, err := os.ReadFile(filepath.Join(executor.GetDataDir(), "mysql", "accounts.json"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "report_reader")
}

func TestPartialRevokesPreserveGlobalGrantAndExposeRestriction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global partial_revokes = 'ON'")
	mustExecSQL(t, executor, "", "create user 'restricted_user'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, insert on *.* to 'restricted_user'@'localhost'")
	mustExecSQL(t, executor, "", "revoke insert on app.* from 'restricted_user'@'localhost'")

	rows := mustQuerySQL(t, executor, "", "show grants for 'restricted_user'@'localhost'")
	require.Len(t, rows, 2)
	require.Contains(t, fmt.Sprint(rows[0][0]), "GRANT SELECT, INSERT ON *.*")
	require.Contains(t, fmt.Sprint(rows[1][0]), "REVOKE INSERT ON app.*")

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "restricted_user", "localhost")
	require.NotNil(t, account)
	require.Equal(t, []string{"INSERT"}, account.Restrictions["app"])
	var attributes map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(account.UserAttributes), &attributes))
	require.Contains(t, fmt.Sprint(attributes["Restrictions"]), "app")
	effective := effectiveAccountGrants(file, *account, nil)
	require.True(t, grantsContain(effective, "other.table", "INSERT"))
	require.False(t, grantsContain(effective, "app.table", "INSERT"))
	require.True(t, grantsContain(effective, "app.table", "SELECT"))

	mustExecSQL(t, executor, "", "grant insert on app.* to 'restricted_user'@'localhost'")
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account = findPersistedAccount(file, "restricted_user", "localhost")
	require.Empty(t, account.Restrictions)
	mustExecSQL(t, executor, "", "revoke insert on *.* from 'restricted_user'@'localhost'")
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account = findPersistedAccount(file, "restricted_user", "localhost")
	require.NotContains(t, account.Grants["*.*"], "INSERT")
}

func TestInformationSchemaMetadataHonorsPartialRevokeObjectVisibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global partial_revokes = 'ON'")
	mustExecSQL(t, executor, "", "create database restricted_metadata")
	mustExecSQL(t, executor, "restricted_metadata", "create table hidden_table (id int primary key)")
	mustExecSQL(t, executor, "", "create user 'metadata_restricted'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'metadata_restricted'@'localhost'")
	mustExecSQL(t, executor, "", "revoke select on restricted_metadata.* from 'metadata_restricted'@'localhost'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "metadata_restricted")
	session.SetParamByName("host", "localhost")
	for _, query := range []string{
		"select schema_name from information_schema.schemata where schema_name = 'restricted_metadata'",
		"select table_name from information_schema.tables where table_schema = 'restricted_metadata' and table_name = 'hidden_table'",
		"select column_name from information_schema.columns where table_schema = 'restricted_metadata' and table_name = 'hidden_table'",
		"select index_name from information_schema.statistics where table_schema = 'restricted_metadata' and table_name = 'hidden_table'",
	} {
		require.Empty(t, mustQuerySessionSQL(t, executor, session, "", query), query)
	}

	// A direct table grant is not a global privilege restriction and must still
	// make the table metadata visible, matching MySQL's partial-revoke rules.
	mustExecSQL(t, executor, "", "grant select on restricted_metadata.hidden_table to 'metadata_restricted'@'localhost'")
	require.Equal(t, [][]interface{}{{"restricted_metadata"}}, mustQuerySessionSQL(t, executor, session, "", "select schema_name from information_schema.schemata where schema_name = 'restricted_metadata'"))
	require.Equal(t, [][]interface{}{{"hidden_table"}}, mustQuerySessionSQL(t, executor, session, "", "select table_name from information_schema.tables where table_schema = 'restricted_metadata' and table_name = 'hidden_table'"))
	require.Equal(t, [][]interface{}{{"id"}}, mustQuerySessionSQL(t, executor, session, "", "select column_name from information_schema.columns where table_schema = 'restricted_metadata' and table_name = 'hidden_table'"))
	require.Equal(t, [][]interface{}{{"PRIMARY"}}, mustQuerySessionSQL(t, executor, session, "", "select index_name from information_schema.statistics where table_schema = 'restricted_metadata' and table_name = 'hidden_table' and index_name = 'PRIMARY'"))
}

func TestPartialRevokesRequireEnablementForGlobalGrantRestriction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'partial_off'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select on *.* to 'partial_off'@'localhost'")
	result := <-executor.ExecuteQuery(nil, "revoke select on app.* from 'partial_off'@'localhost'", "")
	require.Error(t, result.Err)
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	account := findPersistedAccount(file, "partial_off", "localhost")
	require.NotNil(t, account)
	require.Empty(t, account.Restrictions)
}

func TestPartialRevokesFollowAccountTransactionBoundaries(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global partial_revokes = 'ON'")
	mustExecSQL(t, executor, "", "create user 'partial_txn'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant select, insert on *.* to 'partial_txn'@'localhost'")

	mustExecSessionSQL(t, executor, admin, "", "begin")
	mustExecSessionSQL(t, executor, admin, "", "revoke insert on app.* from 'partial_txn'@'localhost'")
	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Empty(t, findPersistedAccount(file, "partial_txn", "localhost").Restrictions)
	mustExecSessionSQL(t, executor, admin, "", "rollback")
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Empty(t, findPersistedAccount(file, "partial_txn", "localhost").Restrictions)

	mustExecSessionSQL(t, executor, admin, "", "begin")
	mustExecSessionSQL(t, executor, admin, "", "revoke insert on app.* from 'partial_txn'@'localhost'")
	mustExecSessionSQL(t, executor, admin, "", "commit")
	file, err = executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	require.Equal(t, []string{"INSERT"}, findPersistedAccount(file, "partial_txn", "localhost").Restrictions["app"])
}

func TestPartialRevokesApplyThroughRoleInheritance(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global partial_revokes = 'ON'")
	mustExecSQL(t, executor, "", "create role 'restricted_role'@'localhost'")
	mustExecSQL(t, executor, "", "grant select, insert on *.* to 'restricted_role'@'localhost'")
	mustExecSQL(t, executor, "", "revoke insert on app.* from 'restricted_role'@'localhost'")
	mustExecSQL(t, executor, "", "create user 'role_consumer'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant 'restricted_role'@'localhost' to 'role_consumer'@'localhost'")

	file, err := executor.QueryExecutor.loadPersistedAccounts()
	require.NoError(t, err)
	consumer := findPersistedAccount(file, "role_consumer", "localhost")
	require.NotNil(t, consumer)
	effective := effectiveAccountGrants(file, *consumer, nil)
	require.True(t, grantsContain(effective, "other.table", "INSERT"))
	require.False(t, grantsContain(effective, "app.table", "INSERT"))
	require.True(t, grantsContain(effective, "app.table", "SELECT"))
}

func TestMySQLSystemCostAndComponentTablesExposeMySQL84ReadShapes(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	serverCost := mustSelectResultSQL(t, executor, "", "select cost_name, cost_value, last_update, comment, default_value from mysql.server_cost order by cost_name")
	require.Equal(t, []string{"COST_NAME", "COST_VALUE", "LAST_UPDATE", "COMMENT", "DEFAULT_VALUE"}, serverCost.Columns)
	require.Len(t, serverCost.Records, 6)
	serverRows := mustQuerySQL(t, executor, "", "select cost_name, default_value from mysql.server_cost where cost_name in ('row_evaluate_cost', 'missing_cost') order by cost_name")
	require.Equal(t, [][]interface{}{{"row_evaluate_cost", "0.1"}}, serverRows)

	engineRows := mustQuerySQL(t, executor, "", "select engine_name, device_type, cost_name, default_value from mysql.engine_cost where cost_name = 'io_block_read_cost'")
	require.Equal(t, [][]interface{}{{"default", "0", "io_block_read_cost", "1"}}, engineRows)

	passwordHistory := mustSelectResultSQL(t, executor, "", "select Host, User, Password_timestamp, Password from mysql.password_history")
	require.Equal(t, []string{"HOST", "USER", "PASSWORD_TIMESTAMP", "PASSWORD"}, passwordHistory.Columns)
	require.Empty(t, passwordHistory.Records)

	component := mustSelectResultSQL(t, executor, "", "select component_id, component_group_id, component_urn from mysql.component")
	require.Equal(t, []string{"COMPONENT_ID", "COMPONENT_GROUP_ID", "COMPONENT_URN"}, component.Columns)
	require.Empty(t, component.Records)

	tables := mustQuerySQL(t, executor, "", "select table_name from information_schema.tables where table_schema = 'mysql' and table_name in ('component', 'engine_cost', 'password_history', 'server_cost') order by table_name")
	require.Equal(t, [][]interface{}{{"component"}, {"engine_cost"}, {"password_history"}, {"server_cost"}}, tables)
	columns := mustQuerySQL(t, executor, "", "select table_name, column_name from information_schema.columns where table_schema = 'mysql' and table_name = 'component' order by ordinal_position")
	require.Equal(t, [][]interface{}{{"component", "component_id"}, {"component", "component_group_id"}, {"component", "component_urn"}}, columns)
}

func TestMySQLPasswordHistoryTracksPasswordChanges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	mustExecSQL(t, executor, "", "create user 'history_user'@'localhost' identified by 'old-secret'")
	mustExecSQL(t, executor, "", "alter user 'history_user'@'localhost' identified by 'new-secret'")
	mustExecSQL(t, executor, "", "set password for 'history_user'@'localhost' = 'final-secret'")

	history := mustQuerySQL(t, executor, "", "select Host, User, Password_timestamp, Password from mysql.password_history where User = 'history_user'")
	require.Len(t, history, 2)
	require.Equal(t, "localhost", history[0][0])
	require.Equal(t, "history_user", history[0][1])
	require.NotEmpty(t, history[0][2])
	require.Equal(t, nativePasswordHash("old-secret"), history[0][3])
	require.Equal(t, nativePasswordHash("new-secret"), history[1][3])

	filtered := mustQuerySQL(t, executor, "", "select User, Password from mysql.password_history where Password = '"+nativePasswordHash("new-secret")+"'")
	require.Equal(t, [][]interface{}{{"history_user", nativePasswordHash("new-secret")}}, filtered)
}

func TestMySQLComponentInstallAndUninstallPersistRegistryRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	mustExecSQL(t, executor, "", "install component 'file://component_test'")
	installed := mustQuerySQL(t, executor, "", "select component_id, component_group_id, component_urn from mysql.component where component_urn = 'file://component_test'")
	require.Len(t, installed, 1)
	require.Equal(t, "file://component_test", installed[0][2])
	require.NotEmpty(t, installed[0][0])
	require.NotEmpty(t, installed[0][1])

	mustExecSQL(t, executor, "", "uninstall component 'file://component_test'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select component_urn from mysql.component where component_urn = 'file://component_test'"))
}

func TestMySQLComponentLifecycleRequiresTablePrivileges(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'component_operator'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "component_operator")
	session.SetParamByName("host", "localhost")

	deniedInstall := <-executor.ExecuteQuery(session, "install component 'file://component_denied'", "")
	require.Error(t, deniedInstall.Err)
	require.Contains(t, deniedInstall.Err.Error(), "lacks INSERT privilege on table 'mysql.component'")

	mustExecSQL(t, executor, "", "grant insert, select on mysql.component to 'component_operator'@'localhost'")
	mustExecSessionSQL(t, executor, session, "", "install component 'file://component_allowed'")

	deniedUninstall := <-executor.ExecuteQuery(session, "uninstall component 'file://component_allowed'", "")
	require.Error(t, deniedUninstall.Err)
	require.Contains(t, deniedUninstall.Err.Error(), "lacks DELETE privilege on table 'mysql.component'")

	mustExecSQL(t, executor, "", "grant delete on mysql.component to 'component_operator'@'localhost'")
	mustExecSessionSQL(t, executor, session, "", "uninstall component 'file://component_allowed'")
}

func TestFlushOptimizerCostsRequiresDedicatedPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'cost_operator'@'localhost' identified by 'secret'")
	session := newTestMySQLSession()
	session.SetParamByName("user", "cost_operator")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "flush optimizer_costs", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "FLUSH_OPTIMIZER_COSTS")

	session.SetParamByName("dynamic_privileges", []string{"FLUSH_OPTIMIZER_COSTS"})
	require.NoError(t, (<-executor.ExecuteQuery(session, "flush optimizer_costs", "")).Err)
	session.SetParamByName("dynamic_privileges", nil)
	session.SetParamByName("global_privileges", []common.PrivilegeType{common.ReloadPriv})
	require.NoError(t, (<-executor.ExecuteQuery(session, "flush local optimizer_costs", "")).Err)
}

func TestMySQLOptimizerCostTablesSupportDurableDMLAndFlush(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	queryExecutor := executor.QueryExecutor
	require.NoError(t, queryExecutor.ensureOptimizerCostModelLoaded())
	initial, ok := queryExecutor.optimizerCostSnapshotValue("row_evaluate_cost")
	require.True(t, ok)
	require.Equal(t, 0.1, initial)

	mustExecSQL(t, executor, "", "insert into mysql.server_cost (cost_name, cost_value, comment) values ('custom_cost', 2.5, 'custom')")
	mustExecSQL(t, executor, "", "update mysql.server_cost set cost_value = 3.5, comment = 'updated' where cost_name = 'row_evaluate_cost'")
	mustExecSQL(t, executor, "", "insert into mysql.engine_cost (engine_name, device_type, cost_name, cost_value) values ('InnoDB', 0, 'custom_io_cost', 4.25)")

	serverRows := mustQuerySQL(t, executor, "", "select cost_name, cost_value, comment from mysql.server_cost where cost_name in ('custom_cost', 'row_evaluate_cost') order by cost_name")
	require.ElementsMatch(t, [][]interface{}{{"custom_cost", "2.5", "custom"}, {"row_evaluate_cost", "3.5", "updated"}}, serverRows)
	engineRows := mustQuerySQL(t, executor, "", "select engine_name, device_type, cost_name, cost_value from mysql.engine_cost where cost_name = 'custom_io_cost'")
	require.Equal(t, [][]interface{}{{"InnoDB", "0", "custom_io_cost", "4.25"}}, engineRows)

	beforeFlush, ok := queryExecutor.optimizerCostSnapshotValue("row_evaluate_cost")
	require.True(t, ok)
	require.Equal(t, initial, beforeFlush)
	mustExecSQL(t, executor, "", "flush optimizer_costs")
	afterFlush, ok := queryExecutor.optimizerCostSnapshotValue("row_evaluate_cost")
	require.True(t, ok)
	require.Equal(t, 3.5, afterFlush)

	mustExecSQL(t, executor, "", "delete from mysql.server_cost where cost_name = 'custom_cost'")
	mustExecSQL(t, executor, "", "delete from mysql.engine_cost where engine_name = 'InnoDB' and device_type = 0 and cost_name = 'custom_io_cost'")
	require.Empty(t, mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = 'custom_cost'"))
	require.Empty(t, mustQuerySQL(t, executor, "", "select cost_name from mysql.engine_cost where cost_name = 'custom_io_cost'"))
}

func TestMySQLOptimizerCostDMLRollsBackWithSessionTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	otherSession := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "start transaction")
	mustExecSessionSQL(t, executor, session, "", "insert into mysql.server_cost (cost_name, cost_value) values ('transaction_cost', 1.75)")
	require.Len(t, mustQuerySessionSQL(t, executor, session, "", "select cost_name from mysql.server_cost where cost_name = 'transaction_cost'"), 1)
	require.Empty(t, mustQuerySessionSQL(t, executor, otherSession, "", "select cost_name from mysql.server_cost where cost_name = 'transaction_cost'"))
	mustExecSessionSQL(t, executor, session, "", "rollback")
	require.Empty(t, mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = 'transaction_cost'"))
}

func TestMySQLOptimizerCostNullValuesFollowNotInUnknownSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "insert into mysql.server_cost (cost_name, cost_value, comment) values ('null_cost', null, null)")

	rows := mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = 'null_cost' and cost_value is null")
	require.Equal(t, [][]interface{}{{"null_cost"}}, rows)

	inRows := mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = 'null_cost' and cost_value in (1.0)")
	require.Empty(t, inRows)
	notInRows := mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = 'null_cost' and cost_value not in (1.0)")
	require.Empty(t, notInRows)
}

func TestMySQLSystemTableEmptyStringPredicatesDoNotRestoreRows(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	rows := mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name = ''")
	require.Empty(t, rows)
	likeRows := mustQuerySQL(t, executor, "", "select cost_name from mysql.server_cost where cost_name like ''")
	require.Empty(t, likeRows)
}

func TestPasswordReusePolicyBlocksRecentAndConfiguredGlobalHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})

	mustExecSQL(t, executor, "", "create user 'policy_user'@'localhost' identified by 'first-secret' password history 2 password reuse interval 30 day")
	policyRows := mustQuerySQL(t, executor, "", "select User, password_reuse_history, password_reuse_time from mysql.user where User = 'policy_user'")
	require.Equal(t, [][]interface{}{{"policy_user", "2", "30"}}, policyRows)
	mustExecSQL(t, executor, "", "alter user 'policy_user'@'localhost' identified by 'second-secret'")
	blocked := <-executor.ExecuteQuery(nil, "alter user 'policy_user'@'localhost' identified by 'first-secret'", "")
	require.Error(t, blocked.Err)
	require.Contains(t, blocked.Err.Error(), "Password reuse is not allowed")

	mustExecSessionSQL(t, executor, session, "", "set global password_history = 1")
	mustExecSQL(t, executor, "", "create user 'global_policy_user'@'localhost' identified by 'global-first'")
	mustExecSQL(t, executor, "", "alter user 'global_policy_user'@'localhost' identified by 'global-second'")
	globalBlocked := <-executor.ExecuteQuery(nil, "alter user 'global_policy_user'@'localhost' identified by 'global-first'", "")
	require.Error(t, globalBlocked.Err)
	require.Contains(t, globalBlocked.Err.Error(), "Password reuse is not allowed")
}

func TestPasswordReusePolicyIsVisibleInShowCreateUserAndSetPassword(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'set_password_policy'@'localhost' identified by 'first-secret' password history 1")
	mustExecSQL(t, executor, "", "set password for 'set_password_policy'@'localhost' = 'second-secret'")

	show := mustQuerySQL(t, executor, "", "show create user 'set_password_policy'@'localhost'")
	require.Len(t, show, 1)
	require.Contains(t, fmt.Sprint(show[0][1]), "PASSWORD HISTORY 1")

	blocked := <-executor.ExecuteQuery(nil, "set password for 'set_password_policy'@'localhost' = 'first-secret'", "")
	require.Error(t, blocked.Err)
	require.Contains(t, blocked.Err.Error(), "Password reuse is not allowed")
}

func TestPasswordRequireCurrentPolicyAndReplaceSyntax(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'verify_password'@'localhost' identified by 'first-secret' password require current")

	rows := mustQuerySQL(t, executor, "", "select User, password_require_current from mysql.user where User = 'verify_password'")
	require.Equal(t, [][]interface{}{{"verify_password", "Y"}}, rows)
	show := mustQuerySQL(t, executor, "", "show create user 'verify_password'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), "PASSWORD REQUIRE CURRENT")

	session := newTestMySQLSession()
	session.SetParamByName("user", "verify_password")
	session.SetParamByName("host", "localhost")
	missingCurrent := <-executor.ExecuteQuery(session, "alter user 'verify_password'@'localhost' identified by 'second-secret'", "")
	require.Error(t, missingCurrent.Err)
	require.Contains(t, missingCurrent.Err.Error(), "Current password is required")

	mustExecSessionSQL(t, executor, session, "", "alter user 'verify_password'@'localhost' identified by 'second-secret' replace 'first-secret'")
	mustExecSessionSQL(t, executor, session, "", "alter user 'verify_password'@'localhost' password require current optional")
	mustExecSessionSQL(t, executor, session, "", "set password for 'verify_password'@'localhost' = 'third-secret'")

	admin := newTestMySQLSession()
	admin.SetParamByName("user", "root")
	admin.SetParamByName("host", "localhost")
	admin.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN"})
	mustExecSessionSQL(t, executor, admin, "", "set global password_require_current = on")
	mustExecSQL(t, executor, "", "create user 'global_verify_password'@'localhost' identified by 'first-secret'")
	globalSession := newTestMySQLSession()
	globalSession.SetParamByName("user", "global_verify_password")
	globalSession.SetParamByName("host", "localhost")
	globalBlocked := <-executor.ExecuteQuery(globalSession, "alter user 'global_verify_password'@'localhost' identified by 'second-secret'", "")
	require.Error(t, globalBlocked.Err)
	require.Contains(t, globalBlocked.Err.Error(), "Current password is required")
}

func TestPasswordLifetimeAndLastChangedAreProjected(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'lifetime_user'@'localhost' identified by 'secret' password expire interval 90 day")

	rows := mustQuerySQL(t, executor, "", "select User, password_lifetime, password_last_changed from mysql.user where User = 'lifetime_user'")
	require.Len(t, rows, 1)
	require.Equal(t, "lifetime_user", rows[0][0])
	require.Equal(t, "90", rows[0][1])
	require.NotEmpty(t, rows[0][2])
	show := mustQuerySQL(t, executor, "", "show create user 'lifetime_user'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), "PASSWORD EXPIRE INTERVAL 90 DAY")

	mustExecSQL(t, executor, "", "alter user 'lifetime_user'@'localhost' password expire never")
	rows = mustQuerySQL(t, executor, "", "select password_lifetime from mysql.user where User = 'lifetime_user'")
	require.Equal(t, [][]interface{}{{nil}}, rows)
}

func TestMySQLUserProjectsCompleteStaticPrivilegeColumns(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create user 'static_privilege_columns'@'localhost' identified by 'secret'")
	mustExecSQL(t, executor, "", "grant reload on *.* to 'static_privilege_columns'@'localhost'")

	rows := mustQuerySQL(t, executor, "", "select Reload_priv, Shutdown_priv, Process_priv, File_priv, References_priv, Show_db_priv, Super_priv, Create_tmp_table_priv, Lock_tables_priv, Execute_priv, Repl_slave_priv, Repl_client_priv, Create_view_priv, Show_view_priv, Create_routine_priv, Alter_routine_priv, Event_priv, Trigger_priv, Create_tablespace_priv from mysql.user where User = 'static_privilege_columns'")
	require.Equal(t, [][]interface{}{{"Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N"}}, rows)
}

func TestUserAttributesAccountSyntaxPersistsAndProjects(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", `create user 'attribute_sql'@'localhost' identified by 'secret' attribute '{"team":"compatibility","tier":1}'`)

	rows := mustQuerySQL(t, executor, "", "select user, user_attributes from mysql.user where user = 'attribute_sql'")
	require.Equal(t, [][]interface{}{{"attribute_sql", `{"team":"compatibility","tier":1}`}}, rows)
	info := mustQuerySQL(t, executor, "", "select user, host, attribute from information_schema.user_attributes where user = 'attribute_sql'")
	require.Equal(t, [][]interface{}{{"attribute_sql", "localhost", `{"team":"compatibility","tier":1}`}}, info)
	show := mustQuerySQL(t, executor, "", "show create user 'attribute_sql'@'localhost'")
	require.Contains(t, fmt.Sprint(show[0][1]), `ATTRIBUTE '{"team":"compatibility","tier":1}'`)

	mustExecSQL(t, executor, "", `alter user 'attribute_sql'@'localhost' attribute '{"owner":"platform"}'`)
	require.Equal(t, [][]interface{}{{`{"owner":"platform"}`}}, mustQuerySQL(t, executor, "", "select user_attributes from mysql.user where user = 'attribute_sql'"))
	mustExecSQL(t, executor, "", `alter user 'attribute_sql'@'localhost' comment 'owned by platform'`)
	commentRows := mustQuerySQL(t, executor, "", "select user_attributes from mysql.user where user = 'attribute_sql'")
	var commentAttributes map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(commentRows[0][0].(string)), &commentAttributes))
	require.Equal(t, "owned by platform", commentAttributes["comment"])
	require.Equal(t, "platform", commentAttributes["owner"])

	mustExecSQL(t, executor, "", `create user 'comment_sql'@'localhost' identified by 'secret' comment 'created by test'`)
	commentCreate := mustQuerySQL(t, executor, "", "select user_attributes from mysql.user where user = 'comment_sql'")
	require.Equal(t, [][]interface{}{{`{"comment":"created by test"}`}}, commentCreate)
	conflict := <-executor.ExecuteQuery(nil, `create user 'comment_conflict'@'localhost' identified by 'secret' comment 'x' attribute '{"team":"y"}'`, "")
	require.Error(t, conflict.Err)
	require.Contains(t, conflict.Err.Error(), "COMMENT and ATTRIBUTE")
	mustExecSQL(t, executor, "", "alter user 'attribute_sql'@'localhost' attribute default")
	require.Equal(t, [][]interface{}{{nil}}, mustQuerySQL(t, executor, "", "select user_attributes from mysql.user where user = 'attribute_sql'"))
	current := newTestMySQLSession()
	current.SetParamByName("user", "attribute_sql")
	current.SetParamByName("host", "localhost")
	mustExecSessionSQL(t, executor, current, "", `alter user user() attribute '{"via":"current_user"}'`)
	require.Equal(t, [][]interface{}{{`{"via":"current_user"}`}}, mustQuerySQL(t, executor, "", "select user_attributes from mysql.user where user = 'attribute_sql'"))
	mustExecSessionSQL(t, executor, current, "", "set password for user() = 'new-secret'")

	invalid := <-executor.ExecuteQuery(nil, `alter user 'attribute_sql'@'localhost' attribute '{"broken":'`, "")
	require.Error(t, invalid.Err)
	require.Contains(t, invalid.Err.Error(), "ATTRIBUTE JSON")
}
