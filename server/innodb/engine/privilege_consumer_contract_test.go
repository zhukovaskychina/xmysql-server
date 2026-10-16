package engine

import (
	"reflect"
	"strings"
	"testing"
)

func TestPrivilegeConsumerLegacyScopeRows(t *testing.T) {
	cases := map[string][]string{
		"column":  {"SELECT", "INSERT", "UPDATE", "REFERENCES"},
		"table":   {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER", "CREATE VIEW", "SHOW VIEW", "TRIGGER"},
		"schema":  {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER", "CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "EVENT", "TRIGGER"},
		"routine": {"EXECUTE", "ALTER ROUTINE"},
		"global":  {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD", "SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER", "SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW", "SHOW VIEW", "CREATE ROLE", "DROP ROLE", "CREATE USER", "CREATE TABLESPACE", "TRIGGER", "EVENT", "CREATE ROUTINE", "ALTER ROUTINE", "PROXY"},
	}
	for scope, want := range cases {
		if got := informationSchemaPrivilegeRows([]string{"ALL"}, scope); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s ALL: got %v want %v", scope, got, want)
		}
		normalizedWant := append(append([]string(nil), want...), "CUSTOM_DYNAMIC")
		if scope == "routine" {
			normalizedWant = append(normalizedWant, "SELECT")
		}
		if got := informationSchemaPrivilegeRows([]string{" grant option ", "all privileges", "ALL", "custom_dynamic", "select"}, " "+strings.ToUpper(scope)+" "); !reflect.DeepEqual(got, normalizedWant) {
			t.Fatalf("%s dedup: %v", scope, got)
		}
	}
	if got := informationSchemaPrivilegeRows([]string{"ALL", "GRANT OPTION"}, "unknown"); len(got) != 0 {
		t.Fatal(got)
	}
}
func TestPrivilegeConsumerMySQLUserColumnContract(t *testing.T) {
	e := newTestStorageIntegratedExecutor(t, t.TempDir()).QueryExecutor
	columns := []string{
		"ALTER_PRIV", "ALTER_ROUTINE_PRIV", "CREATE_PRIV", "CREATE_ROLE_PRIV", "CREATE_ROUTINE_PRIV", "CREATE_TABLESPACE_PRIV", "CREATE_TMP_TABLE_PRIV", "CREATE_USER_PRIV", "CREATE_VIEW_PRIV", "DELETE_PRIV", "DROP_PRIV", "DROP_ROLE_PRIV", "EVENT_PRIV", "EXECUTE_PRIV", "FILE_PRIV", "GRANT_PRIV", "INDEX_PRIV", "INSERT_PRIV", "LOCK_TABLES_PRIV", "PROCESS_PRIV", "REFERENCES_PRIV", "RELOAD_PRIV", "REPL_CLIENT_PRIV", "REPL_SLAVE_PRIV", "SELECT_PRIV", "SHOW_DB_PRIV", "SHOW_VIEW_PRIV", "SHUTDOWN_PRIV", "SUPER_PRIV", "TRIGGER_PRIV", "UPDATE_PRIV"}
	names := []string{"ALTER", "ALTER ROUTINE", "CREATE", "CREATE ROLE", "CREATE ROUTINE", "CREATE TABLESPACE", "CREATE TEMPORARY TABLES", "CREATE USER", "CREATE VIEW", "DELETE", "DROP", "DROP ROLE", "EVENT", "EXECUTE", "FILE", "GRANT OPTION", "INDEX", "INSERT", "LOCK TABLES", "PROCESS", "REFERENCES", "RELOAD", "REPLICATION CLIENT", "REPLICATION SLAVE", "SELECT", "SHOW DATABASES", "SHOW VIEW", "SHUTDOWN", "SUPER", "TRIGGER", "UPDATE"}
	file := persistedAccountFile{}
	for i, name := range names {
		file.Accounts = append(file.Accounts, persistedAccount{User: columns[i], Host: "localhost", GlobalGrants: []string{name}})
	}
	file.Accounts = append(file.Accounts, persistedAccount{User: "CONFIG", Host: "localhost", GlobalGrants: []string{"CONFIG", "PROXY"}})
	if err := e.savePersistedAccounts(file); err != nil {
		t.Fatal(err)
	}
	query := "select User," + strings.Join(columns, ",") + " from mysql.user"
	result := e.executeMySQLUserSelect(query)
	if len(result.Records) != len(names)+1 {
		t.Fatalf("rows %d", len(result.Records))
	}
	for _, record := range result.Records {
		cells := record.GetValues()
		user, ok := cells[0].Raw().([]byte)
		if !ok {
			t.Fatalf("user type %T", cells[0].Raw())
		}
		for i, column := range columns {
			want := []byte("N")
			if string(user) == column {
				want = []byte("Y")
			}
			if !reflect.DeepEqual(cells[i+1].Raw(), want) {
				t.Fatalf("user %s column %s: got %T:%v want %v", user, column, cells[i+1].Raw(), cells[i+1].Raw(), want)
			}
		}
	}
}
