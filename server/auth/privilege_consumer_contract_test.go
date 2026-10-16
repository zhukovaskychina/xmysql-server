package auth

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"reflect"
	"strings"
	"testing"
)

func TestPrivilegeConsumerLegacyAuthNames(t *testing.T) {
	accepted := map[string]common.PrivilegeType{
		"SELECT":       common.SelectPriv,
		"INSERT":       common.InsertPriv,
		"UPDATE":       common.UpdatePriv,
		"DELETE":       common.DeletePriv,
		"CREATE":       common.CreatePriv,
		"DROP":         common.DropPriv,
		"ALTER":        common.AlterPriv,
		"INDEX":        common.IndexPriv,
		"EXECUTE":      common.ExecutePriv,
		"CREATE VIEW":  common.CreateViewPriv,
		"SHOW VIEW":    common.ShowViewPriv,
		"CREATE USER":  common.CreateUserPriv,
		"GRANT OPTION": common.GrantPriv,
		"TRIGGER":      common.TriggerPriv,
		"EVENT":        common.EventPriv,
		"REFERENCES":   common.ReferencesPriv,
		"USAGE":        common.UsagePriv,
	}
	for name, want := range accepted {
		got := grantNamesToPrivileges([]string{" " + strings.ToLower(name) + " ", name})
		if !reflect.DeepEqual(got, []common.PrivilegeType{want, want}) {
			t.Fatalf("%s => %v", name, got)
		}
	}
	for _, name := range []string{"CONFIG", "PROXY", "RELOAD", "SHUTDOWN", "PROCESS", "FILE", "SHOW DATABASES", "SUPER", "CREATE ROLE", "DROP ROLE", "CREATE TABLESPACE", "CREATE TEMPORARY TABLES", "LOCK TABLES", "CREATE ROUTINE", "ALTER ROUTINE", "REPLICATION CLIENT", "REPLICATION SLAVE", "", "BACKUP_ADMIN"} {
		if got := grantNamesToPrivileges([]string{name}); len(got) != 0 {
			t.Fatalf("old rejected name %s expanded to %v", name, got)
		}
	}
	want := append(append([]common.PrivilegeType{}, common.AllGlobalPrivs...), common.AllGlobalPrivs...)
	if got := grantNamesToPrivileges([]string{" all ", "ALL PRIVILEGES"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("ALL ordering changed: %v", got)
	}
	// ALL has always read the exported mutable compatibility list at call time.
	original := common.AllGlobalPrivs
	t.Cleanup(func() { common.AllGlobalPrivs = original })
	common.AllGlobalPrivs = []common.PrivilegeType{common.ConfigPriv, common.SelectPriv}
	if got := grantNamesToPrivileges([]string{"ALL"}); !reflect.DeepEqual(got, []common.PrivilegeType(common.AllGlobalPrivs)) {
		t.Fatal(got)
	}
}
