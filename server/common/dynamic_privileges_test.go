package common

import "testing"

func TestDynamicPrivilegeRegistryRecognizesMySQLNames(t *testing.T) {
	for _, name := range []string{"ALLOW_NONEXISTENT_DEFINER", "BACKUP_ADMIN", "FLUSH_USER_RESOURCES", "MASKING_DICTIONARIES_ADMIN", "OPTIMIZE_LOCAL_TABLE", "SYSTEM_USER", "SESSION_VARIABLES_ADMIN", "XA_RECOVER_ADMIN"} {
		if !IsRegisteredDynamicPrivilege(name) {
			t.Fatalf("dynamic privilege %q was not registered", name)
		}
	}
	if IsRegisteredDynamicPrivilege("NOT_A_MYSQL_DYNAMIC_PRIVILEGE") {
		t.Fatal("unknown dynamic privilege was registered")
	}
	for _, name := range []string{"INNODB_REDO_LOG_ARCHIVE_ADMIN", "NDB_STORED_USER_PASSWORD", "REPLICATION_APPLIER_ADMIN"} {
		if IsRegisteredDynamicPrivilege(name) {
			t.Fatalf("non-8.4 dynamic privilege %q was registered", name)
		}
	}
}

func TestDynamicPrivilegeRegistryIsSortedAndUnique(t *testing.T) {
	privileges := RegisteredDynamicPrivileges()
	if len(privileges) != 46 {
		t.Fatalf("registry has %d privileges, want the 46 MySQL 8.4 built-ins", len(privileges))
	}
	if len(privileges) == 0 {
		t.Fatal("dynamic privilege registry is empty")
	}
	for i := 1; i < len(privileges); i++ {
		if privileges[i-1] >= privileges[i] {
			t.Fatalf("registry is not strictly sorted at %d: %q >= %q", i, privileges[i-1], privileges[i])
		}
	}
}

func TestDynamicPrivilegeRegistrySupportsRuntimeRegistration(t *testing.T) {
	const custom = "COMPONENT_TEST_ADMIN"
	if IsRegisteredDynamicPrivilege(custom) {
		t.Fatal("test component privilege unexpectedly registered")
	}
	if err := RegisterDynamicPrivilege(custom); err != nil {
		t.Fatalf("register dynamic privilege: %v", err)
	}
	t.Cleanup(func() { _ = UnregisterDynamicPrivilege(custom) })
	if !IsRegisteredDynamicPrivilege(custom) {
		t.Fatal("runtime dynamic privilege was not registered")
	}
	if err := RegisterDynamicPrivilege(custom); err != nil {
		t.Fatalf("duplicate registration should be idempotent: %v", err)
	}
	if err := UnregisterDynamicPrivilege(custom); err != nil {
		t.Fatalf("unregister dynamic privilege: %v", err)
	}
	if IsRegisteredDynamicPrivilege(custom) {
		t.Fatal("runtime dynamic privilege remained registered")
	}
}
