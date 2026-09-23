package common

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
)

// mysqlDynamicPrivileges is the compatibility registry for the 46 MySQL 8.4
// built-in dynamic privileges. Dynamic privileges are global-only names and must not be
// silently accepted as arbitrary strings by GRANT or authorization checks.
var mysqlDynamicPrivileges = map[string]struct{}{
	"ALLOW_NONEXISTENT_DEFINER":    {},
	"APPLICATION_PASSWORD_ADMIN":   {},
	"AUDIT_ABORT_EXEMPT":           {},
	"AUDIT_ADMIN":                  {},
	"AUTHENTICATION_POLICY_ADMIN":  {},
	"BACKUP_ADMIN":                 {},
	"BINLOG_ADMIN":                 {},
	"BINLOG_ENCRYPTION_ADMIN":      {},
	"CLONE_ADMIN":                  {},
	"CONNECTION_ADMIN":             {},
	"ENCRYPTION_KEY_ADMIN":         {},
	"FIREWALL_ADMIN":               {},
	"FIREWALL_EXEMPT":              {},
	"FIREWALL_USER":                {},
	"FLUSH_OPTIMIZER_COSTS":        {},
	"FLUSH_PRIVILEGES":             {},
	"FLUSH_STATUS":                 {},
	"FLUSH_TABLES":                 {},
	"FLUSH_USER_RESOURCES":         {},
	"GROUP_REPLICATION_ADMIN":      {},
	"GROUP_REPLICATION_STREAM":     {},
	"INNODB_REDO_LOG_ARCHIVE":      {},
	"INNODB_REDO_LOG_ENABLE":       {},
	"MASKING_DICTIONARIES_ADMIN":   {},
	"NDB_STORED_USER":              {},
	"OPTIMIZE_LOCAL_TABLE":         {},
	"PASSWORDLESS_USER_ADMIN":      {},
	"PERSIST_RO_VARIABLES_ADMIN":   {},
	"REPLICATION_APPLIER":          {},
	"REPLICATION_SLAVE_ADMIN":      {},
	"RESOURCE_GROUP_ADMIN":         {},
	"RESOURCE_GROUP_USER":          {},
	"ROLE_ADMIN":                   {},
	"SENSITIVE_VARIABLES_OBSERVER": {},
	"SESSION_VARIABLES_ADMIN":      {},
	"SET_ANY_DEFINER":              {},
	"SHOW_ROUTINE":                 {},
	"SKIP_QUERY_REWRITE":           {},
	"SYSTEM_USER":                  {},
	"SYSTEM_VARIABLES_ADMIN":       {},
	"TABLE_ENCRYPTION_ADMIN":       {},
	"TELEMETRY_LOG_ADMIN":          {},
	"TP_CONNECTION_ADMIN":          {},
	"TRANSACTION_GTID_TAG":         {},
	"VERSION_TOKEN_ADMIN":          {},
	"XA_RECOVER_ADMIN":             {},
}

var dynamicPrivilegeMu sync.RWMutex

// IsRegisteredDynamicPrivilege reports whether name is a known MySQL dynamic
// privilege. Comparison is case-insensitive and ignores surrounding spaces.
func IsRegisteredDynamicPrivilege(name string) bool {
	name = normalizeDynamicPrivilegeName(name)
	dynamicPrivilegeMu.RLock()
	defer dynamicPrivilegeMu.RUnlock()
	_, ok := mysqlDynamicPrivileges[name]
	return ok
}

// RegisteredDynamicPrivileges returns a deterministic copy of the registry.
func RegisteredDynamicPrivileges() []string {
	dynamicPrivilegeMu.RLock()
	defer dynamicPrivilegeMu.RUnlock()
	result := make([]string, 0, len(mysqlDynamicPrivileges))
	for name := range mysqlDynamicPrivileges {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// RegisterDynamicPrivilege adds a component/plugin dynamic privilege. Names
// are normalized to uppercase and duplicate registration is idempotent, as in
// MySQL's component privilege registry.
func RegisterDynamicPrivilege(name string) error {
	name = normalizeDynamicPrivilegeName(name)
	if !regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(name) {
		return fmt.Errorf("invalid dynamic privilege name %q", name)
	}
	dynamicPrivilegeMu.Lock()
	defer dynamicPrivilegeMu.Unlock()
	mysqlDynamicPrivileges[name] = struct{}{}
	return nil
}

// UnregisterDynamicPrivilege removes a runtime component/plugin privilege.
func UnregisterDynamicPrivilege(name string) error {
	name = normalizeDynamicPrivilegeName(name)
	if !regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(name) {
		return fmt.Errorf("invalid dynamic privilege name %q", name)
	}
	dynamicPrivilegeMu.Lock()
	defer dynamicPrivilegeMu.Unlock()
	delete(mysqlDynamicPrivileges, name)
	return nil
}

func normalizeDynamicPrivilegeName(name string) string {
	for len(name) > 0 && (name[0] == ' ' || name[0] == '\t' || name[0] == '\n' || name[0] == '\r') {
		name = name[1:]
	}
	for len(name) > 0 {
		last := name[len(name)-1]
		if last != ' ' && last != '\t' && last != '\n' && last != '\r' {
			break
		}
		name = name[:len(name)-1]
	}
	if name == "" {
		return ""
	}
	result := make([]byte, len(name))
	for i := range name {
		if name[i] >= 'a' && name[i] <= 'z' {
			result[i] = name[i] - ('a' - 'A')
		} else {
			result[i] = name[i]
		}
	}
	return string(result)
}
