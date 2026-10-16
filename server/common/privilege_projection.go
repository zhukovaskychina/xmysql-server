package common

import "strings"

// These ordered type selections preserve each existing consumer's subset.
// Names and mysql.user column facts belong only to staticPrivilegeCatalog.
var informationSchemaAllPrivilegeTypes = map[string][]PrivilegeType{
	"column":  {SelectPriv, InsertPriv, UpdatePriv, ReferencesPriv},
	"table":   {SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ReferencesPriv, IndexPriv, AlterPriv, CreateViewPriv, ShowViewPriv, TriggerPriv},
	"schema":  {SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ReferencesPriv, IndexPriv, AlterPriv, CreateTMPTablePriv, LockTablesPriv, ExecutePriv, CreateViewPriv, ShowViewPriv, CreateRoutinePriv, AlterRoutinePriv, EventPriv, TriggerPriv},
	"routine": {ExecutePriv, AlterRoutinePriv},
	"global":  {SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, ReloadPriv, ShutdownPriv, ProcessPriv, FilePriv, ReferencesPriv, IndexPriv, AlterPriv, ShowDBPriv, SuperPriv, CreateTMPTablePriv, LockTablesPriv, ExecutePriv, ReplicationSlavePriv, ReplicationClientPriv, CreateViewPriv, ShowViewPriv, CreateRolePriv, DropRolePriv, CreateUserPriv, CreateTablespacePriv, TriggerPriv, EventPriv, CreateRoutinePriv, AlterRoutinePriv},
}

func privilegeDefinitionByType(privilegeType PrivilegeType) staticPrivilegeDefinition {
	for _, definition := range staticPrivilegeCatalog {
		if definition.Type == privilegeType {
			return definition
		}
	}
	panic("unknown fixed privilege type")
}

// InformationSchemaAllPrivileges returns the historical ordered ALL expansion.
// Global projection additionally includes PROXY, which has no privilege bit.
// CONFIG, USAGE and GRANT OPTION are intentionally absent from this expansion.
func InformationSchemaAllPrivileges(scope string) []string {
	scope = strings.ToLower(strings.TrimSpace(scope))
	privilegeTypes := informationSchemaAllPrivilegeTypes[scope]
	result := make([]string, 0, len(privilegeTypes))
	for _, privilegeType := range privilegeTypes {
		result = append(result, strings.ToUpper(privilegeDefinitionByType(privilegeType).SQLName))
	}
	if scope == "global" {
		for _, definition := range staticPrivilegeCatalog {
			if definition.Type == 0 && definition.GrantScope && definition.ShowName != "" {
				result = append(result, strings.ToUpper(definition.ShowName))
			}
		}
	}
	return result
}

// MySQLUserPrivilegeColumns returns the exact existing mysql.user static flags,
// including its three separately handled role/user flags. CONFIG stays absent.
func MySQLUserPrivilegeColumns() map[string]string {
	result := make(map[string]string)
	for _, privilegeType := range []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, GrantPriv, IndexPriv, AlterPriv, ReloadPriv, ShutdownPriv, ProcessPriv, FilePriv, ReferencesPriv, ShowDBPriv, SuperPriv, CreateTMPTablePriv, LockTablesPriv, ExecutePriv, ReplicationSlavePriv, ReplicationClientPriv, CreateViewPriv, ShowViewPriv, CreateRoutinePriv, AlterRoutinePriv, EventPriv, TriggerPriv, CreateTablespacePriv, CreateUserPriv, CreateRolePriv, DropRolePriv} {
		definition := privilegeDefinitionByType(privilegeType)
		result[strings.ToUpper(definition.UserColumn)] = strings.ToUpper(definition.SQLName)
	}
	return result
}

// AuthGrantPrivilege preserves the persisted-auth reader's limited accepted
// name set. Its caller handles normalization, ALL expansion and duplicate order.
func AuthGrantPrivilege(name string) (PrivilegeType, bool) {
	for _, privilegeType := range []PrivilegeType{SelectPriv, InsertPriv, UpdatePriv, DeletePriv, CreatePriv, DropPriv, AlterPriv, IndexPriv, ExecutePriv, CreateViewPriv, ShowViewPriv, CreateUserPriv, GrantPriv, TriggerPriv, EventPriv, ReferencesPriv, UsagePriv} {
		definition := privilegeDefinitionByType(privilegeType)
		if name == strings.ToUpper(definition.SQLName) {
			return privilegeType, true
		}
	}
	return 0, false
}
