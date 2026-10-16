package common

import (
	"sort"
	"strings"
)

// staticPrivilegeDefinition is the single fixed directory for privilege names,
// columns, ordered scopes and SHOW PRIVILEGES descriptions. Zero scope order
// means absent. AcceptSet keeps the legacy asymmetric reverse-set contract.
type staticPrivilegeDefinition struct {
	Type                               PrivilegeType
	SQLName, SetName, UserColumn       string
	AcceptSet                          bool
	GrantScope                         bool
	ScopeOrder                         [5]int
	ShowName, ShowContext, ShowComment string
}

var staticPrivilegeCatalog = []staticPrivilegeDefinition{
	{CreatePriv, "Create", "Create", "Create_priv", true, true, [5]int{5, 5, 5, 0, 0}, "Create", "Databases, tables, or indexes", "To create new databases and tables"},
	{SelectPriv, "Select", "Select", "Select_priv", true, true, [5]int{1, 1, 1, 1, 0}, "Select", "Tables", "To retrieve rows from table"},
	{InsertPriv, "Insert", "Insert", "Insert_priv", true, true, [5]int{2, 2, 2, 2, 0}, "Insert", "Tables", "To insert data into tables"},
	{UpdatePriv, "Update", "Update", "Update_priv", true, true, [5]int{3, 3, 3, 3, 0}, "Update", "Tables", "To update existing rows"},
	{DeletePriv, "Delete", "Delete", "Delete_priv", true, true, [5]int{4, 4, 4, 0, 0}, "Delete", "Tables", "To delete existing rows"},
	{ShowDBPriv, "Show Databases", "", "Show_db_priv", false, true, [5]int{10, 0, 0, 0, 2}, "Show Databases", "Server Admin", "To see all databases"},
	{SuperPriv, "Super", "", "Super_priv", false, true, [5]int{11, 0, 0, 0, 3}, "Super", "Server Admin", "To use KILL thread, SET GLOBAL, and other admin commands"},
	{CreateUserPriv, "Create User", "", "Create_user_priv", false, true, [5]int{14, 0, 0, 0, 4}, "Create User", "Server Admin", "To create new users"},
	{CreateTablespacePriv, "Create Tablespace", "", "Create_tablespace_priv", false, true, [5]int{15, 0, 0, 0, 5}, "Create tablespace", "Server Admin", "To create, alter, or drop tablespaces and log file groups"},
	{TriggerPriv, "Trigger", "Trigger", "Trigger_priv", true, true, [5]int{16, 18, 12, 0, 0}, "Trigger", "Tables", "To use triggers"},
	{DropPriv, "Drop", "Drop", "Drop_priv", true, true, [5]int{6, 6, 6, 0, 0}, "Drop", "Databases, tables", "To drop databases, tables, and views"},
	{ProcessPriv, "Process", "", "Process_priv", false, true, [5]int{7, 0, 0, 0, 1}, "Process", "Server Admin", "To view the plain text of currently executing queries"},
	{GrantPriv, "Grant Option", "Grant", "Grant_priv", true, true, [5]int{0, 0, 0, 0, 0}, "Grant Option", "Databases, tables, and routines", "To give to other users those privileges you possess"},
	{ReferencesPriv, "References", "References", "References_priv", true, true, [5]int{8, 7, 8, 4, 0}, "References", "Databases, tables", "To have references on tables"},
	{AlterPriv, "Alter", "Alter", "Alter_priv", true, true, [5]int{9, 13, 9, 0, 0}, "Alter", "Tables", "To alter existing tables"},
	{ExecutePriv, "Execute", "Execute", "Execute_priv", true, true, [5]int{12, 14, 0, 0, 0}, "Execute", "Server Admin", "To execute stored routines"},
	{IndexPriv, "Index", "Index", "Index_priv", true, true, [5]int{13, 15, 7, 0, 0}, "Index", "Tables", "To create or drop indexes"},
	{CreateViewPriv, "Create View", "Create View", "Create_view_priv", true, true, [5]int{17, 16, 10, 0, 0}, "Create view", "Tables", "To create new views"},
	{ShowViewPriv, "Show View", "Show View", "Show_view_priv", true, true, [5]int{18, 17, 11, 0, 0}, "Show View", "Tables", "To see views"},
	{CreateRolePriv, "Create Role", "Create Role", "Create_role_priv", false, true, [5]int{19, 0, 0, 0, 0}, "Create role", "Server Admin", "To create new roles"},
	{DropRolePriv, "Drop Role", "Drop Role", "Drop_role_priv", false, true, [5]int{20, 0, 0, 0, 0}, "Drop role", "Server Admin", "To drop roles"},
	{CreateTMPTablePriv, "CREATE TEMPORARY TABLES", "Create Temporary Tables", "Create_tmp_table_priv", true, true, [5]int{21, 9, 0, 0, 0}, "Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"},
	{LockTablesPriv, "LOCK TABLES", "Lock Tables", "Lock_tables_priv", true, true, [5]int{22, 8, 0, 0, 0}, "Lock Tables", "Databases", "To use LOCK TABLES"},
	{CreateRoutinePriv, "CREATE ROUTINE", "Create Routine", "Create_routine_priv", true, true, [5]int{23, 11, 0, 0, 0}, "Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"},
	{AlterRoutinePriv, "ALTER ROUTINE", "Alter Routine", "Alter_routine_priv", true, true, [5]int{24, 12, 0, 0, 0}, "Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"},
	{EventPriv, "EVENT", "Event", "Event_priv", true, true, [5]int{25, 10, 0, 0, 0}, "Event", "Databases", "To create events for the Event Scheduler"},
	{ShutdownPriv, "SHUTDOWN", "Shutdown Role", "Shutdown_priv", false, true, [5]int{26, 0, 0, 0, 6}, "Shutdown", "Server Admin", "To shut down the server"},
	{ReloadPriv, "RELOAD", "", "Reload_priv", false, true, [5]int{27, 0, 0, 0, 7}, "Reload", "Server Admin", "To reload or refresh tables, logs and privileges"},
	{FilePriv, "FILE", "", "File_priv", false, true, [5]int{28, 0, 0, 0, 8}, "File", "File access on server", "To read and write files on the server"},
	{ConfigPriv, "CONFIG", "", "Config_priv", false, false, [5]int{29, 0, 0, 0, 11}, "", "", ""},
	{UsagePriv, "USAGE", "", "", false, true, [5]int{0, 0, 0, 0, 0}, "Usage", "Server Admin", "Synonym for no privileges"},
	{ReplicationClientPriv, "REPLICATION CLIENT", "", "Repl_client_priv", false, true, [5]int{30, 0, 0, 0, 9}, "Replication client", "Server Admin", "To ask where the source or replica is"},
	{ReplicationSlavePriv, "REPLICATION SLAVE", "", "Repl_slave_priv", false, true, [5]int{31, 0, 0, 0, 10}, "Replication slave", "Server Admin", "To read binary logs from the source"},
	{AllPriv, AllPrivilegeLiteral, "", "", false, true, [5]int{0, 0, 0, 0, 0}, "ALL", "Server administration", "All privileges available at a given privilege level"},
	{GrantScope: true, ShowName: "Proxy", ShowContext: "Server Admin", ShowComment: "To make proxy users possible"},
}

func privilegeStringMap(kind int) map[PrivilegeType]string {
	result := make(map[PrivilegeType]string)
	for _, privilege := range staticPrivilegeCatalog {
		value := ""
		switch kind {
		case 0:
			value = privilege.SQLName
		case 1:
			value = privilege.SetName
		case 2:
			value = privilege.UserColumn
		}
		if value != "" {
			result[privilege.Type] = value
		}
	}
	return result
}

func privilegeReverseMap(column bool) map[string]PrivilegeType {
	result := make(map[string]PrivilegeType)
	for _, privilege := range staticPrivilegeCatalog {
		if column && privilege.UserColumn != "" {
			result[privilege.UserColumn] = privilege.Type
		}
		if !column && privilege.AcceptSet {
			result[privilege.SetName] = privilege.Type
		}
	}
	return result
}

func privilegeScope(index int) Privileges {
	definitions := make([]staticPrivilegeDefinition, 0)
	for _, privilege := range staticPrivilegeCatalog {
		if privilege.ScopeOrder[index] > 0 {
			definitions = append(definitions, privilege)
		}
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].ScopeOrder[index] < definitions[j].ScopeOrder[index] })
	result := make(Privileges, 0, len(definitions))
	for _, privilege := range definitions {
		result = append(result, privilege.Type)
	}
	return result
}

// ShowPrivilegeRows returns independent rows and reads dynamic registrations
// at call time, including runtime unregistration of built-in names.
func ShowPrivilegeRows() [][]interface{} {
	rows := make([][]interface{}, 0, len(staticPrivilegeCatalog))
	for _, privilege := range staticPrivilegeCatalog {
		if privilege.ShowName != "" {
			rows = append(rows, []interface{}{privilege.ShowName, privilege.ShowContext, privilege.ShowComment})
		}
	}
	for _, privilege := range RegisteredDynamicPrivileges() {
		rows = append(rows, []interface{}{privilege, "Global", "Dynamic privilege"})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
	return rows
}

// IsStaticGrantPrivilege preserves GRANT's static-name set. CONFIG is a
// common privilege bit but is excluded from this SQL grammar; PROXY is accepted
// without allocating a bit. Callers normalize case as before.
func IsStaticGrantPrivilege(name string) bool {
	if name == "" {
		return false
	}
	for _, privilege := range staticPrivilegeCatalog {
		if privilege.GrantScope && (name == strings.ToUpper(privilege.SQLName) || name == strings.ToUpper(privilege.ShowName)) {
			return true
		}
	}
	return false
}
