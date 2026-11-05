package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

func mysqlSystemTableVirtualDefinitions() map[string][]string {
	return map[string][]string{
		"mysql.component":        {"component_id", "component_group_id", "component_urn"},
		"mysql.password_history": {"Host", "User", "Password_timestamp", "Password"},
		"mysql.server_cost":      {"cost_name", "cost_value", "last_update", "comment", "default_value"},
		"mysql.engine_cost":      {"engine_name", "device_type", "cost_name", "cost_value", "last_update", "comment", "default_value"},
	}
}

func mysqlSystemTableVirtualColumnMetadata(table, column string) frmMetadataColumn {
	metadata := frmMetadataColumn{name: column, nullable: true, charset: "utf8mb3", collation: "utf8mb3_general_ci"}
	switch strings.ToLower(table) + "." + strings.ToLower(column) {
	case "component.component_id", "component.component_group_id":
		metadata.typeName, metadata.nullable, metadata.unsigned = "BIGINT", false, true
	case "component.component_urn":
		metadata.typeName, metadata.length, metadata.nullable = "VARCHAR", 255, false
	case "password_history.host":
		metadata.typeName, metadata.length, metadata.nullable = "CHAR", 255, false
		metadata.charset, metadata.collation = "ascii", "ascii_general_ci"
	case "password_history.user":
		metadata.typeName, metadata.length, metadata.nullable = "CHAR", 32, false
	case "password_history.password_timestamp":
		metadata.typeName, metadata.nullable = "TIMESTAMP", false
	case "password_history.password":
		metadata.typeName, metadata.length, metadata.nullable = "VARCHAR", 255, false
	case "server_cost.cost_name", "server_cost.engine_name", "engine_cost.engine_name":
		metadata.typeName, metadata.length, metadata.nullable = "VARCHAR", 64, false
	case "server_cost.cost_value", "server_cost.default_value", "engine_cost.cost_value", "engine_cost.default_value":
		metadata.typeName = "FLOAT"
	case "server_cost.last_update", "engine_cost.last_update":
		metadata.typeName, metadata.nullable = "TIMESTAMP", false
	case "server_cost.comment", "engine_cost.comment":
		metadata.typeName, metadata.length = "VARCHAR", 1024
	case "engine_cost.device_type":
		metadata.typeName, metadata.nullable, metadata.unsigned = "INT", false, true
	case "engine_cost.cost_name":
		metadata.typeName, metadata.length, metadata.nullable = "VARCHAR", 64, false
	}
	return metadata
}

// MySQL keeps these tables in the mysql system schema even when the
// corresponding optional subsystem has no runtime state. The compatibility
// projection is backed by durable cost/component metadata, while password
// history is loaded from the durable account lifecycle journal.
func (e *XMySQLExecutor) executeMySQLSystemTableSelect(query string, session server.MySQLServerSession, table string) *SelectResult {
	var columns []string
	var rows []map[string]interface{}
	switch table {
	case "component":
		columns = requestedInformationSchemaColumns(query, []string{"component_id", "component_group_id", "component_urn"})
		file, err := e.loadPersistedComponents()
		if err == nil {
			for _, component := range file.Components {
				rows = append(rows, map[string]interface{}{
					"COMPONENT_ID":       component.ComponentID,
					"COMPONENT_GROUP_ID": component.ComponentGroupID,
					"COMPONENT_URN":      component.ComponentURN,
				})
			}
		}
	case "password_history":
		columns = requestedInformationSchemaColumns(query, []string{"Host", "User", "Password_timestamp", "Password"})
		file, err := e.accountFileForSession(&ExecutionContext{Session: session})
		if err == nil {
			for _, account := range file.Accounts {
				for _, history := range account.PasswordHistory {
					rows = append(rows, map[string]interface{}{
						"HOST":               account.Host,
						"USER":               account.User,
						"PASSWORD_TIMESTAMP": history.PasswordTimestamp,
						"PASSWORD":           history.Password,
					})
				}
			}
		}
	case "server_cost":
		columns = requestedInformationSchemaColumns(query, []string{"cost_name", "cost_value", "last_update", "comment", "default_value"})
		rows = e.mysqlOptimizerCostRowsForSession(session, "server_cost")
	case "engine_cost":
		columns = requestedInformationSchemaColumns(query, []string{"engine_name", "device_type", "cost_name", "cost_value", "last_update", "comment", "default_value"})
		rows = e.mysqlOptimizerCostRowsForSession(session, "engine_cost")
	default:
		return newInformationSchemaSelectResult("mysql."+table, nil, nil)
	}

	projected := make([][]interface{}, 0, len(rows))
	for _, values := range rows {
		if !mysqlSystemTableValuesMatch(query, values) {
			continue
		}
		projected = append(projected, projectInformationSchemaRow(columns, values))
	}
	return newInformationSchemaSelectResult("mysql."+table, columns, projected)
}

// mysqlSystemTableValuesMatch evaluates the complete WHERE AST against one
// native catalog row. Projection literals never become row predicates, and
// OR/NOT/NULL retain SQL truth semantics.
func mysqlSystemTableValuesMatch(query string, values map[string]interface{}) bool {
	expr, err := metadataPredicateFromQuery(query)
	if err != nil {
		return false
	}
	if expr == nil {
		return true
	}
	keep, err := evaluateMetadataPredicateCaseInsensitive(expr, values)
	return err == nil && keep
}
