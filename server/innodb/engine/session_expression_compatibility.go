package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
)

const sessionExpressionValuesKey = "__xmysql_session_values"

const defaultServerVersion = "8.0.32"

func newSessionExpressionValues(session server.MySQLServerSession) map[string]interface{} {
	values := map[string]interface{}{
		"last_insert_id": uint64(0),
		"row_count":      int64(0),
		"database":       sessionDatabaseName(session),
		"user":           sessionQualifiedUser(session, false),
		"current_user":   sessionQualifiedUser(session, true),
		"session_user":   sessionQualifiedUser(session, false),
		"system_user":    sessionQualifiedUser(session, false),
		"current_role":   sessionCurrentRole(session),
		"version":        defaultServerVersion,
		"connection_id":  sessionConnectionIDValue(session),
	}
	if session == nil {
		return values
	}
	values["last_insert_id"] = sessionLastInsertID(session)
	values["row_count"] = sessionRowCount(session)
	if version := sessionStringParam(session, "version"); version != "" {
		values["version"] = version
	}
	return values
}

func sessionDatabaseName(session server.MySQLServerSession) interface{} {
	if value := sessionStringParam(session, "database"); value != "" {
		return value
	}
	return nil
}

func sessionQualifiedUser(session server.MySQLServerSession, current bool) interface{} {
	if session == nil {
		return nil
	}
	userKey, hostKey := "user", "host"
	if current {
		userKey, hostKey = "current_user", "current_host"
	}
	user := sessionStringParam(session, userKey)
	host := sessionStringParam(session, hostKey)
	if current && user == "" {
		user = sessionStringParam(session, "user")
	}
	if current && host == "" {
		host = sessionStringParam(session, "host")
	}
	if user == "" && host == "" {
		return nil
	}
	if host == "" {
		return user
	}
	return user + "@" + host
}

func sessionStringParam(session server.MySQLServerSession, name string) string {
	if session == nil {
		return ""
	}
	if value := session.GetParamByName(name); value != nil {
		return strings.TrimSpace(fmt.Sprint(value))
	}
	return ""
}

func sessionConnectionIDValue(session server.MySQLServerSession) int64 {
	if session == nil {
		return 0
	}
	if value := session.GetParamByName("connection_id"); value != nil {
		return sessionRowCountValue(value)
	}
	if ctx := session.SessionContext(); ctx != nil {
		return int64(ctx.GetConnectionID())
	}
	return 0
}

func sessionCurrentRole(session server.MySQLServerSession) string {
	if session != nil {
		if roles, ok := session.GetParamByName("active_roles").([]string); ok && len(roles) > 0 {
			return strings.Join(roles, ",")
		}
	}
	return "NONE"
}

// rewriteSessionMetadataFunctions keeps SCHEMA() (the SQL-standard alias for
// DATABASE()) available even though the embedded parser reserves SCHEMA for
// schema DDL statements and does not accept it as a generic function name.
func rewriteSessionMetadataFunctions(query string) string {
	if !strings.Contains(strings.ToLower(query), "schema") {
		return query
	}
	var builder strings.Builder
	quote := byte(0)
	for index := 0; index < len(query); {
		ch := query[index]
		if quote != 0 {
			builder.WriteByte(ch)
			if ch == quote && (index == 0 || query[index-1] != '\\') {
				quote = 0
			}
			index++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			builder.WriteByte(ch)
			index++
			continue
		}
		if index+len("schema") <= len(query) && strings.EqualFold(query[index:index+len("schema")], "schema") &&
			(index == 0 || !isSQLIdentifierPart(query[index-1])) {
			end := index + len("schema")
			if end == len(query) || !isSQLIdentifierPart(query[end]) {
				lookahead := end
				for lookahead < len(query) && (query[lookahead] == ' ' || query[lookahead] == '\t' || query[lookahead] == '\r' || query[lookahead] == '\n') {
					lookahead++
				}
				if lookahead < len(query) && query[lookahead] == '(' {
					builder.WriteString("database")
					index = end
					continue
				}
			}
		}
		builder.WriteByte(ch)
		index++
	}
	return builder.String()
}

func syncSessionExpressionValues(session server.MySQLServerSession, values map[string]interface{}) {
	if session == nil || values == nil {
		return
	}
	raw, exists := values["last_insert_id"]
	if exists {
		session.SetParamByName("last_insert_id", sessionLastInsertIDValue(raw))
	}
	if raw, exists := values["row_count"]; exists {
		session.SetParamByName("row_count", sessionRowCountValue(raw))
	}
}

func sessionRowCount(session server.MySQLServerSession) int64 {
	if session == nil {
		return 0
	}
	return sessionRowCountValue(session.GetParamByName("row_count"))
}

func sessionRowCountValue(raw interface{}) int64 {
	switch value := raw.(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case uint64:
		if value <= uint64(^uint64(0)>>1) {
			return int64(value)
		}
	case uint:
		if uint64(value) <= uint64(^uint64(0)>>1) {
			return int64(value)
		}
	case uint32:
		return int64(value)
	case string:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

func persistDirectEngineSessionState(session server.MySQLServerSession, result *Result) {
	if session == nil || result == nil || result.Err != nil {
		return
	}
	if dmlResult, ok := result.Data.(*DMLResult); ok {
		session.SetParamByName("row_count", int64(dmlResult.AffectedRows))
		return
	}
	if result.ResultType == common.RESULT_TYPE_SELECT {
		session.SetParamByName("row_count", int64(-1))
		return
	}
	if _, ok := result.Data.(*SelectResult); ok {
		session.SetParamByName("row_count", int64(-1))
		return
	}
	session.SetParamByName("row_count", int64(0))
}

func sessionLastInsertID(session server.MySQLServerSession) uint64 {
	if session == nil {
		return 0
	}
	return sessionLastInsertIDValue(session.GetParamByName("last_insert_id"))
}

func sessionLastInsertIDValue(raw interface{}) uint64 {
	switch value := raw.(type) {
	case uint64:
		return value
	case uint:
		return uint64(value)
	case uint32:
		return uint64(value)
	case int:
		if value > 0 {
			return uint64(value)
		}
	case int64:
		if value > 0 {
			return uint64(value)
		}
	case int32:
		if value > 0 {
			return uint64(value)
		}
	case string:
		value = strings.TrimSpace(value)
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}
