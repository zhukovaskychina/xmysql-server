package engine

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

const mysqlGenericExecutionErrorCode uint16 = 1105

func mysqlExecutionErrorCode(err error) uint16 {
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr == nil {
		return mysqlGenericExecutionErrorCode
	}
	switch executionErr.ErrorCode {
	case ExecutionErrorCodeDuplicateKey:
		return 1062
	case ExecutionErrorCodeSchemaOrTableNotFound, ExecutionErrorCodeMetadataMissing:
		return 1146
	case ExecutionErrorCodeValidation:
		return 1064
	case ExecutionErrorCodeStorageMissing, ExecutionErrorCodeStorageReadFailure, ExecutionErrorCodeStorageWriteFailure:
		return 1030
	default:
		return mysqlGenericExecutionErrorCode
	}
}

func recordSessionError(session server.MySQLServerSession, err error) {
	if session == nil || err == nil {
		return
	}
	warnings, _ := session.GetParamByName("warnings").([]Warning)
	warnings = append(warnings, Warning{
		Level:   "Error",
		Code:    mysqlExecutionErrorCode(err),
		Message: err.Error(),
	})
	session.SetParamByName("warnings", warnings)
}

var showWarningsLimitPattern = regexp.MustCompile(`(?is)^show\s+(?:warnings|errors)\s+limit\s+(?:(\d+)\s*,\s*)?(\d+)(?:\s+offset\s+(\d+))?$`)

func showWarningsBounds(rawQuery string, total int) (int, int) {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawQuery), ";"))
	match := showWarningsLimitPattern.FindStringSubmatch(query)
	if len(match) == 0 {
		return 0, total
	}

	offset := uint64(0)
	countText := match[2]
	if match[1] != "" {
		offset, _ = strconv.ParseUint(match[1], 10, 64)
	} else if match[3] != "" {
		offset, _ = strconv.ParseUint(match[3], 10, 64)
	}
	count, _ := strconv.ParseUint(countText, 10, 64)
	if offset >= uint64(total) {
		return total, total
	}
	end := offset + count
	if end < offset || end > uint64(total) {
		end = uint64(total)
	}
	return int(offset), int(end)
}

const clientFoundRowsCapability uint32 = 1 << 1

func clearSessionWarnings(session server.MySQLServerSession) {
	if session != nil {
		session.SetParamByName("warnings", []Warning{})
	}
}

func shouldClearSessionWarnings(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return !strings.HasPrefix(lower, "show warnings") &&
		!strings.HasPrefix(lower, "show errors") &&
		lower != "show count(*) warnings" &&
		lower != "show count(*) errors"
}

func setSessionWarnings(ctx *ExecutionContext, warnings []Warning) {
	if ctx == nil {
		return
	}
	ctx.Warnings = append([]Warning(nil), warnings...)
	if ctx.Session != nil {
		ctx.Session.SetParamByName("warnings", append([]Warning(nil), warnings...))
	}
}

func sessionFoundRowsEnabled(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	capabilities, ok := session.GetParamByName("client_capabilities").(uint32)
	return ok && capabilities&clientFoundRowsCapability != 0
}

func sessionSQLMode(session server.MySQLServerSession) string {
	if session == nil {
		return "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
	}
	if mode, ok := session.GetParamByName("sql_mode").(string); ok {
		return mode
	}
	return "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
}

func sessionForeignKeyChecksEnabled(session server.MySQLServerSession) bool {
	if session == nil {
		return true
	}
	value := session.GetParamByName("foreign_key_checks")
	switch typed := value.(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case uint64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "0", "off", "false", "no":
			return false
		}
	}
	return true
}

func sessionCheckConstraintChecksEnabled(session server.MySQLServerSession) bool {
	if session == nil {
		return true
	}
	value := session.GetParamByName("check_constraint_checks")
	switch typed := value.(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case uint64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "0", "off", "false", "no":
			return false
		}
	}
	return true
}

func sessionUniqueChecksEnabled(session server.MySQLServerSession) bool {
	if session == nil {
		return true
	}
	value := session.GetParamByName("unique_checks")
	switch typed := value.(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case uint64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "0", "off", "false", "no":
			return false
		}
	}
	return true
}

func (ctx *ExecutionContext) addWarning(level string, code uint16, message string) {
	if ctx == nil {
		return
	}
	ctx.Warnings = append(ctx.Warnings, Warning{Level: level, Code: code, Message: message})
	if ctx.Session != nil {
		ctx.Session.SetParamByName("warnings", append([]Warning(nil), ctx.Warnings...))
	}
}

func sessionWarnings(ctx *ExecutionContext) []Warning {
	if ctx == nil {
		return nil
	}
	if ctx.Session != nil {
		if warnings, ok := ctx.Session.GetParamByName("warnings").([]Warning); ok {
			return warnings
		}
	}
	return ctx.Warnings
}
