package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const persistedVariablesFileName = "mysqld-auto.cnf"

var persistedVariableStatementPattern = regexp.MustCompile(`(?is)^\s*set\s+(persist_only|persist)\s+(.+?)\s*$`)

// persistedSystemVariable is intentionally small and JSON-safe.  It captures
// the fields surfaced by PERFORMANCE_SCHEMA.PERSISTED_VARIABLES while the
// system variable manager remains the source of the effective runtime value.
type persistedSystemVariable struct {
	VariableName  string      `json:"variable_name"`
	VariableValue interface{} `json:"variable_value"`
	SetTime       time.Time   `json:"set_time"`
	SetUser       string      `json:"set_user"`
	SetHost       string      `json:"set_host"`
}

type persistedSystemVariablesFile struct {
	Variables map[string]persistedSystemVariable `json:"variables"`
}

func (e *XMySQLExecutor) persistedVariablesPath() string {
	return filepath.Join(e.getDataDir(), persistedVariablesFileName)
}

func (e *XMySQLExecutor) readPersistedSystemVariables() (map[string]persistedSystemVariable, error) {
	path := e.persistedVariablesPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]persistedSystemVariable{}, nil
		}
		return nil, fmt.Errorf("read persisted variables: %w", err)
	}
	var file persistedSystemVariablesFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse persisted variables: %w", err)
	}
	if file.Variables == nil {
		file.Variables = make(map[string]persistedSystemVariable)
	}
	if e.storageManager != nil && e.storageManager.GetSystemVariablesManager() != nil {
		for name, value := range file.Variables {
			if definition, definitionErr := e.storageManager.GetSystemVariablesManager().GetVariableDefinition(name); definitionErr == nil {
				value.VariableValue = normalizePersistedSystemVariableValue(definition, value.VariableValue)
				file.Variables[name] = value
			}
		}
	}
	return file.Variables, nil
}

func (e *XMySQLExecutor) writePersistedSystemVariables(values map[string]persistedSystemVariable) error {
	if err := os.MkdirAll(e.getDataDir(), 0755); err != nil {
		return fmt.Errorf("create persisted variables directory: %w", err)
	}
	raw, err := json.MarshalIndent(persistedSystemVariablesFile{Variables: values}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal persisted variables: %w", err)
	}
	if err := writeMetadataFileAtomic(e.persistedVariablesPath(), raw); err != nil {
		return fmt.Errorf("write persisted variables: %w", err)
	}
	return nil
}

func (e *XMySQLExecutor) applyPersistedSystemVariables() {
	if e == nil || e.storageManager == nil || e.storageManager.GetSystemVariablesManager() == nil {
		return
	}
	values, err := e.readPersistedSystemVariables()
	if err != nil {
		logger.Warnf("unable to load persisted system variables: %v", err)
		return
	}
	sysVars := e.storageManager.GetSystemVariablesManager()
	keys := make([]string, 0, len(values))
	for name := range values {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		value := values[name].VariableValue
		if err := sysVars.SetVariable("", name, value, manager.GlobalScope); err != nil {
			logger.Warnf("unable to apply persisted system variable %s: %v", name, err)
		}
	}
}

func (e *XMySQLExecutor) executePersistedVariableStatement(ctx *ExecutionContext, session server.MySQLServerSession, query string) (bool, error) {
	match := persistedVariableStatementPattern.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) == 0 {
		return false, nil
	}
	if session == nil {
		return true, fmt.Errorf("SET PERSIST requires a session")
	}
	assignmentSQL := strings.TrimSpace(match[2])
	if assignmentSQL == "" {
		return true, fmt.Errorf("invalid SET PERSIST statement")
	}
	parsed, err := sqlparser.Parse("set global " + assignmentSQL)
	if err != nil {
		return true, fmt.Errorf("invalid SET PERSIST value: %w", err)
	}
	stmt, ok := parsed.(*sqlparser.Set)
	if !ok || len(stmt.Exprs) == 0 {
		return true, fmt.Errorf("SET PERSIST requires at least one variable assignment")
	}
	type assignment struct {
		name       string
		value      interface{}
		definition *manager.SystemVariable
	}
	assignments := make([]assignment, 0, len(stmt.Exprs))
	seen := make(map[string]struct{}, len(stmt.Exprs))
	for _, expr := range stmt.Exprs {
		if expr == nil {
			return true, fmt.Errorf("invalid SET PERSIST assignment")
		}
		name := strings.TrimSpace(expr.Name.String())
		cleanName := strings.ToLower(strings.Trim(name, "`"))
		cleanName = strings.TrimPrefix(cleanName, "@@global.")
		cleanName = strings.TrimPrefix(cleanName, "global.")
		if cleanName == "" || strings.Contains(cleanName, ".") || strings.HasPrefix(cleanName, "@@") {
			return true, fmt.Errorf("invalid persisted system variable %q", name)
		}
		if _, exists := seen[cleanName]; exists {
			return true, fmt.Errorf("duplicate persisted system variable %q", cleanName)
		}
		seen[cleanName] = struct{}{}
		value, valueErr := e.evaluateSetValue(expr.Expr)
		if valueErr != nil {
			return true, valueErr
		}
		definition, definitionErr := e.storageManager.GetSystemVariablesManager().GetVariableDefinition(cleanName)
		if definitionErr != nil {
			return true, definitionErr
		}
		value = normalizePersistedSystemVariableValue(definition, value)
		assignments = append(assignments, assignment{name: cleanName, value: value, definition: definition})
	}
	persistOnly := strings.EqualFold(match[1], "persist_only")
	for _, item := range assignments {
		if item.definition.ReadOnly {
			if !persistOnly {
				return true, fmt.Errorf("variable '%s' is read-only", item.name)
			}
			if !sessionHasPersistedReadOnlyVariableAdmin(session) {
				return true, fmt.Errorf("Access denied; you need the PERSIST_RO_VARIABLES_ADMIN privilege for this operation")
			}
		} else if !sessionHasGlobalVariableAdmin(session) {
			return true, fmt.Errorf("Access denied; you need the SUPER or SYSTEM_VARIABLES_ADMIN privilege for this operation")
		}
	}
	values, err := e.readPersistedSystemVariables()
	if err != nil {
		return true, err
	}
	setTime := time.Now().UTC()
	sysVars := e.storageManager.GetSystemVariablesManager()
	before := sysVars.ListVariables("", manager.GlobalScope)
	rollback := func() {
		for name, value := range before {
			definition, definitionErr := sysVars.GetVariableDefinition(name)
			if definitionErr == nil && !definition.ReadOnly {
				_ = sysVars.SetVariable("", name, value, manager.GlobalScope)
			}
		}
	}
	if !persistOnly {
		for _, item := range assignments {
			if err := e.setGlobalVariable(session, item.name, item.value); err != nil {
				rollback()
				return true, err
			}
		}
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if host == "" {
		host = "localhost"
	}
	for _, item := range assignments {
		values[item.name] = persistedSystemVariable{
			VariableName: item.name, VariableValue: item.value, SetTime: setTime,
			SetUser: user, SetHost: host,
		}
	}
	if err := e.writePersistedSystemVariables(values); err != nil {
		if !persistOnly {
			rollback()
		}
		return true, err
	}
	if ctx != nil {
		ctx.AdminResult = &Result{
			ResultType: common.RESULT_TYPE_SET,
			Message:    fmt.Sprintf("SET %s completed for %d variable(s)", strings.ToUpper(match[1]), len(assignments)),
		}
	}
	return true, nil
}

func normalizePersistedSystemVariableValue(definition *manager.SystemVariable, value interface{}) interface{} {
	if definition == nil {
		return value
	}
	switch definition.DefaultValue.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		switch numeric := value.(type) {
		case float64:
			if numeric == float64(int64(numeric)) {
				return int64(numeric)
			}
		case float32:
			if numeric == float32(int64(numeric)) {
				return int64(numeric)
			}
		}
	}
	return value
}

func sessionHasPersistedReadOnlyVariableAdmin(session server.MySQLServerSession) bool {
	if sessionHasGlobalVariableAdmin(session) {
		return true
	}
	if session == nil {
		return false
	}
	if dynamicPrivileges, ok := session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "PERSIST_RO_VARIABLES_ADMIN") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) executePerformanceSchemaPersistedVariablesSelect(query string) *SelectResult {
	const table = "persisted_variables"
	columns := requestedInformationSchemaColumns(query, performanceSchemaTableColumns(table))
	values, err := e.readPersistedSystemVariables()
	if err != nil {
		return newInformationSchemaSelectResult("performance_schema."+table, columns, nil)
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		if len(filterPerformanceSchemaVariableRows(query, [][]interface{}{{name}})) == 0 {
			continue
		}
		value := values[name]
		rows = append(rows, projectInformationSchemaRow(columns, map[string]interface{}{
			"VARIABLE_NAME":  value.VariableName,
			"VARIABLE_VALUE": fmt.Sprint(value.VariableValue),
			"SET_TIME":       value.SetTime,
			"SET_USER":       value.SetUser,
			"SET_HOST":       value.SetHost,
		}))
	}
	return newInformationSchemaSelectResult("performance_schema."+table, columns, rows)
}
