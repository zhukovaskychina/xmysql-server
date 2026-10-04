package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

const pendingOptimizerCostFileParam = "pending_optimizer_cost_file"

type persistedServerCostRow struct {
	CostName     string   `json:"cost_name"`
	CostValue    *float64 `json:"cost_value"`
	LastUpdate   *string  `json:"last_update"`
	Comment      *string  `json:"comment"`
	DefaultValue *float64 `json:"default_value"`
}

type persistedEngineCostRow struct {
	EngineName   string   `json:"engine_name"`
	DeviceType   int64    `json:"device_type"`
	CostName     string   `json:"cost_name"`
	CostValue    *float64 `json:"cost_value"`
	LastUpdate   *string  `json:"last_update"`
	Comment      *string  `json:"comment"`
	DefaultValue *float64 `json:"default_value"`
}

type persistedOptimizerCostFile struct {
	Server []persistedServerCostRow `json:"server_cost"`
	Engine []persistedEngineCostRow `json:"engine_cost"`
}

type optimizerCostRuntimeSnapshot struct {
	Server map[string]float64
	Engine map[string]float64
}

func optimizerCostFloat(value float64) *float64 {
	return &value
}

func optimizerCostString(value string) *string {
	return &value
}

func defaultOptimizerCostFile() persistedOptimizerCostFile {
	return persistedOptimizerCostFile{
		Server: []persistedServerCostRow{
			{CostName: "disk_temptable_create_cost", DefaultValue: optimizerCostFloat(20)},
			{CostName: "disk_temptable_row_cost", DefaultValue: optimizerCostFloat(0.5)},
			{CostName: "key_compare_cost", DefaultValue: optimizerCostFloat(0.05)},
			{CostName: "memory_temptable_create_cost", DefaultValue: optimizerCostFloat(1)},
			{CostName: "memory_temptable_row_cost", DefaultValue: optimizerCostFloat(0.1)},
			{CostName: "row_evaluate_cost", DefaultValue: optimizerCostFloat(0.1)},
		},
		Engine: []persistedEngineCostRow{
			{EngineName: "default", DeviceType: 0, CostName: "io_block_read_cost", DefaultValue: optimizerCostFloat(1)},
			{EngineName: "default", DeviceType: 0, CostName: "memory_block_read_cost", DefaultValue: optimizerCostFloat(0.25)},
		},
	}
}

func (e *XMySQLExecutor) optimizerCostFilePath() string {
	return filepath.Join(e.getDataDir(), "mysql", "optimizer_costs.json")
}

func (e *XMySQLExecutor) loadPersistedOptimizerCosts() (persistedOptimizerCostFile, error) {
	raw, err := os.ReadFile(e.optimizerCostFilePath())
	if os.IsNotExist(err) {
		return defaultOptimizerCostFile(), nil
	}
	if err != nil {
		return persistedOptimizerCostFile{}, err
	}
	var file persistedOptimizerCostFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return persistedOptimizerCostFile{}, err
	}
	return file, nil
}

func (e *XMySQLExecutor) savePersistedOptimizerCosts(file persistedOptimizerCostFile) error {
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return writeMetadataFileAtomic(e.optimizerCostFilePath(), raw)
}

func (e *XMySQLExecutor) optimizerCostFileForSession(session server.MySQLServerSession) (persistedOptimizerCostFile, error) {
	if session != nil && sessionTransactionActive(session) {
		if raw := session.GetParamByName(pendingOptimizerCostFileParam); raw != nil {
			if file, ok := raw.(*persistedOptimizerCostFile); ok && file != nil {
				return *file, nil
			}
		}
	}
	return e.loadPersistedOptimizerCosts()
}

func (e *XMySQLExecutor) stageOrSaveOptimizerCosts(session server.MySQLServerSession, file persistedOptimizerCostFile) error {
	if session != nil && sessionTransactionActive(session) {
		session.SetParamByName(pendingOptimizerCostFileParam, &file)
		return nil
	}
	return e.savePersistedOptimizerCosts(file)
}

func (e *XMySQLExecutor) commitSessionOptimizerCostChanges(session server.MySQLServerSession) error {
	if session == nil {
		return nil
	}
	raw := session.GetParamByName(pendingOptimizerCostFileParam)
	file, ok := raw.(*persistedOptimizerCostFile)
	if !ok || file == nil {
		return nil
	}
	if err := e.savePersistedOptimizerCosts(*file); err != nil {
		return err
	}
	session.SetParamByName(pendingOptimizerCostFileParam, nil)
	return nil
}

func (e *XMySQLExecutor) discardSessionOptimizerCostChanges(session server.MySQLServerSession) {
	if session != nil {
		session.SetParamByName(pendingOptimizerCostFileParam, nil)
	}
}

func optimizerCostValue(value *float64) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func optimizerCostTimestamp(value *string) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func optimizerCostRows(file persistedOptimizerCostFile, table string) []map[string]interface{} {
	rows := make([]map[string]interface{}, 0)
	if table == "server_cost" {
		for _, entry := range file.Server {
			rows = append(rows, map[string]interface{}{
				"COST_NAME": entry.CostName, "COST_VALUE": optimizerCostValue(entry.CostValue),
				"LAST_UPDATE": optimizerCostTimestamp(entry.LastUpdate), "COMMENT": optimizerCostValueString(entry.Comment),
				"DEFAULT_VALUE": optimizerCostValue(entry.DefaultValue),
			})
		}
		return rows
	}
	for _, entry := range file.Engine {
		rows = append(rows, map[string]interface{}{
			"ENGINE_NAME": entry.EngineName, "DEVICE_TYPE": strconv.FormatInt(entry.DeviceType, 10), "COST_NAME": entry.CostName,
			"COST_VALUE": optimizerCostValue(entry.CostValue), "LAST_UPDATE": optimizerCostTimestamp(entry.LastUpdate),
			"COMMENT": optimizerCostValueString(entry.Comment), "DEFAULT_VALUE": optimizerCostValue(entry.DefaultValue),
		})
	}
	return rows
}

func optimizerCostValueString(value *string) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func (e *XMySQLExecutor) mysqlOptimizerCostRows(table string) []map[string]interface{} {
	return e.mysqlOptimizerCostRowsForSession(nil, table)
}

func (e *XMySQLExecutor) mysqlOptimizerCostRowsForSession(session server.MySQLServerSession, table string) []map[string]interface{} {
	file, err := e.optimizerCostFileForSession(session)
	if err != nil {
		file = defaultOptimizerCostFile()
	}
	return optimizerCostRows(file, table)
}

func (e *XMySQLExecutor) optimizerCostSnapshotValue(name string) (float64, bool) {
	e.optimizerCostMu.RLock()
	value, ok := e.optimizerCostSnapshot.Server[strings.ToLower(strings.TrimSpace(name))]
	e.optimizerCostMu.RUnlock()
	return value, ok
}

func (e *XMySQLExecutor) reloadOptimizerCostModel() error {
	file, err := e.loadPersistedOptimizerCosts()
	if err != nil {
		return err
	}
	snapshot := optimizerCostRuntimeSnapshot{Server: map[string]float64{}, Engine: map[string]float64{}}
	for _, row := range file.Server {
		value := row.CostValue
		if value == nil {
			value = row.DefaultValue
		}
		if value != nil && *value > 0 {
			snapshot.Server[strings.ToLower(row.CostName)] = *value
		}
	}
	for _, row := range file.Engine {
		value := row.CostValue
		if value == nil {
			value = row.DefaultValue
		}
		if value != nil && *value > 0 {
			key := strings.ToLower(row.EngineName) + ":" + strconv.FormatInt(row.DeviceType, 10) + ":" + strings.ToLower(row.CostName)
			snapshot.Engine[key] = *value
		}
	}
	e.optimizerCostMu.Lock()
	e.optimizerCostSnapshot = snapshot
	e.optimizerCostSnapshotLoaded = true
	e.optimizerCostMu.Unlock()
	if optimizerManager, ok := e.optimizerManager.(*manager.OptimizerManager); ok && optimizerManager != nil {
		optimizerManager.SetCostModel(snapshot.Server, snapshot.Engine)
	}
	return nil
}

func (e *XMySQLExecutor) ensureOptimizerCostModelLoaded() error {
	e.optimizerCostMu.RLock()
	loaded := e.optimizerCostSnapshotLoaded
	e.optimizerCostMu.RUnlock()
	if loaded {
		return nil
	}
	return e.reloadOptimizerCostModel()
}

var optimizerCostInsertPattern = regexp.MustCompile(`(?is)^\s*insert\s+(?:ignore\s+)?into\s+mysql\s*\.\s*(server_cost|engine_cost)\s*\((.*?)\)\s*values\s*(.+?)\s*$`)
var optimizerCostUpdatePattern = regexp.MustCompile(`(?is)^\s*update\s+mysql\s*\.\s*(server_cost|engine_cost)\s+set\s+(.+?)(?:\s+where\s+(.+?))?\s*$`)
var optimizerCostDeletePattern = regexp.MustCompile(`(?is)^\s*delete\s+from\s+mysql\s*\.\s*(server_cost|engine_cost)(?:\s+where\s+(.+?))?\s*$`)

func parseOptimizerCostLiteral(raw string) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "null") {
		return "", true, nil
	}
	if len(raw) >= 2 && ((raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"')) {
		if raw[0] == '\'' {
			return strings.ReplaceAll(raw[1:len(raw)-1], "''", "'"), false, nil
		}
		return strings.ReplaceAll(raw[1:len(raw)-1], `""`, `"`), false, nil
	}
	if raw == "" {
		return "", false, fmt.Errorf("empty optimizer cost value")
	}
	return raw, false, nil
}

func parseOptimizerCostFloat(raw string) (*float64, error) {
	value, isNull, err := parseOptimizerCostLiteral(raw)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || parsed <= 0 {
		return nil, fmt.Errorf("optimizer cost must be a positive number")
	}
	return optimizerCostFloat(parsed), nil
}

func parseOptimizerCostComment(raw string) (*string, error) {
	value, isNull, err := parseOptimizerCostLiteral(raw)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return optimizerCostString(value), nil
}

func normalizeOptimizerCostColumn(raw string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(raw), "`"))
}

func splitTopLevelAnd(input string) []string {
	parts := make([]string, 0)
	start, depth := 0, 0
	var quote byte
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == quote && (i == 0 || input[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i+3 <= len(input) && strings.EqualFold(input[i:i+3], "and") &&
			(i == 0 || input[i-1] == ' ' || input[i-1] == '\t') &&
			(i+3 == len(input) || input[i+3] == ' ' || input[i+3] == '\t') {
			if part := strings.TrimSpace(input[start:i]); part != "" {
				parts = append(parts, part)
			}
			start = i + 3
			i += 2
		}
	}
	if part := strings.TrimSpace(input[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseOptimizerCostWhere(where string) (map[string]string, error) {
	conditions := map[string]string{}
	where = strings.TrimSpace(where)
	if where == "" {
		return conditions, nil
	}
	for _, condition := range splitTopLevelAnd(where) {
		parts := strings.SplitN(condition, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("optimizer cost WHERE supports equality predicates only")
		}
		column := normalizeOptimizerCostColumn(parts[0])
		if column != "cost_name" && column != "engine_name" && column != "device_type" {
			return nil, fmt.Errorf("unsupported optimizer cost WHERE column %q", column)
		}
		value, isNull, err := parseOptimizerCostLiteral(parts[1])
		if err != nil || isNull {
			return nil, fmt.Errorf("optimizer cost key cannot be NULL")
		}
		conditions[column] = value
	}
	return conditions, nil
}

func optimizerCostRowMatchesServer(row persistedServerCostRow, conditions map[string]string) bool {
	for column, value := range conditions {
		if column == "cost_name" && !strings.EqualFold(row.CostName, value) {
			return false
		}
	}
	return true
}

func optimizerCostRowMatchesEngine(row persistedEngineCostRow, conditions map[string]string) bool {
	for column, value := range conditions {
		switch column {
		case "engine_name":
			if !strings.EqualFold(row.EngineName, value) {
				return false
			}
		case "device_type":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || row.DeviceType != parsed {
				return false
			}
		case "cost_name":
			if !strings.EqualFold(row.CostName, value) {
				return false
			}
		}
	}
	return true
}

func optimizerCostNow() *string {
	return optimizerCostString(time.Now().UTC().Format(time.RFC3339))
}

func parseOptimizerCostAssignments(raw string) (map[string]string, error) {
	assignments := map[string]string{}
	for _, assignment := range splitTopLevelComma(raw) {
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid optimizer cost assignment %q", assignment)
		}
		column := normalizeOptimizerCostColumn(parts[0])
		if column != "cost_value" && column != "comment" {
			return nil, fmt.Errorf("optimizer cost column %q is read-only or unsupported", column)
		}
		assignments[column] = strings.TrimSpace(parts[1])
	}
	return assignments, nil
}

func (e *XMySQLExecutor) executeMySQLOptimizerCostMutation(ctx *ExecutionContext, session server.MySQLServerSession, query string) (bool, error) {
	trimmed := strings.ReplaceAll(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")), "`", "")
	insert := optimizerCostInsertPattern.FindStringSubmatch(trimmed)
	update := optimizerCostUpdatePattern.FindStringSubmatch(trimmed)
	delete := optimizerCostDeletePattern.FindStringSubmatch(trimmed)
	if len(insert) == 0 && len(update) == 0 && len(delete) == 0 {
		return false, nil
	}
	if ctx == nil {
		ctx = &ExecutionContext{Session: session}
	}
	if len(insert) > 0 {
		return true, e.executeOptimizerCostInsert(ctx, session, insert)
	}
	if len(update) > 0 {
		return true, e.executeOptimizerCostUpdate(ctx, session, update)
	}
	return true, e.executeOptimizerCostDelete(ctx, session, delete)
}

func (e *XMySQLExecutor) executeOptimizerCostInsert(ctx *ExecutionContext, session server.MySQLServerSession, match []string) error {
	table := strings.ToLower(match[1])
	if err := e.checkTablePrivilege(ctx, "mysql", table, "INSERT"); err != nil {
		return err
	}
	file, err := e.optimizerCostFileForSession(session)
	if err != nil {
		return err
	}
	columns := make([]string, 0)
	for _, column := range splitTopLevelComma(match[2]) {
		columns = append(columns, normalizeOptimizerCostColumn(column))
	}
	for _, tuple := range splitTopLevelComma(match[3]) {
		tuple = strings.TrimSpace(tuple)
		if len(tuple) < 2 || tuple[0] != '(' || tuple[len(tuple)-1] != ')' {
			return fmt.Errorf("invalid optimizer cost VALUES tuple")
		}
		values := splitTopLevelComma(tuple[1 : len(tuple)-1])
		if len(values) != len(columns) {
			return fmt.Errorf("optimizer cost column/value count does not match")
		}
		valueByColumn := make(map[string]string, len(columns))
		for index, column := range columns {
			valueByColumn[column] = values[index]
		}
		costName, ok := valueByColumn["cost_name"]
		if !ok {
			return fmt.Errorf("optimizer cost_name is required")
		}
		name, isNull, err := parseOptimizerCostLiteral(costName)
		if err != nil || isNull || strings.TrimSpace(name) == "" {
			return fmt.Errorf("optimizer cost_name is required")
		}
		var costValue *float64
		if rawCostValue, hasValue := valueByColumn["cost_value"]; hasValue {
			costValue, err = parseOptimizerCostFloat(rawCostValue)
			if err != nil {
				return err
			}
		}
		var comment *string
		if rawComment, hasComment := valueByColumn["comment"]; hasComment {
			comment, err = parseOptimizerCostComment(rawComment)
			if err != nil {
				return err
			}
		}
		lastUpdate := optimizerCostNow()
		if table == "server_cost" {
			for _, row := range file.Server {
				if strings.EqualFold(row.CostName, name) {
					return fmt.Errorf("duplicate entry '%s' for key 'PRIMARY'", name)
				}
			}
			file.Server = append(file.Server, persistedServerCostRow{CostName: name, CostValue: costValue, LastUpdate: lastUpdate, Comment: comment})
		} else {
			engineNameRaw, okEngine := valueByColumn["engine_name"]
			deviceRaw, okDevice := valueByColumn["device_type"]
			if !okEngine || !okDevice {
				return fmt.Errorf("engine_name and device_type are required")
			}
			engineName, engineNull, parseErr := parseOptimizerCostLiteral(engineNameRaw)
			if parseErr != nil || engineNull {
				return fmt.Errorf("engine_name is required")
			}
			deviceText, deviceNull, parseErr := parseOptimizerCostLiteral(deviceRaw)
			if parseErr != nil || deviceNull {
				return fmt.Errorf("device_type is required")
			}
			deviceType, parseErr := strconv.ParseInt(strings.TrimSpace(deviceText), 10, 64)
			if parseErr != nil || deviceType < 0 {
				return fmt.Errorf("device_type must be a non-negative integer")
			}
			for _, row := range file.Engine {
				if strings.EqualFold(row.EngineName, engineName) && row.DeviceType == deviceType && strings.EqualFold(row.CostName, name) {
					return fmt.Errorf("duplicate entry for engine_cost primary key")
				}
			}
			file.Engine = append(file.Engine, persistedEngineCostRow{EngineName: engineName, DeviceType: deviceType, CostName: name, CostValue: costValue, LastUpdate: lastUpdate, Comment: comment})
		}
	}
	if err := e.stageOrSaveOptimizerCosts(session, file); err != nil {
		return err
	}
	if ctx != nil {
		ctx.statementRowsAffected.Add(1)
	}
	return nil
}

func (e *XMySQLExecutor) executeOptimizerCostUpdate(ctx *ExecutionContext, session server.MySQLServerSession, match []string) error {
	table := strings.ToLower(match[1])
	if err := e.checkTablePrivilege(ctx, "mysql", table, "UPDATE"); err != nil {
		return err
	}
	assignments, err := parseOptimizerCostAssignments(match[2])
	if err != nil {
		return err
	}
	conditions, err := parseOptimizerCostWhere(match[3])
	if err != nil {
		return err
	}
	file, err := e.optimizerCostFileForSession(session)
	if err != nil {
		return err
	}
	affected := int64(0)
	for index := range file.Server {
		if table != "server_cost" || !optimizerCostRowMatchesServer(file.Server[index], conditions) {
			continue
		}
		if value, ok := assignments["cost_value"]; ok {
			file.Server[index].CostValue, err = parseOptimizerCostFloat(value)
			if err != nil {
				return err
			}
		}
		if value, ok := assignments["comment"]; ok {
			file.Server[index].Comment, err = parseOptimizerCostComment(value)
			if err != nil {
				return err
			}
		}
		file.Server[index].LastUpdate = optimizerCostNow()
		affected++
	}
	for index := range file.Engine {
		if table != "engine_cost" || !optimizerCostRowMatchesEngine(file.Engine[index], conditions) {
			continue
		}
		if value, ok := assignments["cost_value"]; ok {
			file.Engine[index].CostValue, err = parseOptimizerCostFloat(value)
			if err != nil {
				return err
			}
		}
		if value, ok := assignments["comment"]; ok {
			file.Engine[index].Comment, err = parseOptimizerCostComment(value)
			if err != nil {
				return err
			}
		}
		file.Engine[index].LastUpdate = optimizerCostNow()
		affected++
	}
	if err := e.stageOrSaveOptimizerCosts(session, file); err != nil {
		return err
	}
	if ctx != nil {
		ctx.statementRowsAffected.Add(affected)
	}
	return nil
}

func (e *XMySQLExecutor) executeOptimizerCostDelete(ctx *ExecutionContext, session server.MySQLServerSession, match []string) error {
	table := strings.ToLower(match[1])
	if err := e.checkTablePrivilege(ctx, "mysql", table, "DELETE"); err != nil {
		return err
	}
	conditions, err := parseOptimizerCostWhere(match[2])
	if err != nil {
		return err
	}
	file, err := e.optimizerCostFileForSession(session)
	if err != nil {
		return err
	}
	removed := int64(0)
	if table == "server_cost" {
		remaining := file.Server[:0]
		for _, row := range file.Server {
			if optimizerCostRowMatchesServer(row, conditions) {
				removed++
				continue
			}
			remaining = append(remaining, row)
		}
		file.Server = remaining
	} else {
		remaining := file.Engine[:0]
		for _, row := range file.Engine {
			if optimizerCostRowMatchesEngine(row, conditions) {
				removed++
				continue
			}
			remaining = append(remaining, row)
		}
		file.Engine = remaining
	}
	if err := e.stageOrSaveOptimizerCosts(session, file); err != nil {
		return err
	}
	if ctx != nil {
		ctx.statementRowsAffected.Add(removed)
	}
	return nil
}
