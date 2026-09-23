package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isReplicationDMLQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"insert ", "insert\n", "replace ", "replace\n", "update ", "update\n", "delete ", "delete\n"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isReplicationDDLQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"create ", "create\n", "alter ", "alter\n", "drop ", "drop\n", "truncate ", "truncate\n", "rename ", "rename\n"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isReplicationWriteQuery(query string) bool {
	if isReplicationDMLQuery(query) || isReplicationDDLQuery(query) {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(strings.TrimRight(query, ";")))
	for _, prefix := range []string{"create ", "alter ", "drop ", "truncate ", "rename ", "lock tables ", "unlock tables "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func (e *XMySQLEngine) replicaWriteBlocked(session server.MySQLServerSession, query string) bool {
	return e.replicationWriteBlockReason(session, query) != ""
}

func (e *XMySQLEngine) replicationWriteBlockReason(session server.MySQLServerSession, query string) string {
	if e == nil || e.replicationRuntime == nil || !isReplicationWriteQuery(query) {
		return ""
	}
	if session != nil {
		if replaying, _ := session.GetParamByName("replication_replay").(bool); replaying {
			return ""
		}
	}
	if e.conf != nil && e.conf.ReplicationReadOnly && e.replicationRuntime.IsReplica() {
		return "replica is read-only while replication is active"
	}
	if e.replicationRuntime.IsFenced() {
		return "source is fenced and rejects client writes"
	}
	return ""
}

// applyReplicationStatements replays one committed source transaction on a
// replica through the same executor path used by client traffic. This keeps
// constraints, indexes and metadata updates inside the target engine rather
// than maintaining a second storage mutation implementation.
func (e *XMySQLEngine) applyReplicationStatements(statements []replication.Statement) error {
	if len(statements) == 0 {
		return nil
	}
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("autocommit", "0")
	if err := executeReplicationQuery(e, session, "begin", ""); err != nil {
		return err
	}
	for _, statement := range statements {
		session.SetParamByName("database", statement.Database)
		if err := executeReplicationQuery(e, session, statement.SQL, statement.Database); err != nil {
			_ = executeReplicationQuery(e, session, "rollback", statement.Database)
			return fmt.Errorf("replay %q: %w", statement.SQL, err)
		}
	}
	return executeReplicationQuery(e, session, "commit", "")
}

// applyReplicationRows applies a committed row-image transaction through the
// engine's normal DML path. It is preferred by Replica when row images are
// present, so a replica can consume the same atomic row change set even when
// no SQL statement text is available (for example, a native row-event feed).
func (e *XMySQLEngine) applyReplicationRows(changes []replication.RowChange) error {
	if len(changes) == 0 {
		return nil
	}
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("autocommit", "0")
	if err := executeReplicationQuery(e, session, "begin", ""); err != nil {
		return err
	}
	for _, change := range changes {
		sql, database, err := replicationRowChangeSQL(change)
		if err != nil {
			_ = executeReplicationQuery(e, session, "rollback", "")
			return err
		}
		session.SetParamByName("database", database)
		if err := executeReplicationQuery(e, session, sql, database); err != nil {
			_ = executeReplicationQuery(e, session, "rollback", database)
			return fmt.Errorf("replay row change %q: %w", sql, err)
		}
	}
	return executeReplicationQuery(e, session, "commit", "")
}

func replicationRowChangeSQL(change replication.RowChange) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(change.Table), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("replication row change table must be schema.table: %q", change.Table)
	}
	database, table := strings.Trim(parts[0], "` "), strings.Trim(parts[1], "` ")
	tableSQL := quotePartitionIdentifier(database) + "." + quotePartitionIdentifier(table)
	action := strings.ToLower(strings.TrimSpace(change.Action))
	switch action {
	case "insert", "replace":
		columns, values, err := replicationRowImageSQL(change.After)
		if err != nil {
			return "", "", err
		}
		verb := "INSERT"
		if action == "replace" {
			verb = "REPLACE"
		}
		return fmt.Sprintf("%s INTO %s (%s) VALUES (%s)", verb, tableSQL, strings.Join(columns, ", "), strings.Join(values, ", ")), database, nil
	case "delete":
		conditions, err := replicationRowPredicatesSQL(change.Before)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf("DELETE FROM %s WHERE %s", tableSQL, strings.Join(conditions, " AND ")), database, nil
	case "update":
		after, err := replicationCompleteAfterImage(change)
		if err != nil {
			return "", "", err
		}
		setColumns, setValues, err := replicationRowImageSQL(after)
		if err != nil {
			return "", "", err
		}
		conditions, err := replicationRowPredicatesSQL(change.Before)
		if err != nil {
			return "", "", err
		}
		assignments := make([]string, len(setColumns))
		for index := range setColumns {
			assignments[index] = setColumns[index] + " = " + setValues[index]
		}
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableSQL, strings.Join(assignments, ", "), strings.Join(conditions, " AND ")), database, nil
	default:
		return "", "", fmt.Errorf("unsupported replication row action %q", change.Action)
	}
}

func replicationCompleteAfterImage(change replication.RowChange) (map[string]interface{}, error) {
	after := make(map[string]interface{}, len(change.After)+len(change.PartialJSONUpdates))
	for column, value := range change.After {
		after[column] = value
	}
	for column, updates := range change.PartialJSONUpdates {
		if _, exists := after[column]; exists {
			continue
		}
		before, exists := change.Before[column]
		if !exists {
			return nil, fmt.Errorf("partial JSON row image is missing before value for column %q", column)
		}
		materialized, ok := replication.ApplyJSONPartialUpdates(before, updates)
		if !ok {
			return nil, fmt.Errorf("unable to materialize partial JSON row image for column %q", column)
		}
		var decoded interface{}
		if err := json.Unmarshal([]byte(materialized), &decoded); err != nil {
			return nil, fmt.Errorf("invalid materialized partial JSON for column %q: %w", column, err)
		}
		encoded, err := json.Marshal(decoded)
		if err != nil {
			return nil, fmt.Errorf("encode materialized partial JSON for column %q: %w", column, err)
		}
		after[column] = string(encoded)
	}
	return after, nil
}

func replicationRowImageSQL(values map[string]interface{}) ([]string, []string, error) {
	if len(values) == 0 {
		return nil, nil, fmt.Errorf("replication row image is empty")
	}
	columns := make([]string, 0, len(values))
	for column := range values {
		if strings.TrimSpace(column) == "" {
			return nil, nil, fmt.Errorf("replication row image contains an empty column")
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)
	quoted := make([]string, len(columns))
	valuesSQL := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quotePartitionIdentifier(column)
		literal, err := replicationSQLLiteral(values[column])
		if err != nil {
			return nil, nil, err
		}
		valuesSQL[index] = literal
	}
	return quoted, valuesSQL, nil
}

func replicationRowPredicatesSQL(values map[string]interface{}) ([]string, error) {
	columns, valuesSQL, err := replicationRowImageSQL(values)
	if err != nil {
		return nil, err
	}
	conditions := make([]string, len(columns))
	for index := range columns {
		if valuesSQL[index] == "NULL" {
			conditions[index] = columns[index] + " IS NULL"
		} else {
			conditions[index] = columns[index] + " = " + valuesSQL[index]
		}
	}
	return conditions, nil
}

func replicationSQLLiteral(value interface{}) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	switch typed := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'", nil
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'", nil
	case bool:
		if typed {
			return "TRUE", nil
		}
		return "FALSE", nil
	case time.Time:
		return "'" + typed.Format("2006-01-02 15:04:05.999999") + "'", nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return "", fmt.Errorf("replication row image contains non-finite float")
		}
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return "", fmt.Errorf("replication row image contains non-finite float")
		}
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(typed), nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return strconv.FormatUint(typed, 10), nil
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(value), "'", "''") + "'", nil
	}
}

func executeReplicationQuery(e *XMySQLEngine, session server.MySQLServerSession, query, database string) error {
	if e == nil {
		return fmt.Errorf("nil engine")
	}
	results := e.ExecuteQuery(session, query, database)
	for result := range results {
		if result != nil && result.Err != nil {
			return result.Err
		}
	}
	return nil
}

type replicationSession struct {
	mu     sync.RWMutex
	params map[string]interface{}
	ctx    *server.SessionContext
}

func newReplicationSession() *replicationSession {
	return &replicationSession{params: make(map[string]interface{}), ctx: server.NewSessionContext("replication")}
}

func (s *replicationSession) GetLastActiveTime() time.Time { return time.Now() }
func (s *replicationSession) SendOK()                      {}
func (s *replicationSession) SendHandleOk()                {}
func (s *replicationSession) SendSelectFields()            {}
func (s *replicationSession) SessionContext() *server.SessionContext {
	return s.ctx
}
func (s *replicationSession) GetParamByName(name string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value := s.params[name]; value != nil {
		return value
	}
	return nil
}
func (s *replicationSession) SetParamByName(name string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params[name] = value
	if name == "database" {
		s.ctx.SetCurrentDB(fmt.Sprint(value))
	}
	if name == "autocommit" {
		s.ctx.SetAutocommit(fmt.Sprint(value) != "0")
	}
	if name == "in_transaction" {
		if b, ok := value.(bool); ok {
			s.ctx.SetInTransaction(b)
		}
	}
}
