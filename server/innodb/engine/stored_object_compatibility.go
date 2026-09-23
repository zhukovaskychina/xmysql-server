package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type persistedStoredObject struct {
	Schema               string   `json:"schema"`
	Name                 string   `json:"name"`
	ObjectType           string   `json:"object_type"`
	Definition           string   `json:"definition"`
	Parameters           []string `json:"parameters,omitempty"`
	ReturnType           string   `json:"return_type,omitempty"`
	LocalVariables       []string `json:"local_variables,omitempty"`
	ReturnExpression     string   `json:"return_expression,omitempty"`
	Definer              string   `json:"definer,omitempty"`
	SQLSecurity          string   `json:"sql_security,omitempty"`
	RoutineComment       string   `json:"routine_comment,omitempty"`
	Disabled             bool     `json:"disabled,omitempty"`
	OnCompletionPreserve bool     `json:"on_completion_preserve,omitempty"`
	CreatedAt            string   `json:"created_at"`
}

type storedRoutineCursor struct {
	query       string
	rows        []map[string]interface{}
	columnCount int
	position    int
	open        bool
}

type storedRoutineState struct {
	vars             map[string]string
	localVars        map[string]struct{}
	cursors          map[string]*storedRoutineCursor
	notFound         []string
	notFoundKind     string
	sqlException     []string
	sqlExceptionKind string
	sqlWarning       []string
	sqlWarningKind   string
	prepared         map[string]string
	conditions       map[string]string
	handlers         []storedRoutineHandler
	activeCondition  *storedRoutineCondition
	scopeDepth       int
	callDepth        int
	accounting       *statementResultAccounting
}

type storedRoutineHandler struct {
	action     string
	condition  string
	statements []string
	scopeDepth int
}

type storedRoutineCondition struct {
	sqlState    string
	message     string
	messageText string
	mysqlErrno  int
	err         error
}

type routineControlSignal struct {
	kind        string
	label       string
	targetDepth int
}

func (signal *routineControlSignal) Error() string {
	return signal.kind + " " + signal.label
}

func storedObjectSuffix(objectType string) string {
	switch strings.ToLower(objectType) {
	case "procedure", "function":
		return ".routine.json"
	case "trigger":
		return ".trigger.json"
	case "event":
		return ".event.json"
	default:
		return ""
	}
}

func (e *XMySQLExecutor) storedObjectPath(schema, name, objectType string) string {
	return filepath.Join(e.getDataDir(), schema, name+storedObjectSuffix(objectType))
}

func storedObjectMetadataKey(schema, name string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "." + strings.ToLower(strings.TrimSpace(name))
}

func (e *XMySQLExecutor) acquireStoredObjectReadLock(ctx *ExecutionContext, session server.MySQLServerSession, schema, name string) (func(), error) {
	key := storedObjectMetadataKey(schema, name)
	if strings.Trim(key, ".") == "" {
		return func() {}, nil
	}
	return e.acquireStatementTableLocks(ctx, session, "select * from "+key, schema)
}

func (e *XMySQLExecutor) acquireStoredObjectWriteLock(ctx *ExecutionContext, schema, name string) (func(), error) {
	key := storedObjectMetadataKey(schema, name)
	if strings.Trim(key, ".") == "" {
		return func() {}, nil
	}
	return e.acquireDDLWriteLock(ctx, key, false)
}

func (e *XMySQLExecutor) acquireStoredObjectReadLocks(ctx *ExecutionContext, session server.MySQLServerSession, objects []persistedStoredObject, schema string) (func(), error) {
	names := make([]string, 0, len(objects))
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if (strings.TrimSpace(schema) != "" && !strings.EqualFold(object.Schema, schema)) || strings.TrimSpace(object.Name) == "" {
			continue
		}
		key := storedObjectMetadataKey(object.Schema, object.Name)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, key)
	}
	sort.Strings(names)
	releases := make([]func(), 0, len(names))
	for _, key := range names {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			continue
		}
		release, err := e.acquireStoredObjectReadLock(ctx, session, parts[0], parts[1])
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, err
		}
		releases = append(releases, release)
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, nil
}

// acquireTriggerExecutionLocks keeps shared metadata leases on the triggers
// attached to one DML target for the complete statement. Trigger definitions
// are persisted as stored objects, so this is the object-level counterpart to
// the target-table DML lock acquired by the normal statement path.
func (e *XMySQLExecutor) acquireTriggerExecutionLocks(ctx context.Context, session server.MySQLServerSession, schema, table string) (func(), error) {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if e == nil || schema == "" || table == "" {
		return func() {}, nil
	}
	objects := make([]persistedStoredObject, 0)
	for _, object := range e.scanStoredObjects("trigger") {
		if !strings.EqualFold(object.Schema, schema) {
			continue
		}
		_, _, targetTable, _ := parseTriggerInformationSchemaMetadata(object.Definition)
		if strings.EqualFold(strings.TrimSpace(targetTable), table) {
			objects = append(objects, object)
		}
	}
	if len(objects) == 0 {
		return func() {}, nil
	}
	lockCtx := &ExecutionContext{Context: ctx, Session: session}
	release, err := e.acquireStoredObjectReadLocks(lockCtx, session, objects, schema)
	if err != nil {
		return nil, tableLockWaitError(err)
	}
	return release, nil
}

func (e *XMySQLExecutor) executeStoredObjectDDL(ctx *ExecutionContext, query, databaseName string) bool {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	createQuery := regexp.MustCompile(`(?is)^create\s+definer\s*=\s*'[^']*'\s*@\s*'[^']*'\s+`).ReplaceAllString(trimmed, "create ")
	create := regexp.MustCompile(`(?is)^create\s+(?:or\s+replace\s+)?(procedure|function|trigger|event)\s+((?:` + "`?" + `[^\s.(]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.(]+` + "`?" + `)?)(?:\(([^)]*)\))?\s+(.+)$`).FindStringSubmatch(createQuery)
	drop := regexp.MustCompile(`(?is)^drop\s+(procedure|function|trigger|event)\s+(?:if\s+exists\s+)?((?:` + "`?" + `[^\s.]+` + "`?" + `)(?:\s*\.\s*` + "`?" + `[^\s.]+` + "`?" + `)?)\s*$`).FindStringSubmatch(trimmed)
	create = parseStoredObjectCreate(createQuery)
	if len(create) == 0 && len(drop) == 0 {
		return false
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if len(drop) > 0 {
		objectType, schema, name := parseStoredObjectName(drop[1], drop[2], databaseName)
		releaseObjectLock, lockErr := e.acquireStoredObjectWriteLock(ctx, schema, name)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		defer releaseObjectLock()
		if strings.EqualFold(objectType, "procedure") || strings.EqualFold(objectType, "function") {
			if err := e.checkStoredRoutineDDLPrivilege(ctx, schema, "ALTER ROUTINE"); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		if strings.EqualFold(objectType, "event") {
			if err := e.checkStoredEventPrivilege(ctx, schema); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		if strings.EqualFold(objectType, "trigger") {
			if err := e.checkStoredTriggerObjectPrivilege(ctx, schema, name); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		if err := os.Remove(e.storedObjectPath(schema, name, objectType)); err != nil && !os.IsNotExist(err) {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if strings.EqualFold(objectType, "event") && e.eventScheduler != nil {
			e.eventScheduler.RemoveEvent(schema + "." + name)
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: "stored object dropped"}
		return true
	}
	objectType, schema, name := parseStoredObjectName(create[1], create[2], databaseName)
	if schema == "" || name == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("stored object name is empty"), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	releaseObjectLock, lockErr := e.acquireStoredObjectWriteLock(ctx, schema, name)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	defer releaseObjectLock()
	if strings.EqualFold(objectType, "event") {
		if err := e.checkStoredEventPrivilege(ctx, schema); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if strings.EqualFold(objectType, "procedure") || strings.EqualFold(objectType, "function") {
		if err := e.checkStoredRoutineDDLPrivilege(ctx, schema, "CREATE ROUTINE"); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if strings.EqualFold(objectType, "trigger") {
		_, _, triggerTable, _ := parseTriggerInformationSchemaMetadata(trimmed)
		if err := e.checkStoredTriggerPrivilege(ctx, schema, triggerTable); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	objectPath := e.storedObjectPath(schema, name, objectType)
	lowerQuery := strings.ToLower(trimmed)
	if strings.EqualFold(objectType, "trigger") {
		timing, event, tableName, _ := parseTriggerInformationSchemaMetadata(trimmed)
		if _, referencedName, hasOrder := triggerOrderReference(trimmed); hasOrder {
			found := false
			for _, trigger := range loadTableTriggers(e.getDataDir(), schema, tableName, event, timing) {
				if strings.EqualFold(trigger.Name, referencedName) {
					found = true
					break
				}
			}
			if !found {
				ctx.Results <- &Result{Err: fmt.Errorf("trigger ordering reference '%s' does not exist", referencedName), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
	}
	if strings.EqualFold(objectType, "trigger") && !strings.Contains(lowerQuery, "if not exists") && !strings.Contains(lowerQuery, "or replace") {
		if _, err := os.Stat(objectPath); err == nil {
			ctx.Results <- &Result{Err: fmt.Errorf("trigger '%s' already exists", name), ResultType: common.RESULT_TYPE_QUERY}
			return true
		} else if !os.IsNotExist(err) {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if strings.Contains(lowerQuery, "if not exists") && !strings.Contains(lowerQuery, "or replace") {
		if _, err := os.Stat(objectPath); err == nil {
			ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: "stored object already exists"}
			return true
		} else if !os.IsNotExist(err) {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if err := os.MkdirAll(filepath.Join(e.getDataDir(), schema), 0755); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	definer := storedObjectDefiner(trimmed)
	if definer != "" {
		if err := e.validateStoredObjectAccount(definer); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	security := ""
	if strings.Contains(strings.ToLower(create[4]), "sql security invoker") {
		security = "INVOKER"
	} else if definer != "" {
		security = "DEFINER"
	}
	object := persistedStoredObject{
		Schema: schema, Name: name, ObjectType: objectType, Definition: trimmed,
		Parameters:       parseRoutineParameterNames(create[3]),
		ReturnType:       parseRoutineReturnType(objectType, trimmed),
		LocalVariables:   parseRoutineLocalVariables(trimmed),
		ReturnExpression: parseRoutineReturnExpression(objectType, trimmed),
		RoutineComment:   parseRoutineComment(trimmed),
		Definer:          definer, SQLSecurity: security, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if strings.EqualFold(objectType, "event") {
		schedule, err := parseSQLEventSchedule(object.Definition)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		object.OnCompletionPreserve = schedule.OnCompletionPreserve
	}
	raw, err := json.MarshalIndent(object, "", "  ")
	if err == nil {
		err = writeMetadataFileAtomic(objectPath, raw)
	}
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if strings.EqualFold(objectType, "event") {
		if err := e.registerSQLEvent(object); err != nil {
			_ = os.Remove(objectPath)
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: "stored object created"}
	return true
}

// executeStoredObjectAlter handles the operational part of ALTER EVENT. The
// definition remains durable, while the scheduler registration is updated in
// the same statement so ENABLE/DISABLE takes effect without a restart.
func (e *XMySQLExecutor) executeStoredObjectAlter(ctx *ExecutionContext, query, databaseName string) bool {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	routineMatch := regexp.MustCompile(`(?is)^alter\s+(procedure|function)\s+((?:[a-zA-Z0-9_$]+\s*\.\s*)?[a-zA-Z0-9_$]+)\s+(.+)$`).FindStringSubmatch(trimmed)
	if len(routineMatch) == 4 {
		if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		objectType, schema, name := parseStoredObjectName(routineMatch[1], routineMatch[2], databaseName)
		releaseObjectLock, lockErr := e.acquireStoredObjectWriteLock(ctx, schema, name)
		if lockErr != nil {
			ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		defer releaseObjectLock()
		if err := e.checkStoredRoutineDDLPrivilege(ctx, schema, "ALTER ROUTINE"); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		path := e.storedObjectPath(schema, name, objectType)
		raw, err := os.ReadFile(path)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		var object persistedStoredObject
		if err := json.Unmarshal(raw, &object); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if !strings.EqualFold(object.ObjectType, objectType) {
			ctx.Results <- &Result{Err: fmt.Errorf("stored object %s.%s is not a %s", schema, name, objectType), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		options := strings.TrimSpace(routineMatch[3])
		changed := false
		if security := regexp.MustCompile(`(?is)\bsql\s+security\s+(definer|invoker)\b`).FindStringSubmatch(options); len(security) == 2 {
			object.SQLSecurity = strings.ToUpper(security[1])
			object.Definition = replaceRoutineSecurity(object.Definition, object.SQLSecurity)
			changed = true
		}
		if deterministic := regexp.MustCompile(`(?is)\b(?:not\s+)?deterministic\b`).FindString(options); deterministic != "" {
			object.Definition = replaceRoutineClause(object.Definition, `(?:not\s+)?deterministic`, strings.ToUpper(strings.TrimSpace(deterministic)))
			changed = true
		}
		if dataAccess := regexp.MustCompile(`(?is)\b(?:no\s+sql|contains\s+sql|reads\s+sql\s+data|modifies\s+sql\s+data)\b`).FindString(options); dataAccess != "" {
			object.Definition = replaceRoutineClause(object.Definition, `(?:no\s+sql|contains\s+sql|reads\s+sql\s+data|modifies\s+sql\s+data)`, strings.ToUpper(strings.TrimSpace(dataAccess)))
			changed = true
		}
		if comment := regexp.MustCompile(`(?is)\bcomment\s+(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`).FindStringSubmatch(options); len(comment) == 3 {
			object.RoutineComment = comment[1]
			if object.RoutineComment == "" {
				object.RoutineComment = comment[2]
			}
			object.RoutineComment = strings.ReplaceAll(object.RoutineComment, "''", "'")
			object.RoutineComment = strings.ReplaceAll(object.RoutineComment, "\"\"", "\"")
			changed = true
		}
		if !changed {
			ctx.Results <- &Result{Err: fmt.Errorf("unsupported ALTER %s options", strings.ToUpper(objectType)), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		encoded, err := json.MarshalIndent(object, "", "  ")
		if err == nil {
			err = writeMetadataFileAtomic(path, encoded)
		}
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: fmt.Sprintf("%s altered", objectType)}
		return true
	}

	match := regexp.MustCompile(`(?is)^alter\s+event\s+((?:[a-zA-Z0-9_$]+\s*\.\s*)?[a-zA-Z0-9_$]+)\s+(.+?)\s*;?$`).FindStringSubmatch(trimmed)
	if len(match) != 3 {
		return false
	}
	if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	objectType, schema, name := parseStoredObjectName("event", match[1], databaseName)
	releaseObjectLock, lockErr := e.acquireStoredObjectWriteLock(ctx, schema, name)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	defer releaseObjectLock()
	if err := e.checkStoredEventPrivilege(ctx, schema); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	path := e.storedObjectPath(schema, name, objectType)
	raw, err := os.ReadFile(path)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	options := strings.TrimSpace(strings.TrimSuffix(match[2], ";"))
	lower := strings.ToLower(options)
	if renameMatch := regexp.MustCompile(`(?is)^rename\s+to\s+((?:[a-zA-Z0-9_$]+\s*\.\s*)?[a-zA-Z0-9_$]+)$`).FindStringSubmatch(options); len(renameMatch) == 2 {
		newType, newSchema, newName := parseStoredObjectName("event", renameMatch[1], schema)
		if newType != "event" || !strings.EqualFold(newSchema, schema) {
			ctx.Results <- &Result{Err: fmt.Errorf("ALTER EVENT RENAME must stay in the same schema"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		newPath := e.storedObjectPath(schema, newName, "event")
		if _, statErr := os.Stat(newPath); statErr == nil && !strings.EqualFold(newName, name) {
			ctx.Results <- &Result{Err: fmt.Errorf("event %s.%s already exists", schema, newName), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		object.Name = newName
		if scheduleAt := strings.Index(strings.ToLower(object.Definition), "on schedule"); scheduleAt >= 0 {
			object.Definition = fmt.Sprintf("CREATE EVENT %s.%s %s", schema, newName, strings.TrimSpace(object.Definition[scheduleAt:]))
		}
		encoded, encodeErr := json.MarshalIndent(object, "", "  ")
		if encodeErr == nil {
			encodeErr = writeMetadataFileAtomic(newPath, encoded)
		}
		if encodeErr == nil && !strings.EqualFold(newPath, path) {
			encodeErr = os.Remove(path)
		}
		if encodeErr != nil {
			ctx.Results <- &Result{Err: encodeErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if e.eventScheduler != nil {
			e.eventScheduler.RemoveEvent(schema + "." + name)
			if !object.Disabled {
				if encodeErr = e.registerSQLEvent(object); encodeErr != nil {
					ctx.Results <- &Result{Err: encodeErr, ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
			}
		}
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: "event renamed"}
		return true
	}
	if strings.Contains(lower, "disable") {
		object.Disabled = true
	}
	if strings.Contains(lower, "enable") {
		object.Disabled = false
	}
	if strings.Contains(lower, "on schedule") || strings.Contains(lower, " do ") {
		definitionOptions := regexp.MustCompile(`(?is)\b(?:enable|disable)\b`).ReplaceAllString(options, "")
		object.Definition = fmt.Sprintf("CREATE EVENT %s.%s %s", schema, name, strings.TrimSpace(definitionOptions))
		schedule, err := parseSQLEventSchedule(object.Definition)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		object.OnCompletionPreserve = schedule.OnCompletionPreserve
	} else if strings.Contains(lower, "on completion preserve") || strings.Contains(lower, "on completion not preserve") {
		object.OnCompletionPreserve = strings.Contains(lower, "on completion preserve") && !strings.Contains(lower, "on completion not preserve")
	}
	encoded, err := json.MarshalIndent(object, "", "  ")
	if err == nil {
		err = writeMetadataFileAtomic(path, encoded)
	}
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if e.eventScheduler != nil {
		e.eventScheduler.RemoveEvent(schema + "." + name)
		if !object.Disabled {
			if err := e.registerSQLEvent(object); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", nil, nil), Message: "event altered"}
	return true
}

func replaceRoutineSecurity(definition, security string) string {
	return replaceRoutineClause(definition, `sql\s+security\s+(?:definer|invoker)`, "SQL SECURITY "+strings.ToUpper(security))
}

func replaceRoutineClause(definition, pattern, clause string) string {
	clausePattern := regexp.MustCompile(`(?is)\s+` + pattern + `\b`)
	if clausePattern.MatchString(definition) {
		return clausePattern.ReplaceAllString(definition, " "+clause)
	}
	for _, keyword := range []string{"begin", "return"} {
		if index := strings.Index(strings.ToLower(definition), keyword); index >= 0 {
			return definition[:index] + " " + clause + " " + definition[index:]
		}
	}
	return strings.TrimSpace(definition) + " " + clause
}

func parseStoredObjectCreate(query string) []string {
	prefix := regexp.MustCompile("(?is)^create\\s+(?:or\\s+replace\\s+)?(?:if\\s+not\\s+exists\\s+)?(procedure|function|trigger|event)\\s+((?:`?[a-zA-Z0-9_$]+`?)(?:\\s*\\.\\s*`?[a-zA-Z0-9_$]+`?)?)")
	match := prefix.FindStringSubmatch(query)
	if len(match) != 3 {
		return nil
	}
	rest := strings.TrimSpace(query[len(match[0]):])
	parameters := ""
	if strings.HasPrefix(rest, "(") {
		close := matchingParenIndex(rest, 0)
		if close < 0 {
			return nil
		}
		parameters = rest[1:close]
		rest = strings.TrimSpace(rest[close+1:])
	}
	if rest == "" {
		return nil
	}
	return []string{"", match[1], match[2], parameters, rest}
}

// executeStoredObjectCall implements the smallest useful routine execution
// contract: CALL of a persisted procedure whose body contains SELECTs.
func (e *XMySQLExecutor) executeStoredObjectCall(ctx *ExecutionContext, query, databaseName string) bool {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if !storedProcedureCallPattern.MatchString(trimmed) {
		return false
	}
	match := storedProcedureCallPattern.FindStringSubmatch(trimmed)
	_, schema, name := parseStoredObjectName("procedure", match[1], databaseName)
	releaseObjectLock, lockErr := e.acquireStoredObjectReadLock(ctx, ctx.Session, schema, name)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	defer releaseObjectLock()
	if err := e.executeStoredProcedureCall(ctx, query, databaseName, nil); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "procedure executed successfully"}
	return true
}

var storedProcedureCallPattern = regexp.MustCompile(`(?is)^call\s+((?:` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s*\.\s*)?` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `)(?:\s*\(([^)]*)\))?\s*$`)

type storedRoutineLocalReference struct {
	local     string
	temporary string
}

func (e *XMySQLExecutor) resolveStoredProcedureCallWithLocalReferences(ctx *ExecutionContext, databaseName, statement string, state *storedRoutineState) (string, []storedRoutineLocalReference, error) {
	match := storedProcedureCallPattern.FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) == 0 || strings.TrimSpace(match[2]) == "" {
		return strings.TrimSpace(statement), nil, nil
	}
	_, routineSchema, routineName := parseStoredObjectName("procedure", match[1], databaseName)
	modes, err := e.storedProcedureParameterModes(routineSchema, routineName)
	if err != nil {
		return "", nil, err
	}
	arguments := splitTopLevelComma(match[2])
	resolved := make([]string, 0, len(arguments))
	references := make([]storedRoutineLocalReference, 0)
	for index, argument := range arguments {
		expression := strings.TrimSpace(argument)
		localName := strings.ToLower(strings.Trim(expression, "` "))
		mode := "IN"
		if index < len(modes) {
			mode = modes[index]
		}
		if (mode == "OUT" || mode == "INOUT") && state != nil {
			if _, local := state.vars[localName]; local {
				if ctx == nil || ctx.Session == nil {
					return "", nil, fmt.Errorf("routine local %s requires a session for OUT/INOUT propagation", localName)
				}
				value, evalErr := evaluateStoredRoutineScalarExpression(state.vars[localName])
				if evalErr != nil {
					return "", nil, evalErr
				}
				temporary := fmt.Sprintf("@__xmysql_routine_ref_%d_%d", state.callDepth, index)
				ctx.Session.SetParamByName(temporary, value)
				ctx.Session.SetParamByName(strings.TrimPrefix(temporary, "@"), value)
				resolved = append(resolved, temporary)
				references = append(references, storedRoutineLocalReference{local: localName, temporary: strings.TrimPrefix(temporary, "@")})
				continue
			}
		}
		if strings.HasPrefix(expression, "@") {
			resolved = append(resolved, expression)
			continue
		}
		expression = substituteStoredRoutineValues(expression, state.vars)
		value, evalErr := evaluateStoredRoutineScalarExpression(expression)
		if evalErr != nil {
			return "", nil, evalErr
		}
		resolved = append(resolved, sessionUserVariableSQLLiteral(value))
	}
	return "call " + match[1] + "(" + strings.Join(resolved, ", ") + ")", references, nil
}

func (e *XMySQLExecutor) storedProcedureParameterModes(schema, name string) ([]string, error) {
	raw, err := os.ReadFile(e.storedObjectPath(schema, name, "procedure"))
	if err != nil {
		return nil, err
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	parameters := object.Parameters
	if len(parameters) == 0 {
		if header := regexp.MustCompile(`(?is)^create\s+procedure\s+[^\s(]+\s*\(([^)]*)\)`).FindStringSubmatch(object.Definition); len(header) == 2 {
			parameters = parseRoutineParameterNames(header[1])
		}
	}
	modes := make([]string, 0, len(parameters))
	for _, parameter := range parameters {
		fields := strings.Fields(strings.TrimSpace(parameter))
		mode := "IN"
		if len(fields) > 0 && (strings.EqualFold(fields[0], "IN") || strings.EqualFold(fields[0], "OUT") || strings.EqualFold(fields[0], "INOUT")) {
			mode = strings.ToUpper(fields[0])
		}
		modes = append(modes, mode)
	}
	return modes, nil
}

func resolveStoredProcedureCallArguments(statement string, variables map[string]string) (string, error) {
	match := storedProcedureCallPattern.FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) == 0 || strings.TrimSpace(match[2]) == "" {
		return strings.TrimSpace(statement), nil
	}
	arguments := splitTopLevelComma(match[2])
	resolved := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		expression := strings.TrimSpace(argument)
		// Preserve user-variable references for OUT/INOUT arguments. They are
		// references, not values: the callee must write back to the same
		// session variable, including through recursive CALLs.
		if strings.HasPrefix(expression, "@") {
			resolved = append(resolved, expression)
			continue
		}
		expression = substituteStoredRoutineValues(expression, variables)
		value, err := evaluateStoredRoutineScalarExpression(expression)
		if err != nil {
			return "", err
		}
		resolved = append(resolved, sessionUserVariableSQLLiteral(value))
	}
	return "call " + match[1] + "(" + strings.Join(resolved, ", ") + ")", nil
}

// executeStoredProcedureCall executes a procedure in the caller's execution
// context. A child routine state gives recursive calls local variables while
// retaining the same session, transaction and result channel for side effects
// and nested statements.
func (e *XMySQLExecutor) executeStoredProcedureCall(ctx *ExecutionContext, query, databaseName string, parent *storedRoutineState) error {
	match := storedProcedureCallPattern.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) == 0 {
		return fmt.Errorf("invalid CALL statement")
	}
	_, routineSchema, routineName := parseStoredObjectName("procedure", match[1], databaseName)
	startedAt := time.Now()
	releaseObjectLock, lockErr := e.acquireStoredObjectReadLock(ctx, ctx.Session, routineSchema, routineName)
	if lockErr != nil {
		return tableLockWaitError(lockErr)
	}
	defer releaseObjectLock()
	raw, err := os.ReadFile(e.storedObjectPath(routineSchema, routineName, "procedure"))
	if err != nil {
		return err
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if object.Definer != "" {
		if err := e.validateStoredObjectAccount(object.Definer); err != nil {
			return err
		}
	}
	if err := e.checkStoredRoutineExecute(ctx, object.Schema, object.Name); err != nil {
		return err
	}
	body := strings.TrimSpace(object.Definition)
	arguments := splitTopLevelComma(match[2])
	parameters := object.Parameters
	if len(parameters) == 0 {
		if header := regexp.MustCompile(`(?is)^create\s+procedure\s+[^\s(]+\s*\(([^)]*)\)`).FindStringSubmatch(body); len(header) == 2 {
			parameters = parseRoutineParameterNames(header[1])
		}
	}
	for i, parameter := range parameters {
		if i >= len(arguments) {
			break
		}
		fields := strings.Fields(strings.TrimSpace(parameter))
		if len(fields) == 0 {
			continue
		}
		nameIndex := 0
		if len(fields) >= 3 && (strings.EqualFold(fields[0], "in") || strings.EqualFold(fields[0], "out") || strings.EqualFold(fields[0], "inout")) {
			nameIndex = 1
		} else if len(fields) >= 2 {
			nameIndex = len(fields) - 2
		}
		name := strings.Trim(fields[nameIndex], "`")
		body = regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(name)+`\b`).ReplaceAllString(body, strings.TrimSpace(arguments[i]))
	}
	if begin := strings.Index(strings.ToLower(body), "begin"); begin >= 0 {
		body = body[begin+len("begin"):]
		if end := strings.LastIndex(strings.ToLower(body), "end"); end >= 0 {
			body = body[:end]
		}
	}
	state := &storedRoutineState{vars: make(map[string]string), localVars: make(map[string]struct{}), cursors: make(map[string]*storedRoutineCursor), prepared: make(map[string]string), conditions: make(map[string]string), accounting: &statementResultAccounting{}}
	if parent != nil {
		if parent.callDepth >= 64 {
			return fmt.Errorf("stored procedure recursion exceeded maximum depth 64")
		}
		state = cloneStoredRoutineState(parent)
		state.callDepth = parent.callDepth + 1
	} else {
		state.callDepth = 1
	}
	statements := splitStoredObjectStatements(body)
	if err := e.executeStoredRoutineStatements(ctx, databaseName, statements, state); err != nil {
		if signal, ok := err.(*routineControlSignal); !ok || signal.kind != "handler_exit" {
			return err
		}
	}
	statementStats := e.performanceSchemaStoredProgramStatementStats(ctx.Session, startedAt, int64(len(statements)))
	if state.accounting != nil {
		statementStats.rowsAffected = state.accounting.rowsAffected
		statementStats.rowsSent = state.accounting.rowsSent
		statementStats.warnings = state.accounting.warnings
	}
	e.recordPerformanceSchemaProgramExecution("PROCEDURE", routineSchema, routineName, time.Since(startedAt).Nanoseconds()*1000,
		statementStats.count, statementStats.sum, statementStats.min, statementStats.max,
		statementStats.errors, statementStats.warnings, statementStats.rowsAffected, statementStats.rowsSent,
	)
	return nil
}

type performanceSchemaStoredProgramStatementStats struct {
	count, sum, min, max   int64
	errors, warnings       int64
	rowsAffected, rowsSent int64
}

func (e *XMySQLExecutor) performanceSchemaStoredProgramStatementStats(session server.MySQLServerSession, startedAt time.Time, fallbackCount int64) performanceSchemaStoredProgramStatementStats {
	if e == nil || e.metricsRecorder == nil {
		return performanceSchemaStoredProgramStatementStats{count: fallbackCount}
	}
	threadID := int64(sessionConnectionID(session))
	stats := performanceSchemaStoredProgramStatementStats{min: -1}
	for _, event := range e.metricsRecorder.StatementHistory() {
		if !event.Time.IsZero() && event.Time.Before(startedAt) {
			continue
		}
		if threadID != 0 && event.ThreadID != 0 && event.ThreadID != threadID {
			continue
		}
		timer := event.Latency.Nanoseconds() * 1000
		if timer < 0 {
			timer = 0
		}
		stats.count++
		stats.sum += timer
		if stats.min < 0 || timer < stats.min {
			stats.min = timer
		}
		if timer > stats.max {
			stats.max = timer
		}
		if event.Status == "error" {
			stats.errors++
		}
		stats.warnings += event.Warnings
		stats.rowsAffected += event.RowsAffected
		stats.rowsSent += event.RowsSent
	}
	if stats.count == 0 {
		return performanceSchemaStoredProgramStatementStats{count: fallbackCount}
	}
	return stats
}

func (e *XMySQLExecutor) executeStoredRoutineStatements(ctx *ExecutionContext, databaseName string, statements []string, state *storedRoutineState) error {
	for index := 0; index < len(statements); index++ {
		statement := strings.TrimSpace(statements[index])
		rawStatement := statement
		lower := strings.ToLower(statement)
		if routineBlockStart(lower) {
			end := routineBlockEnd(statements, index)
			if end < 0 {
				return fmt.Errorf("routine BEGIN requires END")
			}
			body := make([]string, 0, end-index)
			firstBody := strings.TrimSpace(statement[len("begin"):])
			if firstBody != "" {
				body = append(body, firstBody)
			}
			body = append(body, statements[index+1:end]...)
			child := cloneStoredRoutineState(state)
			if err := e.executeStoredRoutineStatements(ctx, databaseName, body, child); err != nil {
				if signal, ok := err.(*routineControlSignal); !ok || signal.kind != "handler_exit" || signal.targetDepth != child.scopeDepth {
					return err
				}
			}
			for name := range state.vars {
				if _, locallyDeclared := child.localVars[name]; locallyDeclared {
					continue
				}
				if value, exists := child.vars[name]; exists {
					state.vars[name] = value
				}
			}
			index = end
			continue
		}
		if strings.HasPrefix(lower, "begin") && (len(lower) == len("begin") || lower[len("begin")] == ' ' || lower[len("begin")] == '\n' || lower[len("begin")] == '\t') {
			statement = strings.TrimSpace(statement[len("begin"):])
			rawStatement = statement
			lower = strings.ToLower(statement)
			if statement == "" {
				continue
			}
		}
		if conditionMatch := regexp.MustCompile(`(?is)^declare\s+([a-zA-Z0-9_$]+)\s+condition\s+for\s+(?:sqlstate\s+'([^']+)'|([0-9]+))$`).FindStringSubmatch(statement); len(conditionMatch) == 4 {
			condition := conditionMatch[2]
			if condition == "" {
				condition = conditionMatch[3]
			}
			if state.conditions == nil {
				state.conditions = make(map[string]string)
			}
			resolvedCondition := condition
			if conditionMatch[2] != "" {
				resolvedCondition = "sqlstate'" + strings.ToLower(strings.TrimSpace(condition)) + "'"
			}
			state.conditions[strings.ToLower(strings.Trim(conditionMatch[1], "` "))] = resolvedCondition
			continue
		}
		if handlers, matched := parseStoredRoutineHandlerDeclaration(statement, state); matched {
			for _, handler := range handlers {
				state.handlers = append(state.handlers, handler)
				switch handler.condition {
				case "not found":
					state.notFound = handler.statements
					state.notFoundKind = handler.action
				case "sqlexception":
					state.sqlException = handler.statements
					state.sqlExceptionKind = handler.action
				case "sqlwarning":
					state.sqlWarning = handler.statements
					state.sqlWarningKind = handler.action
				}
			}
			continue
		}
		if diagnosticsMatch := regexp.MustCompile(`(?is)^get\s+(?:current\s+)?diagnostics\s+condition\s+([0-9]+)\s+(.+)$`).FindStringSubmatch(statement); len(diagnosticsMatch) == 3 {
			if diagnosticsMatch[1] != "1" {
				return fmt.Errorf("routine GET DIAGNOSTICS only supports condition 1")
			}
			if state.activeCondition == nil {
				return fmt.Errorf("routine GET DIAGNOSTICS without an active condition")
			}
			assignments := splitTopLevelComma(diagnosticsMatch[2])
			for _, assignment := range assignments {
				assignmentMatch := regexp.MustCompile(`(?is)^\s*([@a-zA-Z0-9_$]+)\s*=\s*(returned_sqlstate|message_text|mysql_errno)\s*$`).FindStringSubmatch(assignment)
				if len(assignmentMatch) != 3 {
					return fmt.Errorf("unsupported routine GET DIAGNOSTICS item %q", strings.TrimSpace(assignment))
				}
				value := ""
				switch strings.ToLower(assignmentMatch[2]) {
				case "returned_sqlstate":
					value = state.activeCondition.sqlState
				case "message_text":
					value = state.activeCondition.messageText
					if value == "" {
						value = state.activeCondition.message
					}
				case "mysql_errno":
					value = fmt.Sprint(state.activeCondition.mysqlErrno)
					if state.activeCondition.mysqlErrno == 0 {
						value = "1644"
					}
				}
				target := strings.TrimSpace(assignmentMatch[1])
				if strings.HasPrefix(target, "@") {
					if ctx == nil || ctx.Session == nil {
						return fmt.Errorf("routine GET DIAGNOSTICS user variable requires a session")
					}
					ctx.Session.SetParamByName(target, value)
					ctx.Session.SetParamByName(strings.TrimPrefix(target, "@"), value)
					continue
				}
				name := strings.ToLower(strings.Trim(target, "` "))
				if _, exists := state.vars[name]; !exists {
					return fmt.Errorf("routine GET DIAGNOSTICS target %q is not declared", target)
				}
				state.vars[name] = sessionUserVariableSQLLiteral(value)
			}
			continue
		}
		if cursorMatch := regexp.MustCompile(`(?is)^declare\s+([a-zA-Z0-9_$]+)\s+cursor\s+for\s+(.+)$`).FindStringSubmatch(statement); len(cursorMatch) == 3 {
			state.cursors[strings.ToLower(strings.Trim(cursorMatch[1], "` "))] = &storedRoutineCursor{query: strings.TrimSpace(cursorMatch[2])}
			continue
		}
		if strings.HasPrefix(lower, "declare ") {
			name, value, err := parseRoutineDeclare(statement)
			if err != nil {
				return err
			}
			if _, exists := state.localVars[name]; exists {
				return fmt.Errorf("duplicate routine local variable %q", name)
			}
			state.vars[name] = value
			if state.localVars == nil {
				state.localVars = make(map[string]struct{})
			}
			state.localVars[name] = struct{}{}
			continue
		}
		if setMatch := regexp.MustCompile(`(?is)^set\s+([a-zA-Z0-9_$]+)\s*=\s*(.+)$`).FindStringSubmatch(statement); len(setMatch) == 3 {
			name := strings.Trim(setMatch[1], "` ")
			if _, local := state.vars[strings.ToLower(name)]; local {
				expression := substituteStoredRoutineValues(strings.TrimSpace(setMatch[2]), state.vars)
				value, err := evaluateStoredRoutineScalarExpression(expression)
				if err != nil {
					return err
				}
				state.vars[strings.ToLower(name)] = sessionUserVariableSQLLiteral(value)
				continue
			}
		}
		if prepareMatch := regexp.MustCompile(`(?is)^prepare\s+([a-zA-Z0-9_$]+)\s+from\s+(.+)$`).FindStringSubmatch(statement); len(prepareMatch) == 3 {
			expression := strings.TrimSpace(prepareMatch[2])
			if ctx != nil {
				expression = e.rewriteSessionUserVariables(expression, ctx.Session)
			}
			value, err := evaluateStoredRoutineScalarExpression(expression)
			if err != nil {
				return err
			}
			query, ok := value.(string)
			if !ok || strings.TrimSpace(query) == "" {
				return fmt.Errorf("routine PREPARE source must evaluate to a non-empty string")
			}
			state.prepared[strings.ToLower(strings.Trim(prepareMatch[1], "` "))] = strings.TrimSpace(query)
			continue
		}
		if deallocateMatch := regexp.MustCompile(`(?is)^deallocate\s+prepare\s+([a-zA-Z0-9_$]+)$`).FindStringSubmatch(statement); len(deallocateMatch) == 2 {
			delete(state.prepared, strings.ToLower(strings.Trim(deallocateMatch[1], "` ")))
			continue
		}
		if executeMatch := regexp.MustCompile(`(?is)^execute\s+([a-zA-Z0-9_$]+)(?:\s+using\s+(.+))?$`).FindStringSubmatch(statement); len(executeMatch) >= 2 {
			query, ok := state.prepared[strings.ToLower(strings.Trim(executeMatch[1], "` "))]
			if !ok {
				return fmt.Errorf("unknown prepared routine statement %q", executeMatch[1])
			}
			if len(executeMatch) == 3 && strings.TrimSpace(executeMatch[2]) != "" {
				arguments := splitTopLevelComma(executeMatch[2])
				values := make([]interface{}, 0, len(arguments))
				for _, argument := range arguments {
					argument = strings.TrimSpace(argument)
					value := interface{}(nil)
					if ctx != nil && ctx.Session != nil {
						value = ctx.Session.GetParamByName(argument)
						if value == nil && strings.HasPrefix(argument, "@") {
							value = ctx.Session.GetParamByName(strings.TrimPrefix(argument, "@"))
						}
					}
					if value == nil && state != nil && state.vars != nil {
						localName := strings.ToLower(strings.Trim(argument, "`@ "))
						if literal, local := state.vars[localName]; local {
							var evalErr error
							value, evalErr = evaluateStoredRoutineScalarExpression(literal)
							if evalErr != nil {
								return evalErr
							}
						}
					}
					values = append(values, value)
				}
				var bindErr error
				query, bindErr = bindRoutineParameterMarkers(query, values)
				if bindErr != nil {
					return bindErr
				}
			}
			statement = query
		}
		if openMatch := regexp.MustCompile(`(?is)^open\s+([a-zA-Z0-9_$]+)$`).FindStringSubmatch(statement); len(openMatch) == 2 {
			cursor, ok := state.cursors[strings.ToLower(strings.Trim(openMatch[1], "` "))]
			if !ok {
				return fmt.Errorf("unknown routine cursor %q", openMatch[1])
			}
			if cursor.open {
				return fmt.Errorf("routine cursor %q is already open", openMatch[1])
			}
			query := substituteStoredRoutineValues(cursor.query, state.vars)
			if ctx != nil && ctx.Session != nil {
				query = e.rewriteSessionUserVariables(query, ctx.Session)
			}
			rows, columnCount, err := e.executeStoredRoutineCursorQuery(ctx, databaseName, query)
			if err != nil {
				return err
			}
			cursor.rows = rows
			cursor.columnCount = columnCount
			cursor.position = 0
			cursor.open = true
			continue
		}
		if closeMatch := regexp.MustCompile(`(?is)^close\s+([a-zA-Z0-9_$]+)$`).FindStringSubmatch(statement); len(closeMatch) == 2 {
			name := strings.ToLower(strings.Trim(closeMatch[1], "` "))
			cursor, ok := state.cursors[name]
			if !ok {
				return fmt.Errorf("unknown routine cursor %q", closeMatch[1])
			}
			if !cursor.open {
				return fmt.Errorf("routine cursor %q is not open", closeMatch[1])
			}
			// CLOSE releases the current result set but keeps the declared
			// cursor available for a later OPEN in the same routine invocation.
			// MySQL permits a declared cursor to be opened again after CLOSE.
			cursor.open = false
			cursor.position = 0
			continue
		}
		if fetchMatch := regexp.MustCompile(`(?is)^fetch\s+(?:(?:next\s+)?from\s+)?([a-zA-Z0-9_$]+)\s+into\s+(.+)$`).FindStringSubmatch(statement); len(fetchMatch) == 3 {
			cursor, ok := state.cursors[strings.ToLower(strings.Trim(fetchMatch[1], "` "))]
			if !ok {
				return fmt.Errorf("unknown routine cursor %q", fetchMatch[1])
			}
			if !cursor.open {
				return fmt.Errorf("routine cursor %q is not open", fetchMatch[1])
			}
			targets := splitTopLevelComma(fetchMatch[2])
			if len(targets) != cursor.columnCount {
				return fmt.Errorf("routine FETCH target count %d does not match cursor column count %d", len(targets), cursor.columnCount)
			}
			for _, target := range targets {
				name := strings.ToLower(strings.Trim(strings.TrimSpace(target), "` "))
				if strings.HasPrefix(name, "@") {
					return fmt.Errorf("routine FETCH target %q is not declared", strings.TrimSpace(target))
				}
				if _, declared := state.vars[name]; !declared {
					return fmt.Errorf("routine FETCH target %q is not declared", strings.TrimSpace(target))
				}
			}
			if cursor.position >= len(cursor.rows) {
				handler := findStoredRoutineHandler(state, "not found", nil)
				if handler != nil {
					if err := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state); err != nil {
						return err
					}
					if handler.action == "exit" {
						return routineHandlerExitSignal(handler)
					}
				} else if len(state.notFound) > 0 {
					if err := e.executeStoredRoutineStatements(ctx, databaseName, state.notFound, state); err != nil {
						return err
					}
					if state.notFoundKind == "exit" {
						return &routineControlSignal{kind: "handler_exit", targetDepth: state.scopeDepth}
					}
				}
				continue
			}
			row := cursor.rows[cursor.position]
			cursor.position++
			for targetIndex, target := range targets {
				name := strings.ToLower(strings.Trim(strings.TrimSpace(target), "` @"))
				if targetIndex < len(row) {
					if value, exists := row[name]; exists {
						state.vars[name] = sessionUserVariableSQLLiteral(value)
						continue
					}
				}
				if value, exists := row[fmt.Sprintf("__column_%d", targetIndex)]; exists {
					state.vars[name] = sessionUserVariableSQLLiteral(value)
				}
			}
			continue
		}
		if selectIntoMatch := regexp.MustCompile(`(?is)^select\s+(.+?)\s+into\s+(.+?)\s+from\s+(.+)$`).FindStringSubmatch(statement); len(selectIntoMatch) == 4 {
			handled, err := e.executeStoredRoutineSelectInto(ctx, databaseName, selectIntoMatch[1], selectIntoMatch[2], selectIntoMatch[3], state)
			if err != nil {
				return err
			}
			if handled {
				continue
			}
		}
		if storedProcedureCallPattern.MatchString(statement) {
			resolvedCall, localReferences, err := e.resolveStoredProcedureCallWithLocalReferences(ctx, databaseName, statement, state)
			if err != nil {
				return err
			}
			if err := e.executeStoredProcedureCall(ctx, resolvedCall, databaseName, state); err != nil {
				return err
			}
			if ctx != nil && ctx.Session != nil {
				for _, reference := range localReferences {
					value := ctx.Session.GetParamByName(reference.temporary)
					state.vars[reference.local] = sessionUserVariableSQLLiteral(value)
				}
			}
			continue
		}
		statement = substituteStoredRoutineValues(statement, state.vars)
		lower = strings.ToLower(statement)
		if controlMatch := regexp.MustCompile(`(?is)^(leave|iterate)\s+([a-zA-Z0-9_$]+)$`).FindStringSubmatch(statement); len(controlMatch) == 3 {
			return &routineControlSignal{kind: strings.ToLower(controlMatch[1]), label: strings.ToLower(controlMatch[2])}
		}
		if loopMatch := regexp.MustCompile(`(?is)^(?:([a-zA-Z0-9_$]+)\s*:\s*)?loop(?:\s+(.*))?$`).FindStringSubmatch(rawStatement); len(loopMatch) == 3 {
			label := strings.ToLower(strings.TrimSpace(loopMatch[1]))
			end := index + 1
			for end < len(statements) && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(statements[end])), "end loop") {
				end++
			}
			if end >= len(statements) {
				return fmt.Errorf("routine LOOP requires END LOOP")
			}
			body := make([]string, 0, end-index)
			if firstBody := strings.TrimSpace(loopMatch[2]); firstBody != "" {
				body = append(body, firstBody)
			}
			body = append(body, statements[index+1:end]...)
			for iteration := 0; iteration < 10000; iteration++ {
				err := e.executeStoredRoutineStatements(ctx, databaseName, body, state)
				if err == nil {
					continue
				}
				signal, ok := err.(*routineControlSignal)
				if !ok || (signal.label != "" && signal.label != label) {
					return err
				}
				if signal.kind == "leave" {
					break
				}
			}
			index = end
			continue
		}
		if lower == "case" || strings.HasPrefix(lower, "case ") {
			end := index + 1
			for end < len(statements) && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(statements[end])), "end case") {
				end++
			}
			if end >= len(statements) {
				return fmt.Errorf("routine CASE requires END CASE")
			}
			parts := []string{rawStatement}
			parts = append(parts, statements[index+1:end]...)
			caseText := strings.Join(parts, "; ")
			if endCase := findRoutineKeyword(caseText, "end case", 0); endCase >= 0 {
				caseText = caseText[:endCase]
			}
			simpleExpression, branches, err := parseStoredRoutineCase(caseText)
			if err != nil {
				return err
			}
			for _, branch := range branches {
				matched := branch.isElse
				if !matched {
					condition := branch.condition
					if simpleExpression != "" {
						condition = simpleExpression + " = " + condition
					}
					condition = substituteStoredRoutineValues(condition, state.vars)
					if ctx != nil {
						condition = e.rewriteSessionUserVariables(condition, ctx.Session)
					}
					matched, err = evaluateRoutineCondition(condition)
					if err != nil {
						return err
					}
				}
				if matched {
					if err := e.executeStoredRoutineStatements(ctx, databaseName, splitStoredObjectStatements(branch.body), state); err != nil {
						return err
					}
					break
				}
			}
			index = end
			continue
		}
		if strings.HasPrefix(lower, "repeat") {
			end := index + 1
			untilStatement := ""
			for end < len(statements) {
				candidate := strings.TrimSpace(statements[end])
				if strings.HasPrefix(strings.ToLower(candidate), "until ") {
					untilStatement = candidate
					break
				}
				end++
			}
			if untilStatement == "" {
				return fmt.Errorf("routine REPEAT requires UNTIL")
			}
			condition := strings.TrimSpace(untilStatement[len("until "):])
			condition = strings.TrimSpace(regexp.MustCompile(`(?is)\s+end\s+repeat\s*$`).ReplaceAllString(condition, ""))
			body := make([]string, 0, end-index)
			firstBody := strings.TrimSpace(rawStatement[len("repeat"):])
			if firstBody != "" {
				body = append(body, firstBody)
			}
			body = append(body, statements[index+1:end]...)
			for iteration := 0; iteration < 10000; iteration++ {
				if err := e.executeStoredRoutineStatements(ctx, databaseName, body, state); err != nil {
					return err
				}
				resolvedCondition := substituteStoredRoutineValues(condition, state.vars)
				if ctx != nil {
					resolvedCondition = e.rewriteSessionUserVariables(resolvedCondition, ctx.Session)
				}
				matched, err := evaluateRoutineCondition(resolvedCondition)
				if err != nil {
					return err
				}
				if matched {
					break
				}
				if iteration == 9999 {
					return fmt.Errorf("routine REPEAT exceeded iteration limit")
				}
			}
			index = end
			continue
		}
		if strings.HasPrefix(lower, "while ") {
			rawLower := strings.ToLower(rawStatement)
			doAt := strings.Index(rawLower, " do ")
			if doAt < 0 {
				return fmt.Errorf("routine WHILE requires DO")
			}
			end := index + 1
			for end < len(statements) && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(statements[end])), "end while") {
				end++
			}
			if end >= len(statements) {
				return fmt.Errorf("routine WHILE requires END WHILE")
			}
			condition := strings.TrimSpace(rawStatement[len("while "):doAt])
			body := make([]string, 0, end-index)
			firstBody := strings.TrimSpace(rawStatement[doAt+len(" do "):])
			if firstBody != "" {
				body = append(body, firstBody)
			}
			body = append(body, statements[index+1:end]...)
			for iteration := 0; iteration < 10000; iteration++ {
				resolvedCondition := substituteStoredRoutineValues(condition, state.vars)
				if ctx != nil {
					resolvedCondition = e.rewriteSessionUserVariables(resolvedCondition, ctx.Session)
				}
				matched, err := evaluateRoutineCondition(resolvedCondition)
				if err != nil {
					return err
				}
				if !matched {
					break
				}
				if err := e.executeStoredRoutineStatements(ctx, databaseName, body, state); err != nil {
					return err
				}
				if iteration == 9999 {
					return fmt.Errorf("routine WHILE exceeded iteration limit")
				}
			}
			index = end
			continue
		}
		if strings.HasPrefix(lower, "if ") {
			rawLower := strings.ToLower(rawStatement)
			thenAt := strings.Index(rawLower, " then ")
			if thenAt < 0 {
				return fmt.Errorf("routine IF requires THEN")
			}
			end := index + 1
			for end < len(statements) && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(statements[end])), "end if") {
				end++
			}
			if end >= len(statements) {
				return fmt.Errorf("routine IF requires END IF")
			}
			conditions := []string{substituteStoredRoutineValues(strings.TrimSpace(rawStatement[len("if "):thenAt]), state.vars)}
			branches := [][]string{make([]string, 0, end-index)}
			falseBranch := make([]string, 0, end-index)
			currentBranch := 0
			firstBody := strings.TrimSpace(rawStatement[thenAt+len(" then "):])
			if firstBody != "" {
				branches[0] = append(branches[0], firstBody)
			}
			for branchIndex := index + 1; branchIndex < end; branchIndex++ {
				part := strings.TrimSpace(statements[branchIndex])
				partLower := strings.ToLower(part)
				if strings.HasPrefix(partLower, "elseif ") || strings.HasPrefix(partLower, "else if ") {
					prefixLength := len("elseif ")
					if strings.HasPrefix(partLower, "else if ") {
						prefixLength = len("else if ")
					}
					thenAt := strings.Index(partLower[prefixLength:], " then ")
					if thenAt < 0 {
						return fmt.Errorf("routine ELSEIF requires THEN")
					}
					thenAt += prefixLength
					conditions = append(conditions, substituteStoredRoutineValues(strings.TrimSpace(part[prefixLength:thenAt]), state.vars))
					branches = append(branches, make([]string, 0, end-index))
					currentBranch = len(branches) - 1
					part = strings.TrimSpace(part[thenAt+len(" then "):])
				} else if strings.HasPrefix(partLower, "else") {
					currentBranch = -1
					part = strings.TrimSpace(part[len("else"):])
				}
				if part != "" {
					if currentBranch < 0 {
						falseBranch = append(falseBranch, part)
					} else {
						branches[currentBranch] = append(branches[currentBranch], part)
					}
				}
			}
			branchExecuted := false
			for branchIndex, condition := range conditions {
				resolvedCondition := substituteStoredRoutineValues(condition, state.vars)
				if ctx != nil {
					resolvedCondition = e.rewriteSessionUserVariables(resolvedCondition, ctx.Session)
				}
				matched, err := evaluateRoutineCondition(resolvedCondition)
				if err != nil {
					return err
				}
				if matched {
					if err := e.executeStoredRoutineStatements(ctx, databaseName, branches[branchIndex], state); err != nil {
						return err
					}
					branchExecuted = true
					break
				}
			}
			if !branchExecuted {
				if err := e.executeStoredRoutineStatements(ctx, databaseName, falseBranch, state); err != nil {
					return err
				}
			}
			index = end
			continue
		}
		if signalState, messageText, mysqlErrno, signalOK := parseStoredRoutineSignal(statement); signalOK {
			message := "SIGNAL SQLSTATE " + signalState
			if messageText != "" {
				message += ": " + messageText
			}
			conditionErr := fmt.Errorf("%s", message)
			conditionClass := "sqlexception"
			if strings.HasPrefix(strings.ToUpper(signalState), "01") {
				conditionClass = "sqlwarning"
			}
			if handler := findStoredRoutineHandler(state, conditionClass, conditionErr); handler != nil {
				previousCondition := state.activeCondition
				state.activeCondition = &storedRoutineCondition{sqlState: strings.ToUpper(signalState), message: message, messageText: messageText, mysqlErrno: mysqlErrno, err: conditionErr}
				if err := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state); err != nil {
					state.activeCondition = previousCondition
					return err
				}
				state.activeCondition = previousCondition
				if handler.action == "exit" {
					return routineHandlerExitSignal(handler)
				}
				continue
			}
			return &common.SQLError{Code: uint16(mysqlErrno), State: strings.ToUpper(signalState), Message: message}
		}
		if strings.HasPrefix(lower, "resignal") {
			if state.activeCondition == nil {
				return fmt.Errorf("routine RESIGNAL without an active condition")
			}
			message := state.activeCondition.message
			mysqlErrno := state.activeCondition.mysqlErrno
			if mysqlErrno == 0 {
				mysqlErrno = 1644
			}
			resignalMatch := regexp.MustCompile(`(?is)^resignal(?:\s+set\s+(.+))?$`).FindStringSubmatch(statement)
			if len(resignalMatch) < 2 {
				return fmt.Errorf("unsupported routine RESIGNAL syntax")
			}
			assignments := ""
			if len(resignalMatch) == 2 {
				assignments = resignalMatch[1]
			}
			if strings.TrimSpace(assignments) != "" {
				for _, assignment := range splitTopLevelComma(assignments) {
					assignment = strings.TrimSpace(assignment)
					if messageMatch := regexp.MustCompile(`(?is)^message_text\s*=\s*'([^']*)'$`).FindStringSubmatch(assignment); len(messageMatch) == 2 {
						message = messageMatch[1]
						continue
					}
					if errnoMatch := regexp.MustCompile(`(?is)^mysql_errno\s*=\s*([0-9]+)$`).FindStringSubmatch(assignment); len(errnoMatch) == 2 {
						if _, err := fmt.Sscanf(errnoMatch[1], "%d", &mysqlErrno); err != nil {
							return err
						}
						continue
					}
					return fmt.Errorf("unsupported routine RESIGNAL item %q", assignment)
				}
			}
			if message == "" {
				message = state.activeCondition.message
			}
			return &common.SQLError{Code: uint16(mysqlErrno), State: state.activeCondition.sqlState, Message: fmt.Sprintf("SIGNAL SQLSTATE %s: %s", state.activeCondition.sqlState, message)}
		}
		if lower == "end" || strings.HasPrefix(lower, "else") || strings.HasPrefix(lower, "end ") {
			continue
		}
		// Reuse the normal executor dispatch so INSERT/UPDATE/DELETE/DDL and
		// SELECT inside a procedure share constraint, index, warning and
		// transaction behavior with top-level statements.
		nestedResults := make(chan *Result, 8)
		nestedCtx := &ExecutionContext{
			Context:      ctx.Context,
			Results:      nestedResults,
			Cfg:          ctx.Cfg,
			DatabaseName: databaseName,
			RawQuery:     statement,
			Session:      ctx.Session,
		}
		go e.executeQuery(nestedCtx, ctx.Session, statement, databaseName, nestedResults)
		for result := range nestedResults {
			if result == nil {
				continue
			}
			if state.accounting == nil {
				state.accounting = &statementResultAccounting{}
			}
			resultAccounting := statementResultAccountingFor(result)
			state.accounting.rowsAffected += resultAccounting.rowsAffected
			state.accounting.rowsSent += resultAccounting.rowsSent
			state.accounting.warnings += resultAccounting.warnings
			if len(result.Warnings) > 0 {
				handler := findStoredRoutineHandler(state, "sqlwarning", nil)
				if handler != nil {
					if handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state); handlerErr != nil {
						return handlerErr
					}
					if handler.action == "exit" {
						return routineHandlerExitSignal(handler)
					}
				} else if len(state.sqlWarning) > 0 {
					if handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, state.sqlWarning, state); handlerErr != nil {
						return handlerErr
					}
					if state.sqlWarningKind == "exit" {
						return &routineControlSignal{kind: "handler_exit", targetDepth: state.scopeDepth}
					}
				}
			}
			if result.Err != nil {
				if handler := findStoredRoutineHandler(state, "sqlexception", result.Err); handler != nil {
					previousCondition := state.activeCondition
					state.activeCondition = storedRoutineConditionFromError(result.Err)
					handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state)
					state.activeCondition = previousCondition
					if handlerErr != nil {
						return handlerErr
					}
					if handler.action == "exit" {
						return routineHandlerExitSignal(handler)
					}
					continue
				}
				if len(state.sqlException) > 0 {
					previousCondition := state.activeCondition
					state.activeCondition = storedRoutineConditionFromError(result.Err)
					handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, state.sqlException, state)
					state.activeCondition = previousCondition
					if handlerErr != nil {
						return handlerErr
					}
					if state.sqlExceptionKind == "exit" {
						return &routineControlSignal{kind: "handler_exit", targetDepth: state.scopeDepth}
					}
					continue
				}
				return fmt.Errorf("routine statement %q failed: %w", statement, result.Err)
			}
			ctx.Results <- result
		}
	}
	return nil
}

// bindRoutineParameterMarkers replaces only parameter markers outside SQL
// string and identifier quotes. A naive strings.Replace changes literal '?'
// values and produces a different statement than the prepared statement
// requested by the routine.
func bindRoutineParameterMarkers(query string, values []interface{}) (string, error) {
	var builder strings.Builder
	argumentIndex := 0
	var quote byte
	for index := 0; index < len(query); index++ {
		ch := query[index]
		if quote != 0 {
			builder.WriteByte(ch)
			if ch == '\\' && index+1 < len(query) {
				index++
				builder.WriteByte(query[index])
				continue
			}
			if ch == quote {
				if index+1 < len(query) && query[index+1] == quote {
					index++
					builder.WriteByte(query[index])
					continue
				}
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			builder.WriteByte(ch)
			continue
		}
		if ch != '?' {
			builder.WriteByte(ch)
			continue
		}
		if argumentIndex >= len(values) {
			return "", fmt.Errorf("prepared routine statement has more parameter markers than EXECUTE arguments")
		}
		builder.WriteString(sessionUserVariableSQLLiteral(values[argumentIndex]))
		argumentIndex++
	}
	if argumentIndex != len(values) {
		return "", fmt.Errorf("EXECUTE USING supplies %d arguments, but prepared routine statement has %d parameter marker(s)", len(values), argumentIndex)
	}
	return builder.String(), nil
}

func parseStoredRoutineSignal(statement string) (string, string, int, bool) {
	match := regexp.MustCompile(`(?is)^signal\s+sqlstate\s+'([^']+)'(?:\s+set\s+(.+))?$`).FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) != 3 {
		return "", "", 0, false
	}
	messageText := ""
	mysqlErrno := 1644
	if strings.TrimSpace(match[2]) == "" {
		return match[1], messageText, mysqlErrno, true
	}
	for _, assignment := range splitTopLevelComma(match[2]) {
		assignment = strings.TrimSpace(assignment)
		if messageMatch := regexp.MustCompile(`(?is)^message_text\s*=\s*'([^']*)'$`).FindStringSubmatch(assignment); len(messageMatch) == 2 {
			messageText = messageMatch[1]
			continue
		}
		if errnoMatch := regexp.MustCompile(`(?is)^mysql_errno\s*=\s*([0-9]+)$`).FindStringSubmatch(assignment); len(errnoMatch) == 2 {
			if _, err := fmt.Sscanf(errnoMatch[1], "%d", &mysqlErrno); err != nil {
				return "", "", 0, false
			}
			continue
		}
		return "", "", 0, false
	}
	return match[1], messageText, mysqlErrno, true
}

func routineBlockStart(lower string) bool {
	return strings.HasPrefix(lower, "begin") && (len(lower) == len("begin") || lower[len("begin")] == ' ' || lower[len("begin")] == '\n' || lower[len("begin")] == '\t')
}

func routineBlockEnd(statements []string, start int) int {
	depth := 1
	for index := start + 1; index < len(statements); index++ {
		lower := strings.ToLower(strings.TrimSpace(statements[index]))
		if routineBlockStart(lower) {
			depth++
			continue
		}
		if lower == "end" {
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func cloneStoredRoutineState(state *storedRoutineState) *storedRoutineState {
	if state == nil {
		return &storedRoutineState{vars: map[string]string{}, localVars: map[string]struct{}{}, cursors: map[string]*storedRoutineCursor{}, prepared: map[string]string{}, conditions: map[string]string{}, scopeDepth: 1, accounting: &statementResultAccounting{}}
	}
	child := *state
	child.scopeDepth = state.scopeDepth + 1
	child.vars = make(map[string]string, len(state.vars))
	for name, value := range state.vars {
		child.vars[name] = value
	}
	// Declarations belong to the new lexical scope. Inherited variables remain
	// visible and can be updated, while variables declared by the child must
	// not be copied back into the parent block.
	child.localVars = make(map[string]struct{})
	child.cursors = make(map[string]*storedRoutineCursor, len(state.cursors))
	for name, cursor := range state.cursors {
		if cursor == nil {
			child.cursors[name] = nil
			continue
		}
		cursorCopy := *cursor
		cursorCopy.rows = append([]map[string]interface{}(nil), cursor.rows...)
		child.cursors[name] = &cursorCopy
	}
	child.prepared = make(map[string]string, len(state.prepared))
	for name, query := range state.prepared {
		child.prepared[name] = query
	}
	child.conditions = make(map[string]string, len(state.conditions))
	for name, condition := range state.conditions {
		child.conditions[name] = condition
	}
	child.handlers = append([]storedRoutineHandler(nil), state.handlers...)
	if state.activeCondition != nil {
		conditionCopy := *state.activeCondition
		child.activeCondition = &conditionCopy
	}
	return &child
}

func routineHandlerExitSignal(handler *storedRoutineHandler) *routineControlSignal {
	targetDepth := 0
	if handler != nil {
		targetDepth = handler.scopeDepth
	}
	return &routineControlSignal{kind: "handler_exit", targetDepth: targetDepth}
}

func parseStoredRoutineHandlerDeclaration(statement string, state *storedRoutineState) ([]storedRoutineHandler, bool) {
	match := regexp.MustCompile(`(?is)^declare\s+(continue|exit)\s+handler\s+for\s+(.+)$`).FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) != 3 {
		return nil, false
	}
	action := strings.ToLower(strings.TrimSpace(match[1]))
	rest := strings.TrimSpace(match[2])
	conditions := make([]string, 0, 1)
	for {
		condition, remaining, ok := consumeStoredRoutineHandlerCondition(rest)
		if !ok {
			return nil, false
		}
		conditions = append(conditions, resolveStoredRoutineHandlerCondition(condition, state))
		remaining = strings.TrimSpace(remaining)
		if strings.HasPrefix(remaining, ",") {
			rest = strings.TrimSpace(remaining[1:])
			continue
		}
		if remaining == "" {
			return nil, false
		}
		handlerStatements := splitStoredObjectStatements(remaining)
		handlers := make([]storedRoutineHandler, 0, len(conditions))
		for _, condition := range conditions {
			handlers = append(handlers, storedRoutineHandler{
				action: action, condition: condition, statements: handlerStatements, scopeDepth: state.scopeDepth,
			})
		}
		return handlers, true
	}
}

func consumeStoredRoutineHandlerCondition(input string) (string, string, bool) {
	input = strings.TrimSpace(input)
	lower := strings.ToLower(input)
	for _, keyword := range []string{"not found", "sqlexception", "sqlwarning"} {
		if strings.HasPrefix(lower, keyword) && routineHandlerConditionBoundary(input, len(keyword)) {
			return keyword, input[len(keyword):], true
		}
	}
	if strings.HasPrefix(lower, "sqlstate") {
		match := regexp.MustCompile(`(?is)^sqlstate\s*'([^']+)'`).FindStringSubmatch(input)
		if len(match) == 2 {
			return "sqlstate'" + strings.ToLower(strings.TrimSpace(match[1])) + "'", input[len(match[0]):], true
		}
	}
	if match := regexp.MustCompile(`^[0-9]+`).FindString(input); match != "" && routineHandlerConditionBoundary(input, len(match)) {
		return match, input[len(match):], true
	}
	end := 0
	for end < len(input) && input[end] != ',' && input[end] != ' ' && input[end] != '\t' && input[end] != '\r' && input[end] != '\n' {
		end++
	}
	if end == 0 {
		return "", input, false
	}
	return input[:end], input[end:], true
}

func routineHandlerConditionBoundary(input string, length int) bool {
	return length == len(input) || input[length] == ',' || input[length] == ' ' || input[length] == '\t' || input[length] == '\r' || input[length] == '\n'
}

type storedRoutineCaseBranch struct {
	condition string
	body      string
	isElse    bool
}

func parseStoredRoutineCase(caseText string) (string, []storedRoutineCaseBranch, error) {
	caseText = strings.TrimSpace(caseText)
	if len(caseText) < len("case") || !strings.EqualFold(caseText[:len("case")], "case") {
		return "", nil, fmt.Errorf("invalid routine CASE statement")
	}
	body := strings.TrimSpace(caseText[len("case"):])
	markers := make([]struct {
		keyword string
		start   int
		end     int
	}, 0, 2)
	for _, keyword := range []string{"when", "else"} {
		for offset := 0; ; {
			position := findRoutineKeyword(body, keyword, offset)
			if position < 0 {
				break
			}
			markers = append(markers, struct {
				keyword string
				start   int
				end     int
			}{keyword: keyword, start: position, end: position + len(keyword)})
			offset = position + len(keyword)
		}
	}
	if len(markers) == 0 {
		return "", nil, fmt.Errorf("routine CASE requires WHEN or ELSE")
	}
	for index := 1; index < len(markers); index++ {
		for cursor := index; cursor > 0 && markers[cursor].start < markers[cursor-1].start; cursor-- {
			markers[cursor], markers[cursor-1] = markers[cursor-1], markers[cursor]
		}
	}
	simpleExpression := strings.TrimSpace(body[:markers[0].start])
	branches := make([]storedRoutineCaseBranch, 0, len(markers))
	for index, marker := range markers {
		segmentEnd := len(body)
		if index+1 < len(markers) {
			segmentEnd = markers[index+1].start
		}
		segment := strings.TrimSpace(body[marker.end:segmentEnd])
		if marker.keyword == "else" {
			branches = append(branches, storedRoutineCaseBranch{body: segment, isElse: true})
			continue
		}
		thenAt := findRoutineKeyword(segment, "then", 0)
		if thenAt < 0 {
			return "", nil, fmt.Errorf("routine CASE WHEN requires THEN")
		}
		branches = append(branches, storedRoutineCaseBranch{
			condition: strings.TrimSpace(segment[:thenAt]),
			body:      strings.TrimSpace(segment[thenAt+len("then"):]),
		})
	}
	return simpleExpression, branches, nil
}

func findRoutineKeyword(text, keyword string, start int) int {
	keyword = strings.ToLower(keyword)
	var quote byte
	depth := 0
	for index := start; index+len(keyword) <= len(text); index++ {
		ch := text[index]
		if quote != 0 {
			if ch == quote && (index == 0 || text[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 || !strings.EqualFold(text[index:index+len(keyword)], keyword) {
			continue
		}
		if (index == 0 || !isRoutineIdentifierChar(text[index-1])) && (index+len(keyword) == len(text) || !isRoutineIdentifierChar(text[index+len(keyword)])) {
			return index
		}
	}
	return -1
}

func isRoutineIdentifierChar(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '$'
}

func resolveStoredRoutineHandlerCondition(condition string, state *storedRoutineState) string {
	condition = strings.TrimSpace(strings.ToLower(condition))
	if state != nil && state.conditions != nil {
		if resolved, exists := state.conditions[strings.Trim(condition, "` ")]; exists {
			return resolved
		}
	}
	if strings.HasPrefix(condition, "sqlstate") {
		return strings.ReplaceAll(condition, " ", "")
	}
	return condition
}

func findStoredRoutineHandler(state *storedRoutineState, condition string, err error) *storedRoutineHandler {
	if state == nil {
		return nil
	}
	condition = strings.ToLower(strings.TrimSpace(condition))
	conditionDetails := storedRoutineConditionFromError(err)
	var selected *storedRoutineHandler
	bestRank := 0
	for index := len(state.handlers) - 1; index >= 0; index-- {
		handler := &state.handlers[index]
		handlerCondition := strings.ToLower(strings.TrimSpace(handler.condition))
		rank := 0
		switch {
		case err != nil && regexp.MustCompile(`^\d+$`).MatchString(handlerCondition):
			if conditionDetails.mysqlErrno == parseStoredRoutineHandlerErrorCode(handlerCondition) || (conditionDetails.mysqlErrno == 0 && strings.Contains(err.Error(), handlerCondition)) {
				rank = 3
			}
		case strings.HasPrefix(handlerCondition, "sqlstate") && err != nil:
			handlerState := regexp.MustCompile(`sqlstate'?([0-9a-z]{5})'?`).FindStringSubmatch(handlerCondition)
			errState := regexp.MustCompile(`(?i)sqlstate\s+'?([0-9a-z]{5})'?`).FindStringSubmatch(err.Error())
			matchesState := len(handlerState) == 2 && strings.EqualFold(handlerState[1], conditionDetails.sqlState)
			if len(handlerState) == 2 && len(errState) == 2 && strings.EqualFold(handlerState[1], errState[1]) {
				matchesState = true
			}
			if matchesState {
				rank = 2
			}
		case handlerCondition == condition || (handlerCondition == "sqlexception" && condition == "sqlexception"):
			rank = 1
		}
		if rank > bestRank {
			selected = handler
			bestRank = rank
		}
	}
	return selected
}

func parseStoredRoutineHandlerErrorCode(condition string) int {
	code, _ := strconv.Atoi(strings.TrimSpace(condition))
	return code
}

func storedRoutineConditionFromError(err error) *storedRoutineCondition {
	return storedRoutineConditionFromErrorWithState("HY000", err)
}

func storedRoutineConditionFromErrorWithState(sqlState string, err error) *storedRoutineCondition {
	condition := &storedRoutineCondition{sqlState: sqlState, err: err}
	if err == nil {
		return condition
	}
	condition.message = err.Error()
	condition.messageText = condition.message
	var sqlErr *common.SQLError
	if errors.As(err, &sqlErr) {
		condition.sqlState = sqlErr.State
		condition.message = sqlErr.Error()
		condition.messageText = sqlErr.Message
		condition.mysqlErrno = int(sqlErr.Code)
		return condition
	}
	condition.mysqlErrno = inferStoredRoutineMySQLErrorNumber(err)
	if condition.sqlState == "HY000" && condition.mysqlErrno != 0 {
		if mappedState, exists := common.MySQLState[uint16(condition.mysqlErrno)]; exists {
			condition.sqlState = mappedState
		}
	}
	return condition
}

func inferStoredRoutineMySQLErrorNumber(err error) int {
	if err == nil {
		return 0
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "duplicate entry") || strings.Contains(lower, "duplicate key"):
		return common.ErrDupEntry
	case strings.Contains(lower, "more than one row"):
		return common.ErrTooManyRows
	case strings.Contains(lower, "unknown column") || strings.Contains(lower, "bad field"):
		return common.ErrBadField
	case strings.Contains(lower, "doesn't exist") || strings.Contains(lower, "does not exist"):
		return common.ErrNoSuchTable
	default:
		return 0
	}
}

func (e *XMySQLExecutor) executeStoredRoutineCursorQuery(ctx *ExecutionContext, databaseName, query string) ([]map[string]interface{}, int, error) {
	results := make(chan *Result, 8)
	nestedCtx := &ExecutionContext{Context: ctx.Context, Results: results, Cfg: ctx.Cfg, DatabaseName: databaseName, RawQuery: strings.TrimSpace(query), Session: ctx.Session}
	go e.executeQuery(nestedCtx, ctx.Session, strings.TrimSpace(query), databaseName, results)
	for result := range results {
		if result == nil {
			continue
		}
		if result.Err != nil {
			return nil, 0, result.Err
		}
		selected, ok := result.Data.(*SelectResult)
		if !ok {
			continue
		}
		rows := make([]map[string]interface{}, 0, len(selected.Records))
		for _, record := range selected.Records {
			row := make(map[string]interface{}, len(selected.Columns)*2)
			for index, value := range record.GetValues() {
				converted := basicValueInterface(value)
				row[fmt.Sprintf("__column_%d", index)] = converted
				if index < len(selected.Columns) {
					row[strings.ToLower(strings.Trim(selected.Columns[index], "`"))] = converted
				}
			}
			rows = append(rows, row)
		}
		return rows, len(selected.Columns), nil
	}
	return nil, 0, nil
}

func (e *XMySQLExecutor) executeStoredRoutineSelectInto(ctx *ExecutionContext, databaseName, projection, targetsText, fromClause string, state *storedRoutineState) (bool, error) {
	if ctx == nil {
		return true, fmt.Errorf("routine SELECT INTO requires an execution context")
	}
	query := "select " + strings.TrimSpace(projection) + " from " + substituteStoredRoutineValues(strings.TrimSpace(fromClause), state.vars)
	if ctx.Session != nil {
		query = e.rewriteSessionUserVariables(query, ctx.Session)
	}
	results := make(chan *Result, 8)
	nestedCtx := &ExecutionContext{Context: ctx.Context, Results: results, Cfg: ctx.Cfg, DatabaseName: databaseName, RawQuery: query, Session: ctx.Session}
	go e.executeQuery(nestedCtx, ctx.Session, query, databaseName, results)
	var selected *SelectResult
	for result := range results {
		if result == nil {
			continue
		}
		if result.Err != nil {
			return true, result.Err
		}
		if candidate, ok := result.Data.(*SelectResult); ok {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return true, fmt.Errorf("routine SELECT INTO did not produce a result set")
	}
	if len(selected.Records) == 0 {
		if handler := findStoredRoutineHandler(state, "not found", nil); handler != nil {
			if err := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state); err != nil {
				return true, err
			}
			if handler.action == "exit" {
				return true, routineHandlerExitSignal(handler)
			}
			return true, nil
		}
		if len(state.notFound) > 0 {
			if err := e.executeStoredRoutineStatements(ctx, databaseName, state.notFound, state); err != nil {
				return true, err
			}
			if state.notFoundKind == "exit" {
				return true, &routineControlSignal{kind: "handler_exit", targetDepth: state.scopeDepth}
			}
			return true, nil
		}
		return true, fmt.Errorf("routine SELECT INTO returned no rows")
	}
	if len(selected.Records) > 1 {
		conditionErr := fmt.Errorf("routine SELECT INTO returned more than one row")
		if handler := findStoredRoutineHandler(state, "sqlexception", conditionErr); handler != nil {
			previousCondition := state.activeCondition
			state.activeCondition = storedRoutineConditionFromErrorWithState("21000", conditionErr)
			handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, handler.statements, state)
			state.activeCondition = previousCondition
			if handlerErr != nil {
				return true, handlerErr
			}
			if handler.action == "exit" {
				return true, routineHandlerExitSignal(handler)
			}
			return true, nil
		}
		if len(state.sqlException) > 0 {
			previousCondition := state.activeCondition
			state.activeCondition = storedRoutineConditionFromErrorWithState("21000", conditionErr)
			handlerErr := e.executeStoredRoutineStatements(ctx, databaseName, state.sqlException, state)
			state.activeCondition = previousCondition
			if handlerErr != nil {
				return true, handlerErr
			}
			if state.sqlExceptionKind == "exit" {
				return true, &routineControlSignal{kind: "handler_exit", targetDepth: state.scopeDepth}
			}
			return true, nil
		}
		return true, conditionErr
	}
	targets := splitTopLevelComma(targetsText)
	values := selected.Records[0].GetValues()
	if len(targets) != len(values) {
		return true, fmt.Errorf("routine SELECT INTO target count %d does not match result count %d", len(targets), len(values))
	}
	for index, targetText := range targets {
		target := strings.Trim(strings.TrimSpace(targetText), "` ")
		value := basicValueInterface(values[index])
		if strings.HasPrefix(target, "@") {
			if ctx.Session == nil {
				return true, fmt.Errorf("routine SELECT INTO user variable requires a session")
			}
			name := strings.TrimPrefix(target, "@")
			ctx.Session.SetParamByName(target, value)
			ctx.Session.SetParamByName(name, value)
			continue
		}
		state.vars[strings.ToLower(target)] = sessionUserVariableSQLLiteral(value)
	}
	return true, nil
}

func parseRoutineDeclare(statement string) (string, string, error) {
	match := regexp.MustCompile(`(?is)^declare\s+([a-zA-Z0-9_$]+)\s+.+?(?:\s+default\s+(.+))?$`).FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) != 3 {
		return "", "", fmt.Errorf("invalid routine DECLARE statement: %s", statement)
	}
	value := "NULL"
	if strings.TrimSpace(match[2]) != "" {
		value = strings.TrimSpace(match[2])
	}
	return strings.ToLower(strings.Trim(match[1], "` ")), value, nil
}

func evaluateStoredRoutineScalarExpression(expression string) (interface{}, error) {
	statement, err := sqlparser.Parse("select " + strings.TrimSpace(expression))
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStatement.SelectExprs) != 1 {
		return nil, fmt.Errorf("routine expression is not scalar")
	}
	aliased, ok := selectStatement.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("routine expression is not evaluable")
	}
	return evaluateExpressionWithRow(aliased.Expr, map[string]interface{}{})
}

func evaluateRoutineCondition(condition string) (bool, error) {
	statement, err := sqlparser.Parse("select 1 where " + strings.TrimSpace(condition))
	if err != nil {
		return false, err
	}
	selectStmt, ok := statement.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return false, fmt.Errorf("invalid routine IF condition")
	}
	return evalPredicate(selectStmt.Where.Expr, map[string]interface{}{})
}

// executeStoredFunctionCall handles the scalar stored-function path used by
// SELECT expressions. It intentionally reuses the normal SELECT evaluator for
// the resolved RETURN expression, so arithmetic, literals and built-in
// functions keep one evaluation contract.
func (e *XMySQLExecutor) executeStoredFunctionCall(ctx *ExecutionContext, query, databaseName string) bool {
	match := regexp.MustCompile(`(?is)^select\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s*\((.*)\)\s*(?:as\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `)?\s*$`).FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) == 0 {
		return false
	}
	startedAt := time.Now()
	functionSchema, functionName := databaseName, strings.Trim(match[1], "`")
	if strings.Contains(functionName, ".") {
		parts := strings.SplitN(functionName, ".", 2)
		functionSchema, functionName = parts[0], parts[1]
	}
	releaseObjectLock, lockErr := e.acquireStoredObjectReadLock(ctx, ctx.Session, functionSchema, functionName)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	defer releaseObjectLock()
	raw, err := os.ReadFile(e.storedObjectPath(functionSchema, functionName, "function"))
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if object.Definer != "" {
		if err := e.validateStoredObjectAccount(object.Definer); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if err := e.checkStoredRoutineExecute(ctx, object.Schema, object.Name); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	expression, err := resolveStoredFunctionReturnExpression(object, splitTopLevelComma(match[2]))
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	value, err := e.evaluateStoredFunctionExpression(ctx, functionSchema, expression)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	column := functionName
	if strings.TrimSpace(match[3]) != "" {
		column = strings.Trim(match[3], "`")
	}
	result := newInformationSchemaSelectResult("stored_function", []string{column}, [][]interface{}{{value}})
	timerWait := time.Since(startedAt).Nanoseconds() * 1000
	if timerWait <= 0 {
		timerWait = 1000
	}
	e.recordPerformanceSchemaProgramExecution("FUNCTION", functionSchema, functionName, timerWait, 1, timerWait, timerWait, timerWait, 0, 0, 0, 1)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: "stored function evaluated"}
	return true
}

func resolveStoredFunctionReturnExpression(object persistedStoredObject, arguments []string) (string, error) {
	values := make(map[string]string, len(object.Parameters))
	for i, parameter := range object.Parameters {
		if i >= len(arguments) {
			break
		}
		fields := strings.Fields(strings.TrimSpace(parameter))
		if len(fields) == 0 {
			continue
		}
		nameIndex := 0
		if len(fields) >= 3 && (strings.EqualFold(fields[0], "in") || strings.EqualFold(fields[0], "out") || strings.EqualFold(fields[0], "inout")) {
			nameIndex = 1
		} else if len(fields) >= 2 {
			nameIndex = len(fields) - 2
		}
		values[strings.Trim(fields[nameIndex], "`")] = strings.TrimSpace(arguments[i])
	}
	body := triggerBody(object.Definition)
	returnExpression := strings.TrimSpace(object.ReturnExpression)
	for _, statement := range splitStoredObjectStatements(body) {
		statement = strings.TrimSpace(statement)
		setMatch := regexp.MustCompile("(?is)^set\\s+`?([a-zA-Z0-9_$]+)`?\\s*=\\s*(.+)$").FindStringSubmatch(statement)
		if len(setMatch) == 3 {
			values[strings.Trim(setMatch[1], "`")] = substituteStoredRoutineValues(strings.TrimSpace(setMatch[2]), values)
			continue
		}
		returnMatch := regexp.MustCompile(`(?is)^return\s+(.+)$`).FindStringSubmatch(statement)
		if len(returnMatch) == 2 {
			returnExpression = strings.TrimSpace(returnMatch[1])
			break
		}
	}
	if returnExpression == "" {
		return "", fmt.Errorf("stored function %s.%s has no RETURN expression", object.Schema, object.Name)
	}
	return substituteStoredRoutineValues(returnExpression, values), nil
}

func substituteStoredRoutineValues(expression string, values map[string]string) string {
	for name, value := range values {
		expression = regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(name)+`\b`).ReplaceAllString(expression, "("+value+")")
	}
	return expression
}

func (e *XMySQLExecutor) evaluateStoredFunctionExpression(parent *ExecutionContext, databaseName, expression string) (interface{}, error) {
	_ = parent
	_ = databaseName
	statement, err := sqlparser.Parse("select " + expression)
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*sqlparser.Select)
	if !ok || len(selectStatement.SelectExprs) != 1 {
		return nil, fmt.Errorf("stored function RETURN expression is not scalar")
	}
	aliased, ok := selectStatement.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("stored function RETURN expression is not evaluable")
	}
	return evaluateExpressionWithRow(aliased.Expr, map[string]interface{}{})
}

func storedObjectDefiner(query string) string {
	match := regexp.MustCompile(`(?is)definer\s*=\s*'([^']*)'\s*@\s*'([^']*)'`).FindStringSubmatch(query)
	if len(match) != 3 {
		return ""
	}
	return match[1] + "@" + match[2]
}

func (e *XMySQLExecutor) validateStoredObjectAccount(definer string) error {
	parts := strings.SplitN(definer, "@", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid stored object definer %q", definer)
	}
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return err
	}
	for _, account := range file.Accounts {
		if account.User == parts[0] && (account.Host == parts[1] || account.Host == "%") {
			return nil
		}
	}
	return fmt.Errorf("stored object definer account %q does not exist", definer)
}

func (e *XMySQLExecutor) checkStoredRoutineExecute(ctx *ExecutionContext, schema, routine string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	if strings.TrimSpace(user) == "" {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if host == "" {
		host = "localhost"
	}
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return err
	}
	for _, account := range file.Accounts {
		if account.User != user || (account.Host != host && account.Host != "%") {
			continue
		}
		activeRoles := []string(nil)
		explicitRoles := false
		if raw := ctx.Session.GetParamByName("active_roles"); raw != nil {
			if roles, ok := raw.([]string); ok {
				activeRoles = append([]string(nil), roles...)
				explicitRoles = true
			}
		}
		if !explicitRoles {
			activeRoles = append([]string(nil), account.DefaultRoles...)
		}
		if storedRoutinePrivilegeFromAccount(file, account, schema, routine, activeRoles) {
			return nil
		}
		return fmt.Errorf("access denied: user '%s'@'%s' lacks EXECUTE privilege on routine '%s.%s'", user, host, schema, routine)
	}
	return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
}

func storedRoutinePrivilegeFromAccount(file persistedAccountFile, account persistedAccount, schema, routine string, activeRoles []string) bool {
	visited := make(map[string]struct{})
	var hasPrivilege func(persistedAccount, int) bool
	hasPrivilege = func(candidate persistedAccount, depth int) bool {
		if depth > 16 {
			return false
		}
		for _, scope := range []string{"*.*", schema + ".*", schema + "." + routine} {
			for _, privilege := range candidate.Grants[scope] {
				if strings.EqualFold(privilege, "EXECUTE") || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
					return true
				}
			}
		}
		roles := candidate.Roles
		if candidate.User == account.User && candidate.Host == account.Host {
			roles = activeRoles
		}
		for _, role := range roles {
			parts := strings.SplitN(strings.TrimSpace(role), "@", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.ToLower(parts[0] + "@" + parts[1])
			if _, seen := visited[key]; seen {
				continue
			}
			visited[key] = struct{}{}
			for _, roleAccount := range file.Accounts {
				if strings.EqualFold(roleAccount.User, parts[0]) && strings.EqualFold(roleAccount.Host, parts[1]) {
					if hasPrivilege(roleAccount, depth+1) {
						return true
					}
					break
				}
			}
		}
		return false
	}
	return hasPrivilege(account, 0)
}

func parseRoutineParameterNames(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return splitTopLevelComma(raw)
}

func parseRoutineReturnType(objectType, definition string) string {
	if !strings.EqualFold(objectType, "function") {
		return ""
	}
	match := regexp.MustCompile(`(?is)\breturns\s+([a-zA-Z0-9_()]+)`).FindStringSubmatch(definition)
	if len(match) == 2 {
		return strings.ToUpper(strings.TrimSpace(match[1]))
	}
	return ""
}

func parseRoutineLocalVariables(definition string) []string {
	matches := regexp.MustCompile(`(?is)\bdeclare\s+([a-zA-Z0-9_$]+)\s+([^;]+)`).FindAllStringSubmatch(definition, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 3 {
			result = append(result, strings.TrimSpace(match[1])+" "+strings.TrimSpace(match[2]))
		}
	}
	return result
}

func parseRoutineReturnExpression(objectType, definition string) string {
	if !strings.EqualFold(objectType, "function") {
		return ""
	}
	match := regexp.MustCompile(`(?is)\breturn\s+(.+?)(?:;|\bend\b)`).FindStringSubmatch(definition)
	if len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	return ""
}

func splitStoredObjectStatements(body string) []string {
	statements := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if quote != 0 {
			if ch == quote && (i == 0 || body[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ';':
			if depth == 0 {
				if value := strings.TrimSpace(body[start:i]); value != "" {
					statements = append(statements, value)
				}
				start = i + 1
			}
		}
	}
	if value := strings.TrimSpace(body[start:]); value != "" {
		statements = append(statements, value)
	}
	return statements
}

func parseStoredObjectName(objectType, rawName, databaseName string) (string, string, string) {
	parts := strings.Split(rawName, ".")
	for i := range parts {
		parts[i] = strings.Trim(strings.TrimSpace(parts[i]), "`")
	}
	if len(parts) == 2 {
		return strings.ToLower(objectType), parts[0], parts[1]
	}
	return strings.ToLower(objectType), databaseName, parts[0]
}

func (e *XMySQLExecutor) executeShowCreateStoredObject(ctx *ExecutionContext, query, databaseName string) bool {
	match := regexp.MustCompile(`(?is)^show\s+create\s+(procedure|function|trigger|event)\s+` + "`?" + `([^\s.]+)` + "`?" + `(?:\s*\.\s*` + "`?" + `([^\s.]+)` + "`?" + `)?\s*$`).FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if len(match) == 0 {
		return false
	}
	objectType, schema, name := match[1], databaseName, match[2]
	if match[3] != "" {
		schema, name = match[2], match[3]
	}
	releaseObjectLock, lockErr := e.acquireStoredObjectReadLock(ctx, ctx.Session, schema, name)
	if lockErr != nil {
		ctx.Results <- &Result{Err: tableLockWaitError(lockErr), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	defer releaseObjectLock()
	raw, err := os.ReadFile(e.storedObjectPath(schema, name, objectType))
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if strings.EqualFold(objectType, "procedure") || strings.EqualFold(objectType, "function") {
		if checkErr := e.checkStoredRoutineShow(ctx); checkErr != nil {
			ctx.Results <- &Result{Err: checkErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if strings.EqualFold(objectType, "event") {
		if checkErr := e.checkStoredEventPrivilege(ctx, object.Schema); checkErr != nil {
			ctx.Results <- &Result{Err: checkErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	if strings.EqualFold(objectType, "trigger") {
		_, _, triggerTable, _ := parseTriggerInformationSchemaMetadata(object.Definition)
		if checkErr := e.checkStoredTriggerPrivilege(ctx, object.Schema, triggerTable); checkErr != nil {
			ctx.Results <- &Result{Err: checkErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}
	columns, row := showCreateStoredObjectRow(ctx, e, object)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("stored_object", columns, [][]interface{}{row}), Message: "stored object definition returned"}
	return true
}

func showCreateStoredObjectRow(ctx *ExecutionContext, e *XMySQLExecutor, object persistedStoredObject) ([]string, []interface{}) {
	sqlMode := ""
	timeZone := "SYSTEM"
	if ctx != nil && ctx.Session != nil {
		if value, ok := ctx.Session.GetParamByName("sql_mode").(string); ok {
			sqlMode = value
		}
		if value, ok := ctx.Session.GetParamByName("time_zone").(string); ok && strings.TrimSpace(value) != "" {
			timeZone = value
		}
	}
	charset := "utf8mb4"
	connectionCollation := "utf8mb4_0900_ai_ci"
	databaseCollation := connectionCollation
	if strings.EqualFold(object.ObjectType, "trigger") {
		_, _, tableName, _ := parseTriggerInformationSchemaMetadata(object.Definition)
		if _, collation, _ := persistedTableDisplayOptions(e, object.Schema, tableName); strings.TrimSpace(collation) != "" {
			databaseCollation = collation
		}
	}
	switch strings.ToLower(object.ObjectType) {
	case "procedure", "function":
		kind := strings.Title(strings.ToLower(object.ObjectType))
		return []string{kind, "sql_mode", "Create " + kind, "character_set_client", "collation_connection", "Database Collation"}, []interface{}{
			object.Name, sqlMode, object.Definition, charset, connectionCollation, databaseCollation,
		}
	case "trigger":
		return []string{"Trigger", "sql_mode", "SQL Original Statement", "character_set_client", "collation_connection", "Database Collation"}, []interface{}{
			object.Name, sqlMode, object.Definition, charset, connectionCollation, databaseCollation,
		}
	case "event":
		return []string{"Event", "sql_mode", "time_zone", "Create Event", "character_set_client", "collation_connection", "Database Collation"}, []interface{}{
			object.Name, sqlMode, timeZone, object.Definition, charset, connectionCollation, databaseCollation,
		}
	default:
		kind := strings.Title(strings.ToLower(object.ObjectType))
		return []string{object.ObjectType, "Create " + kind}, []interface{}{object.Name, object.Definition}
	}
}

func (e *XMySQLExecutor) checkStoredRoutineShow(ctx *ExecutionContext) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	if strings.TrimSpace(user) == "" {
		return nil
	}
	// The isolated JDBC compatibility server may start before its persisted
	// mysql system account is materialized. Keep the bootstrap root session
	// administrative for metadata discovery, matching the existing root
	// behavior used by the rest of the executor.
	if strings.EqualFold(strings.TrimSpace(user), "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	for scope, privileges := range grants {
		if !scopeCovers("*.*", scope) {
			continue
		}
		for _, privilege := range privileges {
			if strings.EqualFold(privilege, "SHOW_ROUTINE") || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
				return nil
			}
		}
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks SHOW_ROUTINE dynamic privilege", user, host)
}

func (e *XMySQLExecutor) checkStoredEventPrivilege(ctx *ExecutionContext, schema string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, strings.TrimSpace(schema)+".*", "EVENT") {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks EVENT privilege on database '%s'", user, host, schema)
}

func (e *XMySQLExecutor) checkStoredTriggerPrivilege(ctx *ExecutionContext, schema string, table ...string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	if len(table) > 0 && strings.TrimSpace(table[0]) != "" {
		return e.checkTablePrivilege(ctx, schema, strings.TrimSpace(table[0]), "TRIGGER")
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, strings.TrimSpace(schema)+".*", "TRIGGER") {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks TRIGGER privilege on database '%s'", user, host, schema)
}

func (e *XMySQLExecutor) checkStoredTriggerObjectPrivilege(ctx *ExecutionContext, schema, name string) error {
	path := e.storedObjectPath(schema, name, "trigger")
	raw, err := os.ReadFile(path)
	if err == nil {
		var object persistedStoredObject
		if json.Unmarshal(raw, &object) == nil {
			_, _, table, _ := parseTriggerInformationSchemaMetadata(object.Definition)
			if strings.TrimSpace(table) != "" {
				return e.checkStoredTriggerPrivilege(ctx, schema, table)
			}
		}
	}
	return e.checkStoredTriggerPrivilege(ctx, schema)
}

func (e *XMySQLExecutor) checkStoredRoutineDDLPrivilege(ctx *ExecutionContext, schema, privilege string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, strings.TrimSpace(schema)+".*", privilege) {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege on database '%s'", user, host, privilege, schema)
}

func (e *XMySQLExecutor) checkDatabasePrivilege(ctx *ExecutionContext, schema, privilege string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, strings.TrimSpace(schema)+".*", privilege) {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege on database '%s'", user, host, privilege, schema)
}

func (e *XMySQLExecutor) checkTablePrivilege(ctx *ExecutionContext, schema, table, privilege string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if _, temporary := temporaryPhysicalTableName(ctx.Session, schema, table); temporary {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(table)), "__xmysql_tmp_") {
		if _, temporary := temporaryMetadataTableName(ctx.Session, schema, table); temporary {
			return nil
		}
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	requested := strings.TrimSpace(schema) + "." + strings.TrimSpace(table)
	if grantsContain(effectiveAccountGrants(file, *account, ctx.Session), requested, privilege) {
		return nil
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege on table '%s'", user, host, privilege, requested)
}

func (e *XMySQLExecutor) checkTableHasAnyPrivilege(ctx *ExecutionContext, schema, table string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if _, temporary := temporaryPhysicalTableName(ctx.Session, schema, table); temporary {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(table)), "__xmysql_tmp_") {
		if _, temporary := temporaryMetadataTableName(ctx.Session, schema, table); temporary {
			return nil
		}
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	host, _ := ctx.Session.GetParamByName("host").(string)
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("access denied: user '%s'@'%s' does not exist", user, host)
	}
	requested := strings.TrimSpace(schema) + "." + strings.TrimSpace(table)
	for grantedScope, privileges := range effectiveAccountGrants(file, *account, ctx.Session) {
		if !scopeCovers(requested, grantedScope) {
			continue
		}
		for _, privilege := range privileges {
			privilege = strings.TrimSpace(privilege)
			if privilege != "" && !strings.EqualFold(privilege, "GRANT OPTION") && !strings.EqualFold(privilege, "USAGE") {
				return nil
			}
		}
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks a privilege on table '%s'", user, host, requested)
}

func (e *XMySQLExecutor) checkAlterTablePrivileges(ctx *ExecutionContext, schema, table string) error {
	for _, privilege := range []string{"ALTER", "CREATE", "INSERT"} {
		if err := e.checkTablePrivilege(ctx, schema, table, privilege); err != nil {
			return err
		}
	}
	return nil
}

func (e *XMySQLExecutor) checkRenameTablePrivileges(ctx *ExecutionContext, pairs []renameTablePair) error {
	for _, pair := range pairs {
		if err := e.checkTablePrivilege(ctx, pair.sourceDB, pair.sourceTable, "ALTER"); err != nil {
			return err
		}
		if err := e.checkTablePrivilege(ctx, pair.sourceDB, pair.sourceTable, "DROP"); err != nil {
			return err
		}
		if err := e.checkTablePrivilege(ctx, pair.destinationDB, pair.destinationTbl, "CREATE"); err != nil {
			return err
		}
		if err := e.checkTablePrivilege(ctx, pair.destinationDB, pair.destinationTbl, "INSERT"); err != nil {
			return err
		}
	}
	return nil
}

func (e *XMySQLExecutor) scanStoredObjects(objectType string) []persistedStoredObject {
	suffix := storedObjectSuffix(objectType)
	if suffix == "" {
		return nil
	}
	objects := make([]persistedStoredObject, 0)
	entries, _ := os.ReadDir(e.getDataDir())
	for _, schemaEntry := range entries {
		if !schemaEntry.IsDir() {
			continue
		}
		files, _ := os.ReadDir(filepath.Join(e.getDataDir(), schemaEntry.Name()))
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), suffix) {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(e.getDataDir(), schemaEntry.Name(), file.Name()))
			if err != nil {
				continue
			}
			var object persistedStoredObject
			if json.Unmarshal(raw, &object) == nil && strings.EqualFold(object.ObjectType, objectType) {
				objects = append(objects, object)
			}
		}
	}
	return objects
}

func (e *XMySQLExecutor) findStoredObjectTableDependency(schema, table string) string {
	needle := regexp.MustCompile(`(?i)(?:^|[^a-z0-9_$])` + regexp.QuoteMeta(table) + `(?:[^a-z0-9_$]|$)`)
	viewPath := filepath.Join(e.getDataDir(), schema)
	entries, _ := os.ReadDir(viewPath)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".view.json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(viewPath, entry.Name()))
		if err != nil {
			continue
		}
		var info struct {
			Definition string `json:"definition"`
			ViewName   string `json:"view_name"`
		}
		if json.Unmarshal(raw, &info) == nil && needle.MatchString(info.Definition) {
			return fmt.Sprintf("view %s.%s", schema, info.ViewName)
		}
	}
	for _, objectType := range []string{"procedure", "function", "event"} {
		for _, object := range e.scanStoredObjects(objectType) {
			if !strings.EqualFold(object.Schema, schema) || !needle.MatchString(object.Definition) {
				continue
			}
			return fmt.Sprintf("%s %s.%s", objectType, schema, object.Name)
		}
	}
	return ""
}

func (e *XMySQLExecutor) executeInformationSchemaRoutinesSelect(query string, session server.MySQLServerSession) (*SelectResult, error) {
	lockCtx := &ExecutionContext{Context: context.Background(), Session: session}
	objects := append(e.scanStoredObjects("procedure"), e.scanStoredObjects("function")...)
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(lockCtx, session, objects, "")
	if lockErr != nil {
		return nil, lockErr
	}
	defer releaseObjectLocks()
	columns := jdbcRoutinesMetadataColumns(query)
	lowerQuery := strings.ToLower(query)
	native := informationSchemaSelectsAllColumns(query) ||
		(informationSchemaSelectListContainsAny(query,
			"routine_schema", "routine_catalog", "security_type", "is_deterministic", "sql_data_access",
			"external_name", "external_language", "parameter_style", "created", "last_altered", "sql_mode",
			"character_set_client", "collation_connection", "database_collation") &&
			!strings.Contains(lowerQuery, " as procedure_") && !strings.Contains(lowerQuery, " as function_"))
	if native {
		columns = requestedInformationSchemaColumns(query, []string{
			"SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE",
			"DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION",
			"NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME",
			"DTD_IDENTIFIER", "ROUTINE_BODY", "ROUTINE_DEFINITION", "EXTERNAL_NAME", "EXTERNAL_LANGUAGE",
			"PARAMETER_STYLE", "IS_DETERMINISTIC", "SQL_DATA_ACCESS", "SQL_PATH", "SECURITY_TYPE",
			"CREATED", "LAST_ALTERED", "SQL_MODE", "ROUTINE_COMMENT", "DEFINER", "CHARACTER_SET_CLIENT",
			"COLLATION_CONNECTION", "DATABASE_COLLATION",
		})
	}
	nativeFilters := map[string]string{}
	if native {
		filterPattern := regexp.MustCompile(`(?i)\b(routine_schema|routine_name|routine_type)\b\s*(?:=|like)\s*(?:'([^']*)'|"([^"]*)")`)
		for _, match := range filterPattern.FindAllStringSubmatch(query, -1) {
			value := match[2]
			if value == "" {
				value = match[3]
			}
			nativeFilters[strings.ToLower(match[1])] = value
		}
	}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		if native {
			if !metadataFilterMatches(object.Schema, nativeFilters["routine_schema"]) || !metadataFilterMatches(object.Name, nativeFilters["routine_name"]) ||
				(strings.TrimSpace(nativeFilters["routine_type"]) != "" && !strings.EqualFold(object.ObjectType, nativeFilters["routine_type"])) {
				continue
			}
		}
		if native {
			dataType := interface{}(nil)
			if strings.EqualFold(object.ObjectType, "function") {
				dataType = strings.ToUpper(object.ReturnType)
			}
			values := map[string]interface{}{
				"SPECIFIC_NAME": object.Name, "ROUTINE_CATALOG": "def", "ROUTINE_SCHEMA": object.Schema, "ROUTINE_NAME": object.Name,
				"ROUTINE_TYPE": strings.ToUpper(object.ObjectType), "DATA_TYPE": dataType,
				"CHARACTER_MAXIMUM_LENGTH": nil, "CHARACTER_OCTET_LENGTH": nil, "NUMERIC_PRECISION": nil,
				"NUMERIC_SCALE": nil, "DATETIME_PRECISION": nil, "CHARACTER_SET_NAME": nil, "COLLATION_NAME": nil,
				"DTD_IDENTIFIER": dataType, "ROUTINE_BODY": "SQL", "ROUTINE_DEFINITION": object.Definition,
				"EXTERNAL_NAME": nil, "EXTERNAL_LANGUAGE": nil, "PARAMETER_STYLE": "SQL", "IS_DETERMINISTIC": boolToYesNo(routineIsDeterministic(object.Definition)),
				"SQL_DATA_ACCESS": routineSQLDataAccess(object.Definition), "SQL_PATH": nil, "SECURITY_TYPE": routineSecurityType(object.SQLSecurity),
				"CREATED": nil, "LAST_ALTERED": nil, "SQL_MODE": "", "ROUTINE_COMMENT": object.RoutineComment, "DEFINER": object.Definer,
				"CHARACTER_SET_CLIENT": "utf8mb4", "COLLATION_CONNECTION": "utf8mb4_general_ci", "DATABASE_COLLATION": "utf8mb4_general_ci",
			}
			if strings.TrimSpace(fmt.Sprintf("%v", values["DEFINER"])) == "" {
				values["DEFINER"] = "root@localhost"
			}
			if performanceSchemaLockValuesMatch(query, values) {
				rows = append(rows, projectInformationSchemaRow(columns, values))
			}
			continue
		}
		values := map[string]interface{}{
			"PROCEDURE_CAT": nil, "PROCEDURE_SCHEM": object.Schema, "PROCEDURE_NAME": object.Name,
			"FUNCTION_CAT": nil, "FUNCTION_SCHEM": object.Schema, "FUNCTION_NAME": object.Name,
			"REMARKS": "", "FUNCTION_TYPE": int64(1), "PROCEDURE_TYPE": int64(1), "SPECIFIC_NAME": object.Name,
			"ROUTINE_CATALOG": "def", "ROUTINE_SCHEMA": object.Schema, "ROUTINE_NAME": object.Name,
			"ROUTINE_TYPE": strings.ToUpper(object.ObjectType),
		}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema_routines", columns, rows), nil
}

func routineSecurityType(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "INVOKER") {
		return "INVOKER"
	}
	return "DEFINER"
}

func parseRoutineComment(definition string) string {
	comment := regexp.MustCompile(`(?is)\bcomment\s+(?:'((?:''|[^'])*)'|"((?:""|[^"])*)")`).FindStringSubmatch(definition)
	if len(comment) != 3 {
		return ""
	}
	value := comment[1]
	if value == "" {
		value = comment[2]
	}
	value = strings.ReplaceAll(value, "''", "'")
	return strings.ReplaceAll(value, "\"\"", "\"")
}

func routineIsDeterministic(definition string) bool {
	lower := strings.ToLower(definition)
	if regexp.MustCompile(`\bnot\s+deterministic\b`).MatchString(lower) {
		return false
	}
	return regexp.MustCompile(`\bdeterministic\b`).MatchString(lower)
}

func routineSQLDataAccess(definition string) string {
	lower := strings.ToLower(definition)
	switch {
	case regexp.MustCompile(`\bmodifies\s+sql\s+data\b`).MatchString(lower):
		return "MODIFIES SQL DATA"
	case regexp.MustCompile(`\breads\s+sql\s+data\b`).MatchString(lower):
		return "READS SQL DATA"
	case regexp.MustCompile(`\bno\s+sql\b`).MatchString(lower):
		return "NO SQL"
	default:
		return "CONTAINS SQL"
	}
}

func boolToYesNo(value bool) string {
	if value {
		return "YES"
	}
	return "NO"
}

func (e *XMySQLExecutor) executeInformationSchemaTriggersSelect(query string, session server.MySQLServerSession) (*SelectResult, error) {
	lockCtx := &ExecutionContext{Context: context.Background(), Session: session}
	objects := e.scanStoredObjects("trigger")
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(lockCtx, session, objects, "")
	if lockErr != nil {
		return nil, lockErr
	}
	defer releaseObjectLocks()
	native := informationSchemaSelectsAllColumns(query) || informationSchemaSelectListContains(query, "trigger_schema") || informationSchemaSelectListContains(query, "event_object_schema")
	nativeColumns := []string{"TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "ACTION_ORDER", "ACTION_CONDITION", "ACTION_STATEMENT", "ACTION_ORIENTATION", "ACTION_TIMING", "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE", "ACTION_REFERENCE_OLD_ROW", "ACTION_REFERENCE_NEW_ROW", "CREATED", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"}
	columns := requestedInformationSchemaColumns(query, jdbcTriggersMetadataColumns())
	if native {
		columns = requestedInformationSchemaColumns(query, nativeColumns)
	}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		_, _, tableName, _ := parseTriggerInformationSchemaMetadata(object.Definition)
		if err := e.checkStoredTriggerPrivilege(&ExecutionContext{Session: session}, object.Schema, tableName); err != nil {
			// INFORMATION_SCHEMA hides trigger definitions without TRIGGER.
			continue
		}
		timing, event, tableName, action := parseTriggerInformationSchemaMetadata(object.Definition)
		values := map[string]interface{}{"TRIGGER_CATALOG": "def", "TRIGGER_SCHEMA": object.Schema, "TRIGGER_NAME": object.Name, "EVENT_MANIPULATION": event, "EVENT_OBJECT_CATALOG": "def", "EVENT_OBJECT_SCHEMA": object.Schema, "EVENT_OBJECT_TABLE": tableName, "ACTION_ORDER": int64(1), "ACTION_CONDITION": nil, "ACTION_STATEMENT": action, "ACTION_ORIENTATION": "ROW", "ACTION_TIMING": timing, "ACTION_REFERENCE_OLD_TABLE": nil, "ACTION_REFERENCE_NEW_TABLE": nil, "ACTION_REFERENCE_OLD_ROW": "OLD", "ACTION_REFERENCE_NEW_ROW": "NEW", "CREATED": nil, "SQL_MODE": "", "DEFINER": "root@localhost", "CHARACTER_SET_CLIENT": "utf8mb4", "COLLATION_CONNECTION": "utf8mb4_general_ci", "DATABASE_COLLATION": "utf8mb4_general_ci"}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema_triggers", columns, rows), nil
}

func parseTriggerInformationSchemaMetadata(definition string) (timing, event, tableName, action string) {
	match := regexp.MustCompile(`(?is)^create\s+trigger\s+` + "`?" + `[a-zA-Z0-9_$]+` + "`?" + `\s+(before|after)\s+(insert|update|delete)\s+on\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?" + `\s+for\s+each\s+row\s+(.+)$`).FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(definition, ";")))
	if len(match) == 0 {
		return "", "", "", strings.TrimSpace(definition)
	}
	return strings.ToUpper(match[1]), strings.ToUpper(match[2]), match[3], strings.TrimSpace(match[4])
}

func (e *XMySQLExecutor) executeInformationSchemaEventsSelect(query string, session server.MySQLServerSession) (*SelectResult, error) {
	lockCtx := &ExecutionContext{Context: context.Background(), Session: session}
	objects := e.scanStoredObjects("event")
	releaseObjectLocks, lockErr := e.acquireStoredObjectReadLocks(lockCtx, session, objects, "")
	if lockErr != nil {
		return nil, lockErr
	}
	defer releaseObjectLocks()
	native := informationSchemaSelectsAllColumns(query) || informationSchemaSelectListContains(query, "event_schema") || informationSchemaSelectListContains(query, "event_catalog")
	nativeColumns := []string{"EVENT_CATALOG", "EVENT_SCHEMA", "EVENT_NAME", "DEFINER", "TIME_ZONE", "EVENT_BODY", "EVENT_DEFINITION", "EVENT_TYPE", "EXECUTE_AT", "INTERVAL_VALUE", "INTERVAL_FIELD", "SQL_MODE", "STARTS", "ENDS", "STATUS", "ON_COMPLETION", "CREATED", "LAST_ALTERED", "LAST_EXECUTED", "EVENT_COMMENT", "ORIGINATOR", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"}
	columns := requestedInformationSchemaColumns(query, jdbcEventsMetadataColumns())
	if native {
		columns = requestedInformationSchemaColumns(query, nativeColumns)
	}
	rows := make([][]interface{}, 0)
	for _, object := range objects {
		if err := e.checkStoredEventPrivilege(&ExecutionContext{Session: session}, object.Schema); err != nil {
			// INFORMATION_SCHEMA hides event definitions for databases where
			// the session has no EVENT privilege instead of leaking metadata.
			continue
		}
		eventType, executeAt, intervalValue, intervalField := parseEventInformationSchemaMetadata(object.Definition)
		values := map[string]interface{}{"EVENT_CATALOG": "def", "EVENT_SCHEMA": object.Schema, "EVENT_NAME": object.Name, "DEFINER": "root@localhost", "TIME_ZONE": "SYSTEM", "EVENT_BODY": "SQL", "EVENT_DEFINITION": object.Definition, "EVENT_TYPE": eventType, "EXECUTE_AT": executeAt, "INTERVAL_VALUE": intervalValue, "INTERVAL_FIELD": intervalField, "SQL_MODE": "", "STARTS": nil, "ENDS": nil, "STATUS": "ENABLED", "ON_COMPLETION": "NOT PRESERVE", "CREATED": nil, "LAST_ALTERED": nil, "LAST_EXECUTED": nil, "EVENT_COMMENT": "", "ORIGINATOR": int64(0), "CHARACTER_SET_CLIENT": "utf8mb4", "COLLATION_CONNECTION": "utf8mb4_general_ci", "DATABASE_COLLATION": "utf8mb4_general_ci"}
		if performanceSchemaLockValuesMatch(query, values) {
			rows = append(rows, projectInformationSchemaRow(columns, values))
		}
	}
	return newInformationSchemaSelectResult("information_schema_events", columns, rows), nil
}

func parseEventInformationSchemaMetadata(definition string) (eventType string, executeAt, intervalValue, intervalField interface{}) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(definition, ";"))
	if every := regexp.MustCompile(`(?is)\bon\s+schedule\s+every\s+(\d+)\s+(second|minute|hour|day|week|month|year)`).FindStringSubmatch(trimmed); len(every) == 3 {
		return "RECURRING", nil, every[1], strings.ToUpper(every[2])
	}
	if at := regexp.MustCompile(`(?is)\bon\s+schedule\s+at\s+'([^']+)'`).FindStringSubmatch(trimmed); len(at) == 2 {
		return "ONE TIME", at[1], nil, nil
	}
	return "ONE TIME", nil, nil, nil
}
