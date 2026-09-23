package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// applyBeforeTriggers implements the safe, deterministic subset needed by
// common audit/default triggers: SET NEW.column = literal or another NEW
// column. It runs before constraints and therefore participates in the same
// DML rollback boundary.
func applyBeforeTriggers(dataDir, schemaName, tableName, event string, row map[string]interface{}, observe func(string, string, string, int64, error)) error {
	for _, object := range loadTableTriggers(dataDir, schemaName, tableName, event, "before") {
		startedAt := time.Now()
		recordExecution := func(triggerErr error) {
			if observe == nil {
				return
			}
			timerWait := time.Since(startedAt).Nanoseconds() * 1000
			if timerWait <= 0 {
				timerWait = 1000
			}
			observe(schemaName, tableName, object.Name, timerWait, triggerErr)
		}
		definition := object.Definition
		if len(definition) == 0 {
			recordExecution(nil)
			continue
		}
		setMatches := regexp.MustCompile(`(?is)\bset\s+new\.([a-zA-Z0-9_$]+)\s*=\s*([^;]+)`).FindAllStringSubmatch(definition, -1)
		if len(setMatches) == 0 {
			recordExecution(nil)
			continue
		}
		for _, setMatch := range setMatches {
			if len(setMatch) != 3 {
				continue
			}
			value, err := triggerLiteralValue(strings.TrimSpace(setMatch[2]), row)
			if err != nil {
				triggerErr := fmt.Errorf("trigger %s: %w", object.Name, err)
				recordExecution(triggerErr)
				return triggerErr
			}
			row[setMatch[1]] = value
		}
		recordExecution(nil)
	}
	return nil
}

// applyAfterTriggers is the post-write hook. AFTER triggers cannot mutate NEW,
// but they may execute DML side effects such as an audit INSERT.
func applyAfterTriggers(dataDir, schemaName, tableName, event string, newRow, oldRow map[string]interface{}, execute func(string, string, string, string) error, beginAtomic func(string, string, string) (func(bool), error)) error {
	for _, object := range loadTableTriggers(dataDir, schemaName, tableName, event, "after") {
		definition := object.Definition
		if len(definition) == 0 {
			continue
		}
		if regexp.MustCompile(`(?is)\bset\s+new\.`).MatchString(definition) {
			return fmt.Errorf("trigger %s cannot update NEW in an AFTER trigger", object.Name)
		}
		if execute == nil {
			return fmt.Errorf("trigger %s requires an executor for AFTER side effects", object.Name)
		}
		finishAtomic := func(bool) {}
		if beginAtomic != nil {
			var err error
			finishAtomic, err = beginAtomic(schemaName, tableName, object.Name)
			if err != nil {
				return fmt.Errorf("trigger %s atomic execution setup failed: %w", object.Name, err)
			}
		}
		body := triggerBody(definition)
		newArguments := make(map[string]string, len(newRow))
		for name, value := range newRow {
			newArguments[strings.ToLower(name)] = triggerSQLLiteral(value)
		}
		oldArguments := make(map[string]string, len(oldRow))
		for name, value := range oldRow {
			oldArguments[strings.ToLower(name)] = triggerSQLLiteral(value)
		}
		for _, statement := range splitStoredObjectStatements(body) {
			statement = strings.TrimSpace(statement)
			if statement == "" || strings.HasPrefix(strings.ToLower(statement), "set @") {
				continue
			}
			lowerStatement := strings.ToLower(statement)
			if !strings.HasPrefix(lowerStatement, "insert ") && !strings.HasPrefix(lowerStatement, "insert\n") &&
				!strings.HasPrefix(lowerStatement, "update ") && !strings.HasPrefix(lowerStatement, "update\n") &&
				!strings.HasPrefix(lowerStatement, "delete ") && !strings.HasPrefix(lowerStatement, "delete\n") &&
				!strings.HasPrefix(lowerStatement, "replace ") && !strings.HasPrefix(lowerStatement, "replace\n") {
				finishAtomic(false)
				return fmt.Errorf("trigger %s has unsupported AFTER statement %q", object.Name, statement)
			}
			for name, literal := range newArguments {
				statement = regexp.MustCompile(`(?i)\bnew\.`+regexp.QuoteMeta(name)+`\b`).ReplaceAllString(statement, literal)
			}
			for name, literal := range oldArguments {
				statement = regexp.MustCompile(`(?i)\bold\.`+regexp.QuoteMeta(name)+`\b`).ReplaceAllString(statement, literal)
			}
			if err := execute(schemaName, statement, tableName, object.Name); err != nil {
				finishAtomic(false)
				return fmt.Errorf("trigger %s side effect failed: %w", object.Name, err)
			}
		}
		finishAtomic(true)
	}
	return nil
}

// loadTableTriggers returns triggers for one table/event/timing in MySQL's
// creation order, adjusted by explicit FOLLOWS/PRECEDES dependencies. The
// topological sort is intentionally local to the matching trigger group so a
// malformed or unrelated trigger cannot change another table's execution.
func loadTableTriggers(dataDir, schemaName, tableName, event, timing string) []persistedStoredObject {
	entries, err := os.ReadDir(filepath.Join(dataDir, schemaName))
	if err != nil {
		return nil
	}
	triggers := make([]persistedStoredObject, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".trigger.json") {
			continue
		}
		raw, readErr := os.ReadFile(filepath.Join(dataDir, schemaName, entry.Name()))
		if readErr != nil {
			continue
		}
		var object persistedStoredObject
		if json.Unmarshal(raw, &object) != nil {
			continue
		}
		lower := strings.ToLower(object.Definition)
		if !strings.Contains(lower, " on "+strings.ToLower(tableName)) || !strings.Contains(lower, strings.ToLower(timing)+" "+strings.ToLower(event)) {
			continue
		}
		triggers = append(triggers, object)
	}
	sort.SliceStable(triggers, func(i, j int) bool {
		if triggers[i].CreatedAt != triggers[j].CreatedAt {
			return triggers[i].CreatedAt < triggers[j].CreatedAt
		}
		return strings.ToLower(triggers[i].Name) < strings.ToLower(triggers[j].Name)
	})
	if len(triggers) < 2 {
		return triggers
	}

	byName := make(map[string]int, len(triggers))
	for index, trigger := range triggers {
		byName[strings.ToLower(trigger.Name)] = index
	}
	dependencies := make([]map[int]struct{}, len(triggers))
	dependents := make([][]int, len(triggers))
	indegree := make([]int, len(triggers))
	for index, trigger := range triggers {
		match := regexp.MustCompile(`(?is)\b(follows|precedes)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?").FindStringSubmatch(trigger.Definition)
		if len(match) != 3 {
			continue
		}
		referenced, exists := byName[strings.ToLower(match[2])]
		if !exists || referenced == index {
			continue
		}
		from, to := referenced, index
		if strings.EqualFold(match[1], "precedes") {
			from, to = index, referenced
		}
		if dependencies[to] == nil {
			dependencies[to] = make(map[int]struct{})
		}
		if _, duplicate := dependencies[to][from]; duplicate {
			continue
		}
		dependencies[to][from] = struct{}{}
		dependents[from] = append(dependents[from], to)
		indegree[to]++
	}

	ready := make([]int, 0, len(triggers))
	for index, degree := range indegree {
		if degree == 0 {
			ready = append(ready, index)
		}
	}
	ordered := make([]persistedStoredObject, 0, len(triggers))
	for len(ready) > 0 {
		index := ready[0]
		ready = ready[1:]
		ordered = append(ordered, triggers[index])
		for _, dependent := range dependents[index] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				position := sort.SearchInts(ready, dependent)
				ready = append(ready, 0)
				copy(ready[position+1:], ready[position:])
				ready[position] = dependent
			}
		}
	}
	if len(ordered) != len(triggers) {
		// Cyclic or otherwise contradictory metadata falls back to stable
		// creation order instead of inventing a partial execution order.
		return triggers
	}
	return ordered
}

func triggerOrderReference(definition string) (kind, name string, ok bool) {
	match := regexp.MustCompile("(?is)\\b(follows|precedes)\\s+`?([a-zA-Z0-9_$]+)`?").FindStringSubmatch(definition)
	if len(match) != 3 {
		return "", "", false
	}
	return strings.ToLower(match[1]), match[2], true
}

func triggerBody(definition string) string {
	body := strings.TrimSpace(definition)
	lower := strings.ToLower(body)
	body = stripTriggerOrderPrefix(body)
	lower = strings.ToLower(body)
	if begin := strings.Index(lower, "begin"); begin >= 0 {
		body = body[begin+len("begin"):]
		if end := strings.LastIndex(strings.ToLower(body), "end"); end >= 0 {
			body = body[:end]
		}
		return body
	}
	if match := regexp.MustCompile(`(?is)\bfor\s+each\s+row\s+(.+)$`).FindStringSubmatch(body); len(match) == 2 {
		body = strings.TrimSpace(match[1])
		body = stripTriggerOrderPrefix(body)
		return stripTriggerOrderPrefix(body)
	}
	return body
}

func stripTriggerOrderPrefix(body string) string {
	orderPrefix := "(?is)^(?:follows|precedes)\\s+`?[a-zA-Z0-9_$]+`?\\s+"
	return regexp.MustCompile(orderPrefix).ReplaceAllString(strings.TrimSpace(body), "")
}

func triggerSQLLiteral(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	switch typed := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(typed), "'", "''") + "'"
	default:
		return fmt.Sprint(value)
	}
}

func triggerLiteralValue(raw string, row map[string]interface{}) (interface{}, error) {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, ";"))
	if directColumn := regexp.MustCompile(`(?i)^new\.([a-zA-Z0-9_$]+)$`).FindStringSubmatch(raw); len(directColumn) == 2 {
		return row[directColumn[1]], nil
	}
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		return strings.ReplaceAll(raw[1:len(raw)-1], "''", "'"), nil
	}
	if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return value, nil
	}
	if value, err := strconv.ParseFloat(raw, 64); err == nil {
		return value, nil
	}
	values := make(map[string]interface{}, len(row)*3)
	for name, value := range row {
		values[name] = value
		values[strings.ToLower(name)] = value
		values["new."+name] = value
		values["new."+strings.ToLower(name)] = value
	}
	statement, err := sqlparser.Parse("select " + raw)
	if err == nil {
		if selectStatement, ok := statement.(*sqlparser.Select); ok && len(selectStatement.SelectExprs) == 1 {
			if aliased, ok := selectStatement.SelectExprs[0].(*sqlparser.AliasedExpr); ok {
				if value, evalErr := evaluateExpressionWithRow(aliased.Expr, values); evalErr == nil {
					return value, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("unsupported trigger assignment %q", raw)
}
