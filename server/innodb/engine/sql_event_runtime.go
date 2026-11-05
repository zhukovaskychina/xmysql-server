package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"strings"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type parsedEventSchedule struct {
	ExecuteAt            time.Time
	Interval             time.Duration
	Repeat               bool
	OnCompletionPreserve bool
	Statement            string
}

// SetEventScheduler attaches the explicit scheduler used by SQL EVENT
// definitions. A nil scheduler disables event execution without affecting
// event metadata persistence.
func (e *XMySQLExecutor) SetEventScheduler(scheduler *EventScheduler) {
	if e == nil {
		return
	}
	if scheduler == nil {
		scheduler = NewEventScheduler(false)
	}
	e.eventScheduler = scheduler
}

func (e *XMySQLExecutor) EventScheduler() *EventScheduler {
	if e == nil {
		return nil
	}
	return e.eventScheduler
}

// StartEventScheduler loads persisted SQL EVENT definitions before starting
// the explicitly enabled scheduler. It intentionally does not enable a
// disabled scheduler; callers must opt into background SQL execution.
func (e *XMySQLExecutor) StartEventScheduler(ctx context.Context) error {
	if e == nil || e.eventScheduler == nil {
		return fmt.Errorf("event scheduler is not configured")
	}
	if err := e.reloadPersistedEvents(); err != nil {
		return err
	}
	e.eventScheduler.Start(ctx)
	return nil
}

func (e *XMySQLExecutor) StopEventScheduler() bool {
	if e == nil || e.eventScheduler == nil {
		return false
	}
	return e.eventScheduler.Stop()
}

func parseSQLEventSchedule(definition string) (parsedEventSchedule, error) {
	parsed, err := sqlparser.ParseForExecution(definition)
	if err != nil {
		return parsedEventSchedule{}, fmt.Errorf("invalid CREATE EVENT definition: %w", err)
	}
	object, ok := parsed.AST.(*sqlparser.StoredObjectStatement)
	if !ok || !strings.EqualFold(object.ObjectType, "event") {
		return parsedEventSchedule{}, fmt.Errorf("CREATE EVENT definition did not produce EVENT AST")
	}
	return parseSQLEventScheduleAST(object)
}
func parseSQLEventScheduleAST(object *sqlparser.StoredObjectStatement) (parsedEventSchedule, error) {
	if object == nil {
		return parsedEventSchedule{}, fmt.Errorf("event definition is nil")
	}
	body := strings.TrimSpace(object.Body)
	if strings.HasPrefix(strings.ToLower(body), "do ") {
		body = strings.TrimSpace(body[3:])
	}
	if body == "" {
		return parsedEventSchedule{}, fmt.Errorf("CREATE EVENT requires a non-empty DO statement")
	}
	result := parsedEventSchedule{ExecuteAt: time.Now().UTC(), Statement: strings.TrimSuffix(body, ";"), OnCompletionPreserve: object.OnCompletionPreserve}
	if strings.EqualFold(object.ScheduleKind, "AT") {
		if strings.EqualFold(object.Starts, "current_timestamp") || strings.EqualFold(object.Starts, "current_timestamp()") || object.Starts == "" {
			return result, nil
		}
		parsed, err := parseEventTime(strings.Trim(object.Starts, "'\""))
		if err != nil {
			return parsedEventSchedule{}, err
		}
		result.ExecuteAt = parsed
		return result, nil
	}
	if !strings.EqualFold(object.ScheduleKind, "EVERY") || object.IntervalValue <= 0 {
		return parsedEventSchedule{}, fmt.Errorf("unsupported or missing EVENT schedule")
	}
	multiplier := map[string]time.Duration{"SECOND": time.Second, "MINUTE": time.Minute, "HOUR": time.Hour, "DAY": 24 * time.Hour, "WEEK": 7 * 24 * time.Hour, "MONTH": 30 * 24 * time.Hour, "YEAR": 365 * 24 * time.Hour}[strings.ToUpper(object.IntervalUnit)]
	if multiplier == 0 {
		return parsedEventSchedule{}, fmt.Errorf("unsupported EVENT interval unit %q", object.IntervalUnit)
	}
	result.Interval = time.Duration(object.IntervalValue) * multiplier
	result.Repeat = true
	if object.Starts != "" && !strings.EqualFold(object.Starts, "current_timestamp") {
		parsed, err := parseEventTime(strings.Trim(object.Starts, "'\""))
		if err != nil {
			return parsedEventSchedule{}, err
		}
		result.ExecuteAt = parsed
	}
	return result, nil
}

func parseEventTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339, "2006-01-02T15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid EVENT timestamp %q", value)
}

func (e *XMySQLExecutor) registerSQLEvent(object persistedStoredObject) error {
	if strings.EqualFold(object.ObjectType, "event") == false {
		return nil
	}
	if object.Disabled {
		return nil
	}
	if object.DefinitionAST == nil {
		hydrateStoredObjectAST(&object)
	}
	schedule, err := parseSQLEventScheduleStoredObject(object)
	if err != nil {
		return err
	}
	if e.eventScheduler == nil {
		e.eventScheduler = NewEventScheduler(false)
	}
	eventName := object.Schema + "." + object.Name
	return e.eventScheduler.AddEvent(ScheduledEvent{
		Name:      eventName,
		ExecuteAt: schedule.ExecuteAt,
		Interval:  schedule.Interval,
		Repeat:    schedule.Repeat,
		Execute: func(ctx context.Context) error {
			if err := e.markSQLEventExecuted(object.Schema, object.Name); err != nil {
				return err
			}
			// Remove one-shot metadata before executing the user statement. The
			// statement's committed rows become visible before this callback
			// returns; deleting afterwards creates a race where callers observe
			// the fired event and still see its durable definition.
			if !schedule.Repeat && !schedule.OnCompletionPreserve {
				if err := os.Remove(e.storedObjectPath(object.Schema, object.Name, "event")); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			if err := e.executeSQLEvent(ctx, object.Schema, object.Name, schedule.Statement); err != nil {
				return err
			}
			return nil
		},
	})
}

// parseSQLEventScheduleStoredObject consumes the cached AST when metadata was
// loaded from disk. Rebuilding from the durable definition remains the
// compatibility path for old rows or invalidated in-memory entries.
func parseSQLEventScheduleStoredObject(object persistedStoredObject) (parsedEventSchedule, error) {
	if object.DefinitionAST != nil && strings.EqualFold(object.DefinitionAST.ObjectType, "event") {
		return parseSQLEventScheduleAST(object.DefinitionAST)
	}
	return parseSQLEventSchedule(object.Definition)
}

func (e *XMySQLExecutor) markSQLEventExecuted(schema, name string) error {
	path := e.storedObjectPath(schema, name, "event")
	lockCtx := &ExecutionContext{Context: context.Background()}
	release, err := e.acquireStoredObjectWriteLock(lockCtx, schema, name)
	if err != nil {
		return err
	}
	defer release()
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var object persistedStoredObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	object.LastExecutedAt = time.Now().UTC().Format(time.RFC3339)
	encoded, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		return err
	}
	return writeMetadataFileAtomic(path, encoded)
}

func (e *XMySQLExecutor) executeSQLEvent(parent context.Context, databaseName, eventName, statement string) error {
	if parent == nil {
		parent = context.Background()
	}
	startedAt := time.Now()
	results := make(chan *Result, 8)
	ctx := &ExecutionContext{Context: parent, Cfg: e.conf, DatabaseName: databaseName, RawQuery: statement, Results: results}
	accounting := statementResultAccounting{}
	var eventErr error
	recorded := false
	var observed atomic.Bool
	record := func() {
		if recorded {
			return
		}
		recorded = true
		timerWait := time.Since(startedAt).Nanoseconds() * 1000
		if timerWait <= 0 {
			timerWait = 1000
		}
		e.recordPerformanceSchemaProgramExecutionWithCPU("EVENT", databaseName, eventName, timerWait, 1, timerWait, timerWait, timerWait,
			boolToInt64(eventErr != nil), accounting.warnings, accounting.rowsAffected, accounting.rowsSent, 0,
			ctx.statementCPUTime.Load(), ctx.statementCPUTimeCaptured.Load(),
		)
	}
	ctx.statementResultObserver = func(result *Result) {
		if result == nil || observed.Swap(true) {
			return
		}
		resultAccounting := statementResultAccountingFor(result)
		accounting.rowsAffected += resultAccounting.rowsAffected
		accounting.rowsSent += resultAccounting.rowsSent
		accounting.warnings += resultAccounting.warnings
		if result.Err != nil {
			eventErr = result.Err
		}
		record()
	}
	go e.executeQuery(ctx, nil, statement, databaseName, results)
	for result := range results {
		if result == nil {
			continue
		}
		if observed.Load() {
			continue
		}
		resultAccounting := statementResultAccountingFor(result)
		accounting.rowsAffected += resultAccounting.rowsAffected
		accounting.rowsSent += resultAccounting.rowsSent
		accounting.warnings += resultAccounting.warnings
		if result.Err != nil && eventErr == nil {
			eventErr = result.Err
		}
		// executeQuery publishes the statement result after the DML commit but
		// may still be running lifecycle/metrics defers before closing results.
		// Record the program boundary as soon as that committed result is
		// observable, so Performance Schema does not lag durable event effects.
		record()
	}
	record()
	return eventErr
}

func (e *XMySQLExecutor) reloadPersistedEvents() error {
	root := e.getDataDir()
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		schema := entry.Name()
		files, err := os.ReadDir(filepath.Join(root, schema))
		if err != nil {
			return err
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".event.json") {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(root, schema, file.Name()))
			if err != nil {
				return err
			}
			var object persistedStoredObject
			if err := json.Unmarshal(raw, &object); err != nil {
				return err
			}
			if object.Disabled {
				continue
			}
			if err := e.registerSQLEvent(object); err != nil {
				return err
			}
		}
	}
	return nil
}
