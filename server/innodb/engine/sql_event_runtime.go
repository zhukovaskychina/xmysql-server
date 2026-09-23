package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	trimmed := strings.TrimSpace(strings.TrimSuffix(definition, ";"))
	doMatch := regexp.MustCompile(`(?is)\s+do\s+(.+)$`).FindStringSubmatch(trimmed)
	if len(doMatch) != 2 || strings.TrimSpace(doMatch[1]) == "" {
		return parsedEventSchedule{}, fmt.Errorf("CREATE EVENT requires a non-empty DO statement")
	}
	statement := strings.TrimSpace(strings.TrimSuffix(doMatch[1], ";"))
	schedulePart := trimmed[:len(trimmed)-len(doMatch[0])]

	result := parsedEventSchedule{ExecuteAt: time.Now().UTC(), Statement: statement}
	lowerSchedule := strings.ToLower(schedulePart)
	result.OnCompletionPreserve = strings.Contains(lowerSchedule, "on completion preserve")
	atMatch := regexp.MustCompile(`(?is)\bon\s+schedule\s+at\s+('([^']+)'|current_timestamp(?:\(\))?)`).FindStringSubmatch(schedulePart)
	if len(atMatch) > 0 {
		if strings.HasPrefix(strings.ToLower(atMatch[1]), "current_timestamp") {
			result.ExecuteAt = time.Now().UTC()
		} else {
			parsed, err := parseEventTime(atMatch[2])
			if err != nil {
				return parsedEventSchedule{}, err
			}
			result.ExecuteAt = parsed
		}
		return result, nil
	}

	everyMatch := regexp.MustCompile(`(?is)\bon\s+schedule\s+every\s+(\d+)\s+(second|minute|hour|day|week|month|year)`).FindStringSubmatch(schedulePart)
	if len(everyMatch) == 0 {
		return parsedEventSchedule{}, fmt.Errorf("unsupported or missing EVENT schedule")
	}
	count, err := strconv.ParseInt(everyMatch[1], 10, 64)
	if err != nil || count <= 0 {
		return parsedEventSchedule{}, fmt.Errorf("EVENT interval must be positive")
	}
	unit := strings.ToLower(everyMatch[2])
	var multiplier time.Duration
	switch unit {
	case "second":
		multiplier = time.Second
	case "minute":
		multiplier = time.Minute
	case "hour":
		multiplier = time.Hour
	case "day":
		multiplier = 24 * time.Hour
	case "week":
		multiplier = 7 * 24 * time.Hour
	case "month":
		multiplier = 30 * 24 * time.Hour
	case "year":
		multiplier = 365 * 24 * time.Hour
	default:
		return parsedEventSchedule{}, fmt.Errorf("unsupported EVENT interval unit %q", unit)
	}
	result.Interval = time.Duration(count) * multiplier
	result.Repeat = true
	startsMatch := regexp.MustCompile(`(?is)\bstarts\s+'([^']+)'`).FindStringSubmatch(schedulePart)
	if len(startsMatch) == 2 {
		started, err := parseEventTime(startsMatch[1])
		if err != nil {
			return parsedEventSchedule{}, err
		}
		result.ExecuteAt = started
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
	schedule, err := parseSQLEventSchedule(object.Definition)
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

func (e *XMySQLExecutor) executeSQLEvent(parent context.Context, databaseName, eventName, statement string) error {
	if parent == nil {
		parent = context.Background()
	}
	startedAt := time.Now()
	results := make(chan *Result, 8)
	ctx := &ExecutionContext{Context: parent, Cfg: e.conf, DatabaseName: databaseName, RawQuery: statement, Results: results}
	go e.executeQuery(ctx, nil, statement, databaseName, results)
	accounting := statementResultAccounting{}
	var eventErr error
	for result := range results {
		if result == nil {
			continue
		}
		resultAccounting := statementResultAccountingFor(result)
		accounting.rowsAffected += resultAccounting.rowsAffected
		accounting.rowsSent += resultAccounting.rowsSent
		accounting.warnings += resultAccounting.warnings
		if result.Err != nil && eventErr == nil {
			eventErr = result.Err
		}
	}
	timerWait := time.Since(startedAt).Nanoseconds() * 1000
	if timerWait <= 0 {
		timerWait = 1000
	}
	e.recordPerformanceSchemaProgramExecution("EVENT", databaseName, eventName, timerWait, 1, timerWait, timerWait, timerWait,
		boolToInt64(eventErr != nil), accounting.warnings, accounting.rowsAffected, accounting.rowsSent,
	)
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
