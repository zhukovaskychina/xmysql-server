package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isShowRelaylogEventsQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(q, "show relaylog events")
}

var showRelaylogEventsOptionsPattern = regexp.MustCompile(`(?is)^\s*show\s+relaylog\s+events(?:\s+in\s+['"]([^'"]+)['"])?(?:\s+from\s+([0-9]+))?(?:\s+limit\s+((?:[0-9]+\s*,\s*)?[0-9]+))?\s*;?\s*$`)

func parseShowRelaylogEventsOptions(query string) (string, uint64, int, int, error) {
	matches := showRelaylogEventsOptionsPattern.FindStringSubmatch(query)
	if len(matches) == 0 {
		return "", 0, 0, 0, fmt.Errorf("invalid SHOW RELAYLOG EVENTS options")
	}
	logName := "relaylog.000001"
	if strings.TrimSpace(matches[1]) != "" {
		logName = matches[1]
	}
	position := uint64(4)
	if matches[2] != "" {
		parsed, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return "", 0, 0, 0, fmt.Errorf("invalid SHOW RELAYLOG EVENTS position: %w", err)
		}
		position = parsed
	}
	offset, rowCount := 0, -1
	if matches[3] != "" {
		parts := strings.Split(matches[3], ",")
		if len(parts) == 1 {
			parsed, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil || parsed < 0 {
				return "", 0, 0, 0, fmt.Errorf("invalid SHOW RELAYLOG EVENTS limit")
			}
			rowCount = parsed
		} else {
			parsedOffset, offsetErr := strconv.Atoi(strings.TrimSpace(parts[0]))
			parsedCount, countErr := strconv.Atoi(strings.TrimSpace(parts[1]))
			if offsetErr != nil || countErr != nil || parsedOffset < 0 || parsedCount < 0 {
				return "", 0, 0, 0, fmt.Errorf("invalid SHOW RELAYLOG EVENTS limit")
			}
			offset, rowCount = parsedOffset, parsedCount
		}
	}
	return logName, position, offset, rowCount, nil
}

func (e *XMySQLExecutor) executeShowRelaylogEvents(ctx *ExecutionContext) {
	columns := []string{"Log_name", "Pos", "Event_type", "Server_id", "End_log_pos", "Info"}
	rows := [][]interface{}{}
	logName, startPosition, offset, rowCount, optionsErr := parseShowRelaylogEventsOptions(ctx.RawQuery)
	if optionsErr != nil {
		ctx.Results <- &Result{Err: optionsErr, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if e != nil && e.replicationReplica != nil {
		if replica := e.replicationReplica(); replica != nil {
			for _, event := range replica.RelayLogEvents() {
				position := event.NativePosition
				if position == 0 {
					position = event.Position
				}
				if position < startPosition {
					continue
				}
				endPosition := event.NativeEndPosition
				if endPosition == 0 {
					endPosition = position + 1
				}
				eventType, info := relayEventDisplay(event)
				rows = append(rows, []interface{}{logName, position, eventType, event.ServerID, endPosition, info})
			}
		}
	}
	if offset >= len(rows) {
		rows = [][]interface{}{}
	} else if offset > 0 {
		rows = rows[offset:]
	}
	if rowCount >= 0 && rowCount < len(rows) {
		rows = rows[:rowCount]
	}
	result := newInformationSchemaSelectResult("relaylog_events", columns, rows)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("Found %d relay log events", len(rows))}
}

func relayEventDisplay(event replication.BinlogEvent) (string, string) {
	if event.NativeEventType != "" {
		return event.NativeEventType, relayEventInfo(event)
	}
	switch event.Type {
	case replication.EventBegin:
		return "Gtid", event.GTID.String()
	case replication.EventRow:
		if len(event.Changes) > 0 {
			action := strings.ToLower(strings.TrimSpace(event.Changes[0].Action))
			switch action {
			case "insert", "replace":
				return "Write_rows", "row changes"
			case "update":
				return "Update_rows", "row changes"
			case "delete":
				return "Delete_rows", "row changes"
			}
		}
		return "Query", relayEventInfo(event)
	case replication.EventCommit:
		return "Xid", strconv.FormatUint(event.GTID.Seq, 10)
	case replication.EventRotate:
		return "Rotate", event.NativeFile
	default:
		return "", relayEventInfo(event)
	}
}

func relayEventInfo(event replication.BinlogEvent) string {
	if len(event.Statements) > 0 {
		parts := make([]string, 0, len(event.Statements))
		for _, statement := range event.Statements {
			parts = append(parts, statement.SQL)
		}
		if text := strings.Join(parts, "; "); text != "" {
			return text
		}
	}
	if len(event.Changes) > 0 {
		return event.Changes[0].Table
	}
	return ""
}
