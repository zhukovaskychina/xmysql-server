package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isShowBinaryLogsQuery(query string) bool {
	return strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(query, ";")), "show binary logs")
}

func isShowBinlogEventsQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(q, "show binlog events")
}

var showBinlogEventsOptionsPattern = regexp.MustCompile(`(?is)^\s*show\s+binlog\s+events(?:\s+in\s+['"]([^'"]+)['"])?(?:\s+from\s+([0-9]+))?(?:\s+limit\s+((?:[0-9]+\s*,\s*)?[0-9]+))?\s*;?\s*$`)

func parseShowBinlogEventsOptions(query string) (string, uint64, int, int, error) {
	matches := showBinlogEventsOptionsPattern.FindStringSubmatch(query)
	if len(matches) == 0 {
		return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS options")
	}
	logName := "binlog.000001"
	if strings.TrimSpace(matches[1]) != "" {
		logName = matches[1]
	}
	position := uint64(4)
	if matches[2] != "" {
		parsed, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS position: %w", err)
		}
		position = parsed
	}
	offset, rowCount := 0, -1
	if matches[3] != "" {
		parts := strings.Split(matches[3], ",")
		if len(parts) == 1 {
			parsed, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil || parsed < 0 {
				return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS limit")
			}
			rowCount = parsed
		} else {
			parsedOffset, offsetErr := strconv.Atoi(strings.TrimSpace(parts[0]))
			parsedCount, countErr := strconv.Atoi(strings.TrimSpace(parts[1]))
			if offsetErr != nil || countErr != nil || parsedOffset < 0 || parsedCount < 0 {
				return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS limit")
			}
			offset, rowCount = parsedOffset, parsedCount
		}
	}
	return logName, position, offset, rowCount, nil
}

func (e *XMySQLExecutor) executeShowBinaryLogs(ctx *ExecutionContext) {
	columns := []string{"Log_name", "File_size", "Encrypted"}
	rows := [][]interface{}{}
	if e != nil && e.replicationSource != nil {
		if source := e.replicationSource(); source != nil {
			for _, file := range source.NativeFiles() {
				rows = append(rows, []interface{}{file.Name, file.Size, "No"})
			}
		}
	}
	result := newInformationSchemaSelectResult("binary_logs", columns, rows)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("Found %d binary logs", len(rows))}
}

func (e *XMySQLExecutor) executeShowBinlogEvents(ctx *ExecutionContext) {
	columns := []string{"Log_name", "Pos", "Event_type", "Server_id", "End_log_pos", "Info"}
	rows := [][]interface{}{}
	logName, startPosition, offset, rowCount, optionsErr := parseShowBinlogEventsOptions(ctx.RawQuery)
	if optionsErr != nil {
		ctx.Results <- &Result{Err: optionsErr, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if e != nil && e.replicationSource != nil {
		if source := e.replicationSource(); source != nil {
			nextFile := ""
			files := source.NativeFiles()
			for index, file := range files {
				if file.Name == logName && index+1 < len(files) {
					nextFile = files[index+1].Name
					break
				}
			}
			events, err := source.DumpFileWithNativePositions(logName, startPosition)
			if err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return
			}
			for _, event := range events {
				info := ""
				eventType := ""
				if event.NativeEventType != "" {
					eventType = event.NativeEventType
					switch eventType {
					case "Gtid":
						info = event.GTID.String()
					case "Table_map":
						if len(event.Changes) > 0 {
							info = event.Changes[0].Table
						} else {
							info = "table map"
						}
					case "Write_rows", "Update_rows", "Delete_rows":
						info = "row changes"
					case "Xid":
						info = strconv.FormatUint(event.GTID.Seq, 10)
					case "Rotate":
						info = nextFile
						if info == "" {
							info = "binlog.000001"
						}
					case "Query":
						parts := make([]string, 0, len(event.Statements))
						for _, statement := range event.Statements {
							parts = append(parts, statement.SQL)
						}
						info = strings.Join(parts, "; ")
						if info == "" {
							info = "query"
						}
					}
				} else {
					switch event.Type {
					case replication.EventBegin:
						eventType, info = "Gtid", event.GTID.String()
					case replication.EventRow:
						eventType = "Query"
						parts := make([]string, 0, len(event.Statements))
						for _, statement := range event.Statements {
							parts = append(parts, statement.SQL)
						}
						info = strings.Join(parts, "; ")
						if info == "" {
							info = "logical row changes"
						}
					case replication.EventCommit:
						eventType, info = "Xid", strconv.FormatUint(event.GTID.Seq, 10)
					case replication.EventRotate:
						eventType, info = "Rotate", nextFile
						if info == "" {
							info = "binlog.000001"
						}
					}
				}
				position := event.NativePosition
				endPosition := event.NativeEndPosition
				if position == 0 {
					position = event.Position
				}
				if endPosition == 0 {
					endPosition = position + 1
				}
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
	result := newInformationSchemaSelectResult("binlog_events", columns, rows)
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: result, Message: fmt.Sprintf("Found %d binlog events", len(rows))}
}
