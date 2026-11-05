package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func isShowBinaryLogsQuery(query string) bool {
	return strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(query, ";")), "show binary logs")
}

func isShowBinlogEventsQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(q, "show binlog events")
}

func parseShowBinlogEventsOptions(query string) (string, uint64, int, int, error) {
	parsed, err := sqlparser.ParseForExecution(query)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS options")
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (!strings.EqualFold(show.Type, "binlog events") && !strings.EqualFold(show.Type, "binlog")) {
		return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS options")
	}
	logName := "binlog.000001"
	if strings.TrimSpace(show.LogFile) != "" {
		logName = show.LogFile
	}
	position := uint64(4)
	if show.LogPosition != nil {
		parsedPosition, err := strconv.ParseUint(strings.TrimSpace(sqlparser.String(show.LogPosition)), 10, 64)
		if err != nil {
			return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS position: %w", err)
		}
		position = parsedPosition
	}
	offset, rowCount := 0, -1
	if show.Limit != nil {
		var err error
		rowCount, err = parseShowLimitInteger(show.Limit.Rowcount)
		if err != nil || rowCount < 0 {
			return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS limit")
		}
		if show.Limit.Offset != nil {
			offset, err = parseShowLimitInteger(show.Limit.Offset)
			if err != nil || offset < 0 {
				return "", 0, 0, 0, fmt.Errorf("invalid SHOW BINLOG EVENTS limit")
			}
		}
	}
	return logName, position, offset, rowCount, nil
}

func parseShowLimitInteger(expr sqlparser.Expr) (int, error) {
	value := strings.TrimSpace(sqlparser.String(expr))
	return strconv.Atoi(value)
}

func (e *XMySQLExecutor) executeShowBinaryLogs(ctx *ExecutionContext) {
	if err := e.checkReplicationPrivilege(ctx, "REPLICATION CLIENT"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
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
	if err := e.checkReplicationPrivilege(ctx, "REPLICATION SLAVE"); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
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
