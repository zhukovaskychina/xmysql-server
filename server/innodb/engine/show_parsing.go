package engine

import (
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func stagedShowStatement(ctx *ExecutionContext) *sqlparser.Show {
	if ctx == nil || ctx.Parsed == nil {
		return nil
	}
	show, _ := ctx.Parsed.AST.(*sqlparser.Show)
	return show
}

// normalizedShowExecutionType bridges older callers that construct a partial
// Show node themselves. Parsed execution statements already carry the exact
// multi-word type; the raw checks below are only compatibility detection and
// do not extract execution options.
func normalizedShowExecutionType(stmt *sqlparser.Show, raw string) string {
	if stmt == nil {
		return ""
	}
	typ := strings.ToLower(strings.TrimSpace(stmt.Type))
	switch typ {
	case "table":
		if isShowTableStatusQuery(raw) {
			return "table status"
		}
	case "binlog":
		if isShowBinlogEventsQuery(raw) {
			return "binlog events"
		}
		if isShowBinaryLogsQuery(raw) {
			return "binary logs"
		}
	case "relaylog":
		if isShowRelaylogEventsQuery(raw) {
			return "relaylog events"
		}
	case "master", "source", "binary log":
		if isShowMasterStatusQuery(raw) {
			return "master status"
		}
	case "replicas", "slave":
		if isShowReplicaHostsQuery(raw) {
			return "replica hosts"
		}
		if isShowReplicaStatusQuery(raw) {
			return "replica"
		}
	}
	return typ
}

func showDatabaseFromAST(ctx *ExecutionContext, session server.MySQLServerSession) string {
	if show := stagedShowStatement(ctx); show != nil && show.ShowTablesOpt != nil && strings.TrimSpace(show.ShowTablesOpt.DbName) != "" {
		return strings.TrimSpace(show.ShowTablesOpt.DbName)
	}
	if session != nil {
		if value, ok := session.GetParamByName("database").(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if ctx != nil {
		return strings.TrimSpace(ctx.DatabaseName)
	}
	return ""
}

func extractShowCreateTableName(showType string) string {
	parts := strings.Fields(strings.TrimSpace(showType))
	if len(parts) < 3 {
		return ""
	}
	if strings.EqualFold(parts[0], "create") && strings.EqualFold(parts[1], "table") {
		return normalizeShowTargetName(parts[2])
	}
	return ""
}

func extractShowCreateTableNameFromQuery(rawQuery string) string {
	parts := strings.Fields(strings.TrimSpace(rawQuery))
	if len(parts) < 4 {
		return ""
	}
	if strings.EqualFold(parts[0], "show") && strings.EqualFold(parts[1], "create") && strings.EqualFold(parts[2], "table") {
		return normalizeShowTargetName(parts[3])
	}
	return ""
}

func normalizeShowTargetName(target string) string {
	normalized := strings.TrimSpace(target)
	normalized = strings.TrimSuffix(normalized, ";")
	segments := strings.Split(normalized, ".")
	last := segments[len(segments)-1]
	last = strings.TrimSpace(last)
	last = strings.Trim(last, "`")
	return last
}

func extractShowColumnsTableNameFromQuery(rawQuery string) string {
	parts := strings.Fields(strings.TrimSpace(rawQuery))
	if len(parts) < 4 || !strings.EqualFold(parts[0], "show") {
		return ""
	}

	idx := 1
	if strings.EqualFold(parts[idx], "full") {
		idx++
		if len(parts) < 5 {
			return ""
		}
	}
	if !(strings.EqualFold(parts[idx], "columns") || strings.EqualFold(parts[idx], "fields")) {
		return ""
	}
	idx++
	if !(strings.EqualFold(parts[idx], "from") || strings.EqualFold(parts[idx], "in")) {
		return ""
	}
	idx++
	return normalizeShowTargetName(parts[idx])
}

func extractShowTablesLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "tables")
}

func extractShowTableStatusLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "table status")
}

func extractShowTriggersLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "triggers")
}

func extractShowEventsLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "events")
}

func extractShowRoutineStatusLikePatternFromQuery(rawQuery string) string {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return ""
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (show.Type != "procedure status" && show.Type != "function status") {
		return ""
	}
	return extractShowLikePatternFromStmt(show)
}

func extractShowDatabasesLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "databases")
}

func extractShowVariablesLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "variables")
}

func extractShowStatusLikePatternFromQuery(rawQuery string) string {
	return showLikePatternFromRawQuery(rawQuery, "status")
}

func showLikePatternFromRawQuery(rawQuery, expectedType string) string {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return ""
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || !showTypeMatches(strings.TrimSpace(show.Type), expectedType) {
		return ""
	}
	return extractShowLikePatternFromStmt(show)
}

func ExtractShowVariablesLikePatternFromQuery(rawQuery string) string {
	return extractShowVariablesLikePatternFromQuery(rawQuery)
}

func ExtractShowStatusLikePatternFromQuery(rawQuery string) string {
	return extractShowStatusLikePatternFromQuery(rawQuery)
}

func extractShowLikePatternFromStmt(stmt *sqlparser.Show) string {
	if stmt == nil || stmt.Filter == nil {
		return ""
	}
	return strings.TrimSpace(stmt.Filter.Like)
}

func ExtractShowLikePatternFromStmt(stmt *sqlparser.Show) string {
	return extractShowLikePatternFromStmt(stmt)
}

func extractShowWhereExprFromStmt(stmt *sqlparser.Show) sqlparser.Expr {
	if stmt == nil {
		return nil
	}
	if stmt.Filter != nil && stmt.Filter.Filter != nil {
		return stmt.Filter.Filter
	}
	if stmt.ShowTablesOpt != nil && stmt.ShowTablesOpt.Filter != nil {
		return stmt.ShowTablesOpt.Filter.Filter
	}
	return nil
}

func ExtractShowWhereExprFromStmt(stmt *sqlparser.Show) sqlparser.Expr {
	return extractShowWhereExprFromStmt(stmt)
}

func extractShowTablesWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "tables")
}

func extractShowTableStatusWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "table status")
}

func extractShowTriggersWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "triggers")
}

func extractShowEventsWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "events")
}

func extractShowRoutineStatusWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "procedure status")
}

func showWhereExprFromRawQuery(rawQuery, expectedType string) sqlparser.Expr {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return nil
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || !showTypeMatches(strings.TrimSpace(show.Type), expectedType) {
		return nil
	}
	return extractShowWhereExprFromStmt(show)
}

func showTypeMatches(actual, expected string) bool {
	if strings.EqualFold(actual, expected) {
		return true
	}
	if strings.EqualFold(expected, "tables") && strings.EqualFold(actual, "table") {
		return true
	}
	return false
}

func ResolveShowLikePattern(showType string, stmt *sqlparser.Show, rawQuery string) string {
	normalizedType := strings.ToLower(strings.TrimSpace(showType))
	likePattern := extractShowLikePatternFromStmt(stmt)
	switch normalizedType {
	case "tables":
		if stmt != nil && stmt.ShowTablesOpt != nil && stmt.ShowTablesOpt.Filter != nil {
			if v := strings.TrimSpace(stmt.ShowTablesOpt.Filter.Like); v != "" {
				return v
			}
		}
		if likePattern != "" {
			return likePattern
		}
		return extractShowTablesLikePatternFromQuery(rawQuery)
	case "table status":
		if likePattern != "" {
			return likePattern
		}
		return extractShowTableStatusLikePatternFromQuery(rawQuery)
	case "triggers":
		if likePattern != "" {
			return likePattern
		}
		return extractShowTriggersLikePatternFromQuery(rawQuery)
	case "events":
		if likePattern != "" {
			return likePattern
		}
		return extractShowEventsLikePatternFromQuery(rawQuery)
	case "procedure status", "function status":
		if likePattern != "" {
			return likePattern
		}
		return extractShowRoutineStatusLikePatternFromQuery(rawQuery)
	case "databases":
		if likePattern != "" {
			return likePattern
		}
		return extractShowDatabasesLikePatternFromQuery(rawQuery)
	case "variables":
		if likePattern != "" {
			return likePattern
		}
		return extractShowVariablesLikePatternFromQuery(rawQuery)
	case "status":
		if likePattern != "" {
			return likePattern
		}
		return extractShowStatusLikePatternFromQuery(rawQuery)
	case "columns", "fields":
		if likePattern != "" {
			return likePattern
		}
		return extractShowColumnsLikePatternFromQuery(rawQuery)
	case "character set", "collation":
		if likePattern != "" {
			return likePattern
		}
		return extractShowCharsetLikePatternFromQuery(rawQuery)
	default:
		return likePattern
	}
}

func ResolveShowWhereExpr(showType string, stmt *sqlparser.Show, rawQuery string) sqlparser.Expr {
	whereExpr := extractShowWhereExprFromStmt(stmt)
	normalizedType := strings.ToLower(strings.TrimSpace(showType))
	if whereExpr == nil {
		switch normalizedType {
		case "tables":
			return extractShowTablesWhereExprFromQuery(rawQuery)
		case "table status":
			return extractShowTableStatusWhereExprFromQuery(rawQuery)
		case "triggers":
			return extractShowTriggersWhereExprFromQuery(rawQuery)
		case "events":
			return extractShowEventsWhereExprFromQuery(rawQuery)
		case "procedure status", "function status":
			return extractShowRoutineStatusWhereExprFromQuery(rawQuery)
		case "columns", "fields":
			return extractShowColumnsWhereExprFromQuery(rawQuery)
		case "character set", "collation":
			return extractShowCharsetWhereExprFromQuery(rawQuery)
		}
	}
	return whereExpr
}

func extractShowCharsetLikePatternFromQuery(rawQuery string) string {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return ""
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (show.Type != "character set" && show.Type != "charset" && show.Type != "collation") {
		return ""
	}
	return extractShowLikePatternFromStmt(show)
}

func extractShowCharsetWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return nil
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (show.Type != "character set" && show.Type != "charset" && show.Type != "collation") {
		return nil
	}
	return extractShowWhereExprFromStmt(show)
}

func extractShowColumnsLikePatternFromQuery(rawQuery string) string {
	parsed, err := sqlparser.ParseForExecution(rawQuery)
	if err != nil {
		return ""
	}
	show, ok := parsed.AST.(*sqlparser.Show)
	if !ok || (show.Type != "columns" && show.Type != "fields") {
		return ""
	}
	return extractShowLikePatternFromStmt(show)
}

func extractShowColumnsWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	return showWhereExprFromRawQuery(rawQuery, "columns")
}
