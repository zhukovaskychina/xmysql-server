package engine

import (
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

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
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowTableStatusLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show table status") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowTriggersLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show triggers") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowEventsLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show events") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowRoutineStatusLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show procedure status") && !strings.HasPrefix(normalized, "show function status") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowDatabasesLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show databases") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowVariablesLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show variables") &&
		!strings.HasPrefix(normalized, "show global variables") &&
		!strings.HasPrefix(normalized, "show session variables") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowStatusLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show status") &&
		!strings.HasPrefix(normalized, "show global status") &&
		!strings.HasPrefix(normalized, "show session status") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
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
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show tables") &&
		!strings.HasPrefix(normalized, "show full tables") &&
		!strings.HasPrefix(normalized, "show extended tables") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	selectSQL := "select 1 from dual where " + exprSQL
	stmt, err := sqlparser.Parse(selectSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}

func extractShowTableStatusWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show table status") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	stmt, err := sqlparser.Parse("select 1 from dual where " + exprSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}

func extractShowTriggersWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show triggers") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	stmt, err := sqlparser.Parse("select 1 from dual where " + exprSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}

func extractShowEventsWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show events") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	stmt, err := sqlparser.Parse("select 1 from dual where " + exprSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}

func extractShowRoutineStatusWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show procedure status") && !strings.HasPrefix(normalized, "show function status") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	stmt, err := sqlparser.Parse("select 1 from dual where " + exprSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
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
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show character set") && !strings.HasPrefix(normalized, "show charset") && !strings.HasPrefix(normalized, "show collation") {
		return ""
	}
	match := regexp.MustCompile(`(?is)\blike\s+['"]([^'"]*)['"]`).FindStringSubmatch(rawQuery)
	if len(match) == 2 {
		return match[1]
	}
	return ""
}

func extractShowCharsetWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show character set") && !strings.HasPrefix(normalized, "show charset") && !strings.HasPrefix(normalized, "show collation") {
		return nil
	}
	match := regexp.MustCompile(`(?is)\bwhere\b\s+(.+)$`).FindStringSubmatch(rawQuery)
	if len(match) != 2 {
		return nil
	}
	stmt, err := sqlparser.Parse("select 1 from dual where " + strings.TrimSpace(strings.TrimSuffix(match[1], ";")))
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}

func extractShowColumnsLikePatternFromQuery(rawQuery string) string {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show columns") && !strings.HasPrefix(normalized, "show fields") &&
		!strings.HasPrefix(normalized, "show full columns") && !strings.HasPrefix(normalized, "show full fields") {
		return ""
	}
	re := regexp.MustCompile(`(?i)\blike\s+'([^']+)'`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func extractShowColumnsWhereExprFromQuery(rawQuery string) sqlparser.Expr {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	if !strings.HasPrefix(normalized, "show columns") && !strings.HasPrefix(normalized, "show fields") &&
		!strings.HasPrefix(normalized, "show full columns") && !strings.HasPrefix(normalized, "show full fields") {
		return nil
	}
	re := regexp.MustCompile(`(?i)\bwhere\b\s+(.+)$`)
	matches := re.FindStringSubmatch(rawQuery)
	if len(matches) < 2 {
		return nil
	}
	exprSQL := strings.TrimSpace(strings.TrimSuffix(matches[1], ";"))
	stmt, err := sqlparser.Parse("select 1 from dual where " + exprSQL)
	if err != nil {
		return nil
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil
	}
	return selectStmt.Where.Expr
}
