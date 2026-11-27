package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type tableCheckInfo struct {
	Checks        []string          `json:"checks"`
	CheckNames    map[string]string `json:"check_names"`
	CheckEnforced map[string]bool   `json:"check_enforced"`
}

func (dml *StorageIntegratedDMLExecutor) validateCheckConstraints(rows []*InsertRowData, schemaName, tableName string) error {
	if dml != nil && dml.checkConstraintChecksSet && !dml.checkConstraintChecks {
		return nil
	}
	if dml == nil || dml.dataDir == "" || schemaName == "" || tableName == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dml.dataDir, schemaName, tableName+".frm"))
	if err != nil {
		return err
	}
	var info tableCheckInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return err
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		for _, check := range info.Checks {
			if !checkConstraintIsEnforced(check, info.CheckNames, info.CheckEnforced) {
				continue
			}
			truth, err := evaluateCheckConstraint(check, row.ColumnValues)
			if err != nil {
				return fmt.Errorf("check constraint evaluation failed: %w", err)
			}
			if truth == sqlTruthTrue || truth == sqlTruthUnknown {
				continue
			}
			return fmt.Errorf("check constraint '%s' is violated", check)
		}
	}
	return nil
}

func evaluateCheckConstraint(expression string, values map[string]interface{}) (int, error) {
	parsed, err := sqlparser.ParseForExecution("SELECT 1 FROM dual WHERE " + expression)
	if err != nil {
		return sqlTruthFalse, err
	}
	selectStmt, ok := parsed.AST.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return sqlTruthFalse, fmt.Errorf("check constraint is not a predicate")
	}
	return evalPredicateTruth(selectStmt.Where.Expr, values)
}

func checkConstraintIsEnforced(expression string, names map[string]string, enforced map[string]bool) bool {
	for name, candidate := range names {
		if strings.EqualFold(candidate, expression) {
			if value, ok := enforced[strings.ToLower(name)]; ok {
				return value
			}
		}
	}
	return true
}

func checkReferencesNull(expression string, values map[string]interface{}) bool {
	if parsed, err := sqlparser.ParseForExecution("SELECT 1 FROM dual WHERE " + expression); err == nil {
		if selectStmt, ok := parsed.AST.(*sqlparser.Select); ok && selectStmt.Where != nil {
			for _, identifier := range predicateASTColumns(selectStmt.Where.Expr) {
				if value, exists := values[identifier]; exists && value == nil {
					return true
				}
			}
			return false
		}
	}
	tokenizer := sqlparser.NewStringTokenizer(expression)
	for {
		kind, raw := tokenizer.Scan()
		if kind == 0 {
			break
		}
		identifier := string(raw)
		if identifier == "" {
			identifier = sqlparser.KeywordString(kind)
		}
		identifier = strings.Trim(identifier, "`")
		if strings.EqualFold(identifier, "and") || strings.EqualFold(identifier, "or") || strings.EqualFold(identifier, "not") {
			continue
		}
		if value, exists := values[identifier]; exists && value == nil {
			return true
		}
	}
	return false
}
