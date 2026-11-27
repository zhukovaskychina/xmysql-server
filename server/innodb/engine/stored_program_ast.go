package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// executeStoredProgram is the structured boundary for routine bodies. The
// existing routine state machine remains responsible for MySQL control-flow
// behavior, while statement boundaries and parsed leaf statements come from
// StoredProgramBlock and are never rediscovered with regular expressions.
func (e *XMySQLExecutor) executeStoredProgram(ctx *ExecutionContext, databaseName string, block *sqlparser.StoredProgramBlock, state *storedRoutineState) error {
	if block == nil {
		return nil
	}
	if state == nil {
		return fmt.Errorf("stored routine state is nil")
	}
	for _, statement := range block.Statements {
		if statement.Raw == "" {
			continue
		}
		if statement.Kind == "set" {
			if setStmt, ok := statement.AST.(*sqlparser.Set); ok {
				for _, expression := range setStmt.Exprs {
					if expression == nil || expression.Expr == nil {
						continue
					}
					name := strings.Trim(expression.Name.String(), "` ")
					valueExpression := substituteStoredRoutineValues(sqlparser.String(expression.Expr), state.vars)
					value, err := evaluateStoredRoutineScalarExpression(valueExpression)
					if err != nil {
						return err
					}
					state.vars[strings.ToLower(name)] = sessionUserVariableSQLLiteral(value)
				}
				continue
			}
		}
		if statement.Children != nil {
			if err := e.executeStoredProgram(ctx, databaseName, statement.Children, state); err != nil {
				return err
			}
			continue
		}
		if err := e.executeStoredRoutineStatements(ctx, databaseName, []string{statement.Raw}, state); err != nil {
			return err
		}
	}
	return nil
}

func storedProgramBlockSimple(block *sqlparser.StoredProgramBlock) bool {
	if block == nil {
		return false
	}
	for _, statement := range block.Statements {
		if statement.Children != nil || statement.ElseBranch != nil || statement.HandlerAction != "" {
			return false
		}
		switch statement.Kind {
		case "set":
		default:
			return false
		}
	}
	return true
}
