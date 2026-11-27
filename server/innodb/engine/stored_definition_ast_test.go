package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoredCallASTHandlesNestedFunctionsAndQuotedParentheses(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create procedure echo_value(IN input_value varchar(40)) begin select input_value as result; end")
	require.Equal(t, [][]interface{}{{"a),b"}}, mustQuerySQL(t, engine, "app", "call echo_value(concat('a)', ',b'))"))
}

func TestStoredMetadataASTIgnoresQuotedAttributeKeywords(t *testing.T) {
	definition := "create function f() returns int not deterministic reads sql data comment 'modifies sql data begin return deterministic' return 1"
	require.False(t, routineIsDeterministic(definition))
	require.Equal(t, "READS SQL DATA", routineSQLDataAccess(definition))
	require.Equal(t, "modifies sql data begin return deterministic", parseRoutineComment(definition))
	require.Equal(t, "return 1", storedRoutineBodyDefinition(definition))
}
