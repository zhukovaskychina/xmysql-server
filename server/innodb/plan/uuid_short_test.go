package plan

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUUIDShortGeneratorPersistsSequenceAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "uuid_short.state")
	first, err := NewUUIDShortGenerator(7, statePath)
	require.NoError(t, err)
	value1, err := first.Next()
	require.NoError(t, err)
	value2, err := first.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), value2-value1)

	restarted, err := NewUUIDShortGenerator(7, statePath)
	require.NoError(t, err)
	value3, err := restarted.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), value3-value2)
	require.NotEqual(t, uint64(0), value1)
}

func TestUUIDShortFunctionUsesConfiguredGenerator(t *testing.T) {
	generator, err := NewUUIDShortGenerator(11, "")
	require.NoError(t, err)
	expression := &Function{FuncName: "UUID_SHORT"}
	first, err := expression.Eval(&EvalContext{UUIDShortGenerator: generator})
	require.NoError(t, err)
	second, err := expression.Eval(&EvalContext{UUIDShortGenerator: generator})
	require.NoError(t, err)
	require.Equal(t, uint64(1), second.(uint64)-first.(uint64))
	_, err = (&Function{FuncName: "UUID_SHORT", FuncArgs: []Expression{&Constant{Value: int64(1)}}}).Eval(&EvalContext{UUIDShortGenerator: generator})
	require.EqualError(t, err, "UUID_SHORT requires no arguments")
}
