package engine

import (
	"errors"
	"fmt"
	"testing"
)

func TestExecutionErrorCodeSurvivesWrappedCause(t *testing.T) {
	base := NewExecutionErrorWithCause("test", "read", ExecutionErrorCodeStorageReadFailure, "app", "items", "select 1", 7, errors.New("disk read failed"), "read failed")
	wrapped := fmt.Errorf("request failed: %w", base)

	engine := &XMySQLEngine{}
	if got := engine.getExecutionErrorCode(wrapped); got != string(ExecutionErrorCodeStorageReadFailure) {
		t.Fatalf("expected wrapped execution error code %s, got %s", ExecutionErrorCodeStorageReadFailure, got)
	}
}

func TestExecutorErrorHelpersPreserveWrappedExecutionError(t *testing.T) {
	base := NewExecutionErrorWithCause("test", "read", ExecutionErrorCodeStorageReadFailure, "app", "items", "select 1", 7, errors.New("disk read failed"), "read failed")
	wrapped := fmt.Errorf("request failed: %w", base)

	got := newExecutorErrorf("outer", ExecutionErrorCodeUnknown, "", "", "", wrapped, "outer failed")
	var execErr *ExecutionError
	if !errors.As(got, &execErr) || execErr != base {
		t.Fatalf("executor helper should preserve wrapped ExecutionError, got %T: %v", got, got)
	}

	got = newUnifiedExecutorError("outer", ExecutionErrorCodeUnknown, "", "", "", wrapped, "outer failed")
	if !errors.As(got, &execErr) || execErr != base {
		t.Fatalf("unified executor helper should preserve wrapped ExecutionError, got %T: %v", got, got)
	}
}
