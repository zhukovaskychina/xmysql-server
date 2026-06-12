package engine

import (
	"fmt"
)

// ExecutionErrorCode 表示执行器错误码。
type ExecutionErrorCode string

const (
	ExecutionErrorCodeUnknown               ExecutionErrorCode = "E_UNKNOWN"
	ExecutionErrorCodeMetadataMissing       ExecutionErrorCode = "E_METADATA_MISSING"
	ExecutionErrorCodeStorageMissing        ExecutionErrorCode = "E_STORAGE_MISSING"
	ExecutionErrorCodeStorageReadFailure    ExecutionErrorCode = "E_STORAGE_READ_FAIL"
	ExecutionErrorCodeStorageWriteFailure   ExecutionErrorCode = "E_STORAGE_WRITE_FAIL"
	ExecutionErrorCodeTxnBeginFailed        ExecutionErrorCode = "E_TXN_BEGIN"
	ExecutionErrorCodeTxnCommitFailed       ExecutionErrorCode = "E_TXN_COMMIT"
	ExecutionErrorCodeTxnRollbackFailed     ExecutionErrorCode = "E_TXN_ROLLBACK"
	ExecutionErrorCodeTxnContextInvalid     ExecutionErrorCode = "E_TXN_CONTEXT_INVALID"
	ExecutionErrorCodeDuplicateKey          ExecutionErrorCode = "E_DUPLICATE_KEY"
	ExecutionErrorCodeSchemaOrTableNotFound ExecutionErrorCode = "E_SCHEMA_TABLE_NOT_FOUND"
	ExecutionErrorCodeOperatorNotOpened     ExecutionErrorCode = "E_OPERATOR_NOT_OPENED"
	ExecutionErrorCodeIndexOperation        ExecutionErrorCode = "E_INDEX_OPERATION"
	ExecutionErrorCodeOptimizer             ExecutionErrorCode = "E_OPTIMIZER"
	ExecutionErrorCodeValidation            ExecutionErrorCode = "E_VALIDATION"
)

// ExecutionError 是可观测的执行错误，包含结构化上下文。
type ExecutionError struct {
	Module    string
	Stage     string
	SQL       string
	Schema    string
	Table     string
	TxnID     uint64
	ErrorCode ExecutionErrorCode
	Message   string
	Cause     error
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return "<nil execution error>"
	}

	location := ""
	if e.Schema != "" || e.Table != "" {
		location = fmt.Sprintf(" scope=%s.%s", e.Schema, e.Table)
	}

	return fmt.Sprintf(
		"[%s][%s] code=%s tx=%d%s: %s: %v",
		e.Module,
		e.Stage,
		e.ErrorCode,
		e.TxnID,
		location,
		e.Message,
		e.Cause,
	)
}

func (e *ExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newExecutionError(module, stage string, code ExecutionErrorCode, schema, table, sql string, txnID uint64, msg string, err error) *ExecutionError {
	if msg == "" {
		msg = "execution failed"
	}
	return &ExecutionError{
		Module:    module,
		Stage:     stage,
		SQL:       sql,
		Schema:    schema,
		Table:     table,
		TxnID:     txnID,
		ErrorCode: code,
		Message:   msg,
		Cause:     err,
	}
}

func NewExecutionErrorWithCause(module, stage string, code ExecutionErrorCode, schema, table, sql string, txnID uint64, err error, message string) error {
	if err == nil {
		return nil
	}

	if message == "" {
		return newExecutionError(module, stage, code, schema, table, sql, txnID, "failed", err)
	}

	return newExecutionError(module, stage, code, schema, table, sql, txnID, message, err)
}

func NewExecutionErrorf(module, stage string, code ExecutionErrorCode, schema, table, sql string, txnID uint64, err error, messageFormat string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	if messageFormat == "" {
		return NewExecutionErrorWithCause(module, stage, code, schema, table, sql, txnID, err, "")
	}
	return NewExecutionErrorWithCause(module, stage, code, schema, table, sql, txnID, err, fmt.Sprintf(messageFormat, args...))
}
