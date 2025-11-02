package protocol

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// ErrorHelper 错误处理辅助类
type ErrorHelper struct{}

// NewErrorHelper 创建错误处理辅助类
func NewErrorHelper() *ErrorHelper {
	return &ErrorHelper{}
}

// CreateSQLError 创建SQL错误（使用common.SQLError）
func (h *ErrorHelper) CreateSQLError(errCode uint16, args ...interface{}) *common.SQLError {
	return common.NewErr(errCode, args...)
}

// CreateSQLErrorf 创建SQL错误（自定义格式）
func (h *ErrorHelper) CreateSQLErrorf(errCode uint16, format string, args ...interface{}) *common.SQLError {
	return common.NewErrf(errCode, format, nil, args...)
}

// EncodeError 编码SQL错误为MySQL错误包
func (h *ErrorHelper) EncodeError(err *common.SQLError) []byte {
	return EncodeErrorPacket(err.Code, err.State, err.Message)
}

// EncodeErrorFromCode 从错误码创建并编码错误包
func (h *ErrorHelper) EncodeErrorFromCode(errCode uint16, args ...interface{}) []byte {
	sqlErr := common.NewErr(errCode, args...)
	return EncodeErrorPacket(sqlErr.Code, sqlErr.State, sqlErr.Message)
}

// EncodeErrorFromGoError 从Go error创建并编码错误包（使用通用错误码）
func (h *ErrorHelper) EncodeErrorFromGoError(err error) []byte {
	if err == nil {
		return nil
	}

	// 检查是否是SQLError
	if sqlErr, ok := err.(*common.SQLError); ok {
		return EncodeErrorPacket(sqlErr.Code, sqlErr.State, sqlErr.Message)
	}

	// 使用通用错误码
	return EncodeErrorPacket(common.ER_UNKNOWN_ERROR, "HY000", err.Error())
}

// CreateErrorMessage 创建ErrorMessage对象
func (h *ErrorHelper) CreateErrorMessage(sessionID string, errCode uint16, args ...interface{}) *ErrorMessage {
	sqlErr := common.NewErr(errCode, args...)
	return &ErrorMessage{
		BaseMessage: NewBaseMessage(MSG_ERROR, sessionID, nil),
		Code:        sqlErr.Code,
		State:       sqlErr.State,
		Message:     sqlErr.Message,
	}
}

// CreateErrorMessageFromGoError 从Go error创建ErrorMessage
func (h *ErrorHelper) CreateErrorMessageFromGoError(sessionID string, err error) *ErrorMessage {
	if err == nil {
		return nil
	}

	// 检查是否是SQLError
	if sqlErr, ok := err.(*common.SQLError); ok {
		return &ErrorMessage{
			BaseMessage: NewBaseMessage(MSG_ERROR, sessionID, nil),
			Code:        sqlErr.Code,
			State:       sqlErr.State,
			Message:     sqlErr.Message,
		}
	}

	// 使用通用错误码
	return &ErrorMessage{
		BaseMessage: NewBaseMessage(MSG_ERROR, sessionID, nil),
		Code:        common.ER_UNKNOWN_ERROR,
		State:       "HY000",
		Message:     err.Error(),
	}
}

// 常用错误快捷方法

// EncodeSyntaxError 编码语法错误
func (h *ErrorHelper) EncodeSyntaxError(message string) []byte {
	return h.EncodeErrorFromCode(common.ErrParse, message)
}

// EncodeAccessDeniedError 编码访问拒绝错误
func (h *ErrorHelper) EncodeAccessDeniedError(user, host string) []byte {
	return h.EncodeErrorFromCode(common.ErrAccessDenied, user, host, "YES")
}

// EncodeTableNotFoundError 编码表不存在错误
func (h *ErrorHelper) EncodeTableNotFoundError(database, table string) []byte {
	return h.EncodeErrorFromCode(common.ErrNoSuchTable, database, table)
}

// EncodeDatabaseNotFoundError 编码数据库不存在错误
func (h *ErrorHelper) EncodeDatabaseNotFoundError(database string) []byte {
	return h.EncodeErrorFromCode(common.ErrBadDB, database)
}

// EncodeColumnNotFoundError 编码列不存在错误
func (h *ErrorHelper) EncodeColumnNotFoundError(column, table string) []byte {
	return h.EncodeErrorFromCode(common.ErrBadField, column, table)
}

// EncodeDuplicateKeyError 编码重复键错误
func (h *ErrorHelper) EncodeDuplicateKeyError(key, table string) []byte {
	return h.EncodeErrorFromCode(common.ErrDupEntry, key, table)
}

// EncodeTableExistsError 编码表已存在错误
func (h *ErrorHelper) EncodeTableExistsError(table string) []byte {
	return h.EncodeErrorFromCode(common.ErrTableExists, table)
}

// EncodeDatabaseExistsError 编码数据库已存在错误
func (h *ErrorHelper) EncodeDatabaseExistsError(database string) []byte {
	return h.EncodeErrorFromCode(common.ErrDBCreateExists, database)
}

// EncodeLockWaitTimeoutError 编码锁等待超时错误
func (h *ErrorHelper) EncodeLockWaitTimeoutError() []byte {
	return h.EncodeErrorFromCode(common.ErrLockWaitTimeout)
}

// EncodeDeadlockError 编码死锁错误
func (h *ErrorHelper) EncodeDeadlockError() []byte {
	return h.EncodeErrorFromCode(common.ErrLockDeadlock)
}

// EncodeTransactionError 编码事务错误
func (h *ErrorHelper) EncodeTransactionError(message string) []byte {
	return h.EncodeErrorFromCode(common.ER_UNKNOWN_ERROR, message)
}

// EncodeNotSupportedError 编码不支持的功能错误
func (h *ErrorHelper) EncodeNotSupportedError(feature string) []byte {
	return h.EncodeErrorFromCode(common.ErrNotSupportedYet, feature)
}

// CreateErrorMessageFromCode 从错误码创建ErrorMessage
func (h *ErrorHelper) CreateErrorMessageFromCode(sessionID string, errCode uint16, args ...interface{}) *ErrorMessage {
	return h.CreateErrorMessage(sessionID, errCode, args...)
}

// 全局错误处理器实例
var GlobalErrorHelper = NewErrorHelper()

// 便捷函数（使用全局实例）

// NewSQLError 创建SQL错误
func NewSQLError(errCode uint16, args ...interface{}) *common.SQLError {
	return GlobalErrorHelper.CreateSQLError(errCode, args...)
}

// NewSQLErrorf 创建SQL错误（自定义格式）
func NewSQLErrorf(errCode uint16, format string, args ...interface{}) *common.SQLError {
	return GlobalErrorHelper.CreateSQLErrorf(errCode, format, args...)
}

// EncodeError 编码SQL错误
func EncodeError(err *common.SQLError) []byte {
	return GlobalErrorHelper.EncodeError(err)
}

// EncodeErrorFromCode 从错误码编码错误包
func EncodeErrorFromCode(errCode uint16, args ...interface{}) []byte {
	return GlobalErrorHelper.EncodeErrorFromCode(errCode, args...)
}

// EncodeErrorFromGoError 从Go error编码错误包
func EncodeErrorFromGoError(err error) []byte {
	return GlobalErrorHelper.EncodeErrorFromGoError(err)
}

// NewErrorMessage 创建ErrorMessage
func NewErrorMessage(sessionID string, errCode uint16, args ...interface{}) *ErrorMessage {
	return GlobalErrorHelper.CreateErrorMessage(sessionID, errCode, args...)
}

// NewErrorMessageFromGoError 从Go error创建ErrorMessage
func NewErrorMessageFromGoError(sessionID string, err error) *ErrorMessage {
	return GlobalErrorHelper.CreateErrorMessageFromGoError(sessionID, err)
}

// 常用错误快捷函数

// EncodeSyntaxError 编码语法错误
func EncodeSyntaxError(message string) []byte {
	return GlobalErrorHelper.EncodeSyntaxError(message)
}

// EncodeAccessDeniedError 编码访问拒绝错误
func EncodeAccessDeniedError(user, host string) []byte {
	return GlobalErrorHelper.EncodeAccessDeniedError(user, host)
}

// EncodeTableNotFoundError 编码表不存在错误
func EncodeTableNotFoundError(database, table string) []byte {
	return GlobalErrorHelper.EncodeTableNotFoundError(database, table)
}

// EncodeDatabaseNotFoundError 编码数据库不存在错误
func EncodeDatabaseNotFoundError(database string) []byte {
	return GlobalErrorHelper.EncodeDatabaseNotFoundError(database)
}

// EncodeColumnNotFoundError 编码列不存在错误
func EncodeColumnNotFoundError(column, table string) []byte {
	return GlobalErrorHelper.EncodeColumnNotFoundError(column, table)
}

// EncodeDuplicateKeyError 编码重复键错误
func EncodeDuplicateKeyError(key, table string) []byte {
	return GlobalErrorHelper.EncodeDuplicateKeyError(key, table)
}

// EncodeTableExistsError 编码表已存在错误
func EncodeTableExistsError(table string) []byte {
	return GlobalErrorHelper.EncodeTableExistsError(table)
}

// EncodeDatabaseExistsError 编码数据库已存在错误
func EncodeDatabaseExistsError(database string) []byte {
	return GlobalErrorHelper.EncodeDatabaseExistsError(database)
}

// EncodeLockWaitTimeoutError 编码锁等待超时错误
func EncodeLockWaitTimeoutError() []byte {
	return GlobalErrorHelper.EncodeLockWaitTimeoutError()
}

// EncodeDeadlockError 编码死锁错误
func EncodeDeadlockError() []byte {
	return GlobalErrorHelper.EncodeDeadlockError()
}

// EncodeTransactionError 编码事务错误
func EncodeTransactionError(message string) []byte {
	return GlobalErrorHelper.EncodeTransactionError(message)
}

// EncodeNotSupportedError 编码不支持的功能错误
func EncodeNotSupportedError(feature string) []byte {
	return GlobalErrorHelper.EncodeNotSupportedError(feature)
}

// GetErrorCodeName 获取错误码的名称（用于调试）
func GetErrorCodeName(errCode uint16) string {
	if errMsg, ok := common.MySQLErrName[errCode]; ok {
		return fmt.Sprintf("ER_%d: %s", errCode, errMsg.Raw)
	}
	return fmt.Sprintf("UNKNOWN_ERROR_%d", errCode)
}

// GetErrorState 获取错误码对应的SQL State
func GetErrorState(errCode uint16) string {
	if state, ok := common.MySQLState[errCode]; ok {
		return state
	}
	return common.DefaultMySQLState
}
