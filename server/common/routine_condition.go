package common

// SQLSTATE classes and parsed stored-program tokens. These are wire/parser values,
// not an instruction to change the existing handler matching order.
const (
	SQLStateClassSuccess              = "00"
	SQLStateClassWarning              = "01"
	SQLStateClassNotFound             = "02"
	RoutineConditionWarning           = "sqlwarning"
	RoutineConditionException         = "sqlexception"
	RoutineConditionNotFound          = "not found"
	RoutineHandlerContinue            = "continue"
	RoutineHandlerExit                = "exit"
	RoutineDiagnosticReturnedSQLState = "returned_sqlstate"
	RoutineDiagnosticMessageText      = "message_text"
	RoutineDiagnosticMySQLErrno       = "mysql_errno"
)
