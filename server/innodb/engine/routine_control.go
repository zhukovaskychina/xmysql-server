package engine

// Internal control-flow tokens never form part of the public SQLSTATE catalog.
const (
	routineControlHandlerExit = "handler_exit"
	routineControlLeave       = "leave"
	routineControlIterate     = "iterate"
)
