package engine

import "time"

type TaskStatus int

const (
	// RunningTask is set when the task is running.
	RunningTask TaskStatus = iota + 1

	// KilledTask is set when the task is killed, but resources are still
	// being used.
	KilledTask
)

func (t TaskStatus) String() string {
	switch t {
	case RunningTask:
		return "running"
	case KilledTask:
		return "killed"
	default:
		return "unknown"
	}
}

type Task struct {
	query     string
	database  string
	startTime time.Time
	closing   chan struct{}
}

type TaskManager struct {
	QueryTimeout time.Duration

	MaxConcurrentQueries int
}

func (t *TaskManager) executeShowQueriesStatement() {

}
