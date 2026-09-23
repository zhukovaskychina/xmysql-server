package engine

import (
	"context"
	"github.com/pelletier/go-toml/query"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"sync"
	"sync/atomic"
)

// 定义查询上下文环境，
type ExecutionContext struct {
	context.Context

	statementId int64

	QueryId uint64

	Results chan *Result

	mu sync.RWMutex

	done chan struct{}

	err error

	DatabaseName string

	RawQuery string

	Session              server.MySQLServerSession
	Warnings             []Warning
	AdminResult          *Result
	ViewSecurityRequired bool
	// triggerStack carries the active AFTER-trigger call chain through nested
	// SQL execution. It is intentionally context-local so recursive trigger
	// cycles are rejected without leaking state across connections.
	triggerStack []string

	// SpatialCandidateKeys is an optional storage-key set produced by the
	// spatial MBR index. Select execution can use it to avoid decoding rows
	// that cannot satisfy the spatial predicate.
	SpatialCandidateKeys map[string]struct{}

	// ddlWriteLocks tracks re-entrant compound ALTER calls. The outer ALTER
	// owns the actual table lock; inner clause execution only increments this
	// counter so it cannot self-deadlock on the same table.
	ddlWriteLocks map[string]int

	Cfg *conf.Cfg

	statementRowsAffected atomic.Int64
	statementRowsSent     atomic.Int64
	statementRowsExamined atomic.Int64
	statementSelectScan   atomic.Int64
	statementWarnings     atomic.Int64
}

func (ctx *ExecutionContext) recordStatementResult(result *Result) {
	if ctx == nil {
		return
	}
	accounting := statementResultAccountingFor(result)
	ctx.statementRowsAffected.Add(accounting.rowsAffected)
	ctx.statementRowsSent.Add(accounting.rowsSent)
	ctx.statementWarnings.Add(accounting.warnings)
}

func (ctx *ExecutionContext) recordRowsExamined(rows int64) {
	if ctx == nil || rows <= 0 {
		return
	}
	ctx.statementRowsExamined.Add(rows)
}

func (ctx *ExecutionContext) recordSelectScan(scans int64) {
	if ctx == nil || scans <= 0 {
		return
	}
	ctx.statementSelectScan.Add(scans)
}

func (ctx *ExecutionContext) watch() {
	ctx.done = make(chan struct{})
	if ctx.err != nil {
		close(ctx.done)
		return
	}

	//go func() {
	//	defer close(ctx.done)
	//
	//	var taskCtx <-chan struct{}
	//	if ctx.task != nil {
	//		taskCtx = ctx.task.closing
	//	}
	//
	//	select {
	//	case <-taskCtx:
	//		ctx.err = ctx.task.Error()
	//		if ctx.err == nil {
	//			ctx.err = ErrQueryInterrupted
	//		}
	//	case <-ctx.AbortCh:
	//		ctx.err = ErrQueryAborted
	//	case <-ctx.Context.Done():
	//		ctx.err = ctx.Context.Err()
	//	}
	//}()
}

func (ctx *ExecutionContext) Done() <-chan struct{} {
	ctx.mu.RLock()
	if ctx.done != nil {
		defer ctx.mu.RUnlock()
		return ctx.done
	}
	ctx.mu.RUnlock()

	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.done == nil {
		ctx.watch()
	}
	return ctx.done
}

func (ctx *ExecutionContext) Err() error {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.err
}

func (ctx *ExecutionContext) Value(key interface{}) interface{} {
	//switch key {
	//case monitorContextKey{}:
	//	return ctx.task
	//}
	return ctx.Context.Value(key)
}

// send sends a Result to the Results channel and will exit if the plan has
// been aborted.
func (ctx *ExecutionContext) send(result *query.Result) error {
	//result.StatementID = ctx.statementID
	//select {
	//case <-ctx.AbortCh:
	//	return ErrQueryAborted
	//case ctx.Results <- result:
	//}
	return nil
}

// Send sends a Result to the Results channel and will exit if the plan has
// been interrupted or aborted.
func (ctx *ExecutionContext) Send(result *Result) error {
	result.StatementID = ctx.statementId

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ctx.Results <- result:
		logger.Debug("成功了！！！！")
	}
	return nil
}
