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

	statementRowsAffected        atomic.Int64
	statementRowsSent            atomic.Int64
	statementRowsExamined        atomic.Int64
	statementSelectScan          atomic.Int64
	statementSelectRange         atomic.Int64
	statementSelectFullJoin      atomic.Int64
	statementSelectFullRangeJoin atomic.Int64
	statementSelectRangeCheck    atomic.Int64
	statementNoIndexUsed         atomic.Int64
	statementNoGoodIndexUsed     atomic.Int64
	statementSortRows            atomic.Int64
	statementSortScan            atomic.Int64
	statementSortRange           atomic.Int64
	statementWarnings            atomic.Int64
	// statementMetricsDeferred is used by ExecuteWithQuery, whose result
	// forwarding goroutine owns result accounting. The worker must not publish
	// statement history before that accounting has completed.
	statementMetricsDeferred bool
	// errorMetricsDeferred is used by stored-routine child statements. The
	// routine boundary records the raised/handled error with its final SQL
	// handler outcome, while the child still publishes its statement summary.
	errorMetricsDeferred  bool
	statementMetricStatus string
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

func (ctx *ExecutionContext) recordSelectRange(ranges int64) {
	if ctx == nil || ranges <= 0 {
		return
	}
	ctx.statementSelectRange.Add(ranges)
}

func (ctx *ExecutionContext) recordSelectFullJoin(joins int64) {
	if ctx == nil || joins <= 0 {
		return
	}
	ctx.statementSelectFullJoin.Add(joins)
}

func (ctx *ExecutionContext) recordSelectFullRangeJoin(joins int64) {
	if ctx == nil || joins <= 0 {
		return
	}
	ctx.statementSelectFullRangeJoin.Add(joins)
}

func (ctx *ExecutionContext) recordSelectRangeCheck(checks int64) {
	if ctx == nil || checks <= 0 {
		return
	}
	ctx.statementSelectRangeCheck.Add(checks)
}

func (ctx *ExecutionContext) recordNoIndexUsed(used bool) {
	if ctx == nil || !used {
		return
	}
	ctx.statementNoIndexUsed.Store(1)
}

func (ctx *ExecutionContext) recordNoGoodIndexUsed(used bool) {
	if ctx == nil || !used {
		return
	}
	ctx.statementNoGoodIndexUsed.Add(1)
}

func (ctx *ExecutionContext) recordSortRows(rows int64) {
	if ctx == nil || rows <= 0 {
		return
	}
	ctx.statementSortRows.Add(rows)
}

func (ctx *ExecutionContext) recordSortScan(scans int64) {
	if ctx == nil || scans <= 0 {
		return
	}
	ctx.statementSortScan.Add(scans)
}

func (ctx *ExecutionContext) recordSortRange(ranges int64) {
	if ctx == nil || ranges <= 0 {
		return
	}
	ctx.statementSortRange.Add(ranges)
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
