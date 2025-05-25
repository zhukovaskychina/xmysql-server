package engine

import (
	"context"
	"fmt"
	"github.com/pelletier/go-toml/query"
	"sync"
	"xmysql-server/server/conf"
)

//定义查询上下文环境，
type ExecutionContext struct {
	context.Context

	statementId int64

	QueryId uint64

	Results chan *Result

	mu sync.RWMutex

	done chan struct{}

	err error

	DatabaseName string

	Cfg *conf.Cfg
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
		fmt.Println("成功了！！！！")
	}
	return nil
}
