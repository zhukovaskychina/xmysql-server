package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

var ErrModelBudgetExceeded = errors.New("model query budget exceeded")

// ModelExecutionPolicy is server-owned. SQL clients cannot increase these
// limits. A statement and its child contexts share one reservation counter.
type ModelExecutionPolicy struct {
	MaxCalls      uint64
	MaxInputBytes uint64
	CallTimeout   time.Duration
}

func DefaultModelExecutionPolicy() ModelExecutionPolicy {
	limits := conf.DefaultAIModelExecutionConfig()
	return ModelExecutionPolicy{MaxCalls: limits.MaxCallsPerQuery, MaxInputBytes: limits.MaxInputBytesPerQuery, CallTimeout: limits.CallTimeout}
}

func modelExecutionPolicyFromConfig(cfg *conf.Cfg) ModelExecutionPolicy {
	policy := DefaultModelExecutionPolicy()
	if cfg != nil {
		if cfg.AIModelExecution.MaxCallsPerQuery > 0 {
			policy.MaxCalls = cfg.AIModelExecution.MaxCallsPerQuery
		}
		if cfg.AIModelExecution.MaxInputBytesPerQuery > 0 {
			policy.MaxInputBytes = cfg.AIModelExecution.MaxInputBytesPerQuery
		}
		if cfg.AIModelExecution.CallTimeout > 0 {
			policy.CallTimeout = cfg.AIModelExecution.CallTimeout
		}
	}
	return policy
}

func (e *XMySQLExecutor) SetModelExecutionPolicy(policy ModelExecutionPolicy) error {
	if e == nil || policy.MaxCalls == 0 || policy.MaxInputBytes == 0 || policy.CallTimeout <= 0 {
		return fmt.Errorf("model policy requires positive call, input-byte and timeout limits")
	}
	e.embeddingRuntimeMu.Lock()
	e.modelExecutionPolicy = policy
	e.embeddingRuntimeMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) modelPolicySnapshot() ModelExecutionPolicy {
	if e == nil {
		return DefaultModelExecutionPolicy()
	}
	e.embeddingRuntimeMu.RLock()
	policy := e.modelExecutionPolicy
	e.embeddingRuntimeMu.RUnlock()
	if policy.MaxCalls == 0 {
		return DefaultModelExecutionPolicy()
	}
	return policy
}

type modelQueryBudgetKey struct{}

type modelQueryBudget struct {
	sync.Mutex
	policy     ModelExecutionPolicy
	calls      uint64
	inputBytes uint64
}

func (e *XMySQLExecutor) withModelQueryBudget(ctx context.Context) context.Context {
	ctx = ctxOrBackground(ctx)
	if _, exists := ctx.Value(modelQueryBudgetKey{}).(*modelQueryBudget); exists {
		return ctx
	}
	return context.WithValue(ctx, modelQueryBudgetKey{}, &modelQueryBudget{policy: e.modelPolicySnapshot()})
}

func beginModelCall(ctx context.Context, inputBytes uint64) (context.Context, context.CancelFunc, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	budget, ok := ctx.Value(modelQueryBudgetKey{}).(*modelQueryBudget)
	if !ok {
		// Standalone projection evaluators still receive a bounded call. Normal
		// SQL entry points attach the shared statement budget before evaluation.
		budget = &modelQueryBudget{policy: DefaultModelExecutionPolicy()}
	}
	budget.Lock()
	if budget.calls >= budget.policy.MaxCalls || inputBytes > budget.policy.MaxInputBytes-budget.inputBytes {
		budget.Unlock()
		return nil, nil, ErrModelBudgetExceeded
	}
	budget.calls++
	budget.inputBytes += inputBytes
	timeout := budget.policy.CallTimeout
	budget.Unlock()
	// Reservations are retained after failures and unknown outcomes. Retrying
	// within this statement must not reset already attempted work.
	callContext, cancel := context.WithTimeout(ctx, timeout)
	return callContext, cancel, nil
}

func invokeEmbedding(ctx context.Context, runtime EmbeddingRuntime, request EmbeddingRequest) (EmbeddingResult, error) {
	callContext, cancel, err := beginModelCall(ctx, uint64(len(request.Text))+uint64(len(request.Media)))
	if err != nil {
		return EmbeddingResult{}, err
	}
	defer cancel()
	result, err := runtime.Embed(callContext, request)
	if contextErr := callContext.Err(); contextErr != nil {
		return EmbeddingResult{}, contextErr
	}
	return result, err
}

func invokeGeneration(ctx context.Context, runtime GenerationRuntime, request GenerationRequest) (GenerationResult, error) {
	callContext, cancel, err := beginModelCall(ctx, uint64(len(request.Prompt))+uint64(len(request.Media)))
	if err != nil {
		return GenerationResult{}, err
	}
	defer cancel()
	result, err := runtime.Generate(callContext, request)
	if contextErr := callContext.Err(); contextErr != nil {
		return GenerationResult{}, contextErr
	}
	return result, err
}
