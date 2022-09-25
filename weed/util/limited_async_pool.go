package util

// initial version comes from https://hackernoon.com/asyncawait-in-golang-an-introductory-guide-ol1e34sg

import "context"

type Future interface {
	Await() interface{}
}

type future struct {
	await func(ctx context.Context) interface{}
}

func (f future) Await() interface{} {
	return f.await(context.Background())
}

type LimitedAsyncExecutor struct {
	executor *LimitedConcurrentExecutor
}

func NewLimitedAsyncExecutor(limit int) *LimitedAsyncExecutor {
	return &LimitedAsyncExecutor{
		executor: NewLimitedConcurrentExecutor(limit),
	}
}

func (ae *LimitedAsyncExecutor) Execute(job func() interface{}) Future {
	var result interface{}
	c := make(chan struct{})
	ae.executor.Execute(func() {
		defer close(c)
		result = job()
	})
	return future{await: func(ctx context.Context) interface{} {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c:
			return result
		}
	}}
}
