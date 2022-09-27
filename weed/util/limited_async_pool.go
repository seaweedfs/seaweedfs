package util

// initial version comes from https://hackernoon.com/asyncawait-in-golang-an-introductory-guide-ol1e34sg

import (
	"container/list"
	"context"
	"sync"
)

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
	executor       *LimitedConcurrentExecutor
	futureList     *list.List
	futureListCond *sync.Cond
}

func NewLimitedAsyncExecutor(limit int) *LimitedAsyncExecutor {
	return &LimitedAsyncExecutor{
		executor:       NewLimitedConcurrentExecutor(limit),
		futureList:     list.New(),
		futureListCond: sync.NewCond(&sync.Mutex{}),
	}
}

func (ae *LimitedAsyncExecutor) Execute(job func() interface{}) {
	var result interface{}
	c := make(chan struct{})
	ae.executor.Execute(func() {
		defer close(c)
		result = job()
	})
	f := future{await: func(ctx context.Context) interface{} {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c:
			return result
		}
	}}
	ae.futureListCond.L.Lock()
	ae.futureList.PushBack(f)
	ae.futureListCond.Signal()
	ae.futureListCond.L.Unlock()
}

func (ae *LimitedAsyncExecutor) NextFuture() Future {
	ae.futureListCond.L.Lock()
	for ae.futureList.Len() == 0 {
		ae.futureListCond.Wait()
	}
	f := ae.futureList.Remove(ae.futureList.Front())
	ae.futureListCond.L.Unlock()
	return f.(Future)
}
