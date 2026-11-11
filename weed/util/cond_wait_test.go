package util

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWaitUntilReturnsWhenReady(t *testing.T) {
	cond := sync.NewCond(new(sync.Mutex))
	ready := false
	go func() {
		time.Sleep(20 * time.Millisecond)
		cond.L.Lock()
		ready = true
		cond.L.Unlock()
		cond.Broadcast()
	}()
	cond.L.Lock()
	err := WaitUntil(context.Background(), cond, time.Minute, func() bool { return ready })
	cond.L.Unlock()
	if err != nil {
		t.Fatalf("WaitUntil: %v", err)
	}
}

func TestWaitUntilTimesOutWithoutWakeup(t *testing.T) {
	cond := sync.NewCond(new(sync.Mutex))
	start := time.Now()
	cond.L.Lock()
	err := WaitUntil(context.Background(), cond, 50*time.Millisecond, func() bool { return false })
	cond.L.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("returned after %v", elapsed)
	}
}

func TestWaitUntilStopsOnCancel(t *testing.T) {
	cond := sync.NewCond(new(sync.Mutex))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	cond.L.Lock()
	err := WaitUntil(ctx, cond, 0, func() bool { return false })
	cond.L.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want canceled", err)
	}
}
