package util

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const HttpStatusCancelled = 499

func WaitWithTimeout(ctx context.Context, cond *sync.Cond, timer *time.Timer) int {
	waitDone := make(chan struct{})

	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		cond.Wait()
		defer close(waitDone)
	}()

	select {
	case <-waitDone:
		return http.StatusOK
	case <-timer.C:
		cond.Broadcast()
		return http.StatusTooManyRequests
	case <-ctx.Done():
		cond.Broadcast()
		return HttpStatusCancelled
	}
}
