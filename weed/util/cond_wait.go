package util

import (
	"context"
	"sync"
	"time"
)

const HttpStatusCancelled = 499

// WaitUntil waits on cond, whose lock the caller holds, until ready returns
// true, ctx is done, or timeout elapses; a zero timeout waits without a bound.
func WaitUntil(ctx context.Context, cond *sync.Cond, timeout time.Duration, ready func() bool) error {
	if ready() {
		return nil
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	stop := context.AfterFunc(ctx, func() {
		cond.L.Lock()
		cond.Broadcast()
		cond.L.Unlock()
	})
	defer stop()
	for !ready() {
		if err := ctx.Err(); err != nil {
			return err
		}
		cond.Wait()
	}
	return nil
}
