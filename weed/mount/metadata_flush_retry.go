package mount

import (
	"context"
	"time"
)

const metadataFlushRetries = 3

// metadataFlushSleep waits for d or until ctx is cancelled. Overridable in tests.
var metadataFlushSleep = func(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func retryMetadataFlush(ctx context.Context, flush func() error, onRetry func(nextAttempt, totalAttempts int, backoff time.Duration, err error)) error {
	return retryMetadataFlushIf(ctx, flush, nil, onRetry)
}

// retryMetadataFlushIf retries flush with exponential backoff, stopping early
// when shouldRetry returns false (clearly permanent errors) or when ctx is
// cancelled (the FUSE request was interrupted, e.g. the process was killed).
func retryMetadataFlushIf(ctx context.Context, flush func() error, shouldRetry func(error) bool, onRetry func(nextAttempt, totalAttempts int, backoff time.Duration, err error)) error {
	totalAttempts := metadataFlushRetries + 1
	var err error
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		err = flush()
		if err == nil {
			break
		}
		if attempt == totalAttempts {
			break
		}
		if shouldRetry != nil && !shouldRetry(err) {
			break
		}
		if ctx.Err() != nil {
			break
		}

		backoff := time.Duration(1<<uint(attempt-1)) * time.Second
		if onRetry != nil {
			onRetry(attempt+1, totalAttempts, backoff, err)
		}
		metadataFlushSleep(ctx, backoff)
		if ctx.Err() != nil {
			break
		}
	}
	return err
}
