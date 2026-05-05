package mount

import "time"

const metadataFlushRetries = 3

var metadataFlushSleep = time.Sleep

func retryMetadataFlush(flush func() error, onRetry func(nextAttempt, totalAttempts int, backoff time.Duration, err error)) error {
	return retryMetadataFlushIf(flush, nil, onRetry)
}

// retryMetadataFlushIf is retryMetadataFlush with an optional shouldRetry
// predicate. If shouldRetry is nil or returns true, the flush is retried with
// exponential backoff; if it returns false, the error is returned immediately
// so callers don't pay retry latency on clearly permanent errors (e.g.
// ENOENT/EACCES/EINVAL from a synchronous setattr).
func retryMetadataFlushIf(flush func() error, shouldRetry func(error) bool, onRetry func(nextAttempt, totalAttempts int, backoff time.Duration, err error)) error {
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

		backoff := time.Duration(1<<uint(attempt-1)) * time.Second
		if onRetry != nil {
			onRetry(attempt+1, totalAttempts, backoff, err)
		}
		metadataFlushSleep(backoff)
	}
	return err
}
