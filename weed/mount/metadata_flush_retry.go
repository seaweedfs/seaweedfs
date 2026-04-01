package mount

import "time"

const metadataFlushRetries = 3

var metadataFlushSleep = time.Sleep

func retryMetadataFlush(flush func() error, onRetry func(nextAttempt, totalAttempts int, backoff time.Duration, err error)) error {
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

		backoff := time.Duration(1<<uint(attempt-1)) * time.Second
		if onRetry != nil {
			onRetry(attempt+1, totalAttempts, backoff, err)
		}
		metadataFlushSleep(backoff)
	}
	return err
}
