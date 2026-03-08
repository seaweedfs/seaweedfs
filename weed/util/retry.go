package util

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var RetryWaitTime = 6 * time.Second

func Retry(name string, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully", name)
			}
			waitTime = time.Second
			break
		}
		if strings.Contains(err.Error(), "transport") {
			hasErr = true
			glog.V(0).Infof("retry %s: err: %v", name, err)
		} else {
			break
		}
		time.Sleep(waitTime)
		waitTime += waitTime / 2
	}
	return err
}

func MultiRetry(name string, errList []string, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully", name)
			}
			waitTime = time.Second
			break
		}
		if containErr(err.Error(), errList) {
			hasErr = true
			glog.V(0).Infof("retry %s: err: %v", name, err)
		} else {
			break
		}
		time.Sleep(waitTime)
		waitTime += waitTime / 2
	}
	return err
}

// RetryUntil retries until the job returns no error or onErrFn returns false
func RetryUntil(name string, job func() error, onErrFn func(err error) (shouldContinue bool)) error {
	waitTime := time.Second
	for {
		err := job()
		if err == nil {
			waitTime = time.Second
			return nil
		}
		if onErrFn(err) {
			if strings.Contains(err.Error(), "transport") || strings.Contains(err.Error(), "ResourceExhausted") || strings.Contains(err.Error(), "Unavailable") {
				glog.V(0).Infof("retry %s: err: %v", name, err)
			}
			time.Sleep(waitTime)
			if waitTime < RetryWaitTime {
				waitTime += waitTime / 2
			}
			continue
		} else {
			return err
		}
	}
}

// RetryWithBackoff retries an operation on codes.Unavailable errors with exponential
// backoff, respecting context cancellation and a maximum retry duration.
// Returns nil on success, ctx.Err() on context cancellation, or the last error
// when maxDuration is exceeded or a non-retriable error occurs.
func RetryWithBackoff(ctx context.Context, name string, maxDuration time.Duration, shouldRetry func(error) bool, operation func() error) error {
	waitTime := time.Second
	maxWaitTime := RetryWaitTime
	deadline := time.Now().Add(maxDuration)
	for {
		err := operation()
		if err == nil {
			return nil
		}
		if !shouldRetry(err) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			glog.V(0).Infof("retry %s: giving up after %v: %v", name, maxDuration, err)
			return err
		}
		sleepTime := waitTime
		if sleepTime > maxWaitTime {
			sleepTime = maxWaitTime
		}
		if sleepTime > remaining {
			sleepTime = remaining
		}
		glog.V(0).Infof("retry %s: retrying in %v: %v", name, sleepTime, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepTime):
		}
		waitTime += waitTime / 2
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
	}
}

// Nvl return the first non-empty string
func Nvl(values ...string) string {
	for _, s := range values {
		if s != "" {
			return s
		}
	}
	return ""
}

func containErr(err string, errList []string) bool {
	for _, e := range errList {
		if strings.Contains(err, e) {
			return true
		}
	}
	return false
}
