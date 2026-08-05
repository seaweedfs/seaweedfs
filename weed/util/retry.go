package util

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var RetryWaitTime = 6 * time.Second

// transientErrorMessages are substrings of failures that a later attempt is
// likely to get past: connection resets, timeouts, and the throttling or
// overload replies S3 and gRPC hand back. Cloud SDKs bury the underlying net
// error in an opaque wrapper with no Unwrap, so the message is often all
// that is left to match on. Compared case-insensitively, so entries are lower
// case: the same condition reaches different layers capitalized differently
// (a volume server relays its idle timeout as "I/O timeout" inside JSON).
var transientErrorMessages = []string{
	"transport",
	"connection reset",
	"connection refused",
	"broken pipe",
	"unexpected eof",
	"i/o timeout",
	"tls handshake timeout",
	"no such host",
	"no route to host",
	"network is unreachable",
	"client.timeout",
	"requesterror",
	"requesttimeout",
	"slowdown",
	"throttling",
	"internalerror",
	"resourceexhausted",
	"unavailable",
}

// IsTransientErrorMessage reports whether an error message describes a network
// or service condition worth retrying. Callers holding an error should use
// IsTransientError; this is for paths that only carry the text, such as the
// per-file status strings in a batch delete response.
func IsTransientErrorMessage(msg string) bool {
	lower := strings.ToLower(msg)
	for _, transient := range transientErrorMessages {
		if strings.Contains(lower, transient) {
			return true
		}
	}
	return false
}

// IsTransientError reports whether err is a network or service condition worth
// retrying. A cancelled or expired context never is: the caller is already gone.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return IsTransientErrorMessage(err.Error())
}

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
		if IsTransientError(err) {
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

// RetryOnError retries job with the same bounded backoff as MultiRetry, but
// decides retriability with a predicate instead of an error-substring list.
func RetryOnError(name string, shouldRetry func(error) bool, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully", name)
			}
			break
		}
		if shouldRetry(err) {
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
	var lastErr error
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Until(deadline) <= 0 {
			if lastErr != nil {
				glog.V(0).Infof("retry %s: giving up after %v: %v", name, maxDuration, lastErr)
				return lastErr
			}
		}
		err := operation()
		if err == nil {
			return nil
		}
		lastErr = err
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
		glog.V(1).Infof("retry %s: retrying in %v: %v", name, sleepTime, err)
		timer := time.NewTimer(sleepTime)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
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
