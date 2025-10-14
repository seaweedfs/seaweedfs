package testutil

import (
	"fmt"
	"testing"
	"time"
)

// AssertEventually retries an assertion until it passes or times out
func AssertEventually(t *testing.T, assertion func() error, timeout time.Duration, interval time.Duration, msgAndArgs ...interface{}) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		if err := assertion(); err == nil {
			return // Success
		} else {
			lastErr = err
		}
		time.Sleep(interval)
	}

	// Format the failure message
	var msg string
	if len(msgAndArgs) > 0 {
		if format, ok := msgAndArgs[0].(string); ok {
			msg = fmt.Sprintf(format, msgAndArgs[1:]...)
		} else {
			msg = fmt.Sprint(msgAndArgs...)
		}
	} else {
		msg = "assertion failed"
	}

	t.Fatalf("%s after %v: %v", msg, timeout, lastErr)
}

// AssertNoError fails the test if err is not nil
func AssertNoError(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err != nil {
		var msg string
		if len(msgAndArgs) > 0 {
			if format, ok := msgAndArgs[0].(string); ok {
				msg = fmt.Sprintf(format, msgAndArgs[1:]...)
			} else {
				msg = fmt.Sprint(msgAndArgs...)
			}
		} else {
			msg = "unexpected error"
		}
		t.Fatalf("%s: %v", msg, err)
	}
}

// AssertError fails the test if err is nil
func AssertError(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err == nil {
		var msg string
		if len(msgAndArgs) > 0 {
			if format, ok := msgAndArgs[0].(string); ok {
				msg = fmt.Sprintf(format, msgAndArgs[1:]...)
			} else {
				msg = fmt.Sprint(msgAndArgs...)
			}
		} else {
			msg = "expected error but got nil"
		}
		t.Fatal(msg)
	}
}

// AssertEqual fails the test if expected != actual
func AssertEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if expected != actual {
		var msg string
		if len(msgAndArgs) > 0 {
			if format, ok := msgAndArgs[0].(string); ok {
				msg = fmt.Sprintf(format, msgAndArgs[1:]...)
			} else {
				msg = fmt.Sprint(msgAndArgs...)
			}
		} else {
			msg = "values not equal"
		}
		t.Fatalf("%s: expected %v, got %v", msg, expected, actual)
	}
}

// AssertNotEqual fails the test if expected == actual
func AssertNotEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if expected == actual {
		var msg string
		if len(msgAndArgs) > 0 {
			if format, ok := msgAndArgs[0].(string); ok {
				msg = fmt.Sprintf(format, msgAndArgs[1:]...)
			} else {
				msg = fmt.Sprint(msgAndArgs...)
			}
		} else {
			msg = "values should not be equal"
		}
		t.Fatalf("%s: both values are %v", msg, expected)
	}
}

// AssertGreaterThan fails the test if actual <= expected
func AssertGreaterThan(t *testing.T, expected, actual int, msgAndArgs ...interface{}) {
	t.Helper()
	if actual <= expected {
		var msg string
		if len(msgAndArgs) > 0 {
			if format, ok := msgAndArgs[0].(string); ok {
				msg = fmt.Sprintf(format, msgAndArgs[1:]...)
			} else {
				msg = fmt.Sprint(msgAndArgs...)
			}
		} else {
			msg = "value not greater than expected"
		}
		t.Fatalf("%s: expected > %d, got %d", msg, expected, actual)
	}
}

// AssertContains fails the test if slice doesn't contain item
func AssertContains(t *testing.T, slice []string, item string, msgAndArgs ...interface{}) {
	t.Helper()
	for _, s := range slice {
		if s == item {
			return // Found it
		}
	}

	var msg string
	if len(msgAndArgs) > 0 {
		if format, ok := msgAndArgs[0].(string); ok {
			msg = fmt.Sprintf(format, msgAndArgs[1:]...)
		} else {
			msg = fmt.Sprint(msgAndArgs...)
		}
	} else {
		msg = "item not found in slice"
	}
	t.Fatalf("%s: %q not found in %v", msg, item, slice)
}
