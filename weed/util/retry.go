package util

import (
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
func RetryUntil(name string, job func() error, onErrFn func(err error) (shouldContinue bool)) {
	waitTime := time.Second
	for {
		err := job()
		if err == nil {
			waitTime = time.Second
			break
		}
		if onErrFn(err) {
			if strings.Contains(err.Error(), "transport") {
				glog.V(0).Infof("retry %s: err: %v", name, err)
			}
			time.Sleep(waitTime)
			if waitTime < RetryWaitTime {
				waitTime += waitTime / 2
			}
			continue
		} else {
			break
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
