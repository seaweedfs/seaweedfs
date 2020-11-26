package util

import (
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
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
			break
		}
		if strings.Contains(err.Error(), "transport") {
			hasErr = true
			glog.V(0).Infof("retry %s: err: %v", name, err)
			time.Sleep(waitTime)
			waitTime += waitTime / 2
		} else {
			break
		}
	}
	return err
}

// return the first non empty string
func Nvl(values ...string) string {
	for _, s := range values {
		if s != "" {
			return s
		}
	}
	return ""
}
