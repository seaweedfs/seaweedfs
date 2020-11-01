package util

import (
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func Retry(name string, waitTimeLimit time.Duration, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	for waitTime < waitTimeLimit {
		err = job()
		if err == nil {
			if hasErr {
				glog.V(0).Infof("retry %s successfully", name)
			}
			break
		}
		if strings.Contains(err.Error(), "transport") {
			hasErr = true
			glog.V(0).Infof("retry %s", name)
			time.Sleep(waitTime)
			waitTime += waitTime / 2
		}
	}
	return err
}