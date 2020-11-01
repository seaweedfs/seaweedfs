package util

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func Retry(name string, waitTimeLimit time.Duration, job func() error) (err error) {
	waitTime := time.Second
	for waitTime < waitTimeLimit {
		err = job()
		if err == nil {
			break
		}
		glog.V(1).Infof("retry %s", name)
		time.Sleep(waitTime)
		waitTime += waitTime / 2
	}
	return err
}