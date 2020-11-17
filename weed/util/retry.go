package util

import (
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util/log"
)

var RetryWaitTime = 6 * time.Second

func Retry(name string, job func() error) (err error) {
	waitTime := time.Second
	hasErr := false
	for waitTime < RetryWaitTime {
		err = job()
		if err == nil {
			if hasErr {
				log.Infof("retry %s successfully", name)
			}
			break
		}
		if strings.Contains(err.Error(), "transport") {
			hasErr = true
			log.Infof("retry %s", name)
			time.Sleep(waitTime)
			waitTime += waitTime / 2
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
