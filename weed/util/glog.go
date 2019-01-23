package util

import "github.com/chrislusf/seaweedfs/weed/glog"

func LogFatalIfError(err error, format string, args ...interface{}) {
	if err != nil {
		glog.Fatalf(format, args...)
	}
}

func LogFatalIf(condition bool, format string, args ...interface{}) {
	if condition {
		glog.Fatalf(format, args...)
	}
}
