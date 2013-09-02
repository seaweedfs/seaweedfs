package glog

import ()

/*
Copying the original glog because it is missing several convenient methods.
1. use ToStderrAndLog() in the weed.go
2. remove nano time in log format
*/

func ToStderr() {
	logging.toStderr = true
}
func ToStderrAndLog() {
	logging.alsoToStderr = true
}
