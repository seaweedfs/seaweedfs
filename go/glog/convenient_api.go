package glog

import ()

/*
Copying the original glog because it is missing several convenient methods.
1. change log file size limit to 180MB
2. use ToStderrAndLog() in the weed.go
3. remove nano time in log format
*/

func ToStderr() {
	logging.toStderr = true
}
func ToStderrAndLog() {
  logging.alsoToStderr = true
}
