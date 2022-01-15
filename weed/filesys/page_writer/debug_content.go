package page_writer

import "github.com/chrislusf/seaweedfs/weed/glog"

func checkByteZero(message string, p []byte, start, stop int64) {
	isAllZero := true
	for i := start; i < stop; i++ {
		if p[i] != 0 {
			isAllZero = false
			break
		}
	}
	if isAllZero {
		glog.Errorf("%s is all zeros [%d,%d)", message, start, stop)
	}

}
