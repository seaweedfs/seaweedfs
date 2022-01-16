package page_writer

import "github.com/chrislusf/seaweedfs/weed/glog"

func CheckByteZero(message string, p []byte, start, stop int64) {
	isAllZero := true
	for i := start; i < stop; i++ {
		if p[i] != 0 {
			isAllZero = false
			break
		}
	}
	if isAllZero {
		if start != stop {
			glog.Errorf("%s is all zeros [%d,%d)", message, start, stop)
		}
	} else {
		glog.V(4).Infof("%s read some non-zero data [%d,%d)", message, start, stop)
	}

}
