package util

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/dustin/go-humanize"
)

func ParseVolumeSizeLimit(volumeSizeLimitMiB uint, volumeSizeLimitArg string) uint64 {
	volumeSizeLimit := uint64(volumeSizeLimitMiB) * humanize.MiByte
	if volumeSizeLimitArg != "" {
		var err error
		volumeSizeLimit, err = humanize.ParseBytes(volumeSizeLimitArg)
		Assert(err != nil, "Parse volumeSizeLimit %s : %s", volumeSizeLimitArg, err)
	}

	Assert(volumeSizeLimit > 30*1000*humanize.MiByte, "volumeSizeLimit should be smaller than 30000MB")
	return volumeSizeLimit
}

func Assert(condition bool, format string, args ...interface{}) {
	if condition {
		glog.Fatalf(format, args...)
	}
}
