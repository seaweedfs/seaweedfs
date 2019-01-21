package util

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/dustin/go-humanize"
)

func ParseVolumeSizeLimit(volumeSizeLimitMiBValue uint, volumeSizeLimitArgValue string) uint64 {
	volumeSizeLimit := uint64(volumeSizeLimitMiBValue) * 1024 * 1024
	if volumeSizeLimitArgValue != "" {
		var err error
		volumeSizeLimit, err = humanize.ParseBytes(volumeSizeLimitArgValue)
		if err != nil {
			glog.Fatalf("Parse volumeSizeLimit %s : %s", volumeSizeLimitMiBValue, err)
		}
	}
	if volumeSizeLimit > uint64(30*1000)*1024*1024 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	return volumeSizeLimit
}
