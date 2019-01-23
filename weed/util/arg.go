package util

import (
	"github.com/dustin/go-humanize"
)

func ParseVolumeSizeLimit(volumeSizeLimitMiBValue uint, volumeSizeLimitArgValue string) uint64 {
	volumeSizeLimit := uint64(volumeSizeLimitMiBValue) * 1024 * 1024
	if volumeSizeLimitArgValue != "" {
		var err error
		volumeSizeLimit, err = humanize.ParseBytes(volumeSizeLimitArgValue)
		LogFatalIfError(err, "Parse volumeSizeLimit %d : %s", volumeSizeLimitMiBValue, err)
	}

	LogFatalIf(volumeSizeLimit > uint64(30*1000)*1024*1024, "volumeSizeLimitMB should be smaller than 30000")

	return volumeSizeLimit
}
