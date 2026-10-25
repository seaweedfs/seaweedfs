package pluginworker

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ShouldSkipDetectionByInterval returns true when less than minIntervalSeconds
// have elapsed since lastSuccessfulRun. Exported so sub-packages can reuse it.
func ShouldSkipDetectionByInterval(lastSuccessfulRun *timestamppb.Timestamp, minIntervalSeconds int) bool {
	if lastSuccessfulRun == nil || minIntervalSeconds <= 0 {
		return false
	}
	lastRun := lastSuccessfulRun.AsTime()
	if lastRun.IsZero() {
		return false
	}
	return time.Since(lastRun) < time.Duration(minIntervalSeconds)*time.Second
}
