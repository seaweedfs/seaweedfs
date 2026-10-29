package pluginworker

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
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

func SkipDetectionByInterval(request *plugin_pb.RunDetectionRequest, sender DetectionSender, minIntervalSeconds int) (bool, error) {
	if !ShouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), minIntervalSeconds) {
		return false, nil
	}
	until := request.GetLastSuccessfulRun().AsTime().Add(time.Duration(minIntervalSeconds) * time.Second)
	reason := fmt.Sprintf("Cooldown after last completed job (%s)", time.Duration(minIntervalSeconds)*time.Second)
	if err := sender.SendActivity(BuildDetectorActivity("skipped_by_interval", reason, map[string]*plugin_pb.ConfigValue{
		"eligible_after": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: until.UTC().Format(time.RFC3339)}},
	})); err != nil {
		return true, err
	}
	if err := sender.SendProposals(&plugin_pb.DetectionProposals{JobType: request.GetJobType()}); err != nil {
		return true, err
	}
	return true, sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:       request.GetJobType(),
		Success:       true,
		SkippedReason: reason,
		EligibleAfter: timestamppb.New(until),
	})
}
