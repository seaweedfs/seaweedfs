package pluginworker

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BuildExecutorActivity creates an executor activity event. Exported for sub-packages.
func BuildExecutorActivity(stage string, message string) *plugin_pb.ActivityEvent {
	return &plugin_pb.ActivityEvent{
		Source:    plugin_pb.ActivitySource_ACTIVITY_SOURCE_EXECUTOR,
		Stage:     stage,
		Message:   message,
		CreatedAt: timestamppb.Now(),
	}
}

// BuildDetectorActivity creates a detector activity event. Exported for sub-packages.
func BuildDetectorActivity(stage string, message string, details map[string]*plugin_pb.ConfigValue) *plugin_pb.ActivityEvent {
	return &plugin_pb.ActivityEvent{
		Source:    plugin_pb.ActivitySource_ACTIVITY_SOURCE_DETECTOR,
		Stage:     stage,
		Message:   message,
		Details:   details,
		CreatedAt: timestamppb.Now(),
	}
}
