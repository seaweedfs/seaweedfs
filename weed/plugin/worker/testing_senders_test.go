package pluginworker

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

type noopDetectionSender struct{}

func (noopDetectionSender) SendProposals(*plugin_pb.DetectionProposals) error { return nil }
func (noopDetectionSender) SendComplete(*plugin_pb.DetectionComplete) error   { return nil }
func (noopDetectionSender) SendActivity(*plugin_pb.ActivityEvent) error       { return nil }

type noopExecutionSender struct{}

func (noopExecutionSender) SendProgress(*plugin_pb.JobProgressUpdate) error { return nil }
func (noopExecutionSender) SendCompleted(*plugin_pb.JobCompleted) error     { return nil }

type recordingDetectionSender struct {
	proposals *plugin_pb.DetectionProposals
	complete  *plugin_pb.DetectionComplete
	events    []*plugin_pb.ActivityEvent
}

func (r *recordingDetectionSender) SendProposals(proposals *plugin_pb.DetectionProposals) error {
	r.proposals = proposals
	return nil
}

func (r *recordingDetectionSender) SendComplete(complete *plugin_pb.DetectionComplete) error {
	r.complete = complete
	return nil
}

func (r *recordingDetectionSender) SendActivity(event *plugin_pb.ActivityEvent) error {
	if event != nil {
		r.events = append(r.events, event)
	}
	return nil
}
