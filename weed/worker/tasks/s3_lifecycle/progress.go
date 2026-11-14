package s3_lifecycle

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
)

// A long pass must keep refreshing the admin's stale-job clock, but only
// with work it actually did: an idle tick sends nothing.
const progressInterval = 5 * time.Minute

func runWork(s dailyrun.RunSummary) int64 {
	work := s.EntriesVisited + s.EventsScanned
	for _, n := range s.Outcomes {
		work += n
	}
	return work
}

func progressMessage(s dailyrun.RunSummary) string {
	return fmt.Sprintf("s3 lifecycle daily replay running: last_phase=%s last_bucket=%s entries=%d objects=%d events=%d rpc_done=%d rpc_errors=%d",
		s.Phase, s.Bucket, s.EntriesVisited, s.ObjectsVisited, s.EventsScanned, s.Outcomes["DONE"], s.Outcomes["RPC_ERROR"])
}

// startProgressReporter returns stop, which returns only once no further
// progress can be sent, so the caller can send the completion after it.
func startProgressReporter(request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender, summarize func() dailyrun.RunSummary, interval time.Duration) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var reported int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			summary := summarize()
			work := runWork(summary)
			if work == reported {
				continue
			}
			select {
			case <-done:
				return
			default:
			}
			if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
				JobId: request.Job.JobId, JobType: jobType,
				State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "running",
				Message: progressMessage(summary),
			}); err != nil {
				glog.V(1).Infof("s3 lifecycle job %s: send progress: %v", request.Job.JobId, err)
				continue
			}
			reported = work
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
