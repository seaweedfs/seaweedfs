package plugin

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// orphanedExecutionMisses is how many itemized heartbeats in a row from a
// newer stream of the worker must leave a dispatched job out before the
// admin fails it. The second rides out a heartbeat the worker built while
// it was still picking the job back up.
const orphanedExecutionMisses = 2

// pendingExecutionState is an execution request waiting for its JobCompleted.
type pendingExecutionState struct {
	completed chan *plugin_pb.JobCompleted
	jobID     string
	jobType   string
	// session is the stream the request went out on.
	session *streamSession
	misses  int
	failed  bool
	// completionSent is set once a completion is handed to the dispatcher,
	// which may already have taken it from the buffer.
	completionSent bool
}

// failExecutionsMissingFromHeartbeat fails the executions sent on an earlier
// stream of the heartbeat's worker that the worker no longer lists. A worker
// that restarted comes back under the same id without the old process's
// jobs, and nothing else ends such a dispatch before its execution timeout,
// so its slot stays reserved all that time. Jobs sent on the heartbeat's own
// stream are left alone, since a worker counts a job before it lists it, and
// so is everything while the heartbeat counts slots it does not itemize,
// since a missing id then proves nothing.
func (r *Plugin) failExecutionsMissingFromHeartbeat(session *streamSession, heartbeat *plugin_pb.WorkerHeartbeat) {
	if session == nil || heartbeat == nil {
		return
	}
	itemized := 0
	listed := make(map[string]struct{}, len(heartbeat.GetRunningWork()))
	for _, work := range heartbeat.GetRunningWork() {
		if work.GetKind() == plugin_pb.WorkKind_WORK_KIND_EXECUTION {
			listed[strings.TrimSpace(work.GetWorkId())] = struct{}{}
			itemized++
		}
	}
	itemizesAll := int(heartbeat.GetExecutionSlotsUsed()) <= itemized

	var orphaned []activeJobSnapshot
	r.pendingExecutionMu.Lock()
	for requestID, pending := range r.pendingExecution {
		if pending.failed || pending.session == nil || pending.session == session || pending.session.workerID != session.workerID {
			continue
		}
		if _, ok := listed[strings.TrimSpace(pending.jobID)]; ok {
			pending.misses = 0
			continue
		}
		if !itemizesAll {
			continue
		}
		if pending.completionSent {
			continue
		}
		pending.misses++
		if pending.misses < orphanedExecutionMisses {
			continue
		}
		pending.failed = true
		orphaned = append(orphaned, activeJobSnapshot{
			jobID:     pending.jobID,
			jobType:   pending.jobType,
			workerID:  session.workerID,
			requestID: requestID,
		})
	}
	r.pendingExecutionMu.Unlock()

	for _, snap := range orphaned {
		reason := fmt.Sprintf("worker %s re-registered without job %s", snap.workerID, snap.jobID)
		glog.Warningf("Plugin failing %s request %s: %s", snap.jobType, snap.requestID, reason)
		// off the stream's receive loop: the cancel may wait on a full buffer
		go r.expireActiveJob(snap, reason, time.Now().UTC())
	}
}
