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
		go r.expireActiveJob(snap, reason, time.Now().UTC(), nil)
	}
}

// restoredExecutionState is a job a previous admin process had sent to a
// worker. This process never dispatched it, so no pendingExecution entry
// covers it and only the stale sweep would otherwise end it.
type restoredExecutionState struct {
	workerID  string
	requestID string
	misses    int
}

// trackRestoredExecution remembers an active job loaded at startup that was
// already sent to its worker, so the worker's heartbeats can confirm or
// disown it. A job still queued has a worker bound but no request yet, and
// no worker would list it.
func (r *Plugin) trackRestoredExecution(job *TrackedJob) {
	if job == nil || !isActiveTrackedJobState(job.State) || isQueuedTrackedJobState(job.State) {
		return
	}
	workerID := strings.TrimSpace(job.WorkerID)
	if workerID == "" || strings.TrimSpace(job.RequestID) == "" {
		return
	}
	r.restoredExecutionsMu.Lock()
	r.restoredExecutions[job.JobID] = &restoredExecutionState{workerID: workerID, requestID: job.RequestID}
	r.restoredExecutionsMu.Unlock()
}

func isQueuedTrackedJobState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "pending", "job_state_pending":
		return true
	}
	return false
}

// failRestoredJobsMissingFromHeartbeat fails the restored jobs of the
// heartbeat's worker that it leaves out of two itemized heartbeats in a row.
// A worker drops the jobs of an admin stream that closed, and a job it kept
// can no longer report its completion, so after an admin restart such a
// record would block dedupe and scheduling of its job type until the stale
// timeout. A listed job only resets its count: it can still be dropped
// later. The tracker goes once the job is failed here or found no longer to
// be the restored attempt.
func (r *Plugin) failRestoredJobsMissingFromHeartbeat(workerID string, heartbeat *plugin_pb.WorkerHeartbeat) {
	if heartbeat == nil {
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

	type candidate struct {
		jobID     string
		requestID string
	}
	var missing []candidate
	r.restoredExecutionsMu.Lock()
	for jobID, restored := range r.restoredExecutions {
		if restored.workerID != workerID {
			continue
		}
		if _, ok := listed[strings.TrimSpace(jobID)]; ok {
			restored.misses = 0
			continue
		}
		if !itemizesAll {
			continue
		}
		restored.misses++
		if restored.misses < orphanedExecutionMisses {
			continue
		}
		delete(r.restoredExecutions, jobID)
		missing = append(missing, candidate{jobID: jobID, requestID: restored.requestID})
	}
	r.restoredExecutionsMu.Unlock()

	now := time.Now().UTC()
	for _, c := range missing {
		isRestoredAttempt := func(job *TrackedJob) bool {
			return job != nil && isActiveTrackedJobState(job.State) &&
				job.RequestID == c.requestID && strings.TrimSpace(job.WorkerID) == workerID
		}
		r.jobsMu.RLock()
		job := r.jobs[c.jobID]
		var snap activeJobSnapshot
		current := isRestoredAttempt(job)
		if current {
			snap = activeJobSnapshot{
				jobID:      job.JobID,
				jobType:    job.JobType,
				workerID:   job.WorkerID,
				requestID:  job.RequestID,
				lastUpdate: jobLastUpdated(job),
			}
		}
		r.jobsMu.RUnlock()
		if !current {
			continue
		}
		reason := fmt.Sprintf("worker %s no longer runs job %s after the admin restarted", workerID, c.jobID)
		glog.Warningf("Plugin failing restored %s job: %s", snap.jobType, reason)
		// the record may change before the expiry takes the lock, so the
		// expiry judges it again there
		r.restoredExpiries.Add(1)
		go func() {
			defer r.restoredExpiries.Done()
			r.expireActiveJob(snap, reason, now, isRestoredAttempt)
		}()
	}
}
