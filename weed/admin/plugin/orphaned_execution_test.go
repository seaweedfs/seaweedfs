package plugin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const orphanTestJobType = "ec_vacuum"

var restoredTestStateRunning = strings.ToLower(plugin_pb.JobState_JOB_STATE_RUNNING.String())

type orphanTestResult struct {
	completed *plugin_pb.JobCompleted
	err       error
}

func connectExecutor(p *Plugin, workerID string) *streamSession {
	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId:         workerID,
		WorkerInstanceId: workerID + "-instance-" + time.Now().Format("150405.000000"),
		Capabilities:     []*plugin_pb.JobTypeCapability{{JobType: orphanTestJobType, CanExecute: true, MaxExecutionConcurrency: 2}},
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	p.putSession(session)
	return session
}

// dispatchToExecutor runs one execution on session's worker and returns the
// request the worker was sent along with the dispatch's eventual result.
func dispatchToExecutor(t *testing.T, p *Plugin, session *streamSession, jobID string) (string, <-chan orphanTestResult) {
	t.Helper()
	executor, ok := p.registry.Get(session.workerID)
	if !ok {
		t.Fatalf("worker %s is not registered", session.workerID)
	}
	resultCh := make(chan orphanTestResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		completed, err := p.executeJobWithExecutor(ctx, executor, &plugin_pb.JobSpec{JobId: jobID, JobType: orphanTestJobType}, &plugin_pb.ClusterContext{}, 1)
		resultCh <- orphanTestResult{completed: completed, err: err}
	}()
	select {
	case msg := <-session.outgoing:
		if msg.GetExecuteJobRequest() == nil {
			t.Fatalf("expected an execute_job_request, got %+v", msg)
		}
		return msg.RequestId, resultCh
	case <-time.After(2 * time.Second):
		t.Fatal("no execute request")
		return "", nil
	}
}

func sendExecutionHeartbeat(p *Plugin, session *streamSession, slotsUsed int32, jobIDs ...string) {
	heartbeat := &plugin_pb.WorkerHeartbeat{WorkerId: session.workerID, ExecutionSlotsUsed: slotsUsed, ExecutionSlotsTotal: 2}
	for _, jobID := range jobIDs {
		heartbeat.RunningWork = append(heartbeat.RunningWork, &plugin_pb.RunningWork{
			WorkId:  jobID,
			Kind:    plugin_pb.WorkKind_WORK_KIND_EXECUTION,
			JobType: orphanTestJobType,
			State:   plugin_pb.JobState_JOB_STATE_RUNNING,
		})
	}
	p.handleWorkerMessage(session, &plugin_pb.WorkerToAdminMessage{Body: &plugin_pb.WorkerToAdminMessage_Heartbeat{Heartbeat: heartbeat}})
}

func expectDispatchStillPending(t *testing.T, resultCh <-chan orphanTestResult) {
	t.Helper()
	select {
	case got := <-resultCh:
		t.Fatalf("dispatch ended early: completed=%+v err=%v", got.completed, got.err)
	case <-time.After(100 * time.Millisecond):
	}
}

// The worker process restarted after the job was dispatched: it comes back
// under the same id on a new stream and its heartbeats no longer list the
// job. The dispatch fails instead of holding its slot until the execution
// timeout.
func TestExecutionFailsWhenWorkerRestartsWithoutJob(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	const jobID = "job-lost-in-restart"
	requestID, resultCh := dispatchToExecutor(t, p, connectExecutor(p, "worker-a"), jobID)
	restarted := connectExecutor(p, "worker-a")

	sendExecutionHeartbeat(p, restarted, 0)
	expectDispatchStillPending(t, resultCh)
	sendExecutionHeartbeat(p, restarted, 0)

	select {
	case got := <-resultCh:
		if got.err == nil || got.completed == nil || got.completed.Success {
			t.Fatalf("expected a failed completion, got completed=%+v err=%v", got.completed, got.err)
		}
		if want := "worker worker-a re-registered without job " + jobID; !strings.Contains(got.err.Error(), want) {
			t.Fatalf("error %q does not say %q", got.err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch was not failed after its worker restarted without the job")
	}

	tracked, ok := p.GetTrackedJob(jobID)
	if !ok || tracked == nil {
		t.Fatalf("job %s is not tracked", jobID)
	}
	if tracked.State != StateFailed || tracked.RequestID != requestID {
		t.Fatalf("expected request %s failed, got state=%s request=%s", requestID, tracked.State, tracked.RequestID)
	}
}

// A worker that kept the job running across a reconnect lists it from the
// new stream; the dispatch keeps waiting and takes the completion from there.
func TestExecutionKeptWhenWorkerReconnectsWithJob(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	const jobID = "job-kept-across-reconnect"
	requestID, resultCh := dispatchToExecutor(t, p, connectExecutor(p, "worker-a"), jobID)
	reconnected := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, reconnected, 1, jobID)
	}
	expectDispatchStillPending(t, resultCh)

	p.handleWorkerMessage(reconnected, &plugin_pb.WorkerToAdminMessage{Body: &plugin_pb.WorkerToAdminMessage_JobCompleted{JobCompleted: &plugin_pb.JobCompleted{
		RequestId: requestID,
		JobId:     jobID,
		JobType:   orphanTestJobType,
		Success:   true,
	}}})
	select {
	case got := <-resultCh:
		if got.err != nil || got.completed == nil || !got.completed.Success {
			t.Fatalf("expected success, got completed=%+v err=%v", got.completed, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("completion from the reconnected stream did not reach the dispatch")
	}
}

// Work ids are compared trimmed, so a padded job id still matches its listing.
func TestExecutionKeptWhenReconnectedWorkerListsPaddedJobID(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	const jobID = " job-padded "
	_, resultCh := dispatchToExecutor(t, p, connectExecutor(p, "worker-a"), jobID)
	reconnected := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, reconnected, 1, jobID)
	}
	expectDispatchStillPending(t, resultCh)
}

// The worker counts a job before it lists it, and heartbeats can lag; the
// stream the job went out on is never judged by what it has not listed yet.
func TestExecutionKeptWhileDispatchSessionHasNotListedJob(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	session := connectExecutor(p, "worker-a")
	_, resultCh := dispatchToExecutor(t, p, session, "job-just-dispatched")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, session, 0)
		sendExecutionHeartbeat(p, session, 1)
	}
	expectDispatchStillPending(t, resultCh)
}

// A heartbeat that counts busy slots without itemizing them (the Rust worker
// reports only the count) cannot show a job is gone.
func TestExecutionKeptWhenHeartbeatDoesNotItemize(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	_, resultCh := dispatchToExecutor(t, p, connectExecutor(p, "worker-a"), "job-count-only")
	reconnected := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, reconnected, 1)
		sendExecutionHeartbeat(p, reconnected, 2, "some-other-job")
	}
	expectDispatchStillPending(t, resultCh)
}

// A listing between two omissions shows the worker still runs the job, even
// when that heartbeat also counts a slot it does not itemize.
func TestExecutionKeptWhenReplacementStreamListsJobBetweenOmissions(t *testing.T) {
	t.Parallel()
	for name, listing := range map[string]int32{"itemized": 1, "partly itemized": 2} {
		t.Run(name, func(t *testing.T) {
			p, err := New(Options{})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer p.Shutdown()

			const jobID = "job-listed-between-omissions"
			_, resultCh := dispatchToExecutor(t, p, connectExecutor(p, "worker-a"), jobID)
			reconnected := connectExecutor(p, "worker-a")

			sendExecutionHeartbeat(p, reconnected, 0)
			sendExecutionHeartbeat(p, reconnected, listing, jobID)
			sendExecutionHeartbeat(p, reconnected, 0)
			expectDispatchStillPending(t, resultCh)
		})
	}
}

func TestExecutionNotTrackedWhenWorkerNotConnected(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId:     "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{{JobType: orphanTestJobType, CanExecute: true, MaxExecutionConcurrency: 2}},
	})
	executor, ok := p.registry.Get("worker-a")
	if !ok {
		t.Fatal("worker-a is not registered")
	}
	const jobID = "job-without-session"
	if _, err := p.executeJobWithExecutor(context.Background(), executor, &plugin_pb.JobSpec{JobId: jobID, JobType: orphanTestJobType}, &plugin_pb.ClusterContext{}, 1); err == nil {
		t.Fatal("expected an error for a worker without a stream")
	}
	if tracked, ok := p.GetTrackedJob(jobID); ok && tracked != nil {
		t.Fatalf("job %s tracked as %s for a dispatch that never went out", jobID, tracked.State)
	}
}

// The captured stream closed before the request went out, as when the worker
// reconnects mid-dispatch: no report will ever close the attempt.
func TestExecutionFailedWhenSendToSessionFails(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId:     "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{{JobType: orphanTestJobType, CanExecute: true, MaxExecutionConcurrency: 2}},
	})
	closed := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage), done: make(chan struct{})}
	closed.close()
	p.putSession(closed)
	executor, ok := p.registry.Get("worker-a")
	if !ok {
		t.Fatal("worker-a is not registered")
	}
	const jobID = "job-send-failed"
	if _, err := p.executeJobWithExecutor(context.Background(), executor, &plugin_pb.JobSpec{JobId: jobID, JobType: orphanTestJobType}, &plugin_pb.ClusterContext{}, 1); err == nil {
		t.Fatal("expected an error for a closed stream")
	}
	tracked, ok := p.GetTrackedJob(jobID)
	if !ok || tracked == nil || tracked.State != StateFailed {
		t.Fatalf("expected job %s failed, got %+v", jobID, tracked)
	}
}

// Shutdown unregisters every pending dispatch before a blocked send returns
// its error; the attempt is still recorded as failed.
func TestExecutionFailedWhenSendFailsAfterShutdownUnregisters(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId:     "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{{JobType: orphanTestJobType, CanExecute: true, MaxExecutionConcurrency: 2}},
	})
	blocked := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage), done: make(chan struct{})}
	p.putSession(blocked)
	executor, ok := p.registry.Get("worker-a")
	if !ok {
		t.Fatal("worker-a is not registered")
	}
	const jobID = "job-send-after-shutdown"
	errCh := make(chan error, 1)
	go func() {
		_, err := p.executeJobWithExecutor(context.Background(), executor, &plugin_pb.JobSpec{JobId: jobID, JobType: orphanTestJobType}, &plugin_pb.ClusterContext{}, 1)
		errCh <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for unregistered := false; !unregistered; {
		if time.Now().After(deadline) {
			t.Fatal("dispatch never registered")
		}
		p.pendingExecutionMu.Lock()
		for requestID, pending := range p.pendingExecution {
			if pending.jobID == jobID {
				close(pending.completed)
				delete(p.pendingExecution, requestID)
				unregistered = true
			}
		}
		p.pendingExecutionMu.Unlock()
		time.Sleep(time.Millisecond)
	}
	blocked.close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error for a closed stream")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not return")
	}
	tracked, ok := p.GetTrackedJob(jobID)
	if !ok || tracked == nil || tracked.State != StateFailed {
		t.Fatalf("expected job %s failed, got %+v", jobID, tracked)
	}
}

// The dispatcher can take the completion from the buffer before it
// unregisters the attempt; heartbeats in that gap must not count misses.
func TestExecutionNotOrphanedAfterDispatcherTookCompletion(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	const jobID = "job-completion-taken"
	const requestID = "exec-completion-taken"
	original := connectExecutor(p, "worker-a")
	pending := &pendingExecutionState{
		completed: make(chan *plugin_pb.JobCompleted, 1),
		jobID:     jobID,
		jobType:   orphanTestJobType,
		session:   original,
	}
	p.pendingExecutionMu.Lock()
	p.pendingExecution[requestID] = pending
	p.pendingExecutionMu.Unlock()

	p.safeSendJobCompleted(requestID, &plugin_pb.JobCompleted{RequestId: requestID, JobId: jobID, JobType: orphanTestJobType, Success: true})
	<-pending.completed

	reconnected := connectExecutor(p, "worker-a")
	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, reconnected, 0)
	}
	p.pendingExecutionMu.Lock()
	failed := pending.failed
	p.pendingExecutionMu.Unlock()
	if failed {
		t.Fatal("attempt whose completion was already delivered was failed as orphaned")
	}
}

// newPluginWithRestoredJob starts an admin whose persisted state holds the
// job a previous admin process had sent to worker-a.
func newPluginWithRestoredJob(t *testing.T, job TrackedJob, details ...TrackedJob) *Plugin {
	t.Helper()
	dataDir := t.TempDir()
	store, err := NewConfigStore(dataDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	now := time.Now().UTC()
	job.JobType = orphanTestJobType
	job.WorkerID = "worker-a"
	if job.State == "" {
		job.State = restoredTestStateRunning
	}
	if job.RequestID == "" && job.State == restoredTestStateRunning {
		job.RequestID = "exec-before-restart"
	}
	if job.Attempt == 0 {
		job.Attempt = 1
	}
	job.CreatedAt = timeToPtr(now.Add(-time.Hour))
	job.UpdatedAt = timeToPtr(now.Add(-time.Minute))
	if err := store.SaveTrackedJobs([]TrackedJob{job}); err != nil {
		t.Fatalf("SaveTrackedJobs: %v", err)
	}
	for _, detail := range details {
		if err := store.SaveJobDetail(detail); err != nil {
			t.Fatalf("SaveJobDetail: %v", err)
		}
	}
	p, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(p.Shutdown)
	return p
}

func waitTrackedState(t *testing.T, p *Plugin, jobID, want string) *TrackedJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tracked, ok := p.GetTrackedJob(jobID)
		if ok && tracked != nil && tracked.State == want {
			return tracked
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s: want state %s, got %+v", jobID, want, tracked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// expectTrackedStateHolds checks the state once every restored-job expiry
// the heartbeats started has finished.
func expectTrackedStateHolds(t *testing.T, p *Plugin, jobID, want string) {
	t.Helper()
	p.restoredExpiries.Wait()
	if tracked, _ := p.GetTrackedJob(jobID); tracked == nil || tracked.State != want {
		t.Fatalf("job %s: want state %s, got %+v", jobID, want, tracked)
	}
}

// The admin restarted while worker-a ran the job, and the worker dropped it
// with the closed stream. Its heartbeats to the new admin do not list it, so
// the restored record is failed instead of blocking its job type for a day.
func TestRestoredJobFailsWhenWorkerDoesNotList(t *testing.T) {
	t.Parallel()
	const jobID = "job-from-previous-admin"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	session := connectExecutor(p, "worker-a")

	sendExecutionHeartbeat(p, session, 0)
	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
	sendExecutionHeartbeat(p, session, 0)

	tracked := waitTrackedState(t, p, jobID, StateFailed)
	if want := "worker worker-a no longer runs job " + jobID; !strings.Contains(tracked.ErrorMessage, want) {
		t.Fatalf("error %q does not say %q", tracked.ErrorMessage, want)
	}
}

// A worker that keeps listing the job keeps its restored record running.
func TestRestoredJobKeptWhileWorkerLists(t *testing.T) {
	t.Parallel()
	const jobID = "job-survived-admin-restart"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	session := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, session, 1, jobID)
	}
	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
}

// A job the worker listed at first and then dropped is still failed: being
// listed once does not end the tracking.
func TestRestoredJobFailsWhenWorkerStopsListing(t *testing.T) {
	t.Parallel()
	const jobID = "job-dropped-after-reconnect"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	session := connectExecutor(p, "worker-a")

	sendExecutionHeartbeat(p, session, 1, jobID)
	sendExecutionHeartbeat(p, session, 0)
	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
	sendExecutionHeartbeat(p, session, 0)
	waitTrackedState(t, p, jobID, StateFailed)
}

// Heartbeat ids are trimmed, so a padded job id still matches its listing.
func TestRestoredJobWithPaddedIDKeptWhileListed(t *testing.T) {
	t.Parallel()
	const jobID = " job-padded "
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	session := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, session, 1, jobID)
	}
	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
}

// A job the old admin had only queued names a worker but was never sent to
// it; no heartbeat would list it, and it never ran, so it is not failed as
// a lost execution.
func TestRestoredQueuedJobNotFailed(t *testing.T) {
	t.Parallel()
	const jobID = "job-queued-before-restart"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID, State: "job_state_pending"})
	session := connectExecutor(p, "worker-a")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, session, 0)
	}
	expectTrackedStateHolds(t, p, jobID, "job_state_pending")
}

// Another worker's heartbeats say nothing about the job.
func TestRestoredJobIgnoresOtherWorkers(t *testing.T) {
	t.Parallel()
	const jobID = "job-on-worker-a"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	other := connectExecutor(p, "worker-b")

	for i := 0; i < orphanedExecutionMisses+1; i++ {
		sendExecutionHeartbeat(p, other, 0)
	}
	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
}

// The expiry judges the record under the lock that fails it: once the job
// runs as a newer attempt, the restored attempt's expiry leaves it alone and
// sends no cancel.
func TestRestoredExpiryLeavesNewerAttempt(t *testing.T) {
	t.Parallel()
	const jobID = "job-dispatched-again"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	session := connectExecutor(p, "worker-a")

	p.jobsMu.Lock()
	p.jobs[jobID].RequestID = "exec-after-restart"
	p.jobsMu.Unlock()

	isRestoredAttempt := func(job *TrackedJob) bool {
		return job != nil && job.RequestID == "exec-before-restart"
	}
	p.expireActiveJob(activeJobSnapshot{jobID: jobID, jobType: orphanTestJobType, workerID: "worker-a", requestID: "exec-before-restart"},
		"stale", time.Now().UTC(), isRestoredAttempt)

	expectTrackedStateHolds(t, p, jobID, restoredTestStateRunning)
	select {
	case msg := <-session.outgoing:
		t.Fatalf("expected no cancel, got %+v", msg)
	default:
	}
}

// The expiry leaves the stored detail alone once a newer dispatch wrote it,
// so a restart does not load the newer attempt as failed.
func TestRestoredExpiryKeepsNewerAttemptDetail(t *testing.T) {
	t.Parallel()
	const jobID = "job-detail-moved-on"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID})
	p.persistJobDetailSnapshot(jobID, func(detail *TrackedJob) bool {
		detail.RequestID = "exec-after-restart"
		detail.State = restoredTestStateRunning
		detail.Attempt = 2
		detail.UpdatedAt = timeToPtr(time.Now().UTC())
		return true
	})

	p.expireActiveJob(activeJobSnapshot{jobID: jobID, jobType: orphanTestJobType, workerID: "worker-a", requestID: "exec-before-restart"},
		"stale", time.Now().UTC(), func(*TrackedJob) bool { return true })

	detail, err := p.store.LoadJobDetail(jobID)
	if err != nil || detail == nil {
		t.Fatalf("LoadJobDetail: %+v %v", detail, err)
	}
	if detail.RequestID != "exec-after-restart" || detail.State != restoredTestStateRunning {
		t.Fatalf("newer attempt's detail was overwritten: request=%s state=%s", detail.RequestID, detail.State)
	}
}

// A retry that finished after the job list was last saved is restored from
// its detail, request id included, rather than as the running attempt the
// job list still names.
func TestRestoreTakesFinishedRetryFromDetail(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		trackedAttempt, detailAttempt int32
	}{
		{name: "by attempt", trackedAttempt: 1, detailAttempt: 2},
		{name: "by time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			store, err := NewConfigStore(dataDir)
			if err != nil {
				t.Fatalf("NewConfigStore: %v", err)
			}
			now := time.Now().UTC()
			const jobID = "job-retried"
			if err := store.SaveTrackedJobs([]TrackedJob{{JobID: jobID, JobType: orphanTestJobType, RequestID: "exec-a", WorkerID: "worker-a", State: restoredTestStateRunning, Attempt: tc.trackedAttempt,
				CreatedAt: timeToPtr(now.Add(-time.Hour)), UpdatedAt: timeToPtr(now.Add(-time.Minute))}}); err != nil {
				t.Fatalf("SaveTrackedJobs: %v", err)
			}
			if err := store.SaveJobDetail(TrackedJob{JobID: jobID, JobType: orphanTestJobType, RequestID: "exec-b", WorkerID: "worker-b", State: StateSucceeded, Attempt: tc.detailAttempt,
				UpdatedAt: timeToPtr(now), CompletedAt: timeToPtr(now)}); err != nil {
				t.Fatalf("SaveJobDetail: %v", err)
			}
			p, err := New(Options{DataDir: dataDir})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer p.Shutdown()
			tracked, _ := p.GetTrackedJob(jobID)
			if tracked == nil || tracked.State != StateSucceeded || tracked.RequestID != "exec-b" ||
				tracked.WorkerID != "worker-b" || tracked.Attempt != tc.detailAttempt {
				t.Fatalf("want exec-b on worker-b attempt %d succeeded, got %+v", tc.detailAttempt, tracked)
			}
		})
	}
}

// A detail still holding an older finished attempt, after the newer retry's
// detail write failed, does not hide the retry the job list names.
func TestRestoreKeepsNewerRetryOverOlderDetail(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		trackedAttempt, detailAttempt int32
	}{
		{name: "by attempt", trackedAttempt: 2, detailAttempt: 1},
		{name: "by time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			store, err := NewConfigStore(dataDir)
			if err != nil {
				t.Fatalf("NewConfigStore: %v", err)
			}
			now := time.Now().UTC()
			const jobID = "job-retry-running"
			if err := store.SaveTrackedJobs([]TrackedJob{{JobID: jobID, JobType: orphanTestJobType, RequestID: "exec-b", State: restoredTestStateRunning, Attempt: tc.trackedAttempt,
				CreatedAt: timeToPtr(now.Add(-time.Hour)), UpdatedAt: timeToPtr(now)}}); err != nil {
				t.Fatalf("SaveTrackedJobs: %v", err)
			}
			if err := store.SaveJobDetail(TrackedJob{JobID: jobID, JobType: orphanTestJobType, RequestID: "exec-a", State: StateFailed, Attempt: tc.detailAttempt,
				UpdatedAt: timeToPtr(now.Add(-time.Minute)), CompletedAt: timeToPtr(now.Add(-time.Minute))}); err != nil {
				t.Fatalf("SaveJobDetail: %v", err)
			}
			p, err := New(Options{DataDir: dataDir})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer p.Shutdown()
			tracked, _ := p.GetTrackedJob(jobID)
			if tracked == nil || tracked.State != restoredTestStateRunning || tracked.RequestID != "exec-b" {
				t.Fatalf("want exec-b running, got %+v", tracked)
			}
		})
	}
}

// A job the list saved while still queued takes its request id from the
// finished detail, so the ended attempt stays recognizable.
func TestRestoreFillsRequestIDOfQueuedJobFromDetail(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, err := NewConfigStore(dataDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	now := time.Now().UTC()
	const jobID = "job-queued-then-done"
	if err := store.SaveTrackedJobs([]TrackedJob{{JobID: jobID, JobType: orphanTestJobType, State: "job_state_pending",
		CreatedAt: timeToPtr(now.Add(-time.Hour)), UpdatedAt: timeToPtr(now.Add(-time.Minute))}}); err != nil {
		t.Fatalf("SaveTrackedJobs: %v", err)
	}
	if err := store.SaveJobDetail(TrackedJob{JobID: jobID, JobType: orphanTestJobType, RequestID: "exec-a", State: StateSucceeded,
		UpdatedAt: timeToPtr(now), CompletedAt: timeToPtr(now)}); err != nil {
		t.Fatalf("SaveJobDetail: %v", err)
	}
	p, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()
	tracked, _ := p.GetTrackedJob(jobID)
	if tracked == nil || tracked.State != StateSucceeded || tracked.RequestID != "exec-a" {
		t.Fatalf("want exec-a succeeded, got %+v", tracked)
	}
}

// The restored attempt's detail write failed before the restart, so the
// detail still holds an older finished attempt. Failing the restored
// attempt replaces that detail, or the next restart would load it running.
func TestRestoredExpiryReplacesOlderAttemptDetail(t *testing.T) {
	t.Parallel()
	const jobID = "job-detail-behind"
	older := time.Now().UTC().Add(-2 * time.Hour)
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID}, TrackedJob{JobID: jobID, JobType: orphanTestJobType,
		RequestID: "exec-older", State: StateFailed, UpdatedAt: timeToPtr(older), CompletedAt: timeToPtr(older)})
	session := connectExecutor(p, "worker-a")

	sendExecutionHeartbeat(p, session, 0)
	sendExecutionHeartbeat(p, session, 0)
	waitTrackedState(t, p, jobID, StateFailed)
	p.restoredExpiries.Wait()

	detail, err := p.store.LoadJobDetail(jobID)
	if err != nil || detail == nil {
		t.Fatalf("LoadJobDetail: %+v %v", detail, err)
	}
	if detail.RequestID != "exec-before-restart" || detail.State != StateFailed {
		t.Fatalf("want exec-before-restart failed in the detail, got request=%s state=%s", detail.RequestID, detail.State)
	}
	if detail.WorkerID != "worker-a" || detail.Attempt != 1 || detail.CompletedAt == nil || !detail.CompletedAt.After(older) {
		t.Fatalf("detail kept the older attempt's fields: worker=%s attempt=%d completed=%v", detail.WorkerID, detail.Attempt, detail.CompletedAt)
	}
}

// A completion that arrives for an earlier attempt after a retry started
// leaves the retry's record alone.
func TestLateCompletionOfEarlierAttemptKeepsRetry(t *testing.T) {
	t.Parallel()
	const jobID = "job-retry-running"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID, RequestID: "exec-b"})

	p.handleJobCompleted(&plugin_pb.JobCompleted{JobId: jobID, JobType: orphanTestJobType, RequestId: "exec-a", Success: false, ErrorMessage: "late"}, nil)

	tracked, _ := p.GetTrackedJob(jobID)
	if tracked == nil || tracked.State != restoredTestStateRunning || tracked.RequestID != "exec-b" {
		t.Fatalf("want exec-b running, got %+v", tracked)
	}
}

// A late progress report of an earlier attempt neither takes over the
// retry's request id nor makes the retry's own completion look foreign.
func TestLateProgressOfEarlierAttemptKeepsRetry(t *testing.T) {
	t.Parallel()
	const jobID = "job-retry-late-progress"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID, RequestID: "exec-b"})

	p.handleJobProgressUpdate("worker-a", &plugin_pb.JobProgressUpdate{JobId: jobID, JobType: orphanTestJobType, RequestId: "exec-a",
		State: plugin_pb.JobState_JOB_STATE_RUNNING, ProgressPercent: 10})
	if tracked, _ := p.GetTrackedJob(jobID); tracked == nil || tracked.RequestID != "exec-b" {
		t.Fatalf("late progress took over the record: %+v", tracked)
	}

	p.handleJobCompleted(&plugin_pb.JobCompleted{JobId: jobID, JobType: orphanTestJobType, RequestId: "exec-b", Success: true}, nil)
	if tracked, _ := p.GetTrackedJob(jobID); tracked == nil || tracked.State != StateSucceeded || tracked.RequestID != "exec-b" {
		t.Fatalf("want exec-b succeeded, got %+v", tracked)
	}
}

// Replacing an older attempt's detail writes the restored attempt's
// identity: its request, attempt number and worker, not the older ones.
func TestRestoredExpiryReplacesOlderAttemptIdentity(t *testing.T) {
	t.Parallel()
	const jobID = "job-detail-older-attempt"
	older := time.Now().UTC().Add(-2 * time.Hour)
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID, RequestID: "exec-b", Attempt: 2}, TrackedJob{JobID: jobID, JobType: orphanTestJobType,
		RequestID: "exec-a", WorkerID: "worker-old", Attempt: 1, State: StateFailed, UpdatedAt: timeToPtr(older), CompletedAt: timeToPtr(older)})
	session := connectExecutor(p, "worker-a")

	sendExecutionHeartbeat(p, session, 0)
	sendExecutionHeartbeat(p, session, 0)
	waitTrackedState(t, p, jobID, StateFailed)
	p.restoredExpiries.Wait()

	detail, err := p.store.LoadJobDetail(jobID)
	if err != nil || detail == nil {
		t.Fatalf("LoadJobDetail: %+v %v", detail, err)
	}
	if detail.RequestID != "exec-b" || detail.Attempt != 2 || detail.WorkerID != "worker-a" || detail.State != StateFailed {
		t.Fatalf("want exec-b/2 on worker-a failed, got request=%s attempt=%d worker=%s state=%s",
			detail.RequestID, detail.Attempt, detail.WorkerID, detail.State)
	}
}

// The job list names attempt A, but the detail shows retry B was running
// when the admin stopped. The record takes B's identity, so B's completion
// is accepted rather than dropped as another attempt's.
func TestRestoreTakesNewerRunningAttemptFromDetail(t *testing.T) {
	t.Parallel()
	const jobID = "job-retry-running-at-restart"
	p := newPluginWithRestoredJob(t, TrackedJob{JobID: jobID, RequestID: "exec-a"}, TrackedJob{JobID: jobID, JobType: orphanTestJobType,
		RequestID: "exec-b", WorkerID: "worker-a", Attempt: 2, State: restoredTestStateRunning, UpdatedAt: timeToPtr(time.Now().UTC())})

	tracked, _ := p.GetTrackedJob(jobID)
	if tracked == nil || tracked.RequestID != "exec-b" || tracked.Attempt != 2 || tracked.State != restoredTestStateRunning {
		t.Fatalf("want exec-b/2 running, got %+v", tracked)
	}
	p.handleJobCompleted(&plugin_pb.JobCompleted{JobId: jobID, JobType: orphanTestJobType, RequestId: "exec-b", Success: true}, nil)
	if tracked, _ := p.GetTrackedJob(jobID); tracked == nil || tracked.State != StateSucceeded {
		t.Fatalf("want exec-b succeeded, got %+v", tracked)
	}
}

// A dispatch without an attempt number is still the admin's own attempt, so a
// late completion of another request for the job does not close it.
func TestDispatchWithoutAttemptKeepsOwnRequest(t *testing.T) {
	t.Parallel()
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	session := connectExecutor(p, "worker-a")
	executor, ok := p.registry.Get(session.workerID)
	if !ok {
		t.Fatal("worker-a is not registered")
	}
	const jobID = "job-attempt-zero"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.executeJobWithExecutor(ctx, executor, &plugin_pb.JobSpec{JobId: jobID, JobType: orphanTestJobType}, &plugin_pb.ClusterContext{}, 0)
	}()
	defer func() {
		cancel()
		<-done
	}()

	var requestID string
	select {
	case msg := <-session.outgoing:
		if got := msg.GetExecuteJobRequest().GetAttempt(); got != 1 {
			t.Fatalf("want attempt 1 sent to the worker, got %d", got)
		}
		requestID = msg.RequestId
	case <-time.After(2 * time.Second):
		t.Fatal("no execute request")
	}

	p.handleJobCompleted(&plugin_pb.JobCompleted{JobId: jobID, JobType: orphanTestJobType, RequestId: "exec-earlier", Success: false, ErrorMessage: "late"}, nil)
	tracked, _ := p.GetTrackedJob(jobID)
	if tracked == nil || tracked.RequestID != requestID || !isActiveTrackedJobState(tracked.State) {
		t.Fatalf("want %s still active, got %+v", requestID, tracked)
	}
}
