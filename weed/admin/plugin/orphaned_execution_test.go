package plugin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const orphanTestJobType = "ec_vacuum"

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
