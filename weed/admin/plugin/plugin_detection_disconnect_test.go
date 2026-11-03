package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/require"
)

func newDetectorSession(p *Plugin, workerID, jobType string) *streamSession {
	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId:         workerID,
		WorkerInstanceId: workerID + "-instance-" + time.Now().Format("150405.000000"),
		Capabilities:     []*plugin_pb.JobTypeCapability{{JobType: jobType, CanDetect: true, MaxDetectionConcurrency: 1}},
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	p.putSession(session)
	return session
}

func awaitDetectionRequest(t *testing.T, session *streamSession) *plugin_pb.AdminToWorkerMessage {
	t.Helper()
	select {
	case request := <-session.outgoing:
		require.NotNil(t, request.GetRunDetectionRequest(), "expected run_detection_request, got %+v", request)
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("no detection request")
		return nil
	}
}

func findActivity(activities []JobActivity, source, stage string) *JobActivity {
	for i := range activities {
		if activities[i].Source == source && activities[i].Stage == stage {
			return &activities[i]
		}
	}
	return nil
}

func (r *Plugin) detectionInFlightForTest(jobType string) bool {
	r.schedulerMu.Lock()
	defer r.schedulerMu.Unlock()
	return r.detectionInFlight[jobType]
}

func (r *Plugin) pendingDetectionCountForTest() int {
	r.pendingDetectionMu.Lock()
	defer r.pendingDetectionMu.Unlock()
	return len(r.pendingDetection)
}

func TestRunDetectionFailsWhenDetectorDisconnects(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	type result struct {
		report *DetectionReport
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		report, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		resultCh <- result{report: report, err: runErr}
	}()
	request := awaitDetectionRequest(t, session)

	p.WorkerDisconnectForTest("worker-a")

	var got result
	select {
	case got = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not fail after its worker disconnected")
	}
	require.Error(t, got.err)
	var lost *detectionWorkerDisconnectedError
	require.True(t, errors.As(got.err, &lost), "unexpected error type: %v", got.err)
	require.Equal(t, "detection worker worker-a disconnected", got.err.Error())
	require.NotNil(t, got.report)
	require.Equal(t, request.RequestId, got.report.RequestID)
	require.Equal(t, 0, p.pendingDetectionCountForTest())
	require.Equal(t, "", p.getDetectorLease(jobType), "detector lease must not pin the lost worker")

	activity := findActivity(p.ListActivities(jobType, 50), "detector", "disconnected")
	require.NotNil(t, activity, "expected a disconnected detector activity")
	require.Equal(t, request.RequestId, activity.RequestID)
	require.Equal(t, "worker-a", activity.WorkerID)

	// A replacement worker can serve the next detection right away.
	next := newDetectorSession(p, "worker-b", jobType)
	go func() {
		_, _ = p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
	}()
	awaitDetectionRequest(t, next)
}

func TestRunDetectionFailsWhenDetectorReconnectsWithNewSession(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()
	awaitDetectionRequest(t, session)

	// The worker process restarted: same worker ID, new instance, new stream.
	replacement := newDetectorSession(p, "worker-a", jobType)

	select {
	case runErr := <-errCh:
		var lost *detectionWorkerDisconnectedError
		require.True(t, errors.As(runErr, &lost), "unexpected error: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not fail after its worker reconnected with a new session")
	}
	require.Len(t, replacement.outgoing, 0, "the replacement session must not receive a cancel for the old request")

	// The old stream unwinding afterwards must not evict the new session.
	p.cleanupSession(session)
	current, ok := p.getSession("worker-a")
	require.True(t, ok)
	require.Same(t, replacement, current)
	_, registered := p.registry.Get("worker-a")
	require.True(t, registered)
}

func TestRunDetectionSurvivesOtherWorkerDisconnect(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	sessionA := newDetectorSession(p, "worker-a", jobType)
	sessionB := newDetectorSession(p, "worker-b", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()

	var request *plugin_pb.AdminToWorkerMessage
	detector, other := "", ""
	select {
	case request = <-sessionA.outgoing:
		detector, other = "worker-a", "worker-b"
	case request = <-sessionB.outgoing:
		detector, other = "worker-b", "worker-a"
	case <-time.After(2 * time.Second):
		t.Fatal("no detection request")
	}
	require.NotNil(t, request.GetRunDetectionRequest())

	p.WorkerDisconnectForTest(other)

	select {
	case runErr := <-errCh:
		t.Fatalf("detection on %s ended after %s disconnected: %v", detector, other, runErr)
	case <-time.After(300 * time.Millisecond):
	}
	require.Equal(t, 1, p.pendingDetectionCountForTest())

	p.handleDetectionComplete(detector, &plugin_pb.DetectionComplete{RequestId: request.RequestId, JobType: jobType, Success: true})
	select {
	case runErr := <-errCh:
		require.NoError(t, runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("detection did not complete")
	}
}

func TestRunDetectionFailsWhenDetectorHeartbeatGoesStale(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	p.registry.staleAfter = 50 * time.Millisecond
	p.detectionLivenessInterval = 20 * time.Millisecond

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()
	request := awaitDetectionRequest(t, session)

	select {
	case runErr := <-errCh:
		require.Error(t, runErr)
		require.Equal(t, "detection worker worker-a disconnected: heartbeat stale", runErr.Error())
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not fail after its worker went stale")
	}

	// The stream may still be open, so the worker is asked to stop the request.
	select {
	case message := <-session.outgoing:
		cancelReq := message.GetCancelRequest()
		require.NotNil(t, cancelReq, "expected cancel_request, got %+v", message)
		require.Equal(t, request.RequestId, cancelReq.TargetId)
		require.Equal(t, plugin_pb.WorkKind_WORK_KIND_DETECTION, cancelReq.TargetKind)
	case <-time.After(2 * time.Second):
		t.Fatal("no cancel request after stale worker")
	}
}

func TestScheduledDetectionFailsFastOnWorkerDisconnect(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	p.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
		return &plugin_pb.ClusterContext{}, nil
	}

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	done := make(chan struct{})
	go func() {
		p.runJobTypeIteration(jobType, schedulerPolicy{DetectionTimeout: time.Hour, JobTypeMaxRuntime: time.Hour})
		close(done)
	}()
	awaitDetectionRequest(t, session)
	require.True(t, p.detectionInFlightForTest(jobType))

	p.WorkerDisconnectForTest("worker-a")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled iteration kept waiting after its detector disconnected")
	}
	require.False(t, p.detectionInFlightForTest(jobType))
	require.Equal(t, 0, p.pendingDetectionCountForTest())
	require.Equal(t, "error", p.snapshotSchedulerRun(jobType).lastRunStatus)
	require.False(t, p.snapshotSchedulerDetection(jobType).lastErrorAt.IsZero())

	activities := p.ListActivities(jobType, 50)
	require.NotNil(t, findActivity(activities, "detector", "disconnected"))
	retrying := findActivity(activities, "admin_scheduler", "retrying")
	require.NotNil(t, retrying, "the scheduler should have tried a replacement pick")
	require.Contains(t, retrying.Message, "detection worker worker-a disconnected")
	failed := findActivity(activities, "admin_scheduler", "failed")
	require.NotNil(t, failed, "with no other detector the run must end as failed")

	// The next cycle can detect again.
	next := newDetectorSession(p, "worker-b", jobType)
	go func() {
		p.runJobTypeIteration(jobType, schedulerPolicy{DetectionTimeout: time.Hour, JobTypeMaxRuntime: time.Hour})
	}()
	awaitDetectionRequest(t, next)
}

func TestScheduledDetectionRetriesOnAnotherWorkerAfterDisconnect(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	p.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
		return &plugin_pb.ClusterContext{}, nil
	}

	const jobType = "vacuum"
	sessions := map[string]*streamSession{
		"worker-a": newDetectorSession(p, "worker-a", jobType),
		"worker-b": newDetectorSession(p, "worker-b", jobType),
	}

	done := make(chan struct{})
	go func() {
		p.runJobTypeIteration(jobType, schedulerPolicy{DetectionTimeout: time.Hour, JobTypeMaxRuntime: time.Hour})
		close(done)
	}()

	first, other := "", ""
	select {
	case <-sessions["worker-a"].outgoing:
		first, other = "worker-a", "worker-b"
	case <-sessions["worker-b"].outgoing:
		first, other = "worker-b", "worker-a"
	case <-time.After(2 * time.Second):
		t.Fatal("no detection request")
	}

	p.WorkerDisconnectForTest(first)

	retry := awaitDetectionRequest(t, sessions[other])
	p.handleDetectionComplete(other, &plugin_pb.DetectionComplete{RequestId: retry.RequestId, JobType: jobType, Success: true})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled iteration did not finish after the retry completed")
	}
	require.False(t, p.detectionInFlightForTest(jobType))
	require.Equal(t, "success", p.snapshotSchedulerRun(jobType).lastRunStatus)
	require.False(t, p.snapshotSchedulerDetection(jobType).lastDetectedAt.IsZero())
	require.NotNil(t, findActivity(p.ListActivities(jobType, 50), "admin_scheduler", "retrying"))
}

func TestRegisterSessionKeepsRegistryOnLiveStream(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	helloA := &plugin_pb.WorkerHello{WorkerId: "worker-a", WorkerInstanceId: "a-1", Capabilities: []*plugin_pb.JobTypeCapability{{JobType: "ec_vacuum", CanDetect: true}}}
	helloB := &plugin_pb.WorkerHello{WorkerId: "worker-a", WorkerInstanceId: "a-2", Capabilities: []*plugin_pb.JobTypeCapability{{JobType: "s3_lifecycle", CanDetect: true}}}
	streamA := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	streamB := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}

	p.registerSession(streamA, helloA)
	p.registerSession(streamB, helloB)
	select {
	case <-streamA.done:
	default:
		t.Fatal("the replaced stream must be closed")
	}

	// Stream A's hello and heartbeat arriving after it was replaced must not
	// describe the live stream B, and neither must A's own cleanup.
	p.handleWorkerMessage(streamA, &plugin_pb.WorkerToAdminMessage{Body: &plugin_pb.WorkerToAdminMessage_Hello{Hello: helloA}})
	p.handleWorkerMessage(streamA, &plugin_pb.WorkerToAdminMessage{Body: &plugin_pb.WorkerToAdminMessage_Heartbeat{Heartbeat: &plugin_pb.WorkerHeartbeat{DetectionSlotsTotal: 9}}})
	p.cleanupSession(streamA)

	worker, ok := p.registry.Get("worker-a")
	require.True(t, ok)
	require.Equal(t, "a-2", worker.WorkerInstance)
	require.Nil(t, worker.Capabilities["ec_vacuum"])
	require.NotNil(t, worker.Capabilities["s3_lifecycle"])
	require.Nil(t, worker.Heartbeat, "a superseded stream's heartbeat must be dropped")
	current, ok := p.getSession("worker-a")
	require.True(t, ok)
	require.Same(t, streamB, current)
	_, err = p.pickDetector("ec_vacuum", nil)
	require.Error(t, err, "the capability only stream A advertised must be gone")

	p.handleWorkerMessage(streamB, &plugin_pb.WorkerToAdminMessage{Body: &plugin_pb.WorkerToAdminMessage_Heartbeat{Heartbeat: &plugin_pb.WorkerHeartbeat{DetectionSlotsTotal: 2}}})
	worker, ok = p.registry.Get("worker-a")
	require.True(t, ok)
	require.NotNil(t, worker.Heartbeat)
	require.EqualValues(t, 2, worker.Heartbeat.DetectionSlotsTotal)
}

func TestScheduledDetectionRetryAvoidsClosedDetectorStream(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	p.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
		return &plugin_pb.ClusterContext{}, nil
	}

	const jobType = "vacuum"
	sessionA := newDetectorSession(p, "worker-a", jobType)
	sessionB := newDetectorSession(p, "worker-b", jobType)
	// worker-a ranks first on free detection slots, for the first pick and
	// for any retry that still sees it in the registry.
	p.registry.UpdateHeartbeat("worker-a", &plugin_pb.WorkerHeartbeat{DetectionSlotsTotal: 4})
	p.registry.UpdateHeartbeat("worker-b", &plugin_pb.WorkerHeartbeat{DetectionSlotsTotal: 1})

	done := make(chan struct{})
	go func() {
		p.runJobTypeIteration(jobType, schedulerPolicy{DetectionTimeout: time.Hour, JobTypeMaxRuntime: time.Hour})
		close(done)
	}()
	awaitDetectionRequest(t, sessionA)

	// Close the stream while its registry entry is still present: the
	// ordering where the retry picks before the disconnect is fully cleaned up.
	sessionA.close()

	retry := awaitDetectionRequest(t, sessionB)
	require.Equal(t, "worker-b", p.getDetectorLease(jobType))
	p.handleDetectionComplete("worker-b", &plugin_pb.DetectionComplete{RequestId: retry.RequestId, JobType: jobType, Success: true})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled iteration did not finish after the retry completed")
	}
	require.Equal(t, "success", p.snapshotSchedulerRun(jobType).lastRunStatus)
	require.Len(t, sessionA.outgoing, 0, "nothing may be sent on the closed stream")
}

// The stream delivered the completion and then closed before the waiter
// reached its select, so both channels are ready at once.
func TestAwaitDetectionOutcomePrefersBufferedCompletionOverClosedStream(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)
	p.setDetectorLease(jobType, "worker-a")
	state := &pendingDetectionState{
		complete: make(chan *plugin_pb.DetectionComplete, 1),
		jobType:  jobType,
		workerID: "worker-a",
	}
	state.proposals = []*plugin_pb.JobProposal{{ProposalId: "p-1", JobType: jobType}}
	state.complete <- &plugin_pb.DetectionComplete{RequestId: "detect-1", JobType: jobType, Success: true, TotalProposals: 1}
	p.WorkerDisconnectForTest("worker-a")

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	report, err := p.awaitDetectionOutcome(ctx, jobType, "detect-1", session, state)
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Equal(t, "detect-1", report.RequestID)
	require.Equal(t, "worker-a", report.WorkerID)
	require.True(t, report.Complete.GetSuccess())
	require.Len(t, report.Proposals, 1)
	require.Equal(t, "p-1", report.Proposals[0].ProposalId)
	require.Nil(t, findActivity(p.ListActivities(jobType, 50), "detector", "disconnected"), "a completed detection must not be reported as disconnected")

	// The next pick must not be pinned to the gone worker.
	newDetectorSession(p, "worker-b", jobType)
	picked, err := p.pickDetector(jobType, nil)
	require.NoError(t, err)
	require.Equal(t, "worker-b", picked.WorkerID)
	require.Equal(t, "worker-b", p.getDetectorLease(jobType))
}

func TestRunDetectionKeepsCompletionDeliveredBeforeDisconnect(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	type result struct {
		report *DetectionReport
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		report, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		resultCh <- result{report: report, err: runErr}
	}()
	request := awaitDetectionRequest(t, session)

	// The worker's stream carries proposals and the completion, then ends:
	// the worker finished its scan and restarted.
	p.handleDetectionProposals("worker-a", &plugin_pb.DetectionProposals{
		RequestId: request.RequestId,
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{{ProposalId: "p-1", JobType: jobType}},
	})
	p.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{RequestId: request.RequestId, JobType: jobType, Success: true, TotalProposals: 1})
	p.WorkerDisconnectForTest("worker-a")

	var got result
	select {
	case got = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not return")
	}
	require.NoError(t, got.err)
	require.NotNil(t, got.report)
	require.Equal(t, request.RequestId, got.report.RequestID)
	require.True(t, got.report.Complete.GetSuccess())
	require.Len(t, got.report.Proposals, 1)
	require.Equal(t, "p-1", got.report.Proposals[0].ProposalId)
	require.Equal(t, 0, p.pendingDetectionCountForTest())
	require.Nil(t, findActivity(p.ListActivities(jobType, 50), "detector", "disconnected"))

	// The next detection goes to a live worker, not the gone one.
	next := newDetectorSession(p, "worker-b", jobType)
	go func() {
		_, _ = p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
	}()
	awaitDetectionRequest(t, next)
	require.Equal(t, "worker-b", p.getDetectorLease(jobType))
}

// A completion that lands after the waiter already gave up on the request
// must not satisfy the next detection of the same job type.
func TestLateDetectionCompletionDoesNotReachNextDetection(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()

	const jobType = "vacuum"
	session := newDetectorSession(p, "worker-a", jobType)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()
	lost := awaitDetectionRequest(t, session)
	p.WorkerDisconnectForTest("worker-a")
	select {
	case runErr := <-errCh:
		var disconnected *detectionWorkerDisconnectedError
		require.True(t, errors.As(runErr, &disconnected), "unexpected error: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not fail after its worker disconnected")
	}
	require.Equal(t, 0, p.pendingDetectionCountForTest())

	next := newDetectorSession(p, "worker-b", jobType)
	type result struct {
		report *DetectionReport
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		report, runErr := p.RunDetectionWithReport(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		resultCh <- result{report: report, err: runErr}
	}()
	request := awaitDetectionRequest(t, next)
	require.NotEqual(t, lost.RequestId, request.RequestId)

	// The old worker's answer arrives late, under the old request ID.
	p.handleDetectionProposals("worker-a", &plugin_pb.DetectionProposals{
		RequestId: lost.RequestId,
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{{ProposalId: "stale", JobType: jobType}},
	})
	p.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{RequestId: lost.RequestId, JobType: jobType, Success: true, TotalProposals: 1})
	select {
	case got := <-resultCh:
		t.Fatalf("the late completion ended the next detection: %+v %v", got.report, got.err)
	case <-time.After(300 * time.Millisecond):
	}
	require.Equal(t, 1, p.pendingDetectionCountForTest())

	p.handleDetectionProposals("worker-b", &plugin_pb.DetectionProposals{
		RequestId: request.RequestId,
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{{ProposalId: "fresh", JobType: jobType}},
	})
	p.handleDetectionComplete("worker-b", &plugin_pb.DetectionComplete{RequestId: request.RequestId, JobType: jobType, Success: true, TotalProposals: 1})
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.Equal(t, request.RequestId, got.report.RequestID)
		require.Len(t, got.report.Proposals, 1)
		require.Equal(t, "fresh", got.report.Proposals[0].ProposalId)
	case <-time.After(5 * time.Second):
		t.Fatal("detection did not complete")
	}
}
