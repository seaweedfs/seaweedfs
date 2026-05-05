package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRunDetectionSendsCancelOnContextDone(t *testing.T) {
	t.Parallel()
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-detect"
	const jobType = "vacuum"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, MaxDetectionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 4), done: make(chan struct{})}
	pluginSvc.putSession(session)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.RunDetection(ctx, jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()

	first := <-session.outgoing
	if first.GetRunDetectionRequest() == nil {
		t.Fatalf("expected first message to be run_detection_request")
	}

	cancel()

	second := <-session.outgoing
	cancelReq := second.GetCancelRequest()
	if cancelReq == nil {
		t.Fatalf("expected second message to be cancel_request")
	}
	if cancelReq.TargetId != first.RequestId {
		t.Fatalf("unexpected cancel target id: got=%s want=%s", cancelReq.TargetId, first.RequestId)
	}
	if cancelReq.TargetKind != plugin_pb.WorkKind_WORK_KIND_DETECTION {
		t.Fatalf("unexpected cancel target kind: %v", cancelReq.TargetKind)
	}

	runErr := <-errCh
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", runErr)
	}
}

func TestExecuteJobSendsCancelOnContextDone(t *testing.T) {
	t.Parallel()
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-exec"
	const jobType = "vacuum"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 4), done: make(chan struct{})}
	pluginSvc.putSession(session)

	job := &plugin_pb.JobSpec{JobId: "job-1", JobType: jobType}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.ExecuteJob(ctx, job, &plugin_pb.ClusterContext{}, 1)
		errCh <- runErr
	}()

	first := <-session.outgoing
	if first.GetExecuteJobRequest() == nil {
		t.Fatalf("expected first message to be execute_job_request")
	}

	cancel()

	second := <-session.outgoing
	cancelReq := second.GetCancelRequest()
	if cancelReq == nil {
		t.Fatalf("expected second message to be cancel_request")
	}
	if cancelReq.TargetId != first.RequestId {
		t.Fatalf("unexpected cancel target id: got=%s want=%s", cancelReq.TargetId, first.RequestId)
	}
	if cancelReq.TargetKind != plugin_pb.WorkKind_WORK_KIND_EXECUTION {
		t.Fatalf("unexpected cancel target kind: %v", cancelReq.TargetKind)
	}

	runErr := <-errCh
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", runErr)
	}
}

func TestAdminScriptExecutionBlocksOtherDetection(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	const adminWorkerID = "worker-admin-script"
	const otherWorkerID = "worker-vacuum"

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: adminWorkerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: otherWorkerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, MaxDetectionConcurrency: 1},
		},
	})
	adminSession := &streamSession{workerID: adminWorkerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	otherSession := &streamSession{workerID: otherWorkerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	pluginSvc.putSession(adminSession)
	pluginSvc.putSession(otherSession)

	adminErrCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.ExecuteJob(context.Background(), &plugin_pb.JobSpec{
			JobId:   "job-admin-script-1",
			JobType: "admin_script",
		}, &plugin_pb.ClusterContext{}, 1)
		adminErrCh <- runErr
	}()

	adminExecMessage := <-adminSession.outgoing
	if adminExecMessage.GetExecuteJobRequest() == nil {
		t.Fatalf("expected admin_script execute request")
	}

	detectErrCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.RunDetection(context.Background(), "vacuum", &plugin_pb.ClusterContext{}, 10)
		detectErrCh <- runErr
	}()

	select {
	case unexpected := <-otherSession.outgoing:
		t.Fatalf("expected vacuum detection to wait while admin_script runs, got message: %+v", unexpected)
	case <-time.After(100 * time.Millisecond):
	}

	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId:   adminExecMessage.RequestId,
		JobId:       "job-admin-script-1",
		JobType:     "admin_script",
		Success:     true,
		CompletedAt: timestamppb.Now(),
	})
	if runErr := <-adminErrCh; runErr != nil {
		t.Fatalf("admin_script ExecuteJob error: %v", runErr)
	}

	detectMessage := <-otherSession.outgoing
	detectRequest := detectMessage.GetRunDetectionRequest()
	if detectRequest == nil {
		t.Fatalf("expected vacuum detection request after admin_script completion")
	}
	pluginSvc.handleDetectionComplete(otherWorkerID, &plugin_pb.DetectionComplete{
		RequestId: detectMessage.RequestId,
		JobType:   "vacuum",
		Success:   true,
	})
	if runErr := <-detectErrCh; runErr != nil {
		t.Fatalf("vacuum RunDetection error: %v", runErr)
	}
}

func TestAdminScriptExecutionBlocksOtherExecution(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	const adminWorkerID = "worker-admin-script"
	const otherWorkerID = "worker-vacuum"

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: adminWorkerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: otherWorkerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	adminSession := &streamSession{workerID: adminWorkerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	otherSession := &streamSession{workerID: otherWorkerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	pluginSvc.putSession(adminSession)
	pluginSvc.putSession(otherSession)

	adminErrCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.ExecuteJob(context.Background(), &plugin_pb.JobSpec{
			JobId:   "job-admin-script-2",
			JobType: "admin_script",
		}, &plugin_pb.ClusterContext{}, 1)
		adminErrCh <- runErr
	}()

	adminExecMessage := <-adminSession.outgoing
	if adminExecMessage.GetExecuteJobRequest() == nil {
		t.Fatalf("expected admin_script execute request")
	}

	otherErrCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.ExecuteJob(context.Background(), &plugin_pb.JobSpec{
			JobId:   "job-vacuum-1",
			JobType: "vacuum",
		}, &plugin_pb.ClusterContext{}, 1)
		otherErrCh <- runErr
	}()

	select {
	case unexpected := <-otherSession.outgoing:
		t.Fatalf("expected vacuum execute to wait while admin_script runs, got message: %+v", unexpected)
	case <-time.After(100 * time.Millisecond):
	}

	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId:   adminExecMessage.RequestId,
		JobId:       "job-admin-script-2",
		JobType:     "admin_script",
		Success:     true,
		CompletedAt: timestamppb.Now(),
	})
	if runErr := <-adminErrCh; runErr != nil {
		t.Fatalf("admin_script ExecuteJob error: %v", runErr)
	}

	otherExecMessage := <-otherSession.outgoing
	if otherExecMessage.GetExecuteJobRequest() == nil {
		t.Fatalf("expected vacuum execute request after admin_script completion")
	}
	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId:   otherExecMessage.RequestId,
		JobId:       "job-vacuum-1",
		JobType:     "vacuum",
		Success:     true,
		CompletedAt: timestamppb.Now(),
	})
	if runErr := <-otherErrCh; runErr != nil {
		t.Fatalf("vacuum ExecuteJob error: %v", runErr)
	}
}
