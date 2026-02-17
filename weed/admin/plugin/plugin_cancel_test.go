package plugin

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestRunDetectionSendsCancelOnContextDone(t *testing.T) {
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
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 4)}
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
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 4)}
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
