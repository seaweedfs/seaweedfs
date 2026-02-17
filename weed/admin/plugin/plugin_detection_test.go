package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestRunDetectionIncludesLatestSuccessfulRun(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	jobType := "vacuum"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, MaxDetectionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1)}
	pluginSvc.putSession(session)

	oldSuccess := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	latestSuccess := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: oldSuccess}); err != nil {
		t.Fatalf("AppendRunRecord old success: %v", err)
	}
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeError, CompletedAt: latestSuccess.Add(2 * time.Hour)}); err != nil {
		t.Fatalf("AppendRunRecord error run: %v", err)
	}
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: latestSuccess}); err != nil {
		t.Fatalf("AppendRunRecord latest success: %v", err)
	}

	resultCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.RunDetection(context.Background(), jobType, &plugin_pb.ClusterContext{}, 10)
		resultCh <- runErr
	}()

	message := <-session.outgoing
	detectRequest := message.GetRunDetectionRequest()
	if detectRequest == nil {
		t.Fatalf("expected run detection request message")
	}
	if detectRequest.LastSuccessfulRun == nil {
		t.Fatalf("expected last_successful_run to be set")
	}
	if got := detectRequest.LastSuccessfulRun.AsTime().UTC(); !got.Equal(latestSuccess) {
		t.Fatalf("unexpected last_successful_run, got=%s want=%s", got, latestSuccess)
	}

	pluginSvc.handleDetectionComplete(&plugin_pb.DetectionComplete{
		RequestId: message.RequestId,
		JobType:   jobType,
		Success:   true,
	})

	if runErr := <-resultCh; runErr != nil {
		t.Fatalf("RunDetection error: %v", runErr)
	}
}

func TestRunDetectionOmitsLastSuccessfulRunWhenNoSuccessHistory(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	jobType := "vacuum"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, MaxDetectionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1)}
	pluginSvc.putSession(session)

	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{
		Outcome:     RunOutcomeError,
		CompletedAt: time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("AppendRunRecord error run: %v", err)
	}

	resultCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.RunDetection(context.Background(), jobType, &plugin_pb.ClusterContext{}, 10)
		resultCh <- runErr
	}()

	message := <-session.outgoing
	detectRequest := message.GetRunDetectionRequest()
	if detectRequest == nil {
		t.Fatalf("expected run detection request message")
	}
	if detectRequest.LastSuccessfulRun != nil {
		t.Fatalf("expected last_successful_run to be nil when no success history")
	}

	pluginSvc.handleDetectionComplete(&plugin_pb.DetectionComplete{
		RequestId: message.RequestId,
		JobType:   jobType,
		Success:   true,
	})

	if runErr := <-resultCh; runErr != nil {
		t.Fatalf("RunDetection error: %v", runErr)
	}
}
