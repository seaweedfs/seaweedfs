package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/require"
)

func TestScheduledDetectionDistinguishesSkipFromEmptyScan(t *testing.T) {
	for _, reason := range []string{"", "Cooldown after successful job (10h0m0s)"} {
		t.Run(reason, func(t *testing.T) {
			p, err := New(Options{})
			require.NoError(t, err)
			defer p.Shutdown()
			p.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
				return &plugin_pb.ClusterContext{}, nil
			}
			p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
				WorkerId: "worker-a", Capabilities: []*plugin_pb.JobTypeCapability{{JobType: "vacuum", CanDetect: true}},
			})
			session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
			p.putSession(session)
			done := make(chan struct{})
			go func() {
				p.runJobTypeIteration("vacuum", schedulerPolicy{DetectionTimeout: time.Second, JobTypeMaxRuntime: time.Second})
				close(done)
			}()
			select {
			case request := <-session.outgoing:
				p.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{
					RequestId: request.RequestId, JobType: "vacuum", Success: true, SkippedReason: reason,
				})
			case <-time.After(2 * time.Second):
				t.Fatal("no detection request")
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("detection did not complete")
			}
			info := p.snapshotSchedulerDetection("vacuum")
			if reason == "" {
				require.False(t, info.lastDetectedAt.IsZero())
				require.Equal(t, "success", p.snapshotSchedulerRun("vacuum").lastRunStatus)
			} else {
				require.True(t, info.lastDetectedAt.IsZero(), "a skipped scan must not count as an empty scan")
				require.Equal(t, reason, info.lastSkippedReason)
				require.Equal(t, "skipped", p.snapshotSchedulerRun("vacuum").lastRunStatus)
			}
		})
	}
}

func TestManualDetectionSkipDoesNotChangeScheduledStatus(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	p.recordSchedulerDetectionSuccess("vacuum", 5)
	p.pendingDetection["manual"] = &pendingDetectionState{
		jobType: "vacuum", workerID: "worker-a", complete: make(chan *plugin_pb.DetectionComplete, 1),
	}
	p.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{
		RequestId: "manual", JobType: "vacuum", Success: true, SkippedReason: "Cooldown",
	})
	require.Empty(t, p.snapshotSchedulerDetection("vacuum").lastSkippedReason)
	require.Equal(t, 5, p.snapshotSchedulerDetection("vacuum").lastDetectedCount)
	activities := p.ListActivities("vacuum", 10)
	require.Len(t, activities, 1)
	require.Equal(t, "skipped", activities[0].Stage)
}

func TestPrefetchRefreshesSavedDescriptorFromReconnectingWorker(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	require.NoError(t, p.store.SaveDescriptor("vacuum", &plugin_pb.JobTypeDescriptor{JobType: "vacuum", DescriptorVersion: 1}))
	require.NoError(t, p.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: "vacuum", WorkerConfigValues: map[string]*plugin_pb.ConfigValue{"cooldown": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 600}}},
	}))
	session := &streamSession{workerID: "upgraded", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	p.putSession(session)
	done := make(chan struct{})
	go func() {
		p.prefetchDescriptorsFromHello(&plugin_pb.WorkerHello{
			WorkerId: "upgraded", Capabilities: []*plugin_pb.JobTypeCapability{{JobType: "vacuum", CanDetect: true}},
		})
		close(done)
	}()
	select {
	case request := <-session.outgoing:
		require.True(t, request.GetRequestConfigSchema().GetForceRefresh())
		p.handleConfigSchemaResponse(&plugin_pb.ConfigSchemaResponse{
			RequestId: request.RequestId, JobType: "vacuum", Success: true,
			JobTypeDescriptor: &plugin_pb.JobTypeDescriptor{JobType: "vacuum", DescriptorVersion: 2, DetectionCooldownConfigField: "cooldown"},
		})
	case <-time.After(time.Second):
		t.Fatal("saved descriptor was not refreshed")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not finish")
	}
	got, err := p.store.LoadDescriptor("vacuum")
	require.NoError(t, err)
	require.Equal(t, "cooldown", got.DetectionCooldownConfigField)
	cfg, err := p.LoadJobTypeConfig("vacuum")
	require.NoError(t, err)
	require.Equal(t, int64(600), cfg.WorkerConfigValues["cooldown"].GetInt64Value())
}

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
	session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	pluginSvc.putSession(session)

	oldSuccess := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	latestSuccess := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: timeToPtr(oldSuccess)}); err != nil {
		t.Fatalf("AppendRunRecord old success: %v", err)
	}
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeError, CompletedAt: timeToPtr(latestSuccess.Add(2 * time.Hour))}); err != nil {
		t.Fatalf("AppendRunRecord error run: %v", err)
	}
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: timeToPtr(latestSuccess)}); err != nil {
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

	pluginSvc.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{
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
	session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	pluginSvc.putSession(session)

	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{
		Outcome:     RunOutcomeError,
		CompletedAt: timeToPtr(time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)),
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

	pluginSvc.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{
		RequestId: message.RequestId,
		JobType:   jobType,
		Success:   true,
	})

	if runErr := <-resultCh; runErr != nil {
		t.Fatalf("RunDetection error: %v", runErr)
	}
}

func TestRunDetectionWithReportCapturesDetectionActivities(t *testing.T) {
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
	session := &streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	pluginSvc.putSession(session)

	reportCh := make(chan *DetectionReport, 1)
	errCh := make(chan error, 1)
	go func() {
		report, runErr := pluginSvc.RunDetectionWithReport(context.Background(), jobType, &plugin_pb.ClusterContext{}, 10)
		reportCh <- report
		errCh <- runErr
	}()

	message := <-session.outgoing
	requestID := message.GetRequestId()
	if requestID == "" {
		t.Fatalf("expected request id in detection request")
	}

	pluginSvc.handleDetectionProposals("worker-a", &plugin_pb.DetectionProposals{
		RequestId: requestID,
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{
			{
				ProposalId: "proposal-1",
				JobType:    jobType,
				Summary:    "vacuum proposal",
				Detail:     "based on garbage ratio",
			},
		},
	})
	pluginSvc.handleDetectionComplete("worker-a", &plugin_pb.DetectionComplete{
		RequestId:      requestID,
		JobType:        jobType,
		Success:        true,
		TotalProposals: 1,
	})

	report := <-reportCh
	if report == nil {
		t.Fatalf("expected detection report")
	}
	if report.RequestID == "" {
		t.Fatalf("expected detection report request id")
	}
	if report.WorkerID != "worker-a" {
		t.Fatalf("expected worker-a, got %q", report.WorkerID)
	}
	if len(report.Proposals) != 1 {
		t.Fatalf("expected one proposal in report, got %d", len(report.Proposals))
	}
	if runErr := <-errCh; runErr != nil {
		t.Fatalf("RunDetectionWithReport error: %v", runErr)
	}

	activities := pluginSvc.ListActivities(jobType, 0)
	stages := map[string]bool{}
	for _, activity := range activities {
		if activity.RequestID != report.RequestID {
			continue
		}
		stages[activity.Stage] = true
	}
	if !stages["requested"] || !stages["proposal"] || !stages["completed"] {
		t.Fatalf("expected requested/proposal/completed activities, got stages=%v", stages)
	}
}

func TestRunDetectionAdminScriptUsesLastCompletedRun(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New plugin error: %v", err)
	}
	defer pluginSvc.Shutdown()

	jobType := "admin_script"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-admin-script",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, MaxDetectionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: "worker-admin-script", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})}
	pluginSvc.putSession(session)

	successCompleted := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	errorCompleted := successCompleted.Add(45 * time.Minute)
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{
		Outcome:     RunOutcomeSuccess,
		CompletedAt: timeToPtr(successCompleted),
	}); err != nil {
		t.Fatalf("AppendRunRecord success run: %v", err)
	}
	if err := pluginSvc.store.AppendRunRecord(jobType, &JobRunRecord{
		Outcome:     RunOutcomeError,
		CompletedAt: timeToPtr(errorCompleted),
	}); err != nil {
		t.Fatalf("AppendRunRecord error run: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, runErr := pluginSvc.RunDetection(context.Background(), jobType, &plugin_pb.ClusterContext{}, 10)
		errCh <- runErr
	}()

	message := <-session.outgoing
	detectRequest := message.GetRunDetectionRequest()
	if detectRequest == nil {
		t.Fatalf("expected run detection request message")
	}
	if detectRequest.LastSuccessfulRun == nil {
		t.Fatalf("expected last_successful_run to be set")
	}
	if got := detectRequest.LastSuccessfulRun.AsTime().UTC(); !got.Equal(errorCompleted) {
		t.Fatalf("unexpected last_successful_run, got=%s want=%s", got, errorCompleted)
	}

	pluginSvc.handleDetectionComplete("worker-admin-script", &plugin_pb.DetectionComplete{
		RequestId: message.RequestId,
		JobType:   jobType,
		Success:   true,
	})

	if runErr := <-errCh; runErr != nil {
		t.Fatalf("RunDetection error: %v", runErr)
	}
}
