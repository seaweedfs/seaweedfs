package pluginworker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	balancetask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDecodeVolumeBalanceTaskParamsFromPayload(t *testing.T) {
	expected := &worker_pb.TaskParams{
		TaskId:     "task-1",
		VolumeId:   42,
		Collection: "photos",
		Sources: []*worker_pb.TaskSource{
			{
				Node:     "10.0.0.1:8080",
				VolumeId: 42,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:     "10.0.0.2:8080",
				VolumeId: 42,
			},
		},
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      true,
				TimeoutSeconds: 1200,
			},
		},
	}
	payload, err := proto.Marshal(expected)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	job := &plugin_pb.JobSpec{
		JobId: "job-from-admin",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload}},
		},
	}

	actual, err := decodeVolumeBalanceTaskParams(job)
	if err != nil {
		t.Fatalf("decodeVolumeBalanceTaskParams() err = %v", err)
	}
	if !proto.Equal(expected, actual) {
		t.Fatalf("decoded params mismatch\nexpected: %+v\nactual:   %+v", expected, actual)
	}
}

func TestDecodeVolumeBalanceTaskParamsFallback(t *testing.T) {
	job := &plugin_pb.JobSpec{
		JobId: "job-2",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7}},
			"source_server": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "127.0.0.1:8080"}},
			"target_server": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "127.0.0.2:8080"}},
			"collection":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "videos"}},
		},
	}

	params, err := decodeVolumeBalanceTaskParams(job)
	if err != nil {
		t.Fatalf("decodeVolumeBalanceTaskParams() err = %v", err)
	}
	if params.TaskId != "job-2" || params.VolumeId != 7 || params.Collection != "videos" {
		t.Fatalf("unexpected basic params: %+v", params)
	}
	if len(params.Sources) != 1 || params.Sources[0].Node != "127.0.0.1:8080" {
		t.Fatalf("unexpected sources: %+v", params.Sources)
	}
	if len(params.Targets) != 1 || params.Targets[0].Node != "127.0.0.2:8080" {
		t.Fatalf("unexpected targets: %+v", params.Targets)
	}
	if params.GetBalanceParams() == nil {
		t.Fatalf("expected fallback balance params")
	}
}

func TestDeriveBalanceWorkerConfig(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"imbalance_threshold": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.45},
		},
		"min_server_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 5},
		},
		"min_interval_seconds": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 33},
		},
	}

	cfg := deriveBalanceWorkerConfig(values)
	if cfg.TaskConfig.ImbalanceThreshold != 0.45 {
		t.Fatalf("expected imbalance_threshold 0.45, got %v", cfg.TaskConfig.ImbalanceThreshold)
	}
	if cfg.TaskConfig.MinServerCount != 5 {
		t.Fatalf("expected min_server_count 5, got %d", cfg.TaskConfig.MinServerCount)
	}
	if cfg.MinIntervalSeconds != 33 {
		t.Fatalf("expected min_interval_seconds 33, got %d", cfg.MinIntervalSeconds)
	}
}

func TestBuildVolumeBalanceProposal(t *testing.T) {
	params := &worker_pb.TaskParams{
		TaskId:     "balance-task-1",
		VolumeId:   55,
		Collection: "images",
		Sources: []*worker_pb.TaskSource{
			{
				Node:     "source-a:8080",
				VolumeId: 55,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:     "target-b:8080",
				VolumeId: 55,
			},
		},
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				TimeoutSeconds: 600,
			},
		},
	}
	result := &workertypes.TaskDetectionResult{
		TaskID:      "balance-task-1",
		TaskType:    workertypes.TaskTypeBalance,
		VolumeID:    55,
		Server:      "source-a",
		Collection:  "images",
		Priority:    workertypes.TaskPriorityHigh,
		Reason:      "imbalanced load",
		TypedParams: params,
	}

	proposal, err := buildVolumeBalanceProposal(result)
	if err != nil {
		t.Fatalf("buildVolumeBalanceProposal() err = %v", err)
	}
	if proposal.JobType != "volume_balance" {
		t.Fatalf("unexpected job type %q", proposal.JobType)
	}
	if proposal.DedupeKey == "" {
		t.Fatalf("expected dedupe key")
	}
	if proposal.Parameters["task_params_pb"] == nil {
		t.Fatalf("expected serialized task params")
	}
	if proposal.Labels["source_node"] != "source-a:8080" {
		t.Fatalf("unexpected source label %q", proposal.Labels["source_node"])
	}
	if proposal.Labels["target_node"] != "target-b:8080" {
		t.Fatalf("unexpected target label %q", proposal.Labels["target_node"])
	}
}

func TestVolumeBalanceHandlerRejectsUnsupportedJobType(t *testing.T) {
	handler := NewVolumeBalanceHandler(nil)
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: "vacuum",
	}, noopDetectionSender{})
	if err == nil {
		t.Fatalf("expected detect job type mismatch error")
	}

	err = handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{JobId: "job-1", JobType: "vacuum"},
	}, noopExecutionSender{})
	if err == nil {
		t.Fatalf("expected execute job type mismatch error")
	}
}

func TestVolumeBalanceHandlerDetectSkipsByMinInterval(t *testing.T) {
	handler := NewVolumeBalanceHandler(nil)
	sender := &recordingDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           "volume_balance",
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-3 * time.Second)),
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			"min_interval_seconds": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 10}},
		},
	}, sender)
	if err != nil {
		t.Fatalf("detect returned err = %v", err)
	}
	if sender.proposals == nil {
		t.Fatalf("expected proposals message")
	}
	if len(sender.proposals.Proposals) != 0 {
		t.Fatalf("expected zero proposals, got %d", len(sender.proposals.Proposals))
	}
	if sender.complete == nil || !sender.complete.Success {
		t.Fatalf("expected successful completion message")
	}
	if len(sender.events) == 0 {
		t.Fatalf("expected detector activity events")
	}
	if !strings.Contains(sender.events[0].Message, "min interval") {
		t.Fatalf("unexpected skip-by-interval message: %q", sender.events[0].Message)
	}
}

func TestEmitVolumeBalanceDetectionDecisionTraceNoTasks(t *testing.T) {
	sender := &recordingDetectionSender{}
	config := balancetask.NewDefaultConfig()
	config.ImbalanceThreshold = 0.2
	config.MinServerCount = 2

	metrics := []*workertypes.VolumeHealthMetrics{
		{VolumeID: 1, Server: "server-a", DiskType: "hdd"},
		{VolumeID: 2, Server: "server-a", DiskType: "hdd"},
		{VolumeID: 3, Server: "server-b", DiskType: "hdd"},
		{VolumeID: 4, Server: "server-b", DiskType: "hdd"},
	}

	if err := emitVolumeBalanceDetectionDecisionTrace(sender, metrics, nil, config, nil); err != nil {
		t.Fatalf("emitVolumeBalanceDetectionDecisionTrace error: %v", err)
	}
	if len(sender.events) < 2 {
		t.Fatalf("expected at least 2 detection events, got %d", len(sender.events))
	}
	if sender.events[0].Source != plugin_pb.ActivitySource_ACTIVITY_SOURCE_DETECTOR {
		t.Fatalf("expected detector source, got %v", sender.events[0].Source)
	}
	if !strings.Contains(sender.events[0].Message, "BALANCE: No tasks created for 4 volumes") {
		t.Fatalf("unexpected summary message: %q", sender.events[0].Message)
	}
	foundDiskTypeDecision := false
	for _, event := range sender.events {
		if strings.Contains(event.Message, "BALANCE [hdd]: No tasks created - cluster well balanced") {
			foundDiskTypeDecision = true
			break
		}
	}
	if !foundDiskTypeDecision {
		t.Fatalf("expected per-disk-type decision message")
	}
}

func TestVolumeBalanceDescriptorOmitsExecutionTuningFields(t *testing.T) {
	descriptor := NewVolumeBalanceHandler(nil).Descriptor()
	if descriptor == nil || descriptor.WorkerConfigForm == nil {
		t.Fatalf("expected worker config form in descriptor")
	}
	if workerConfigFormHasField(descriptor.WorkerConfigForm, "timeout_seconds") {
		t.Fatalf("unexpected timeout_seconds in volume balance worker config form")
	}
	if workerConfigFormHasField(descriptor.WorkerConfigForm, "force_move") {
		t.Fatalf("unexpected force_move in volume balance worker config form")
	}
}

type recordingExecutionSender struct {
	mu        sync.Mutex
	progress  []*plugin_pb.JobProgressUpdate
	completed *plugin_pb.JobCompleted
}

func (r *recordingExecutionSender) SendProgress(p *plugin_pb.JobProgressUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress = append(r.progress, p)
	return nil
}

func (r *recordingExecutionSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = c
	return nil
}

func TestBuildMoveTaskParams(t *testing.T) {
	move := &worker_pb.BalanceMoveSpec{
		VolumeId:   42,
		SourceNode: "10.0.0.1:8080",
		TargetNode: "10.0.0.2:8080",
		Collection: "photos",
		VolumeSize: 1024 * 1024,
	}

	params := buildMoveTaskParams(move, 300)
	if params.VolumeId != 42 {
		t.Fatalf("expected volume_id 42, got %d", params.VolumeId)
	}
	if params.Collection != "photos" {
		t.Fatalf("expected collection photos, got %s", params.Collection)
	}
	if params.VolumeSize != 1024*1024 {
		t.Fatalf("expected volume_size %d, got %d", 1024*1024, params.VolumeSize)
	}
	if len(params.Sources) != 1 || params.Sources[0].Node != "10.0.0.1:8080" {
		t.Fatalf("unexpected sources: %+v", params.Sources)
	}
	if len(params.Targets) != 1 || params.Targets[0].Node != "10.0.0.2:8080" {
		t.Fatalf("unexpected targets: %+v", params.Targets)
	}
	bp := params.GetBalanceParams()
	if bp == nil {
		t.Fatalf("expected balance params")
	}
	if bp.TimeoutSeconds != 300 {
		t.Fatalf("expected timeout 300, got %d", bp.TimeoutSeconds)
	}
}

func TestBuildMoveTaskParamsDefaultTimeout(t *testing.T) {
	move := &worker_pb.BalanceMoveSpec{
		VolumeId:   1,
		SourceNode: "a:8080",
		TargetNode: "b:8080",
	}
	params := buildMoveTaskParams(move, 0)
	if params.GetBalanceParams().TimeoutSeconds != defaultBalanceTimeoutSeconds {
		t.Fatalf("expected default timeout %d, got %d", defaultBalanceTimeoutSeconds, params.GetBalanceParams().TimeoutSeconds)
	}
}

func TestExecuteDispatchesBatchPath(t *testing.T) {
	// Build a job with batch moves in BalanceTaskParams
	bp := &worker_pb.BalanceTaskParams{
		TimeoutSeconds:     60,
		MaxConcurrentMoves: 2,
		Moves: []*worker_pb.BalanceMoveSpec{
			{VolumeId: 1, SourceNode: "s1:8080", TargetNode: "t1:8080", Collection: "c1"},
			{VolumeId: 2, SourceNode: "s2:8080", TargetNode: "t2:8080", Collection: "c1"},
		},
	}
	taskParams := &worker_pb.TaskParams{
		TaskId: "batch-1",
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: bp,
		},
	}
	payload, err := proto.Marshal(taskParams)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	job := &plugin_pb.JobSpec{
		JobId:   "batch-job-1",
		JobType: "volume_balance",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload}},
		},
	}

	handler := NewVolumeBalanceHandler(nil)
	sender := &recordingExecutionSender{}

	// Execute will enter the batch path. It will fail because there's no real gRPC server,
	// but we verify it sends the assigned progress and eventually a completion.
	err = handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: job,
	}, sender)

	// Expect an error since no real volume servers exist
	// But verify the batch path was taken by checking the assigned message
	sender.mu.Lock()
	defer sender.mu.Unlock()

	if len(sender.progress) == 0 {
		t.Fatalf("expected progress messages from batch path")
	}

	// First progress should be "assigned" with batch info
	first := sender.progress[0]
	if first.Stage != "assigned" {
		t.Fatalf("expected first stage 'assigned', got %q", first.Stage)
	}
	if !strings.Contains(first.Message, "batch") || !strings.Contains(first.Message, "2 moves") {
		t.Fatalf("expected batch assigned message, got %q", first.Message)
	}

	// Should have a completion with failure details (since no servers)
	if sender.completed == nil {
		t.Fatalf("expected completion message")
	}
	if sender.completed.Success {
		t.Fatalf("expected failure since no real gRPC servers")
	}
	// Should report 0 succeeded, 2 failed
	if v, ok := sender.completed.Result.OutputValues["failed"]; !ok || v.GetInt64Value() != 2 {
		t.Fatalf("expected 2 failed moves, got %+v", sender.completed.Result.OutputValues)
	}
}

func TestExecuteSingleMovePathUnchanged(t *testing.T) {
	// Build a single-move job (no batch moves)
	taskParams := &worker_pb.TaskParams{
		TaskId:     "single-1",
		VolumeId:   99,
		Collection: "videos",
		Sources: []*worker_pb.TaskSource{
			{Node: "src:8080", VolumeId: 99},
		},
		Targets: []*worker_pb.TaskTarget{
			{Node: "dst:8080", VolumeId: 99},
		},
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				TimeoutSeconds: 60,
			},
		},
	}
	payload, err := proto.Marshal(taskParams)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	job := &plugin_pb.JobSpec{
		JobId:   "single-job-1",
		JobType: "volume_balance",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload}},
		},
	}

	handler := NewVolumeBalanceHandler(nil)
	sender := &recordingExecutionSender{}

	// Execute single-move path. Will fail on gRPC but verify it takes the single-move path.
	_ = handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: job,
	}, sender)

	sender.mu.Lock()
	defer sender.mu.Unlock()

	if len(sender.progress) == 0 {
		t.Fatalf("expected progress messages")
	}

	// Single-move path sends "volume balance job accepted" not "batch volume balance"
	first := sender.progress[0]
	if first.Stage != "assigned" {
		t.Fatalf("expected first stage 'assigned', got %q", first.Stage)
	}
	if strings.Contains(first.Message, "batch") {
		t.Fatalf("single-move path should not mention batch, got %q", first.Message)
	}
}

func workerConfigFormHasField(form *plugin_pb.ConfigForm, fieldName string) bool {
	if form == nil {
		return false
	}
	for _, section := range form.Sections {
		if section == nil {
			continue
		}
		for _, field := range section.Fields {
			if field != nil && field.Name == fieldName {
				return true
			}
		}
	}
	return false
}
