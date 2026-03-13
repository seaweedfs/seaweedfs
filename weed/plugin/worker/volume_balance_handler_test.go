package pluginworker

import (
	"context"
	"fmt"
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
	// Defaults for batch config when not specified
	if cfg.MaxConcurrentMoves != defaultMaxConcurrentMoves {
		t.Fatalf("expected default max_concurrent_moves %d, got %d", defaultMaxConcurrentMoves, cfg.MaxConcurrentMoves)
	}
	if cfg.BatchSize != 20 {
		t.Fatalf("expected default batch_size 20, got %d", cfg.BatchSize)
	}
}

func TestDeriveBalanceWorkerConfigBatchFields(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"max_concurrent_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 10},
		},
		"batch_size": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 50},
		},
	}

	cfg := deriveBalanceWorkerConfig(values)
	if cfg.MaxConcurrentMoves != 10 {
		t.Fatalf("expected max_concurrent_moves 10, got %d", cfg.MaxConcurrentMoves)
	}
	if cfg.BatchSize != 50 {
		t.Fatalf("expected batch_size 50, got %d", cfg.BatchSize)
	}
}

func TestDeriveBalanceWorkerConfigBatchClamping(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"max_concurrent_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 999},
		},
		"batch_size": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0},
		},
	}

	cfg := deriveBalanceWorkerConfig(values)
	if cfg.MaxConcurrentMoves != 50 {
		t.Fatalf("expected max_concurrent_moves clamped to 50, got %d", cfg.MaxConcurrentMoves)
	}
	if cfg.BatchSize != 1 {
		t.Fatalf("expected batch_size clamped to 1, got %d", cfg.BatchSize)
	}
}

func makeDetectionResult(volumeID uint32, source, target, collection string) *workertypes.TaskDetectionResult {
	return &workertypes.TaskDetectionResult{
		TaskID:     fmt.Sprintf("balance-%d", volumeID),
		TaskType:   workertypes.TaskTypeBalance,
		VolumeID:   volumeID,
		Server:     source,
		Collection: collection,
		Priority:   workertypes.TaskPriorityNormal,
		Reason:     "imbalanced",
		TypedParams: &worker_pb.TaskParams{
			VolumeId:   volumeID,
			Collection: collection,
			VolumeSize: 1024,
			Sources: []*worker_pb.TaskSource{
				{Node: source, VolumeId: volumeID},
			},
			Targets: []*worker_pb.TaskTarget{
				{Node: target, VolumeId: volumeID},
			},
			TaskParams: &worker_pb.TaskParams_BalanceParams{
				BalanceParams: &worker_pb.BalanceTaskParams{TimeoutSeconds: 600},
			},
		},
	}
}

func TestBuildBatchVolumeBalanceProposals_SingleBatch(t *testing.T) {
	results := []*workertypes.TaskDetectionResult{
		makeDetectionResult(1, "s1:8080", "t1:8080", "c1"),
		makeDetectionResult(2, "s2:8080", "t2:8080", "c1"),
		makeDetectionResult(3, "s1:8080", "t2:8080", "c1"),
	}

	proposals := buildBatchVolumeBalanceProposals(results, 10, 5)
	if len(proposals) != 1 {
		t.Fatalf("expected 1 batch proposal, got %d", len(proposals))
	}

	p := proposals[0]
	if p.Labels["batch"] != "true" {
		t.Fatalf("expected batch label")
	}
	if p.Labels["batch_size"] != "3" {
		t.Fatalf("expected batch_size label '3', got %q", p.Labels["batch_size"])
	}

	// Decode and verify moves
	payload := p.Parameters["task_params_pb"].GetBytesValue()
	if len(payload) == 0 {
		t.Fatalf("expected task_params_pb payload")
	}
	decoded := &worker_pb.TaskParams{}
	if err := proto.Unmarshal(payload, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	moves := decoded.GetBalanceParams().GetMoves()
	if len(moves) != 3 {
		t.Fatalf("expected 3 moves, got %d", len(moves))
	}
	if moves[0].VolumeId != 1 || moves[1].VolumeId != 2 || moves[2].VolumeId != 3 {
		t.Fatalf("unexpected volume IDs: %v", moves)
	}
	if decoded.GetBalanceParams().MaxConcurrentMoves != 5 {
		t.Fatalf("expected MaxConcurrentMoves 5, got %d", decoded.GetBalanceParams().MaxConcurrentMoves)
	}
}

func TestBuildBatchVolumeBalanceProposals_MultipleBatches(t *testing.T) {
	results := make([]*workertypes.TaskDetectionResult, 5)
	for i := range results {
		results[i] = makeDetectionResult(uint32(i+1), "s1:8080", "t1:8080", "c1")
	}

	proposals := buildBatchVolumeBalanceProposals(results, 2, 3)
	// 5 results / batch_size 2 = 3 proposals (2, 2, 1)
	if len(proposals) != 3 {
		t.Fatalf("expected 3 proposals, got %d", len(proposals))
	}

	// First two should be batch proposals
	if proposals[0].Labels["batch"] != "true" {
		t.Fatalf("first proposal should be batch")
	}
	if proposals[1].Labels["batch"] != "true" {
		t.Fatalf("second proposal should be batch")
	}
	// Last one has only 1 result, should fall back to single-move proposal
	if proposals[2].Labels["batch"] == "true" {
		t.Fatalf("last proposal with 1 result should be single-move, not batch")
	}
}

func TestBuildBatchVolumeBalanceProposals_BatchSizeOne(t *testing.T) {
	results := []*workertypes.TaskDetectionResult{
		makeDetectionResult(1, "s1:8080", "t1:8080", "c1"),
		makeDetectionResult(2, "s2:8080", "t2:8080", "c1"),
	}

	// batch_size=1 should not be called (Detect guards this), but test the function directly
	proposals := buildBatchVolumeBalanceProposals(results, 1, 5)
	// Each result becomes its own single-move proposal
	if len(proposals) != 2 {
		t.Fatalf("expected 2 proposals, got %d", len(proposals))
	}
}

func TestVolumeBalanceDescriptorHasBatchFields(t *testing.T) {
	descriptor := NewVolumeBalanceHandler(nil).Descriptor()
	if !workerConfigFormHasField(descriptor.WorkerConfigForm, "max_concurrent_moves") {
		t.Fatalf("expected max_concurrent_moves in worker config form")
	}
	if !workerConfigFormHasField(descriptor.WorkerConfigForm, "batch_size") {
		t.Fatalf("expected batch_size in worker config form")
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
	r.progress = append(r.progress, proto.Clone(p).(*plugin_pb.JobProgressUpdate))
	return nil
}

func (r *recordingExecutionSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = proto.Clone(c).(*plugin_pb.JobCompleted)
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

	outerParams := &worker_pb.BalanceTaskParams{
		ForceMove:      true,
		TimeoutSeconds: 300,
	}
	params := buildMoveTaskParams(move, outerParams)
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
	if !bp.ForceMove {
		t.Fatalf("expected force_move to be propagated from outer params")
	}
}

func TestBuildMoveTaskParamsDefaultTimeout(t *testing.T) {
	move := &worker_pb.BalanceMoveSpec{
		VolumeId:   1,
		SourceNode: "a:8080",
		TargetNode: "b:8080",
	}
	params := buildMoveTaskParams(move, nil)
	if params.GetBalanceParams().TimeoutSeconds != defaultBalanceTimeoutSeconds {
		t.Fatalf("expected default timeout %d, got %d", defaultBalanceTimeoutSeconds, params.GetBalanceParams().TimeoutSeconds)
	}
	if params.GetBalanceParams().ForceMove {
		t.Fatalf("expected force_move to default to false with nil outer params")
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

func TestVolumeBalanceDescriptorHasLocationFilters(t *testing.T) {
	descriptor := NewVolumeBalanceHandler(nil).Descriptor()
	for _, name := range []string{"data_center_filter", "rack_filter", "node_filter"} {
		if !workerConfigFormHasField(descriptor.AdminConfigForm, name) {
			t.Fatalf("expected %s in admin config form", name)
		}
	}
}

func TestFilterMetricsByLocation(t *testing.T) {
	metrics := []*workertypes.VolumeHealthMetrics{
		{VolumeID: 1, Server: "node-a", DataCenter: "dc1", Rack: "rack1"},
		{VolumeID: 2, Server: "node-b", DataCenter: "dc1", Rack: "rack2"},
		{VolumeID: 3, Server: "node-c", DataCenter: "dc2", Rack: "rack1"},
		{VolumeID: 4, Server: "node-d", DataCenter: "dc2", Rack: "rack3"},
	}

	// Filter by DC
	filtered := filterMetricsByLocation(metrics, "dc1", "", "")
	if len(filtered) != 2 {
		t.Fatalf("DC filter: expected 2, got %d", len(filtered))
	}

	// Filter by rack
	filtered = filterMetricsByLocation(metrics, "", "rack1,rack2", "")
	if len(filtered) != 3 {
		t.Fatalf("rack filter: expected 3, got %d", len(filtered))
	}

	// Filter by node
	filtered = filterMetricsByLocation(metrics, "", "", "node-a,node-c")
	if len(filtered) != 2 {
		t.Fatalf("node filter: expected 2, got %d", len(filtered))
	}

	// Combined DC + rack
	filtered = filterMetricsByLocation(metrics, "dc2", "rack3", "")
	if len(filtered) != 1 {
		t.Fatalf("DC+rack filter: expected 1, got %d", len(filtered))
	}

	// Empty filters pass all
	filtered = filterMetricsByLocation(metrics, "", "", "")
	if len(filtered) != 4 {
		t.Fatalf("no filter: expected 4, got %d", len(filtered))
	}
}

func TestFilterMetricsByVolumeState(t *testing.T) {
	metrics := []*workertypes.VolumeHealthMetrics{
		{VolumeID: 1, FullnessRatio: 0.5},   // active
		{VolumeID: 2, FullnessRatio: 1.0},   // active (below 1.01)
		{VolumeID: 3, FullnessRatio: 1.009}, // active (below 1.01)
		{VolumeID: 4, FullnessRatio: 1.01},  // full (exactly at threshold)
		{VolumeID: 5, FullnessRatio: 1.5},   // full
		{VolumeID: 6, FullnessRatio: 2.0},   // full
	}

	tests := []struct {
		name        string
		state       string
		expectedIDs []uint32
	}{
		{
			name:        "ALL returns everything",
			state:       "ALL",
			expectedIDs: []uint32{1, 2, 3, 4, 5, 6},
		},
		{
			name:        "empty string returns everything",
			state:       "",
			expectedIDs: []uint32{1, 2, 3, 4, 5, 6},
		},
		{
			name:        "ACTIVE keeps FullnessRatio below 1.01",
			state:       "ACTIVE",
			expectedIDs: []uint32{1, 2, 3},
		},
		{
			name:        "FULL keeps FullnessRatio at or above 1.01",
			state:       "FULL",
			expectedIDs: []uint32{4, 5, 6},
		},
		{
			name:        "unknown value returns everything",
			state:       "INVALID",
			expectedIDs: []uint32{1, 2, 3, 4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterMetricsByVolumeState(metrics, tt.state)
			if len(result) != len(tt.expectedIDs) {
				t.Fatalf("expected %d metrics, got %d", len(tt.expectedIDs), len(result))
			}
			for i, m := range result {
				if m.VolumeID != tt.expectedIDs[i] {
					t.Errorf("result[%d].VolumeID = %d, want %d", i, m.VolumeID, tt.expectedIDs[i])
				}
			}
		})
	}
}

func TestFilterMetricsByVolumeState_NilElement(t *testing.T) {
	metrics := []*workertypes.VolumeHealthMetrics{
		nil,
		{VolumeID: 1, FullnessRatio: 0.5},
		nil,
		{VolumeID: 2, FullnessRatio: 1.5},
	}
	result := filterMetricsByVolumeState(metrics, "ACTIVE")
	if len(result) != 1 || result[0].VolumeID != 1 {
		t.Fatalf("expected [vol 1] for ACTIVE with nil elements, got %d results", len(result))
	}
	result = filterMetricsByVolumeState(metrics, "FULL")
	if len(result) != 1 || result[0].VolumeID != 2 {
		t.Fatalf("expected [vol 2] for FULL with nil elements, got %d results", len(result))
	}
}

func TestFilterMetricsByVolumeState_EmptyInput(t *testing.T) {
	result := filterMetricsByVolumeState(nil, "ACTIVE")
	if len(result) != 0 {
		t.Fatalf("expected 0 metrics for nil input, got %d", len(result))
	}

	result = filterMetricsByVolumeState([]*workertypes.VolumeHealthMetrics{}, "FULL")
	if len(result) != 0 {
		t.Fatalf("expected 0 metrics for empty input, got %d", len(result))
	}
}

func TestVolumeBalanceDescriptorHasVolumeStateField(t *testing.T) {
	descriptor := NewVolumeBalanceHandler(nil).Descriptor()
	if descriptor == nil || descriptor.AdminConfigForm == nil {
		t.Fatalf("expected admin config form in descriptor")
	}
	found := false
	for _, section := range descriptor.AdminConfigForm.Sections {
		for _, field := range section.Fields {
			if field.Name == "volume_state" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected volume_state field in admin config form")
	}
	defaultVal, ok := descriptor.AdminConfigForm.DefaultValues["volume_state"]
	if !ok {
		t.Fatalf("expected volume_state default value")
	}
	if defaultVal.GetStringValue() != "ALL" {
		t.Fatalf("expected volume_state default 'ALL', got %q", defaultVal.GetStringValue())
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
