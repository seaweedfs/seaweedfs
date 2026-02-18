package pluginworker

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDecodeErasureCodingTaskParamsFromPayload(t *testing.T) {
	expected := &worker_pb.TaskParams{
		TaskId:     "task-ec-1",
		VolumeId:   88,
		Collection: "images",
		Sources: []*worker_pb.TaskSource{
			{
				Node:     "10.0.0.1:8080",
				VolumeId: 88,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:     "10.0.0.2:8080",
				VolumeId: 88,
				ShardIds: []uint32{0, 10},
			},
		},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:    ecstorage.DataShardsCount,
				ParityShards:  ecstorage.ParityShardsCount,
				WorkingDir:    "/tmp/ec-work",
				CleanupSource: true,
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

	actual, err := decodeErasureCodingTaskParams(job)
	if err != nil {
		t.Fatalf("decodeErasureCodingTaskParams() err = %v", err)
	}
	if !proto.Equal(expected, actual) {
		t.Fatalf("decoded params mismatch\nexpected: %+v\nactual:   %+v", expected, actual)
	}
}

func TestDecodeErasureCodingTaskParamsFallback(t *testing.T) {
	targetServers := make([]string, 0, ecstorage.TotalShardsCount)
	for i := 0; i < ecstorage.TotalShardsCount; i++ {
		targetServers = append(targetServers, "10.0.0."+string(rune('a'+i))+":8080")
	}

	job := &plugin_pb.JobSpec{
		JobId: "job-ec-2",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7},
			},
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "127.0.0.1:8080"},
			},
			"target_servers": {
				Kind: &plugin_pb.ConfigValue_StringList{
					StringList: &plugin_pb.StringList{Values: targetServers},
				},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "videos"},
			},
		},
	}

	params, err := decodeErasureCodingTaskParams(job)
	if err != nil {
		t.Fatalf("decodeErasureCodingTaskParams() err = %v", err)
	}
	if params.TaskId != "job-ec-2" || params.VolumeId != 7 || params.Collection != "videos" {
		t.Fatalf("unexpected basic params: %+v", params)
	}
	if len(params.Sources) != 1 || params.Sources[0].Node != "127.0.0.1:8080" {
		t.Fatalf("unexpected sources: %+v", params.Sources)
	}
	if len(params.Targets) != ecstorage.TotalShardsCount {
		t.Fatalf("unexpected target count: %d", len(params.Targets))
	}
	if params.GetErasureCodingParams() == nil {
		t.Fatalf("expected fallback erasure coding params")
	}
}

func TestDeriveErasureCodingWorkerConfig(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"quiet_for_seconds": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 720},
		},
		"fullness_ratio": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.92},
		},
		"min_size_mb": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 128},
		},
		"min_interval_seconds": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 55},
		},
	}

	cfg := deriveErasureCodingWorkerConfig(values)
	if cfg.TaskConfig.QuietForSeconds != 720 {
		t.Fatalf("expected quiet_for_seconds 720, got %d", cfg.TaskConfig.QuietForSeconds)
	}
	if cfg.TaskConfig.FullnessRatio != 0.92 {
		t.Fatalf("expected fullness_ratio 0.92, got %v", cfg.TaskConfig.FullnessRatio)
	}
	if cfg.TaskConfig.MinSizeMB != 128 {
		t.Fatalf("expected min_size_mb 128, got %d", cfg.TaskConfig.MinSizeMB)
	}
	if cfg.MinIntervalSeconds != 55 {
		t.Fatalf("expected min_interval_seconds 55, got %d", cfg.MinIntervalSeconds)
	}
}

func TestBuildErasureCodingProposal(t *testing.T) {
	params := &worker_pb.TaskParams{
		TaskId:     "ec-task-1",
		VolumeId:   99,
		Collection: "c1",
		Sources: []*worker_pb.TaskSource{
			{
				Node:     "source-a:8080",
				VolumeId: 99,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:     "target-a:8080",
				VolumeId: 99,
				ShardIds: []uint32{0, 10},
			},
			{
				Node:     "target-b:8080",
				VolumeId: 99,
				ShardIds: []uint32{1, 11},
			},
		},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   ecstorage.DataShardsCount,
				ParityShards: ecstorage.ParityShardsCount,
			},
		},
	}
	result := &workertypes.TaskDetectionResult{
		TaskID:      "ec-task-1",
		TaskType:    workertypes.TaskTypeErasureCoding,
		VolumeID:    99,
		Server:      "source-a",
		Collection:  "c1",
		Priority:    workertypes.TaskPriorityLow,
		Reason:      "quiet and full",
		TypedParams: params,
	}

	proposal, err := buildErasureCodingProposal(result)
	if err != nil {
		t.Fatalf("buildErasureCodingProposal() err = %v", err)
	}
	if proposal.JobType != "erasure_coding" {
		t.Fatalf("unexpected job type %q", proposal.JobType)
	}
	if proposal.Parameters["task_params_pb"] == nil {
		t.Fatalf("expected serialized task params")
	}
	if proposal.Labels["source_node"] != "source-a:8080" {
		t.Fatalf("unexpected source label %q", proposal.Labels["source_node"])
	}
}

func TestErasureCodingHandlerRejectsUnsupportedJobType(t *testing.T) {
	handler := NewErasureCodingHandler(nil)
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

func TestErasureCodingHandlerDetectSkipsByMinInterval(t *testing.T) {
	handler := NewErasureCodingHandler(nil)
	sender := &recordingDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           "erasure_coding",
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
}

func TestErasureCodingDescriptorOmitsLocalExecutionFields(t *testing.T) {
	descriptor := NewErasureCodingHandler(nil).Descriptor()
	if descriptor == nil || descriptor.WorkerConfigForm == nil {
		t.Fatalf("expected worker config form in descriptor")
	}
	if workerConfigFormHasField(descriptor.WorkerConfigForm, "working_dir") {
		t.Fatalf("unexpected working_dir in erasure coding worker config form")
	}
	if workerConfigFormHasField(descriptor.WorkerConfigForm, "cleanup_source") {
		t.Fatalf("unexpected cleanup_source in erasure coding worker config form")
	}
}

func TestApplyErasureCodingExecutionDefaultsForcesLocalFields(t *testing.T) {
	params := &worker_pb.TaskParams{
		TaskId:   "ec-test",
		VolumeId: 100,
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:    ecstorage.DataShardsCount,
				ParityShards:  ecstorage.ParityShardsCount,
				WorkingDir:    "/tmp/custom-from-job",
				CleanupSource: false,
			},
		},
	}

	applyErasureCodingExecutionDefaults(params, nil)

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		t.Fatalf("expected erasure coding params")
	}
	if ecParams.WorkingDir != defaultErasureCodingWorkingDir() {
		t.Fatalf("expected local working_dir %q, got %q", defaultErasureCodingWorkingDir(), ecParams.WorkingDir)
	}
	if !ecParams.CleanupSource {
		t.Fatalf("expected cleanup_source true")
	}
}
