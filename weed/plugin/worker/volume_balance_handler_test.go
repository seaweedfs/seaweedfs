package pluginworker

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
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
