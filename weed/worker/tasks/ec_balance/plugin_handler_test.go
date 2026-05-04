package ec_balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/protobuf/proto"
)

func TestDeriveECBalanceWorkerConfig(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]*plugin_pb.ConfigValue
		check  func(t *testing.T, config *ecBalanceWorkerConfig)
	}{
		{
			name:   "nil values uses defaults",
			values: nil,
			check: func(t *testing.T, config *ecBalanceWorkerConfig) {
				if config.TaskConfig.ImbalanceThreshold != 0.2 {
					t.Errorf("expected default threshold 0.2, got %f", config.TaskConfig.ImbalanceThreshold)
				}
				if config.TaskConfig.MinServerCount != 3 {
					t.Errorf("expected default min_server_count 3, got %d", config.TaskConfig.MinServerCount)
				}
			},
		},
		{
			name: "custom threshold",
			values: map[string]*plugin_pb.ConfigValue{
				"imbalance_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3}},
				"min_server_count":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 5}},
			},
			check: func(t *testing.T, config *ecBalanceWorkerConfig) {
				if config.TaskConfig.ImbalanceThreshold != 0.3 {
					t.Errorf("expected threshold 0.3, got %f", config.TaskConfig.ImbalanceThreshold)
				}
				if config.TaskConfig.MinServerCount != 5 {
					t.Errorf("expected min_server_count 5, got %d", config.TaskConfig.MinServerCount)
				}
			},
		},
		{
			name: "threshold clamped to min",
			values: map[string]*plugin_pb.ConfigValue{
				"imbalance_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.01}},
			},
			check: func(t *testing.T, config *ecBalanceWorkerConfig) {
				if config.TaskConfig.ImbalanceThreshold != 0.05 {
					t.Errorf("expected clamped threshold 0.05, got %f", config.TaskConfig.ImbalanceThreshold)
				}
			},
		},
		{
			name: "threshold clamped to max",
			values: map[string]*plugin_pb.ConfigValue{
				"imbalance_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.9}},
			},
			check: func(t *testing.T, config *ecBalanceWorkerConfig) {
				if config.TaskConfig.ImbalanceThreshold != 0.5 {
					t.Errorf("expected clamped threshold 0.5, got %f", config.TaskConfig.ImbalanceThreshold)
				}
			},
		},
		{
			name: "min_server_count clamped to 2",
			values: map[string]*plugin_pb.ConfigValue{
				"min_server_count": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			},
			check: func(t *testing.T, config *ecBalanceWorkerConfig) {
				if config.TaskConfig.MinServerCount != 2 {
					t.Errorf("expected min_server_count 2, got %d", config.TaskConfig.MinServerCount)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := deriveECBalanceWorkerConfig(tt.values)
			tt.check(t, config)
		})
	}
}

func TestBuildECBalanceProposal(t *testing.T) {
	result := &workertypes.TaskDetectionResult{
		TaskID:     "test-task-123",
		TaskType:   workertypes.TaskTypeECBalance,
		VolumeID:   42,
		Collection: "test-col",
		Priority:   workertypes.TaskPriorityMedium,
		Reason:     "cross_rack balance",
		TypedParams: &worker_pb.TaskParams{
			VolumeId:   42,
			Collection: "test-col",
			Sources: []*worker_pb.TaskSource{{
				Node:     "source:8080",
				ShardIds: []uint32{5},
			}},
			Targets: []*worker_pb.TaskTarget{{
				Node:     "target:8080",
				ShardIds: []uint32{5},
			}},
		},
	}

	proposal, err := buildECBalanceProposal(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if proposal.JobType != "ec_balance" {
		t.Errorf("expected job type ec_balance, got %s", proposal.JobType)
	}
	if proposal.ProposalId != "test-task-123" {
		t.Errorf("expected proposal ID test-task-123, got %s", proposal.ProposalId)
	}
	if proposal.DedupeKey != "ec_balance:42:5:source:8080:test-col" {
		t.Errorf("expected dedupe key ec_balance:42:5:source:8080:test-col, got %s", proposal.DedupeKey)
	}
	if proposal.Labels["source_node"] != "source:8080" {
		t.Errorf("expected source_node label source:8080, got %s", proposal.Labels["source_node"])
	}
	if proposal.Labels["target_node"] != "target:8080" {
		t.Errorf("expected target_node label target:8080, got %s", proposal.Labels["target_node"])
	}
}

func TestBuildECBalanceProposalNilResult(t *testing.T) {
	_, err := buildECBalanceProposal(nil)
	if err == nil {
		t.Fatal("expected error for nil result")
	}
}

func TestDecodeECBalanceTaskParamsFromProtobuf(t *testing.T) {
	originalParams := &worker_pb.TaskParams{
		TaskId:     "test-id",
		VolumeId:   100,
		Collection: "test-col",
		Sources: []*worker_pb.TaskSource{{
			Node:     "source:8080",
			ShardIds: []uint32{3},
		}},
		Targets: []*worker_pb.TaskTarget{{
			Node:     "target:8080",
			ShardIds: []uint32{3},
		}},
		TaskParams: &worker_pb.TaskParams_EcBalanceParams{
			EcBalanceParams: &worker_pb.EcBalanceTaskParams{
				DiskType:       "hdd",
				TimeoutSeconds: 300,
			},
		},
	}

	payload, err := proto.Marshal(originalParams)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	job := &plugin_pb.JobSpec{
		JobId: "job-1",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload}},
		},
	}

	decoded, err := decodeECBalanceTaskParams(job)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.VolumeId != 100 {
		t.Errorf("expected volume_id 100, got %d", decoded.VolumeId)
	}
	if decoded.Collection != "test-col" {
		t.Errorf("expected collection test-col, got %s", decoded.Collection)
	}
	if len(decoded.Sources) != 1 || decoded.Sources[0].Node != "source:8080" {
		t.Error("source mismatch")
	}
	if len(decoded.Targets) != 1 || decoded.Targets[0].Node != "target:8080" {
		t.Error("target mismatch")
	}
	ecParams := decoded.GetEcBalanceParams()
	if ecParams == nil {
		t.Fatal("expected ec_balance_params")
	}
	if ecParams.DiskType != "hdd" {
		t.Errorf("expected disk_type hdd, got %s", ecParams.DiskType)
	}
}

func TestDecodeECBalanceTaskParamsFallback(t *testing.T) {
	job := &plugin_pb.JobSpec{
		JobId: "job-2",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 200}},
			"source_server":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "src:8080"}},
			"target_server":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "dst:8080"}},
			"collection":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "fallback-col"}},
			"shard_id":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7}},
			"source_disk_id": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			"target_disk_id": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
		},
	}

	decoded, err := decodeECBalanceTaskParams(job)
	if err != nil {
		t.Fatalf("failed to decode fallback: %v", err)
	}

	if decoded.VolumeId != 200 {
		t.Errorf("expected volume_id 200, got %d", decoded.VolumeId)
	}
	if len(decoded.Sources) != 1 || decoded.Sources[0].Node != "src:8080" {
		t.Error("source mismatch")
	}
	if decoded.Sources[0].ShardIds[0] != 7 {
		t.Errorf("expected shard_id 7, got %d", decoded.Sources[0].ShardIds[0])
	}
	if decoded.Sources[0].DiskId != 1 {
		t.Errorf("expected source disk_id 1, got %d", decoded.Sources[0].DiskId)
	}
	if decoded.Targets[0].DiskId != 2 {
		t.Errorf("expected target disk_id 2, got %d", decoded.Targets[0].DiskId)
	}
}

func TestDecodeECBalanceTaskParamsProtobufValidation(t *testing.T) {
	// Protobuf payload with missing ShardIds should fail validation
	badParams := &worker_pb.TaskParams{
		TaskId:   "test-id",
		VolumeId: 100,
		Sources:  []*worker_pb.TaskSource{{Node: "source:8080"}}, // no ShardIds
		Targets:  []*worker_pb.TaskTarget{{Node: "target:8080", ShardIds: []uint32{3}}},
	}
	payload, _ := proto.Marshal(badParams)
	job := &plugin_pb.JobSpec{
		JobId: "job-validate",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload}},
		},
	}
	_, err := decodeECBalanceTaskParams(job)
	if err == nil {
		t.Fatal("expected error for missing Sources[0].ShardIds in protobuf")
	}
}

func TestDecodeECBalanceTaskParamsMissingShardID(t *testing.T) {
	job := &plugin_pb.JobSpec{
		JobId: "job-3",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 300}},
			"source_server": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "src:8080"}},
			"target_server": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "dst:8080"}},
			"collection":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "test-col"}},
			// shard_id deliberately missing
		},
	}

	_, err := decodeECBalanceTaskParams(job)
	if err == nil {
		t.Fatal("expected error for missing shard_id")
	}
}

func TestECBalanceHandlerCapability(t *testing.T) {
	handler := NewECBalanceHandler(nil)
	cap := handler.Capability()

	if cap.JobType != "ec_balance" {
		t.Errorf("expected job type ec_balance, got %s", cap.JobType)
	}
	if !cap.CanDetect {
		t.Error("expected CanDetect=true")
	}
	if !cap.CanExecute {
		t.Error("expected CanExecute=true")
	}
	if cap.Weight != 60 {
		t.Errorf("expected weight 60, got %d", cap.Weight)
	}
}

func TestECBalanceConfigRoundTrip(t *testing.T) {
	config := NewDefaultConfig()
	config.ImbalanceThreshold = 0.3
	config.MinServerCount = 5
	config.CollectionFilter = "my_col"
	config.DiskType = "ssd"
	config.PreferredTags = []string{"fast", "ssd"}

	policy := config.ToTaskPolicy()
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}

	config2 := NewDefaultConfig()
	if err := config2.FromTaskPolicy(policy); err != nil {
		t.Fatalf("failed to load from policy: %v", err)
	}

	if config2.ImbalanceThreshold != 0.3 {
		t.Errorf("expected threshold 0.3, got %f", config2.ImbalanceThreshold)
	}
	if config2.MinServerCount != 5 {
		t.Errorf("expected min_server_count 5, got %d", config2.MinServerCount)
	}
	if config2.CollectionFilter != "my_col" {
		t.Errorf("expected collection_filter my_col, got %s", config2.CollectionFilter)
	}
	if config2.DiskType != "ssd" {
		t.Errorf("expected disk_type ssd, got %s", config2.DiskType)
	}
	if len(config2.PreferredTags) != 2 || config2.PreferredTags[0] != "fast" {
		t.Errorf("expected preferred_tags [fast ssd], got %v", config2.PreferredTags)
	}
}
