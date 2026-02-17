package pluginworker

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/protobuf/proto"
)

func TestDecodeVacuumTaskParamsFromPayload(t *testing.T) {
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
		TaskParams: &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: 0.33,
				BatchSize:        500,
				VerifyChecksum:   true,
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

	actual, err := decodeVacuumTaskParams(job)
	if err != nil {
		t.Fatalf("decodeVacuumTaskParams() err = %v", err)
	}
	if !proto.Equal(expected, actual) {
		t.Fatalf("decoded params mismatch\nexpected: %+v\nactual:   %+v", expected, actual)
	}
}

func TestDecodeVacuumTaskParamsFallback(t *testing.T) {
	job := &plugin_pb.JobSpec{
		JobId: "job-2",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7}},
			"server":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "127.0.0.1:8080"}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "videos"}},
		},
	}

	params, err := decodeVacuumTaskParams(job)
	if err != nil {
		t.Fatalf("decodeVacuumTaskParams() err = %v", err)
	}
	if params.TaskId != "job-2" || params.VolumeId != 7 || params.Collection != "videos" {
		t.Fatalf("unexpected basic params: %+v", params)
	}
	if len(params.Sources) != 1 || params.Sources[0].Node != "127.0.0.1:8080" {
		t.Fatalf("unexpected sources: %+v", params.Sources)
	}
	if params.GetVacuumParams() == nil {
		t.Fatalf("expected fallback vacuum params")
	}
}

func TestDeriveVacuumConfigAllowsZeroValues(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"garbage_threshold": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0},
		},
		"min_volume_age_seconds": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0},
		},
		"min_interval_seconds": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0},
		},
	}

	cfg := deriveVacuumConfig(values)
	if cfg.GarbageThreshold != 0 {
		t.Fatalf("expected garbage_threshold 0, got %v", cfg.GarbageThreshold)
	}
	if cfg.MinVolumeAgeSeconds != 0 {
		t.Fatalf("expected min_volume_age_seconds 0, got %d", cfg.MinVolumeAgeSeconds)
	}
	if cfg.MinIntervalSeconds != 0 {
		t.Fatalf("expected min_interval_seconds 0, got %d", cfg.MinIntervalSeconds)
	}
}

func TestMasterAddressCandidates(t *testing.T) {
	candidates := masterAddressCandidates("localhost:9333")
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d: %v", len(candidates), candidates)
	}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		seen[candidate] = true
	}
	if !seen["localhost:9333"] {
		t.Fatalf("expected original address in candidates: %v", candidates)
	}
	if !seen["localhost:19333"] {
		t.Fatalf("expected grpc address in candidates: %v", candidates)
	}
}
