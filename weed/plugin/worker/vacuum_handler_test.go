package pluginworker

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestShouldSkipDetectionByInterval(t *testing.T) {
	if shouldSkipDetectionByInterval(nil, 10) {
		t.Fatalf("expected false when timestamp is nil")
	}
	if shouldSkipDetectionByInterval(timestamppb.Now(), 0) {
		t.Fatalf("expected false when min interval is zero")
	}

	recent := timestamppb.New(time.Now().Add(-5 * time.Second))
	if !shouldSkipDetectionByInterval(recent, 10) {
		t.Fatalf("expected true for recent successful run")
	}

	old := timestamppb.New(time.Now().Add(-30 * time.Second))
	if shouldSkipDetectionByInterval(old, 10) {
		t.Fatalf("expected false for old successful run")
	}
}

func TestVacuumHandlerRejectsUnsupportedJobType(t *testing.T) {
	handler := NewVacuumHandler(nil)
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: "balance",
	}, noopDetectionSender{})
	if err == nil {
		t.Fatalf("expected detect job type mismatch error")
	}

	err = handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{JobId: "job-1", JobType: "balance"},
	}, noopExecutionSender{})
	if err == nil {
		t.Fatalf("expected execute job type mismatch error")
	}
}

func TestVacuumHandlerDetectSkipsByMinInterval(t *testing.T) {
	handler := NewVacuumHandler(nil)
	sender := &recordingDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           "vacuum",
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

func TestBuildExecutorActivity(t *testing.T) {
	activity := buildExecutorActivity("running", "vacuum in progress")
	if activity == nil {
		t.Fatalf("expected non-nil activity")
	}
	if activity.Source != plugin_pb.ActivitySource_ACTIVITY_SOURCE_EXECUTOR {
		t.Fatalf("unexpected source: %v", activity.Source)
	}
	if activity.Stage != "running" {
		t.Fatalf("unexpected stage: %q", activity.Stage)
	}
	if activity.Message != "vacuum in progress" {
		t.Fatalf("unexpected message: %q", activity.Message)
	}
	if activity.CreatedAt == nil {
		t.Fatalf("expected created_at timestamp")
	}
}

type noopDetectionSender struct{}

func (noopDetectionSender) SendProposals(*plugin_pb.DetectionProposals) error { return nil }
func (noopDetectionSender) SendComplete(*plugin_pb.DetectionComplete) error   { return nil }

type noopExecutionSender struct{}

func (noopExecutionSender) SendProgress(*plugin_pb.JobProgressUpdate) error { return nil }
func (noopExecutionSender) SendCompleted(*plugin_pb.JobCompleted) error     { return nil }

type recordingDetectionSender struct {
	proposals *plugin_pb.DetectionProposals
	complete  *plugin_pb.DetectionComplete
}

func (r *recordingDetectionSender) SendProposals(proposals *plugin_pb.DetectionProposals) error {
	r.proposals = proposals
	return nil
}

func (r *recordingDetectionSender) SendComplete(complete *plugin_pb.DetectionComplete) error {
	r.complete = complete
	return nil
}
