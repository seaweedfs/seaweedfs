package pluginworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	vacuumtask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
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
	}

	cfg := deriveVacuumConfig(values)
	if cfg.GarbageThreshold != 0 {
		t.Fatalf("expected garbage_threshold 0, got %v", cfg.GarbageThreshold)
	}
	if cfg.MinVolumeAgeSeconds != 0 {
		t.Fatalf("expected min_volume_age_seconds 0, got %d", cfg.MinVolumeAgeSeconds)
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
	if ShouldSkipDetectionByInterval(nil, 10) {
		t.Fatalf("expected false when timestamp is nil")
	}
	if ShouldSkipDetectionByInterval(timestamppb.Now(), 0) {
		t.Fatalf("expected false when min interval is zero")
	}

	recent := timestamppb.New(time.Now().Add(-5 * time.Second))
	if !ShouldSkipDetectionByInterval(recent, 10) {
		t.Fatalf("expected true for recent successful run")
	}

	old := timestamppb.New(time.Now().Add(-30 * time.Second))
	if ShouldSkipDetectionByInterval(old, 10) {
		t.Fatalf("expected false for old successful run")
	}
}

func TestVacuumHandlerRejectsUnsupportedJobType(t *testing.T) {
	handler := NewVacuumHandler(nil, 0)
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

func TestBuildExecutorActivity(t *testing.T) {
	activity := BuildExecutorActivity("running", "vacuum in progress")
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

func TestEmitVacuumDetectionDecisionTraceNoTasks(t *testing.T) {
	sender := &recordingDetectionSender{}
	config := vacuumtask.NewDefaultConfig()
	config.GarbageThreshold = 0.3
	config.MinVolumeAgeSeconds = int((24 * time.Hour).Seconds())

	metrics := []*workertypes.VolumeHealthMetrics{
		{
			VolumeID:     17,
			GarbageRatio: 0,
			Age:          218*time.Hour + 23*time.Minute,
		},
		{
			VolumeID:     16,
			GarbageRatio: 0,
			Age:          218*time.Hour + 22*time.Minute,
		},
		{
			VolumeID:     6,
			GarbageRatio: 0,
			Age:          90*time.Hour + 42*time.Minute,
		},
	}

	if err := emitVacuumDetectionDecisionTrace(sender, metrics, config, nil); err != nil {
		t.Fatalf("emitVacuumDetectionDecisionTrace error: %v", err)
	}
	if len(sender.events) < 4 {
		t.Fatalf("expected at least 4 detection events, got %d", len(sender.events))
	}

	if sender.events[0].Source != plugin_pb.ActivitySource_ACTIVITY_SOURCE_DETECTOR {
		t.Fatalf("expected detector source, got %v", sender.events[0].Source)
	}
	if !strings.Contains(sender.events[0].Message, "VACUUM: No tasks created for 3 volumes") {
		t.Fatalf("unexpected summary message: %q", sender.events[0].Message)
	}
	if !strings.Contains(sender.events[1].Message, "VACUUM: Volume 17: garbage=0.00%") {
		t.Fatalf("unexpected first detail message: %q", sender.events[1].Message)
	}
}

func TestVacuumDescriptorHasNewFields(t *testing.T) {
	handler := NewVacuumHandler(nil, 2)
	desc := handler.Descriptor()
	if desc == nil {
		t.Fatal("descriptor is nil")
	}
	adminForm := desc.AdminConfigForm
	if adminForm == nil {
		t.Fatal("admin config form is nil")
	}
	if len(adminForm.Sections) == 0 {
		t.Fatal("admin config form has no sections")
	}

	scopeSection := adminForm.Sections[0]
	fieldNames := make(map[string]*plugin_pb.ConfigField)
	for _, f := range scopeSection.Fields {
		fieldNames[f.Name] = f
	}

	requiredFields := []string{
		"collection_filter",
		"volume_state",
		"data_center_filter",
		"rack_filter",
		"node_filter",
	}
	for _, name := range requiredFields {
		if _, ok := fieldNames[name]; !ok {
			t.Errorf("missing field %q in admin config scope section", name)
		}
	}

	// Verify volume_state is an enum with correct options.
	vsField := fieldNames["volume_state"]
	if vsField.FieldType != plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_ENUM {
		t.Errorf("volume_state field type = %v, want ENUM", vsField.FieldType)
	}
	if len(vsField.Options) != 3 {
		t.Errorf("volume_state options count = %d, want 3", len(vsField.Options))
	}

	// Verify default values exist for new fields.
	defaultKeys := []string{"volume_state", "data_center_filter", "rack_filter", "node_filter"}
	for _, key := range defaultKeys {
		if _, ok := adminForm.DefaultValues[key]; !ok {
			t.Errorf("missing default value for %q", key)
		}
	}

	// Verify volume_state default is ALL.
	vsDefault := adminForm.DefaultValues["volume_state"]
	if sv, ok := vsDefault.Kind.(*plugin_pb.ConfigValue_StringValue); !ok || sv.StringValue != "ALL" {
		t.Errorf("volume_state default = %v, want ALL", vsDefault)
	}

	// Verify collection_filter description mentions ALL_COLLECTIONS.
	cfField := fieldNames["collection_filter"]
	if cfField.Placeholder != "ALL_COLLECTIONS" {
		t.Errorf("collection_filter placeholder = %q, want ALL_COLLECTIONS", cfField.Placeholder)
	}
}

func TestVacuumFiltersVolumeState(t *testing.T) {
	metrics := []*workertypes.VolumeHealthMetrics{
		{VolumeID: 1, FullnessRatio: 0.5},  // active
		{VolumeID: 2, FullnessRatio: 1.5},  // full
		{VolumeID: 3, FullnessRatio: 0.9},  // active
		{VolumeID: 4, FullnessRatio: 1.01}, // full (at threshold)
	}

	tests := []struct {
		name    string
		state   volumeState
		wantIDs []uint32
	}{
		{"ALL returns all", volumeStateAll, []uint32{1, 2, 3, 4}},
		{"ACTIVE returns writable", volumeStateActive, []uint32{1, 3}},
		{"FULL returns read-only", volumeStateFull, []uint32{2, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filtered := filterMetricsByVolumeState(metrics, tt.state)
			if len(filtered) != len(tt.wantIDs) {
				t.Fatalf("got %d metrics, want %d", len(filtered), len(tt.wantIDs))
			}
			for i, m := range filtered {
				if m.VolumeID != tt.wantIDs[i] {
					t.Errorf("filtered[%d].VolumeID = %d, want %d", i, m.VolumeID, tt.wantIDs[i])
				}
			}
		})
	}
}

func TestVacuumFiltersLocation(t *testing.T) {
	metrics := []*workertypes.VolumeHealthMetrics{
		{VolumeID: 1, DataCenter: "dc1", Rack: "r1", Server: "s1"},
		{VolumeID: 2, DataCenter: "dc1", Rack: "r2", Server: "s2"},
		{VolumeID: 3, DataCenter: "dc2", Rack: "r1", Server: "s3"},
		{VolumeID: 4, DataCenter: "dc2", Rack: "r3", Server: "s4"},
	}

	tests := []struct {
		name    string
		dc      string
		rack    string
		node    string
		wantIDs []uint32
	}{
		{"dc filter", "dc1", "", "", []uint32{1, 2}},
		{"rack filter", "", "r1", "", []uint32{1, 3}},
		{"node filter", "", "", "s2,s4", []uint32{2, 4}},
		{"dc + rack", "dc1", "r1", "", []uint32{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filtered := filterMetricsByLocation(metrics, tt.dc, tt.rack, tt.node)
			if len(filtered) != len(tt.wantIDs) {
				t.Fatalf("got %d metrics, want %d", len(filtered), len(tt.wantIDs))
			}
			for i, m := range filtered {
				if m.VolumeID != tt.wantIDs[i] {
					t.Errorf("filtered[%d].VolumeID = %d, want %d", i, m.VolumeID, tt.wantIDs[i])
				}
			}
		})
	}
}

type noopDetectionSender struct{}

func (noopDetectionSender) SendProposals(*plugin_pb.DetectionProposals) error { return nil }
func (noopDetectionSender) SendComplete(*plugin_pb.DetectionComplete) error   { return nil }
func (noopDetectionSender) SendActivity(*plugin_pb.ActivityEvent) error       { return nil }

type noopExecutionSender struct{}

func (noopExecutionSender) SendProgress(*plugin_pb.JobProgressUpdate) error { return nil }
func (noopExecutionSender) SendCompleted(*plugin_pb.JobCompleted) error     { return nil }

type recordingDetectionSender struct {
	proposals *plugin_pb.DetectionProposals
	complete  *plugin_pb.DetectionComplete
	events    []*plugin_pb.ActivityEvent
}

func (r *recordingDetectionSender) SendProposals(proposals *plugin_pb.DetectionProposals) error {
	r.proposals = proposals
	return nil
}

func (r *recordingDetectionSender) SendComplete(complete *plugin_pb.DetectionComplete) error {
	r.complete = complete
	return nil
}

func (r *recordingDetectionSender) SendActivity(event *plugin_pb.ActivityEvent) error {
	if event != nil {
		r.events = append(r.events, event)
	}
	return nil
}
