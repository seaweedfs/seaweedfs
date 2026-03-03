package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestBuildJobSpecFromProposalDoesNotReuseProposalID(t *testing.T) {
	t.Parallel()

	proposal := &plugin_pb.JobProposal{
		ProposalId: "vacuum-2",
		DedupeKey:  "vacuum:2",
		JobType:    "vacuum",
	}

	jobA := buildJobSpecFromProposal("vacuum", proposal, 0)
	jobB := buildJobSpecFromProposal("vacuum", proposal, 1)

	if jobA.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobA.JobId)
	}
	if jobB.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobB.JobId)
	}
	if jobA.JobId == jobB.JobId {
		t.Fatalf("job ids must be unique across jobs: %s", jobA.JobId)
	}
	if jobA.DedupeKey != proposal.DedupeKey {
		t.Fatalf("dedupe key must be preserved: got=%s want=%s", jobA.DedupeKey, proposal.DedupeKey)
	}
}

func TestApplyDescriptorDefaultsToPersistedConfigBackfillsAdminDefaults(t *testing.T) {
	t.Parallel()

	config := &plugin_pb.PersistedJobTypeConfig{
		JobType:            "admin_script",
		AdminConfigValues:  map[string]*plugin_pb.ConfigValue{},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{},
		AdminRuntime:       &plugin_pb.AdminRuntimeConfig{},
	}
	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType: "admin_script",
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"script": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "volume.balance -apply"},
				},
				"run_interval_minutes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 17},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			DetectionIntervalSeconds: 60,
			DetectionTimeoutSeconds:  300,
		},
	}

	applyDescriptorDefaultsToPersistedConfig(config, descriptor)

	script := config.AdminConfigValues["script"]
	if script == nil {
		t.Fatalf("expected script default to be backfilled")
	}
	scriptKind, ok := script.Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok || scriptKind.StringValue == "" {
		t.Fatalf("expected non-empty script default, got=%+v", script)
	}
	if config.AdminRuntime.DetectionIntervalSeconds != 60 {
		t.Fatalf("expected runtime detection interval default to be backfilled")
	}
}

func TestApplyDescriptorDefaultsToPersistedConfigReplacesBlankAdminScript(t *testing.T) {
	t.Parallel()

	config := &plugin_pb.PersistedJobTypeConfig{
		JobType: "admin_script",
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			"script": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "   "},
			},
		},
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{},
	}
	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType: "admin_script",
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"script": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "volume.fix.replication -apply"},
				},
			},
		},
	}

	applyDescriptorDefaultsToPersistedConfig(config, descriptor)

	script := config.AdminConfigValues["script"]
	if script == nil {
		t.Fatalf("expected script config value")
	}
	scriptKind, ok := script.Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok {
		t.Fatalf("expected string script config value, got=%T", script.Kind)
	}
	if scriptKind.StringValue != "volume.fix.replication -apply" {
		t.Fatalf("expected blank script to be replaced by default, got=%q", scriptKind.StringValue)
	}
}
