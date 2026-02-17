package plugin

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestEnsureJobTypeConfigFromDescriptorBootstrapsDefaults(t *testing.T) {
	t.Parallel()

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType:           "vacuum",
		DescriptorVersion: 3,
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"scan_scope": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "all"}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      60,
			DetectionTimeoutSeconds:       20,
			MaxJobsPerDetection:           30,
			GlobalExecutionConcurrency:    4,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    3,
			RetryBackoffSeconds:           5,
		},
	}

	if err := runtime.ensureJobTypeConfigFromDescriptor("vacuum", descriptor); err != nil {
		t.Fatalf("ensureJobTypeConfigFromDescriptor: %v", err)
	}

	cfg, err := runtime.LoadJobTypeConfig("vacuum")
	if err != nil {
		t.Fatalf("LoadJobTypeConfig: %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected non-nil config")
	}
	if cfg.DescriptorVersion != 3 {
		t.Fatalf("unexpected descriptor version: got=%d", cfg.DescriptorVersion)
	}
	if cfg.AdminRuntime == nil || !cfg.AdminRuntime.Enabled {
		t.Fatalf("expected enabled admin runtime")
	}
	if cfg.AdminRuntime.GlobalExecutionConcurrency != 4 {
		t.Fatalf("unexpected global execution concurrency: %d", cfg.AdminRuntime.GlobalExecutionConcurrency)
	}
	if _, ok := cfg.AdminConfigValues["scan_scope"]; !ok {
		t.Fatalf("missing admin default value")
	}
	if _, ok := cfg.WorkerConfigValues["threshold"]; !ok {
		t.Fatalf("missing worker default value")
	}
}

func TestEnsureJobTypeConfigFromDescriptorDoesNotOverwriteExisting(t *testing.T) {
	t.Parallel()

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	if err := runtime.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: "balance",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                    true,
			GlobalExecutionConcurrency: 9,
		},
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			"custom": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "keep"}},
		},
	}); err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}

	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType:           "balance",
		DescriptorVersion: 7,
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"custom": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "overwrite"}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                    true,
			GlobalExecutionConcurrency: 1,
		},
	}

	if err := runtime.ensureJobTypeConfigFromDescriptor("balance", descriptor); err != nil {
		t.Fatalf("ensureJobTypeConfigFromDescriptor: %v", err)
	}

	cfg, err := runtime.LoadJobTypeConfig("balance")
	if err != nil {
		t.Fatalf("LoadJobTypeConfig: %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected config")
	}
	if cfg.AdminRuntime == nil || cfg.AdminRuntime.GlobalExecutionConcurrency != 9 {
		t.Fatalf("existing runtime config should be preserved, got=%v", cfg.AdminRuntime)
	}
	custom := cfg.AdminConfigValues["custom"]
	if custom == nil || custom.GetStringValue() != "keep" {
		t.Fatalf("existing admin config should be preserved")
	}
}
