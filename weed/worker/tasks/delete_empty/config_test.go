package delete_empty

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

func TestNewDefaultConfig(t *testing.T) {
	cfg := NewDefaultConfig()
	if cfg == nil {
		t.Fatal("NewDefaultConfig returned nil")
	}
	if !cfg.Enabled {
		t.Error("Default config should be enabled")
	}
	if !cfg.DeleteEmptyEnabled {
		t.Error("Default config should have DeleteEmptyEnabled=true")
	}
	if cfg.QuietForSeconds != 24*60*60 {
		t.Errorf("Expected QuietForSeconds=86400, got %d", cfg.QuietForSeconds)
	}
	if cfg.ScanIntervalSeconds != 6*60*60 {
		t.Errorf("Expected ScanIntervalSeconds=21600, got %d", cfg.ScanIntervalSeconds)
	}
	if cfg.MaxConcurrent != 1 {
		t.Errorf("Expected MaxConcurrent=1, got %d", cfg.MaxConcurrent)
	}
}

func TestConfig_IsEnabled(t *testing.T) {
	cfg := NewDefaultConfig()
	if !cfg.IsEnabled() {
		t.Error("Expected IsEnabled()=true")
	}
	cfg.SetEnabled(false)
	if cfg.IsEnabled() {
		t.Error("Expected IsEnabled()=false after SetEnabled(false)")
	}
	cfg.SetEnabled(true)
	if !cfg.IsEnabled() {
		t.Error("Expected IsEnabled()=true after SetEnabled(true)")
	}
}

func TestConfig_ToTaskPolicy_RoundTrip(t *testing.T) {
	original := &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 7200,
			MaxConcurrent:       2,
		},
		DeleteEmptyEnabled: true,
		QuietForSeconds:    12 * 3600,
	}

	policy := original.ToTaskPolicy()
	if policy == nil {
		t.Fatal("ToTaskPolicy returned nil")
	}
	if !policy.Enabled {
		t.Error("Expected policy.Enabled=true")
	}
	if policy.MaxConcurrent != 2 {
		t.Errorf("Expected MaxConcurrent=2, got %d", policy.MaxConcurrent)
	}
	if policy.RepeatIntervalSeconds != 7200 {
		t.Errorf("Expected RepeatIntervalSeconds=7200, got %d", policy.RepeatIntervalSeconds)
	}
	// CheckIntervalSeconds now carries QuietForSeconds (12 h = 43200 s)
	if policy.CheckIntervalSeconds != 12*3600 {
		t.Errorf("Expected CheckIntervalSeconds=%d (QuietForSeconds), got %d", 12*3600, policy.CheckIntervalSeconds)
	}

	// Round-trip: policy → config
	restored := NewDefaultConfig()
	if err := restored.FromTaskPolicy(policy); err != nil {
		t.Fatalf("FromTaskPolicy error: %v", err)
	}
	if restored.Enabled != original.Enabled {
		t.Errorf("Enabled mismatch: %v vs %v", restored.Enabled, original.Enabled)
	}
	if restored.MaxConcurrent != original.MaxConcurrent {
		t.Errorf("MaxConcurrent mismatch: %d vs %d", restored.MaxConcurrent, original.MaxConcurrent)
	}
	if restored.ScanIntervalSeconds != original.ScanIntervalSeconds {
		t.Errorf("ScanIntervalSeconds mismatch: %d vs %d", restored.ScanIntervalSeconds, original.ScanIntervalSeconds)
	}
	if restored.QuietForSeconds != original.QuietForSeconds {
		t.Errorf("QuietForSeconds mismatch: %d vs %d", restored.QuietForSeconds, original.QuietForSeconds)
	}
}

func TestConfig_FromTaskPolicy_Nil(t *testing.T) {
	cfg := NewDefaultConfig()
	// nil policy should not error and should leave config unchanged
	if err := cfg.FromTaskPolicy(nil); err != nil {
		t.Errorf("FromTaskPolicy(nil) should not error, got: %v", err)
	}
	if !cfg.Enabled {
		t.Error("Enabled should remain true after nil policy")
	}
}

func TestConfig_Validate(t *testing.T) {
	cfg := NewDefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should be valid, got: %v", err)
	}
}

func TestGetConfigSpec(t *testing.T) {
	spec := GetConfigSpec()
	if len(spec.Fields) == 0 {
		t.Error("ConfigSpec should have at least one field")
	}

	fieldNames := make(map[string]bool)
	for _, f := range spec.Fields {
		fieldNames[f.JSONName] = true
	}

	required := []string{"enabled", "scan_interval_seconds", "delete_empty_enabled", "quiet_for_seconds"}
	for _, name := range required {
		if !fieldNames[name] {
			t.Errorf("ConfigSpec missing required field: %s", name)
		}
	}
}
