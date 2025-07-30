package handlers

import (
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

func TestParseTaskConfigFromForm_WithEmbeddedStruct(t *testing.T) {
	// Create a maintenance handlers instance for testing
	h := &MaintenanceHandlers{}

	// Test with balance config
	t.Run("Balance Config", func(t *testing.T) {
		// Simulate form data
		formData := url.Values{
			"enabled":                     {"on"},      // checkbox field
			"scan_interval_seconds_value": {"30"},      // interval field
			"scan_interval_seconds_unit":  {"minutes"}, // interval unit
			"max_concurrent":              {"2"},       // number field
			"imbalance_threshold":         {"0.15"},    // float field
			"min_server_count":            {"3"},       // number field
		}

		// Get schema
		schema := tasks.GetTaskConfigSchema("balance")
		if schema == nil {
			t.Fatal("Failed to get balance schema")
		}

		// Create config instance
		config := &balance.Config{}

		// Parse form data
		err := h.parseTaskConfigFromForm(formData, schema, config)
		if err != nil {
			t.Fatalf("Failed to parse form data: %v", err)
		}

		// Verify embedded struct fields were set correctly
		if !config.Enabled {
			t.Errorf("Expected Enabled=true, got %v", config.Enabled)
		}

		if config.ScanIntervalSeconds != 1800 { // 30 minutes * 60
			t.Errorf("Expected ScanIntervalSeconds=1800, got %v", config.ScanIntervalSeconds)
		}

		if config.MaxConcurrent != 2 {
			t.Errorf("Expected MaxConcurrent=2, got %v", config.MaxConcurrent)
		}

		// Verify balance-specific fields were set correctly
		if config.ImbalanceThreshold != 0.15 {
			t.Errorf("Expected ImbalanceThreshold=0.15, got %v", config.ImbalanceThreshold)
		}

		if config.MinServerCount != 3 {
			t.Errorf("Expected MinServerCount=3, got %v", config.MinServerCount)
		}
	})

	// Test with vacuum config
	t.Run("Vacuum Config", func(t *testing.T) {
		// Simulate form data
		formData := url.Values{
			// "enabled" field omitted to simulate unchecked checkbox
			"scan_interval_seconds_value":  {"4"},     // interval field
			"scan_interval_seconds_unit":   {"hours"}, // interval unit
			"max_concurrent":               {"3"},     // number field
			"garbage_threshold":            {"0.4"},   // float field
			"min_volume_age_seconds_value": {"2"},     // interval field
			"min_volume_age_seconds_unit":  {"days"},  // interval unit
			"min_interval_seconds_value":   {"1"},     // interval field
			"min_interval_seconds_unit":    {"days"},  // interval unit
		}

		// Get schema
		schema := tasks.GetTaskConfigSchema("vacuum")
		if schema == nil {
			t.Fatal("Failed to get vacuum schema")
		}

		// Create config instance
		config := &vacuum.Config{}

		// Parse form data
		err := h.parseTaskConfigFromForm(formData, schema, config)
		if err != nil {
			t.Fatalf("Failed to parse form data: %v", err)
		}

		// Verify embedded struct fields were set correctly
		if config.Enabled {
			t.Errorf("Expected Enabled=false, got %v", config.Enabled)
		}

		if config.ScanIntervalSeconds != 14400 { // 4 hours * 3600
			t.Errorf("Expected ScanIntervalSeconds=14400, got %v", config.ScanIntervalSeconds)
		}

		if config.MaxConcurrent != 3 {
			t.Errorf("Expected MaxConcurrent=3, got %v", config.MaxConcurrent)
		}

		// Verify vacuum-specific fields were set correctly
		if config.GarbageThreshold != 0.4 {
			t.Errorf("Expected GarbageThreshold=0.4, got %v", config.GarbageThreshold)
		}

		if config.MinVolumeAgeSeconds != 172800 { // 2 days * 86400
			t.Errorf("Expected MinVolumeAgeSeconds=172800, got %v", config.MinVolumeAgeSeconds)
		}

		if config.MinIntervalSeconds != 86400 { // 1 day * 86400
			t.Errorf("Expected MinIntervalSeconds=86400, got %v", config.MinIntervalSeconds)
		}
	})

	// Test with erasure coding config
	t.Run("Erasure Coding Config", func(t *testing.T) {
		// Simulate form data
		formData := url.Values{
			"enabled":                     {"on"},              // checkbox field
			"scan_interval_seconds_value": {"2"},               // interval field
			"scan_interval_seconds_unit":  {"hours"},           // interval unit
			"max_concurrent":              {"1"},               // number field
			"quiet_for_seconds_value":     {"10"},              // interval field
			"quiet_for_seconds_unit":      {"minutes"},         // interval unit
			"fullness_ratio":              {"0.85"},            // float field
			"collection_filter":           {"test_collection"}, // string field
			"min_size_mb":                 {"50"},              // number field
		}

		// Get schema
		schema := tasks.GetTaskConfigSchema("erasure_coding")
		if schema == nil {
			t.Fatal("Failed to get erasure_coding schema")
		}

		// Create config instance
		config := &erasure_coding.Config{}

		// Parse form data
		err := h.parseTaskConfigFromForm(formData, schema, config)
		if err != nil {
			t.Fatalf("Failed to parse form data: %v", err)
		}

		// Verify embedded struct fields were set correctly
		if !config.Enabled {
			t.Errorf("Expected Enabled=true, got %v", config.Enabled)
		}

		if config.ScanIntervalSeconds != 7200 { // 2 hours * 3600
			t.Errorf("Expected ScanIntervalSeconds=7200, got %v", config.ScanIntervalSeconds)
		}

		if config.MaxConcurrent != 1 {
			t.Errorf("Expected MaxConcurrent=1, got %v", config.MaxConcurrent)
		}

		// Verify erasure coding-specific fields were set correctly
		if config.QuietForSeconds != 600 { // 10 minutes * 60
			t.Errorf("Expected QuietForSeconds=600, got %v", config.QuietForSeconds)
		}

		if config.FullnessRatio != 0.85 {
			t.Errorf("Expected FullnessRatio=0.85, got %v", config.FullnessRatio)
		}

		if config.CollectionFilter != "test_collection" {
			t.Errorf("Expected CollectionFilter='test_collection', got %v", config.CollectionFilter)
		}

		if config.MinSizeMB != 50 {
			t.Errorf("Expected MinSizeMB=50, got %v", config.MinSizeMB)
		}
	})
}

func TestConfigurationValidation(t *testing.T) {
	// Test that config structs can be validated and converted to protobuf format
	taskTypes := []struct {
		name   string
		config interface{}
	}{
		{
			"balance",
			&balance.Config{
				BaseConfig: base.BaseConfig{
					Enabled:             true,
					ScanIntervalSeconds: 2400,
					MaxConcurrent:       3,
				},
				ImbalanceThreshold: 0.18,
				MinServerCount:     4,
			},
		},
		{
			"vacuum",
			&vacuum.Config{
				BaseConfig: base.BaseConfig{
					Enabled:             false,
					ScanIntervalSeconds: 7200,
					MaxConcurrent:       2,
				},
				GarbageThreshold:    0.35,
				MinVolumeAgeSeconds: 86400,
				MinIntervalSeconds:  604800,
			},
		},
		{
			"erasure_coding",
			&erasure_coding.Config{
				BaseConfig: base.BaseConfig{
					Enabled:             true,
					ScanIntervalSeconds: 3600,
					MaxConcurrent:       1,
				},
				QuietForSeconds:  900,
				FullnessRatio:    0.9,
				CollectionFilter: "important",
				MinSizeMB:        100,
			},
		},
	}

	for _, test := range taskTypes {
		t.Run(test.name, func(t *testing.T) {
			// Test that configs can be converted to protobuf TaskPolicy
			switch cfg := test.config.(type) {
			case *balance.Config:
				policy := cfg.ToTaskPolicy()
				if policy == nil {
					t.Fatal("ToTaskPolicy returned nil")
				}
				if policy.Enabled != cfg.Enabled {
					t.Errorf("Expected Enabled=%v, got %v", cfg.Enabled, policy.Enabled)
				}
				if policy.MaxConcurrent != int32(cfg.MaxConcurrent) {
					t.Errorf("Expected MaxConcurrent=%v, got %v", cfg.MaxConcurrent, policy.MaxConcurrent)
				}
			case *vacuum.Config:
				policy := cfg.ToTaskPolicy()
				if policy == nil {
					t.Fatal("ToTaskPolicy returned nil")
				}
				if policy.Enabled != cfg.Enabled {
					t.Errorf("Expected Enabled=%v, got %v", cfg.Enabled, policy.Enabled)
				}
				if policy.MaxConcurrent != int32(cfg.MaxConcurrent) {
					t.Errorf("Expected MaxConcurrent=%v, got %v", cfg.MaxConcurrent, policy.MaxConcurrent)
				}
			case *erasure_coding.Config:
				policy := cfg.ToTaskPolicy()
				if policy == nil {
					t.Fatal("ToTaskPolicy returned nil")
				}
				if policy.Enabled != cfg.Enabled {
					t.Errorf("Expected Enabled=%v, got %v", cfg.Enabled, policy.Enabled)
				}
				if policy.MaxConcurrent != int32(cfg.MaxConcurrent) {
					t.Errorf("Expected MaxConcurrent=%v, got %v", cfg.MaxConcurrent, policy.MaxConcurrent)
				}
			default:
				t.Fatalf("Unknown config type: %T", test.config)
			}

			// Test that configs can be validated
			switch cfg := test.config.(type) {
			case *balance.Config:
				if err := cfg.Validate(); err != nil {
					t.Errorf("Validation failed: %v", err)
				}
			case *vacuum.Config:
				if err := cfg.Validate(); err != nil {
					t.Errorf("Validation failed: %v", err)
				}
			case *erasure_coding.Config:
				if err := cfg.Validate(); err != nil {
					t.Errorf("Validation failed: %v", err)
				}
			}
		})
	}
}

func TestParseFieldFromForm_EdgeCases(t *testing.T) {
	h := &MaintenanceHandlers{}

	// Test checkbox parsing (boolean fields)
	t.Run("Checkbox Fields", func(t *testing.T) {
		tests := []struct {
			name          string
			formData      url.Values
			expectedValue bool
		}{
			{"Checked checkbox", url.Values{"test_field": {"on"}}, true},
			{"Unchecked checkbox", url.Values{}, false},
			{"Empty value checkbox", url.Values{"test_field": {""}}, true}, // Present but empty means checked
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				schema := &tasks.TaskConfigSchema{
					Schema: config.Schema{
						Fields: []*config.Field{
							{
								JSONName:  "test_field",
								Type:      config.FieldTypeBool,
								InputType: "checkbox",
							},
						},
					},
				}

				type TestConfig struct {
					TestField bool `json:"test_field"`
				}

				config := &TestConfig{}
				err := h.parseTaskConfigFromForm(test.formData, schema, config)
				if err != nil {
					t.Fatalf("parseTaskConfigFromForm failed: %v", err)
				}

				if config.TestField != test.expectedValue {
					t.Errorf("Expected %v, got %v", test.expectedValue, config.TestField)
				}
			})
		}
	})

	// Test interval parsing
	t.Run("Interval Fields", func(t *testing.T) {
		tests := []struct {
			name         string
			value        string
			unit         string
			expectedSecs int
		}{
			{"Minutes", "30", "minutes", 1800},
			{"Hours", "2", "hours", 7200},
			{"Days", "1", "days", 86400},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				formData := url.Values{
					"test_field_value": {test.value},
					"test_field_unit":  {test.unit},
				}

				schema := &tasks.TaskConfigSchema{
					Schema: config.Schema{
						Fields: []*config.Field{
							{
								JSONName:  "test_field",
								Type:      config.FieldTypeInterval,
								InputType: "interval",
							},
						},
					},
				}

				type TestConfig struct {
					TestField int `json:"test_field"`
				}

				config := &TestConfig{}
				err := h.parseTaskConfigFromForm(formData, schema, config)
				if err != nil {
					t.Fatalf("parseTaskConfigFromForm failed: %v", err)
				}

				if config.TestField != test.expectedSecs {
					t.Errorf("Expected %d seconds, got %d", test.expectedSecs, config.TestField)
				}
			})
		}
	})
}
