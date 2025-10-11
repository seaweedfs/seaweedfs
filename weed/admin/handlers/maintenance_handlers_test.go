package handlers

import (
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
)

func TestParseTaskConfigFromForm_WithEmbeddedStruct(t *testing.T) {
	// Create a maintenance handlers instance for testing
	h := &MaintenanceHandlers{}

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
