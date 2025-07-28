package app

import (
	"testing"
)

// Test structs that mirror the actual configuration structure
type TestBaseConfigForTemplate struct {
	Enabled             bool `json:"enabled"`
	ScanIntervalSeconds int  `json:"scan_interval_seconds"`
	MaxConcurrent       int  `json:"max_concurrent"`
}

type TestTaskConfigForTemplate struct {
	TestBaseConfigForTemplate
	TaskSpecificField    float64 `json:"task_specific_field"`
	AnotherSpecificField string  `json:"another_specific_field"`
}

func TestGetTaskFieldValue_EmbeddedStructFields(t *testing.T) {
	config := &TestTaskConfigForTemplate{
		TestBaseConfigForTemplate: TestBaseConfigForTemplate{
			Enabled:             true,
			ScanIntervalSeconds: 2400,
			MaxConcurrent:       5,
		},
		TaskSpecificField:    0.18,
		AnotherSpecificField: "test_value",
	}

	// Test embedded struct fields
	tests := []struct {
		fieldName     string
		expectedValue interface{}
		description   string
	}{
		{"enabled", true, "BaseConfig boolean field"},
		{"scan_interval_seconds", 2400, "BaseConfig integer field"},
		{"max_concurrent", 5, "BaseConfig integer field"},
		{"task_specific_field", 0.18, "Task-specific float field"},
		{"another_specific_field", "test_value", "Task-specific string field"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			result := getTaskFieldValue(config, test.fieldName)

			if result != test.expectedValue {
				t.Errorf("Field %s: expected %v (%T), got %v (%T)",
					test.fieldName, test.expectedValue, test.expectedValue, result, result)
			}
		})
	}
}

func TestGetTaskFieldValue_NonExistentField(t *testing.T) {
	config := &TestTaskConfigForTemplate{
		TestBaseConfigForTemplate: TestBaseConfigForTemplate{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       3,
		},
	}

	result := getTaskFieldValue(config, "non_existent_field")

	if result != nil {
		t.Errorf("Expected nil for non-existent field, got %v", result)
	}
}

func TestGetTaskFieldValue_NilConfig(t *testing.T) {
	var config *TestTaskConfigForTemplate = nil

	result := getTaskFieldValue(config, "enabled")

	if result != nil {
		t.Errorf("Expected nil for nil config, got %v", result)
	}
}

func TestGetTaskFieldValue_EmptyStruct(t *testing.T) {
	config := &TestTaskConfigForTemplate{}

	// Test that we can extract zero values
	tests := []struct {
		fieldName     string
		expectedValue interface{}
		description   string
	}{
		{"enabled", false, "Zero value boolean"},
		{"scan_interval_seconds", 0, "Zero value integer"},
		{"max_concurrent", 0, "Zero value integer"},
		{"task_specific_field", 0.0, "Zero value float"},
		{"another_specific_field", "", "Zero value string"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			result := getTaskFieldValue(config, test.fieldName)

			if result != test.expectedValue {
				t.Errorf("Field %s: expected %v (%T), got %v (%T)",
					test.fieldName, test.expectedValue, test.expectedValue, result, result)
			}
		})
	}
}

func TestGetTaskFieldValue_NonStructConfig(t *testing.T) {
	var config interface{} = "not a struct"

	result := getTaskFieldValue(config, "enabled")

	if result != nil {
		t.Errorf("Expected nil for non-struct config, got %v", result)
	}
}

func TestGetTaskFieldValue_PointerToStruct(t *testing.T) {
	config := &TestTaskConfigForTemplate{
		TestBaseConfigForTemplate: TestBaseConfigForTemplate{
			Enabled:             false,
			ScanIntervalSeconds: 900,
			MaxConcurrent:       2,
		},
		TaskSpecificField: 0.35,
	}

	// Test that pointers are handled correctly
	enabledResult := getTaskFieldValue(config, "enabled")
	if enabledResult != false {
		t.Errorf("Expected false for enabled field, got %v", enabledResult)
	}

	intervalResult := getTaskFieldValue(config, "scan_interval_seconds")
	if intervalResult != 900 {
		t.Errorf("Expected 900 for scan_interval_seconds field, got %v", intervalResult)
	}
}

func TestGetTaskFieldValue_FieldsWithJSONOmitempty(t *testing.T) {
	// Test struct with omitempty tags
	type TestConfigWithOmitempty struct {
		TestBaseConfigForTemplate
		OptionalField string `json:"optional_field,omitempty"`
	}

	config := &TestConfigWithOmitempty{
		TestBaseConfigForTemplate: TestBaseConfigForTemplate{
			Enabled:             true,
			ScanIntervalSeconds: 1200,
			MaxConcurrent:       4,
		},
		OptionalField: "optional_value",
	}

	// Test that fields with omitempty are still found
	result := getTaskFieldValue(config, "optional_field")
	if result != "optional_value" {
		t.Errorf("Expected 'optional_value' for optional_field, got %v", result)
	}

	// Test embedded fields still work
	enabledResult := getTaskFieldValue(config, "enabled")
	if enabledResult != true {
		t.Errorf("Expected true for enabled field, got %v", enabledResult)
	}
}

func TestGetTaskFieldValue_DeepEmbedding(t *testing.T) {
	// Test with multiple levels of embedding
	type DeepBaseConfig struct {
		DeepField string `json:"deep_field"`
	}

	type MiddleConfig struct {
		DeepBaseConfig
		MiddleField int `json:"middle_field"`
	}

	type TopConfig struct {
		MiddleConfig
		TopField bool `json:"top_field"`
	}

	config := &TopConfig{
		MiddleConfig: MiddleConfig{
			DeepBaseConfig: DeepBaseConfig{
				DeepField: "deep_value",
			},
			MiddleField: 123,
		},
		TopField: true,
	}

	// Test that deeply embedded fields are found
	deepResult := getTaskFieldValue(config, "deep_field")
	if deepResult != "deep_value" {
		t.Errorf("Expected 'deep_value' for deep_field, got %v", deepResult)
	}

	middleResult := getTaskFieldValue(config, "middle_field")
	if middleResult != 123 {
		t.Errorf("Expected 123 for middle_field, got %v", middleResult)
	}

	topResult := getTaskFieldValue(config, "top_field")
	if topResult != true {
		t.Errorf("Expected true for top_field, got %v", topResult)
	}
}

// Benchmark to ensure performance is reasonable
func BenchmarkGetTaskFieldValue(b *testing.B) {
	config := &TestTaskConfigForTemplate{
		TestBaseConfigForTemplate: TestBaseConfigForTemplate{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       3,
		},
		TaskSpecificField:    0.25,
		AnotherSpecificField: "benchmark_test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test both embedded and regular fields
		_ = getTaskFieldValue(config, "enabled")
		_ = getTaskFieldValue(config, "task_specific_field")
	}
}
