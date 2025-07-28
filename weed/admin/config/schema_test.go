package config

import (
	"testing"
)

// Test structs that mirror the actual configuration structure
type TestBaseConfigForSchema struct {
	Enabled             bool `json:"enabled"`
	ScanIntervalSeconds int  `json:"scan_interval_seconds"`
	MaxConcurrent       int  `json:"max_concurrent"`
}

type TestTaskConfigForSchema struct {
	TestBaseConfigForSchema
	TaskSpecificField    float64 `json:"task_specific_field"`
	AnotherSpecificField string  `json:"another_specific_field"`
}

func createTestSchema() *Schema {
	return &Schema{
		Fields: []*Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         FieldTypeBool,
				DefaultValue: true,
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         FieldTypeInt,
				DefaultValue: 1800,
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         FieldTypeInt,
				DefaultValue: 3,
			},
			{
				Name:         "task_specific_field",
				JSONName:     "task_specific_field",
				Type:         FieldTypeFloat,
				DefaultValue: 0.25,
			},
			{
				Name:         "another_specific_field",
				JSONName:     "another_specific_field",
				Type:         FieldTypeString,
				DefaultValue: "default_value",
			},
		},
	}
}

func TestApplyDefaults_WithEmbeddedStruct(t *testing.T) {
	schema := createTestSchema()

	// Start with zero values
	config := &TestTaskConfigForSchema{}

	err := schema.ApplyDefaults(config)
	if err != nil {
		t.Fatalf("ApplyDefaults failed: %v", err)
	}

	// Verify embedded struct fields got default values
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true (default), got %v", config.Enabled)
	}

	if config.ScanIntervalSeconds != 1800 {
		t.Errorf("Expected ScanIntervalSeconds=1800 (default), got %v", config.ScanIntervalSeconds)
	}

	if config.MaxConcurrent != 3 {
		t.Errorf("Expected MaxConcurrent=3 (default), got %v", config.MaxConcurrent)
	}

	// Verify regular fields got default values
	if config.TaskSpecificField != 0.25 {
		t.Errorf("Expected TaskSpecificField=0.25 (default), got %v", config.TaskSpecificField)
	}

	if config.AnotherSpecificField != "default_value" {
		t.Errorf("Expected AnotherSpecificField='default_value' (default), got %v", config.AnotherSpecificField)
	}
}

func TestApplyDefaults_PartiallySet(t *testing.T) {
	schema := createTestSchema()

	// Start with some values already set
	config := &TestTaskConfigForSchema{
		TestBaseConfigForSchema: TestBaseConfigForSchema{
			Enabled:             true, // Non-zero value, should not be overridden
			ScanIntervalSeconds: 0,    // Zero value, should get default
			MaxConcurrent:       5,    // Non-zero value, should not be overridden
		},
		TaskSpecificField:    0.0,      // Zero value, should get default
		AnotherSpecificField: "custom", // Non-zero value, should not be overridden
	}

	err := schema.ApplyDefaults(config)
	if err != nil {
		t.Fatalf("ApplyDefaults failed: %v", err)
	}

	// Verify non-zero values were preserved
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true (preserved), got %v", config.Enabled)
	}

	if config.MaxConcurrent != 5 {
		t.Errorf("Expected MaxConcurrent=5 (preserved), got %v", config.MaxConcurrent)
	}

	if config.AnotherSpecificField != "custom" {
		t.Errorf("Expected AnotherSpecificField='custom' (preserved), got %v", config.AnotherSpecificField)
	}

	// Verify zero values got defaults
	if config.ScanIntervalSeconds != 1800 {
		t.Errorf("Expected ScanIntervalSeconds=1800 (default applied), got %v", config.ScanIntervalSeconds)
	}

	if config.TaskSpecificField != 0.25 {
		t.Errorf("Expected TaskSpecificField=0.25 (default applied), got %v", config.TaskSpecificField)
	}
}

func TestApplyDefaults_NonPointer(t *testing.T) {
	schema := createTestSchema()

	config := TestTaskConfigForSchema{} // Not a pointer
	err := schema.ApplyDefaults(config)

	if err == nil {
		t.Errorf("Expected error for non-pointer input, but got none")
	}
}

func TestApplyDefaults_NonStruct(t *testing.T) {
	schema := createTestSchema()

	var config interface{} = "not a struct"
	err := schema.ApplyDefaults(config)

	if err == nil {
		t.Errorf("Expected error for non-struct input, but got none")
	}
}

func TestApplyDefaults_EmptySchema(t *testing.T) {
	schema := &Schema{
		Fields: []*Field{}, // No fields
	}

	config := &TestTaskConfigForSchema{}
	err := schema.ApplyDefaults(config)

	if err != nil {
		t.Fatalf("ApplyDefaults failed for empty schema: %v", err)
	}

	// All fields should remain zero values
	if config.Enabled != false {
		t.Errorf("Expected Enabled=false (zero value), got %v", config.Enabled)
	}

	if config.ScanIntervalSeconds != 0 {
		t.Errorf("Expected ScanIntervalSeconds=0 (zero value), got %v", config.ScanIntervalSeconds)
	}
}

func TestApplyDefaults_MissingSchemaField(t *testing.T) {
	// Schema missing some fields that exist in struct
	schema := &Schema{
		Fields: []*Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         FieldTypeBool,
				DefaultValue: true,
			},
			// Missing scan_interval_seconds, max_concurrent, etc.
		},
	}

	config := &TestTaskConfigForSchema{}
	err := schema.ApplyDefaults(config)

	if err != nil {
		t.Fatalf("ApplyDefaults failed: %v", err)
	}

	// Only the field with schema should get default
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true (default applied), got %v", config.Enabled)
	}

	// Fields without schema should remain zero
	if config.ScanIntervalSeconds != 0 {
		t.Errorf("Expected ScanIntervalSeconds=0 (no schema), got %v", config.ScanIntervalSeconds)
	}

	if config.MaxConcurrent != 0 {
		t.Errorf("Expected MaxConcurrent=0 (no schema), got %v", config.MaxConcurrent)
	}
}

// Benchmark to ensure performance is reasonable
func BenchmarkApplyDefaults(b *testing.B) {
	schema := createTestSchema()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config := &TestTaskConfigForSchema{}
		_ = schema.ApplyDefaults(config)
	}
}
