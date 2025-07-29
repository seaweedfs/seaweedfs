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

// ApplySchemaDefaults implements ConfigWithDefaults for test struct
func (c *TestBaseConfigForSchema) ApplySchemaDefaults(schema *Schema) error {
	return schema.ApplyDefaultsToProtobuf(c)
}

// Validate implements ConfigWithDefaults for test struct
func (c *TestBaseConfigForSchema) Validate() error {
	return nil
}

type TestTaskConfigForSchema struct {
	TestBaseConfigForSchema
	TaskSpecificField    float64 `json:"task_specific_field"`
	AnotherSpecificField string  `json:"another_specific_field"`
}

// ApplySchemaDefaults implements ConfigWithDefaults for test struct
func (c *TestTaskConfigForSchema) ApplySchemaDefaults(schema *Schema) error {
	return schema.ApplyDefaultsToProtobuf(c)
}

// Validate implements ConfigWithDefaults for test struct
func (c *TestTaskConfigForSchema) Validate() error {
	return nil
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

	err := schema.ApplyDefaultsToConfig(config)
	if err != nil {
		t.Fatalf("ApplyDefaultsToConfig failed: %v", err)
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

	// Verify task-specific fields got default values
	if config.TaskSpecificField != 0.25 {
		t.Errorf("Expected TaskSpecificField=0.25 (default), got %v", config.TaskSpecificField)
	}

	if config.AnotherSpecificField != "default_value" {
		t.Errorf("Expected AnotherSpecificField='default_value' (default), got %v", config.AnotherSpecificField)
	}
}

func TestApplyDefaults_PartiallySet(t *testing.T) {
	schema := createTestSchema()

	// Start with some pre-set values
	config := &TestTaskConfigForSchema{
		TestBaseConfigForSchema: TestBaseConfigForSchema{
			Enabled:             true, // Non-zero value, should not be overridden
			ScanIntervalSeconds: 0,    // Should get default
			MaxConcurrent:       5,    // Non-zero value, should not be overridden
		},
		TaskSpecificField:    0.0,      // Should get default
		AnotherSpecificField: "custom", // Non-zero value, should not be overridden
	}

	err := schema.ApplyDefaultsToConfig(config)
	if err != nil {
		t.Fatalf("ApplyDefaultsToConfig failed: %v", err)
	}

	// Verify already-set values are preserved
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true (pre-set), got %v", config.Enabled)
	}

	if config.MaxConcurrent != 5 {
		t.Errorf("Expected MaxConcurrent=5 (pre-set), got %v", config.MaxConcurrent)
	}

	if config.AnotherSpecificField != "custom" {
		t.Errorf("Expected AnotherSpecificField='custom' (pre-set), got %v", config.AnotherSpecificField)
	}

	// Verify zero values got defaults
	if config.ScanIntervalSeconds != 1800 {
		t.Errorf("Expected ScanIntervalSeconds=1800 (default), got %v", config.ScanIntervalSeconds)
	}

	if config.TaskSpecificField != 0.25 {
		t.Errorf("Expected TaskSpecificField=0.25 (default), got %v", config.TaskSpecificField)
	}
}

func TestApplyDefaults_NonPointer(t *testing.T) {
	schema := createTestSchema()
	config := TestTaskConfigForSchema{}
	// This should fail since we need a pointer to modify the struct
	err := schema.ApplyDefaultsToProtobuf(config)
	if err == nil {
		t.Fatal("Expected error for non-pointer config, but got nil")
	}
}

func TestApplyDefaults_NonStruct(t *testing.T) {
	schema := createTestSchema()
	var config interface{} = "not a struct"
	err := schema.ApplyDefaultsToProtobuf(config)
	if err == nil {
		t.Fatal("Expected error for non-struct config, but got nil")
	}
}

func TestApplyDefaults_EmptySchema(t *testing.T) {
	schema := &Schema{Fields: []*Field{}}
	config := &TestTaskConfigForSchema{}

	err := schema.ApplyDefaultsToConfig(config)
	if err != nil {
		t.Fatalf("ApplyDefaultsToConfig failed for empty schema: %v", err)
	}

	// All fields should remain at zero values since no defaults are defined
	if config.Enabled != false {
		t.Errorf("Expected Enabled=false (zero value), got %v", config.Enabled)
	}
}

func TestApplyDefaults_MissingSchemaField(t *testing.T) {
	// Schema with fewer fields than the struct
	schema := &Schema{
		Fields: []*Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         FieldTypeBool,
				DefaultValue: true,
			},
			// Note: missing scan_interval_seconds and other fields
		},
	}

	config := &TestTaskConfigForSchema{}
	err := schema.ApplyDefaultsToConfig(config)
	if err != nil {
		t.Fatalf("ApplyDefaultsToConfig failed: %v", err)
	}

	// Only the field with a schema definition should get a default
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true (has schema), got %v", config.Enabled)
	}

	// Fields without schema should remain at zero values
	if config.ScanIntervalSeconds != 0 {
		t.Errorf("Expected ScanIntervalSeconds=0 (no schema), got %v", config.ScanIntervalSeconds)
	}
}

func BenchmarkApplyDefaults(b *testing.B) {
	schema := createTestSchema()
	config := &TestTaskConfigForSchema{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = schema.ApplyDefaultsToConfig(config)
	}
}
