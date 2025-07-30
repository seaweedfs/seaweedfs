package base

import (
	"reflect"
	"testing"
)

// Test structs that mirror the actual configuration structure
type TestBaseConfig struct {
	Enabled             bool `json:"enabled"`
	ScanIntervalSeconds int  `json:"scan_interval_seconds"`
	MaxConcurrent       int  `json:"max_concurrent"`
}

type TestTaskConfig struct {
	TestBaseConfig
	TaskSpecificField    float64 `json:"task_specific_field"`
	AnotherSpecificField string  `json:"another_specific_field"`
}

type TestNestedConfig struct {
	TestBaseConfig
	NestedStruct struct {
		NestedField string `json:"nested_field"`
	} `json:"nested_struct"`
	TaskField int `json:"task_field"`
}

func TestStructToMap_WithEmbeddedStruct(t *testing.T) {
	// Test case 1: Basic embedded struct
	config := &TestTaskConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       3,
		},
		TaskSpecificField:    0.25,
		AnotherSpecificField: "test_value",
	}

	result := StructToMap(config)

	// Verify all fields are present
	expectedFields := map[string]interface{}{
		"enabled":                true,
		"scan_interval_seconds":  1800,
		"max_concurrent":         3,
		"task_specific_field":    0.25,
		"another_specific_field": "test_value",
	}

	if len(result) != len(expectedFields) {
		t.Errorf("Expected %d fields, got %d. Result: %+v", len(expectedFields), len(result), result)
	}

	for key, expectedValue := range expectedFields {
		if actualValue, exists := result[key]; !exists {
			t.Errorf("Missing field: %s", key)
		} else if !reflect.DeepEqual(actualValue, expectedValue) {
			t.Errorf("Field %s: expected %v (%T), got %v (%T)", key, expectedValue, expectedValue, actualValue, actualValue)
		}
	}
}

func TestStructToMap_WithNestedStruct(t *testing.T) {
	config := &TestNestedConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             false,
			ScanIntervalSeconds: 3600,
			MaxConcurrent:       1,
		},
		NestedStruct: struct {
			NestedField string `json:"nested_field"`
		}{
			NestedField: "nested_value",
		},
		TaskField: 42,
	}

	result := StructToMap(config)

	// Verify embedded struct fields are included
	if enabled, exists := result["enabled"]; !exists || enabled != false {
		t.Errorf("Expected enabled=false from embedded struct, got %v", enabled)
	}

	if scanInterval, exists := result["scan_interval_seconds"]; !exists || scanInterval != 3600 {
		t.Errorf("Expected scan_interval_seconds=3600 from embedded struct, got %v", scanInterval)
	}

	if maxConcurrent, exists := result["max_concurrent"]; !exists || maxConcurrent != 1 {
		t.Errorf("Expected max_concurrent=1 from embedded struct, got %v", maxConcurrent)
	}

	// Verify regular fields are included
	if taskField, exists := result["task_field"]; !exists || taskField != 42 {
		t.Errorf("Expected task_field=42, got %v", taskField)
	}

	// Verify nested struct is included as a whole
	if nestedStruct, exists := result["nested_struct"]; !exists {
		t.Errorf("Missing nested_struct field")
	} else {
		// The nested struct should be included as-is, not flattened
		if nested, ok := nestedStruct.(struct {
			NestedField string `json:"nested_field"`
		}); !ok || nested.NestedField != "nested_value" {
			t.Errorf("Expected nested_struct with NestedField='nested_value', got %v", nestedStruct)
		}
	}
}

func TestMapToStruct_WithEmbeddedStruct(t *testing.T) {
	// Test data with all fields including embedded struct fields
	data := map[string]interface{}{
		"enabled":                true,
		"scan_interval_seconds":  2400,
		"max_concurrent":         5,
		"task_specific_field":    0.15,
		"another_specific_field": "updated_value",
	}

	config := &TestTaskConfig{}
	err := MapToStruct(data, config)

	if err != nil {
		t.Fatalf("MapToStruct failed: %v", err)
	}

	// Verify embedded struct fields were set
	if config.Enabled != true {
		t.Errorf("Expected Enabled=true, got %v", config.Enabled)
	}

	if config.ScanIntervalSeconds != 2400 {
		t.Errorf("Expected ScanIntervalSeconds=2400, got %v", config.ScanIntervalSeconds)
	}

	if config.MaxConcurrent != 5 {
		t.Errorf("Expected MaxConcurrent=5, got %v", config.MaxConcurrent)
	}

	// Verify regular fields were set
	if config.TaskSpecificField != 0.15 {
		t.Errorf("Expected TaskSpecificField=0.15, got %v", config.TaskSpecificField)
	}

	if config.AnotherSpecificField != "updated_value" {
		t.Errorf("Expected AnotherSpecificField='updated_value', got %v", config.AnotherSpecificField)
	}
}

func TestMapToStruct_PartialData(t *testing.T) {
	// Test with only some fields present (simulating form data)
	data := map[string]interface{}{
		"enabled":             false,
		"max_concurrent":      2,
		"task_specific_field": 0.30,
	}

	// Start with some initial values
	config := &TestTaskConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       1,
		},
		TaskSpecificField:    0.20,
		AnotherSpecificField: "initial_value",
	}

	err := MapToStruct(data, config)

	if err != nil {
		t.Fatalf("MapToStruct failed: %v", err)
	}

	// Verify updated fields
	if config.Enabled != false {
		t.Errorf("Expected Enabled=false (updated), got %v", config.Enabled)
	}

	if config.MaxConcurrent != 2 {
		t.Errorf("Expected MaxConcurrent=2 (updated), got %v", config.MaxConcurrent)
	}

	if config.TaskSpecificField != 0.30 {
		t.Errorf("Expected TaskSpecificField=0.30 (updated), got %v", config.TaskSpecificField)
	}

	// Verify unchanged fields remain the same
	if config.ScanIntervalSeconds != 1800 {
		t.Errorf("Expected ScanIntervalSeconds=1800 (unchanged), got %v", config.ScanIntervalSeconds)
	}

	if config.AnotherSpecificField != "initial_value" {
		t.Errorf("Expected AnotherSpecificField='initial_value' (unchanged), got %v", config.AnotherSpecificField)
	}
}

func TestRoundTripSerialization(t *testing.T) {
	// Test complete round-trip: struct -> map -> struct
	original := &TestTaskConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 3600,
			MaxConcurrent:       4,
		},
		TaskSpecificField:    0.18,
		AnotherSpecificField: "round_trip_test",
	}

	// Convert to map
	dataMap := StructToMap(original)

	// Convert back to struct
	roundTrip := &TestTaskConfig{}
	err := MapToStruct(dataMap, roundTrip)

	if err != nil {
		t.Fatalf("Round-trip MapToStruct failed: %v", err)
	}

	// Verify all fields match
	if !reflect.DeepEqual(original.TestBaseConfig, roundTrip.TestBaseConfig) {
		t.Errorf("BaseConfig mismatch:\nOriginal: %+v\nRound-trip: %+v", original.TestBaseConfig, roundTrip.TestBaseConfig)
	}

	if original.TaskSpecificField != roundTrip.TaskSpecificField {
		t.Errorf("TaskSpecificField mismatch: %v != %v", original.TaskSpecificField, roundTrip.TaskSpecificField)
	}

	if original.AnotherSpecificField != roundTrip.AnotherSpecificField {
		t.Errorf("AnotherSpecificField mismatch: %v != %v", original.AnotherSpecificField, roundTrip.AnotherSpecificField)
	}
}

func TestStructToMap_EmptyStruct(t *testing.T) {
	config := &TestTaskConfig{}
	result := StructToMap(config)

	// Should still include all fields, even with zero values
	expectedFields := []string{"enabled", "scan_interval_seconds", "max_concurrent", "task_specific_field", "another_specific_field"}

	for _, field := range expectedFields {
		if _, exists := result[field]; !exists {
			t.Errorf("Missing field: %s", field)
		}
	}
}

func TestStructToMap_NilPointer(t *testing.T) {
	var config *TestTaskConfig = nil
	result := StructToMap(config)

	if len(result) != 0 {
		t.Errorf("Expected empty map for nil pointer, got %+v", result)
	}
}

func TestMapToStruct_InvalidInput(t *testing.T) {
	data := map[string]interface{}{
		"enabled": "not_a_bool", // Wrong type
	}

	config := &TestTaskConfig{}
	err := MapToStruct(data, config)

	if err == nil {
		t.Errorf("Expected error for invalid input type, but got none")
	}
}

func TestMapToStruct_NonPointer(t *testing.T) {
	data := map[string]interface{}{
		"enabled": true,
	}

	config := TestTaskConfig{} // Not a pointer
	err := MapToStruct(data, config)

	if err == nil {
		t.Errorf("Expected error for non-pointer input, but got none")
	}
}

// Benchmark tests to ensure performance is reasonable
func BenchmarkStructToMap(b *testing.B) {
	config := &TestTaskConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       3,
		},
		TaskSpecificField:    0.25,
		AnotherSpecificField: "benchmark_test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StructToMap(config)
	}
}

func BenchmarkMapToStruct(b *testing.B) {
	data := map[string]interface{}{
		"enabled":                true,
		"scan_interval_seconds":  1800,
		"max_concurrent":         3,
		"task_specific_field":    0.25,
		"another_specific_field": "benchmark_test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config := &TestTaskConfig{}
		_ = MapToStruct(data, config)
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	original := &TestTaskConfig{
		TestBaseConfig: TestBaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 1800,
			MaxConcurrent:       3,
		},
		TaskSpecificField:    0.25,
		AnotherSpecificField: "benchmark_test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataMap := StructToMap(original)
		roundTrip := &TestTaskConfig{}
		_ = MapToStruct(dataMap, roundTrip)
	}
}
