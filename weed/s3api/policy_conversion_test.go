package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

func TestConvertPolicyDocumentWithMixedTypes(t *testing.T) {
	// Test that numeric and boolean values in arrays are properly converted
	src := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "TestMixedTypes",
				Effect: "Allow",
				Action: []string{"s3:GetObject"},
				Resource: []string{"arn:aws:s3:::bucket/*"},
				Principal: []interface{}{"user1", 123, true}, // Mixed types
				Condition: map[string]map[string]interface{}{
					"NumericEquals": {
						"s3:max-keys": []interface{}{100, 200, "300"}, // Mixed types
					},
					"StringEquals": {
						"s3:prefix": []interface{}{"test", 123, false}, // Mixed types
					},
				},
			},
		},
	}

	// Convert
	dest, err := ConvertPolicyDocumentToPolicyEngine(src)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify document structure
	if dest == nil {
		t.Fatal("Expected non-nil result")
	}
	if dest.Version != "2012-10-17" {
		t.Errorf("Expected version '2012-10-17', got '%s'", dest.Version)
	}
	if len(dest.Statement) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(dest.Statement))
	}

	stmt := dest.Statement[0]

	// Verify Principal conversion (should have 3 items converted to strings)
	if stmt.Principal == nil {
		t.Fatal("Expected non-nil Principal")
	}
	principals := stmt.Principal.Strings()
	if len(principals) != 3 {
		t.Errorf("Expected 3 principals, got %d", len(principals))
	}
	// Check that numeric and boolean were converted
	expectedPrincipals := []string{"user1", "123", "true"}
	for i, expected := range expectedPrincipals {
		if principals[i] != expected {
			t.Errorf("Principal[%d]: expected '%s', got '%s'", i, expected, principals[i])
		}
	}

	// Verify Condition conversion
	if len(stmt.Condition) != 2 {
		t.Errorf("Expected 2 condition blocks, got %d", len(stmt.Condition))
	}

	// Check NumericEquals condition
	numericCond, ok := stmt.Condition["NumericEquals"]
	if !ok {
		t.Fatal("Expected NumericEquals condition")
	}
	maxKeys, ok := numericCond["s3:max-keys"]
	if !ok {
		t.Fatal("Expected s3:max-keys in NumericEquals")
	}
	maxKeysStrs := maxKeys.Strings()
	expectedMaxKeys := []string{"100", "200", "300"}
	if len(maxKeysStrs) != len(expectedMaxKeys) {
		t.Errorf("Expected %d max-keys values, got %d", len(expectedMaxKeys), len(maxKeysStrs))
	}
	for i, expected := range expectedMaxKeys {
		if maxKeysStrs[i] != expected {
			t.Errorf("max-keys[%d]: expected '%s', got '%s'", i, expected, maxKeysStrs[i])
		}
	}

	// Check StringEquals condition  
	stringCond, ok := stmt.Condition["StringEquals"]
	if !ok {
		t.Fatal("Expected StringEquals condition")
	}
	prefix, ok := stringCond["s3:prefix"]
	if !ok {
		t.Fatal("Expected s3:prefix in StringEquals")
	}
	prefixStrs := prefix.Strings()
	expectedPrefix := []string{"test", "123", "false"}
	if len(prefixStrs) != len(expectedPrefix) {
		t.Errorf("Expected %d prefix values, got %d", len(expectedPrefix), len(prefixStrs))
	}
	for i, expected := range expectedPrefix {
		if prefixStrs[i] != expected {
			t.Errorf("prefix[%d]: expected '%s', got '%s'", i, expected, prefixStrs[i])
		}
	}
}

func TestConvertPrincipalWithMapAndMixedTypes(t *testing.T) {
	// Test AWS-style principal map with mixed types
	principalMap := map[string]interface{}{
		"AWS": []interface{}{
			"arn:aws:iam::123456789012:user/Alice",
			456, // User ID as number
			true, // Some boolean value
		},
	}

	result, err := convertPrincipal(principalMap)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	strs := result.Strings()
	if len(strs) != 3 {
		t.Errorf("Expected 3 values, got %d", len(strs))
	}

	expectedValues := []string{
		"arn:aws:iam::123456789012:user/Alice",
		"456",
		"true",
	}

	for i, expected := range expectedValues {
		if strs[i] != expected {
			t.Errorf("Value[%d]: expected '%s', got '%s'", i, expected, strs[i])
		}
	}
}

func TestConvertConditionValueWithMixedTypes(t *testing.T) {
	// Test []interface{} with mixed types
	mixedValues := []interface{}{
		"string",
		123,
		true,
		456.78,
	}

	result, err := convertConditionValue(mixedValues)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	strs := result.Strings()

	expectedValues := []string{"string", "123", "true", "456.78"}
	if len(strs) != len(expectedValues) {
		t.Errorf("Expected %d values, got %d", len(expectedValues), len(strs))
	}

	for i, expected := range expectedValues {
		if strs[i] != expected {
			t.Errorf("Value[%d]: expected '%s', got '%s'", i, expected, strs[i])
		}
	}
}

func TestConvertPolicyDocumentNil(t *testing.T) {
	result, err := ConvertPolicyDocumentToPolicyEngine(nil)
	if err != nil {
		t.Errorf("Unexpected error for nil input: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for nil input")
	}
}

func TestConvertPrincipalNil(t *testing.T) {
	result, err := convertPrincipal(nil)
	if err != nil {
		t.Errorf("Unexpected error for nil input: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for nil input")
	}
}

func TestConvertPrincipalEmptyArray(t *testing.T) {
	// Empty array should return nil
	result, err := convertPrincipal([]interface{}{})
	if err != nil {
		t.Errorf("Unexpected error for empty array: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for empty array")
	}
}

func TestConvertPrincipalUnknownType(t *testing.T) {
	// Unknown types should return an error
	result, err := convertPrincipal(12345) // Just a number, not valid principal
	if err == nil {
		t.Error("Expected error for unknown type")
	}
	if result != nil {
		t.Error("Expected nil result for unknown type")
	}
}

func TestConvertPrincipalWithNilValues(t *testing.T) {
	// Test that nil values in arrays are skipped for security
	principalArray := []interface{}{
		"arn:aws:iam::123456789012:user/Alice",
		nil, // Should be skipped
		"arn:aws:iam::123456789012:user/Bob",
		nil, // Should be skipped
	}

	result, err := convertPrincipal(principalArray)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	strs := result.Strings()
	// Should only have 2 values (nil values skipped)
	if len(strs) != 2 {
		t.Errorf("Expected 2 values (nil values skipped), got %d", len(strs))
	}

	expectedValues := []string{
		"arn:aws:iam::123456789012:user/Alice",
		"arn:aws:iam::123456789012:user/Bob",
	}

	for i, expected := range expectedValues {
		if strs[i] != expected {
			t.Errorf("Value[%d]: expected '%s', got '%s'", i, expected, strs[i])
		}
	}
}

func TestConvertConditionValueWithNilValues(t *testing.T) {
	// Test that nil values in condition arrays are skipped
	mixedValues := []interface{}{
		"string",
		nil, // Should be skipped
		123,
		nil, // Should be skipped
		true,
	}

	result, err := convertConditionValue(mixedValues)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	strs := result.Strings()

	// Should only have 3 values (nil values skipped)
	expectedValues := []string{"string", "123", "true"}
	if len(strs) != len(expectedValues) {
		t.Errorf("Expected %d values (nil values skipped), got %d", len(expectedValues), len(strs))
	}

	for i, expected := range expectedValues {
		if strs[i] != expected {
			t.Errorf("Value[%d]: expected '%s', got '%s'", i, expected, strs[i])
		}
	}
}

func TestConvertPrincipalMapWithNilValues(t *testing.T) {
	// Test AWS-style principal map with nil values
	principalMap := map[string]interface{}{
		"AWS": []interface{}{
			"arn:aws:iam::123456789012:user/Alice",
			nil, // Should be skipped
			"arn:aws:iam::123456789012:user/Bob",
		},
	}

	result, err := convertPrincipal(principalMap)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	strs := result.Strings()
	// Should only have 2 values (nil value skipped)
	if len(strs) != 2 {
		t.Errorf("Expected 2 values (nil value skipped), got %d", len(strs))
	}

	expectedValues := []string{
		"arn:aws:iam::123456789012:user/Alice",
		"arn:aws:iam::123456789012:user/Bob",
	}

	for i, expected := range expectedValues {
		if strs[i] != expected {
			t.Errorf("Value[%d]: expected '%s', got '%s'", i, expected, strs[i])
		}
	}
}

func TestConvertToStringUnsupportedType(t *testing.T) {
	// Test that unsupported types (e.g., nested maps/slices) return empty string
	// This should trigger a warning log but not fail
	
	type customStruct struct {
		Field string
	}
	
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "nested map",
			input:    map[string]interface{}{"key": "value"},
			expected: "", // Unsupported, returns empty string
		},
		{
			name:     "nested slice",
			input:    []int{1, 2, 3},
			expected: "", // Unsupported, returns empty string
		},
		{
			name:     "custom struct",
			input:    customStruct{Field: "test"},
			expected: "", // Unsupported, returns empty string
		},
		{
			name:     "function",
			input:    func() {},
			expected: "", // Unsupported, returns empty string
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertToString(tc.input)
			// For unsupported types, we expect an error
			if err == nil {
				t.Error("Expected error for unsupported type")
			}
			if result != tc.expected {
				t.Errorf("Expected '%s', got '%s'", tc.expected, result)
			}
		})
	}
}

func TestConvertToStringSupportedTypes(t *testing.T) {
	// Test that all supported types convert correctly
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string", "test", "test"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", 123, "123"},
		{"int8", int8(127), "127"},
		{"int16", int16(32767), "32767"},
		{"int32", int32(2147483647), "2147483647"},
		{"int64", int64(9223372036854775807), "9223372036854775807"},
		{"uint", uint(123), "123"},
		{"uint8", uint8(255), "255"},
		{"uint16", uint16(65535), "65535"},
		{"uint32", uint32(4294967295), "4294967295"},
		{"uint64", uint64(18446744073709551615), "18446744073709551615"},
		{"float32", float32(3.14), "3.14"},
		{"float64", float64(3.14159265359), "3.14159265359"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertToString(tc.input)
			if err != nil {
				t.Errorf("Unexpected error for supported type %s: %v", tc.name, err)
			}
			if result != tc.expected {
				t.Errorf("Expected '%s', got '%s'", tc.expected, result)
			}
		})
	}
}

func TestConvertPrincipalUnsupportedTypes(t *testing.T) {
	// Test that unsupported principal types return errors
	testCases := []struct {
		name      string
		principal interface{}
	}{
		{
			name:      "Service principal",
			principal: map[string]interface{}{"Service": "s3.amazonaws.com"},
		},
		{
			name:      "Federated principal",
			principal: map[string]interface{}{"Federated": "arn:aws:iam::123456789012:saml-provider/ExampleProvider"},
		},
		{
			name:      "Multiple keys",
			principal: map[string]interface{}{"AWS": "arn:...", "Service": "s3.amazonaws.com"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertPrincipal(tc.principal)
			if err == nil {
				t.Error("Expected error for unsupported principal type")
			}
			if result != nil {
				t.Error("Expected nil result for unsupported principal type")
			}
		})
	}
}

