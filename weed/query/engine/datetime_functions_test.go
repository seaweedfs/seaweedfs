package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestDateTimeFunctions(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("CURRENT_DATE function tests", func(t *testing.T) {
		before := time.Now()
		result, err := engine.CurrentDate()
		after := time.Now()

		if err != nil {
			t.Errorf("CurrentDate failed: %v", err)
		}

		if result == nil {
			t.Errorf("CurrentDate returned nil result")
			return
		}

		stringVal, ok := result.Kind.(*schema_pb.Value_StringValue)
		if !ok {
			t.Errorf("CurrentDate should return string value, got %T", result.Kind)
			return
		}

		// Check format (YYYY-MM-DD) with tolerance for midnight boundary crossings
		beforeDate := before.Format("2006-01-02")
		afterDate := after.Format("2006-01-02")

		if stringVal.StringValue != beforeDate && stringVal.StringValue != afterDate {
			t.Errorf("Expected current date %s or %s (due to potential midnight boundary), got %s",
				beforeDate, afterDate, stringVal.StringValue)
		}
	})

	t.Run("CURRENT_TIMESTAMP function tests", func(t *testing.T) {
		before := time.Now()
		result, err := engine.CurrentTimestamp()
		after := time.Now()

		if err != nil {
			t.Errorf("CurrentTimestamp failed: %v", err)
		}

		if result == nil {
			t.Errorf("CurrentTimestamp returned nil result")
			return
		}

		timestampVal, ok := result.Kind.(*schema_pb.Value_TimestampValue)
		if !ok {
			t.Errorf("CurrentTimestamp should return timestamp value, got %T", result.Kind)
			return
		}

		timestamp := time.UnixMicro(timestampVal.TimestampValue.TimestampMicros)

		// Check that timestamp is within reasonable range with small tolerance buffer
		// Allow for small timing variations, clock precision differences, and NTP adjustments
		tolerance := 100 * time.Millisecond
		beforeWithTolerance := before.Add(-tolerance)
		afterWithTolerance := after.Add(tolerance)

		if timestamp.Before(beforeWithTolerance) || timestamp.After(afterWithTolerance) {
			t.Errorf("Timestamp %v should be within tolerance of %v to %v (tolerance: %v)",
				timestamp, before, after, tolerance)
		}
	})

	t.Run("NOW function tests", func(t *testing.T) {
		result, err := engine.Now()
		if err != nil {
			t.Errorf("Now failed: %v", err)
		}

		if result == nil {
			t.Errorf("Now returned nil result")
			return
		}

		// Should return same type as CurrentTimestamp
		_, ok := result.Kind.(*schema_pb.Value_TimestampValue)
		if !ok {
			t.Errorf("Now should return timestamp value, got %T", result.Kind)
		}
	})

	t.Run("CURRENT_TIME function tests", func(t *testing.T) {
		result, err := engine.CurrentTime()
		if err != nil {
			t.Errorf("CurrentTime failed: %v", err)
		}

		if result == nil {
			t.Errorf("CurrentTime returned nil result")
			return
		}

		stringVal, ok := result.Kind.(*schema_pb.Value_StringValue)
		if !ok {
			t.Errorf("CurrentTime should return string value, got %T", result.Kind)
			return
		}

		// Check format (HH:MM:SS)
		if len(stringVal.StringValue) != 8 || stringVal.StringValue[2] != ':' || stringVal.StringValue[5] != ':' {
			t.Errorf("CurrentTime should return HH:MM:SS format, got %s", stringVal.StringValue)
		}
	})
}

func TestExtractFunction(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create a test timestamp: 2023-06-15 14:30:45
	// Use local time to avoid timezone conversion issues
	testTime := time.Date(2023, 6, 15, 14, 30, 45, 0, time.Local)
	testTimestamp := &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: testTime.UnixMicro(),
			},
		},
	}

	tests := []struct {
		name      string
		part      DatePart
		value     *schema_pb.Value
		expected  int64
		expectErr bool
	}{
		{
			name:      "Extract YEAR",
			part:      PartYear,
			value:     testTimestamp,
			expected:  2023,
			expectErr: false,
		},
		{
			name:      "Extract MONTH",
			part:      PartMonth,
			value:     testTimestamp,
			expected:  6,
			expectErr: false,
		},
		{
			name:      "Extract DAY",
			part:      PartDay,
			value:     testTimestamp,
			expected:  15,
			expectErr: false,
		},
		{
			name:      "Extract HOUR",
			part:      PartHour,
			value:     testTimestamp,
			expected:  14,
			expectErr: false,
		},
		{
			name:      "Extract MINUTE",
			part:      PartMinute,
			value:     testTimestamp,
			expected:  30,
			expectErr: false,
		},
		{
			name:      "Extract SECOND",
			part:      PartSecond,
			value:     testTimestamp,
			expected:  45,
			expectErr: false,
		},
		{
			name:      "Extract QUARTER from June",
			part:      PartQuarter,
			value:     testTimestamp,
			expected:  2, // June is in Q2
			expectErr: false,
		},
		{
			name:      "Extract from string date",
			part:      PartYear,
			value:     &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "2023-06-15"}},
			expected:  2023,
			expectErr: false,
		},
		{
			name:      "Extract from Unix timestamp",
			part:      PartYear,
			value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: testTime.Unix()}},
			expected:  2023,
			expectErr: false,
		},
		{
			name:      "Extract from null value",
			part:      PartYear,
			value:     nil,
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Extract invalid part",
			part:      DatePart("INVALID"),
			value:     testTimestamp,
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Extract from invalid string",
			part:      PartYear,
			value:     &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "invalid-date"}},
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Extract(tt.part, tt.value)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Errorf("Extract returned nil result")
				return
			}

			intVal, ok := result.Kind.(*schema_pb.Value_Int64Value)
			if !ok {
				t.Errorf("Extract should return int64 value, got %T", result.Kind)
				return
			}

			if intVal.Int64Value != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, intVal.Int64Value)
			}
		})
	}
}

func TestDateTruncFunction(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create a test timestamp: 2023-06-15 14:30:45.123456
	testTime := time.Date(2023, 6, 15, 14, 30, 45, 123456000, time.Local) // nanoseconds
	testTimestamp := &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: testTime.UnixMicro(),
			},
		},
	}

	tests := []struct {
		name          string
		precision     string
		value         *schema_pb.Value
		expectErr     bool
		expectedCheck func(result time.Time) bool // Custom check function
	}{
		{
			name:      "Truncate to second",
			precision: "second",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 15 &&
					result.Hour() == 14 && result.Minute() == 30 && result.Second() == 45 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to minute",
			precision: "minute",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 15 &&
					result.Hour() == 14 && result.Minute() == 30 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to hour",
			precision: "hour",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 15 &&
					result.Hour() == 14 && result.Minute() == 0 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to day",
			precision: "day",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 15 &&
					result.Hour() == 0 && result.Minute() == 0 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to month",
			precision: "month",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 1 &&
					result.Hour() == 0 && result.Minute() == 0 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to quarter",
			precision: "quarter",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				// June (month 6) should truncate to April (month 4) - start of Q2
				return result.Year() == 2023 && result.Month() == 4 && result.Day() == 1 &&
					result.Hour() == 0 && result.Minute() == 0 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate to year",
			precision: "year",
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 1 && result.Day() == 1 &&
					result.Hour() == 0 && result.Minute() == 0 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate with plural precision",
			precision: "minutes", // Test plural form
			value:     testTimestamp,
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				return result.Year() == 2023 && result.Month() == 6 && result.Day() == 15 &&
					result.Hour() == 14 && result.Minute() == 30 && result.Second() == 0 &&
					result.Nanosecond() == 0
			},
		},
		{
			name:      "Truncate from string date",
			precision: "day",
			value:     &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "2023-06-15 14:30:45"}},
			expectErr: false,
			expectedCheck: func(result time.Time) bool {
				// The result should be the start of day 2023-06-15 in local timezone
				expectedDay := time.Date(2023, 6, 15, 0, 0, 0, 0, result.Location())
				return result.Equal(expectedDay)
			},
		},
		{
			name:          "Truncate null value",
			precision:     "day",
			value:         nil,
			expectErr:     true,
			expectedCheck: nil,
		},
		{
			name:          "Invalid precision",
			precision:     "invalid",
			value:         testTimestamp,
			expectErr:     true,
			expectedCheck: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.DateTrunc(tt.precision, tt.value)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Errorf("DateTrunc returned nil result")
				return
			}

			timestampVal, ok := result.Kind.(*schema_pb.Value_TimestampValue)
			if !ok {
				t.Errorf("DateTrunc should return timestamp value, got %T", result.Kind)
				return
			}

			resultTime := time.UnixMicro(timestampVal.TimestampValue.TimestampMicros)

			if !tt.expectedCheck(resultTime) {
				t.Errorf("DateTrunc result check failed for precision %s, got time: %v", tt.precision, resultTime)
			}
		})
	}
}

// TestZeroArgumentFunctionsBugFix tests the fix for the CURRENT_TIME empty values bug
// This test reproduces the original issue where zero-argument functions like CURRENT_TIME
// were failing in evaluateStringFunction because they were treated as single-argument functions
func TestZeroArgumentFunctionsBugFix(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("CURRENT_TIME in evaluateStringFunction context", func(t *testing.T) {
		// Create a FuncExpr that represents CURRENT_TIME() with zero arguments
		funcExpr := &FuncExpr{
			Name:  testStringValue("CURRENT_TIME"),
			Exprs: []SelectExpr{}, // Zero arguments - this was the bug
		}

		// Create a mock result (the function shouldn't use it for zero-arg functions)
		mockResult := HybridScanResult{}

		// This would have failed before the fix with "function CURRENT_TIME expects exactly 1 argument"
		result, err := engine.evaluateStringFunction(funcExpr, mockResult)

		if err != nil {
			t.Errorf("evaluateStringFunction failed for CURRENT_TIME: %v", err)
			return
		}

		if result == nil {
			t.Errorf("evaluateStringFunction returned nil for CURRENT_TIME")
			return
		}

		stringVal, ok := result.Kind.(*schema_pb.Value_StringValue)
		if !ok {
			t.Errorf("CURRENT_TIME should return string value, got %T", result.Kind)
			return
		}

		// Verify HH:MM:SS format
		if len(stringVal.StringValue) != 8 || stringVal.StringValue[2] != ':' || stringVal.StringValue[5] != ':' {
			t.Errorf("CURRENT_TIME should return HH:MM:SS format, got %s", stringVal.StringValue)
		}

		t.Logf("CURRENT_TIME via evaluateStringFunction returned: %s", stringVal.StringValue)
	})

	t.Run("CURRENT_DATE in evaluateStringFunction context", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name:  testStringValue("CURRENT_DATE"),
			Exprs: []SelectExpr{}, // Zero arguments
		}

		mockResult := HybridScanResult{}
		result, err := engine.evaluateStringFunction(funcExpr, mockResult)

		if err != nil {
			t.Errorf("evaluateStringFunction failed for CURRENT_DATE: %v", err)
			return
		}

		if result == nil {
			t.Errorf("evaluateStringFunction returned nil for CURRENT_DATE")
			return
		}

		stringVal, ok := result.Kind.(*schema_pb.Value_StringValue)
		if !ok {
			t.Errorf("CURRENT_DATE should return string value, got %T", result.Kind)
			return
		}

		// Verify YYYY-MM-DD format
		if len(stringVal.StringValue) != 10 || stringVal.StringValue[4] != '-' || stringVal.StringValue[7] != '-' {
			t.Errorf("CURRENT_DATE should return YYYY-MM-DD format, got %s", stringVal.StringValue)
		}

		t.Logf("CURRENT_DATE via evaluateStringFunction returned: %s", stringVal.StringValue)
	})

	t.Run("NOW in evaluateStringFunction context", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name:  testStringValue("NOW"),
			Exprs: []SelectExpr{}, // Zero arguments
		}

		mockResult := HybridScanResult{}
		result, err := engine.evaluateStringFunction(funcExpr, mockResult)

		if err != nil {
			t.Errorf("evaluateStringFunction failed for NOW: %v", err)
			return
		}

		if result == nil {
			t.Errorf("evaluateStringFunction returned nil for NOW")
			return
		}

		timestampVal, ok := result.Kind.(*schema_pb.Value_TimestampValue)
		if !ok {
			t.Errorf("NOW should return timestamp value, got %T", result.Kind)
			return
		}

		if timestampVal.TimestampValue.TimestampMicros <= 0 {
			t.Errorf("NOW should return positive timestamp, got %d", timestampVal.TimestampValue.TimestampMicros)
		}

		t.Logf("NOW via evaluateStringFunction returned timestamp: %d", timestampVal.TimestampValue.TimestampMicros)
	})

	t.Run("CURRENT_TIMESTAMP in evaluateStringFunction context", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name:  testStringValue("CURRENT_TIMESTAMP"),
			Exprs: []SelectExpr{}, // Zero arguments
		}

		mockResult := HybridScanResult{}
		result, err := engine.evaluateStringFunction(funcExpr, mockResult)

		if err != nil {
			t.Errorf("evaluateStringFunction failed for CURRENT_TIMESTAMP: %v", err)
			return
		}

		if result == nil {
			t.Errorf("evaluateStringFunction returned nil for CURRENT_TIMESTAMP")
			return
		}

		timestampVal, ok := result.Kind.(*schema_pb.Value_TimestampValue)
		if !ok {
			t.Errorf("CURRENT_TIMESTAMP should return timestamp value, got %T", result.Kind)
			return
		}

		if timestampVal.TimestampValue.TimestampMicros <= 0 {
			t.Errorf("CURRENT_TIMESTAMP should return positive timestamp, got %d", timestampVal.TimestampValue.TimestampMicros)
		}

		t.Logf("CURRENT_TIMESTAMP via evaluateStringFunction returned timestamp: %d", timestampVal.TimestampValue.TimestampMicros)
	})
}

// TestFunctionArgumentCountHandling tests that the function evaluation correctly handles
// both zero-argument and single-argument functions
func TestFunctionArgumentCountHandling(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("Zero-argument function should work", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name:  testStringValue("CURRENT_TIME"),
			Exprs: []SelectExpr{}, // Zero arguments - should work
		}

		result, err := engine.evaluateStringFunction(funcExpr, HybridScanResult{})
		if err != nil {
			t.Errorf("Zero-argument function failed: %v", err)
		}
		if result == nil {
			t.Errorf("Zero-argument function returned nil")
		}
	})

	t.Run("Single-argument function should still work", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name: testStringValue("UPPER"),
			Exprs: []SelectExpr{
				&AliasedExpr{
					Expr: &SQLVal{
						Type: StrVal,
						Val:  []byte("test"),
					},
				},
			}, // Single argument - should work
		}

		// Create a mock result
		mockResult := HybridScanResult{}

		result, err := engine.evaluateStringFunction(funcExpr, mockResult)
		if err != nil {
			t.Errorf("Single-argument function failed: %v", err)
		}
		if result == nil {
			t.Errorf("Single-argument function returned nil")
		}
	})

	t.Run("Unsupported zero-argument function should fail gracefully", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name:  testStringValue("INVALID_FUNCTION"),
			Exprs: []SelectExpr{}, // Zero arguments but invalid function
		}

		result, err := engine.evaluateStringFunction(funcExpr, HybridScanResult{})
		if err == nil {
			t.Errorf("Expected error for unsupported zero-argument function, got nil")
		}
		if result != nil {
			t.Errorf("Expected nil result for unsupported function, got %v", result)
		}

		expectedError := "unsupported zero-argument function: INVALID_FUNCTION"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	t.Run("Wrong argument count for single-arg function should fail", func(t *testing.T) {
		funcExpr := &FuncExpr{
			Name: testStringValue("UPPER"),
			Exprs: []SelectExpr{
				&AliasedExpr{Expr: &SQLVal{Type: StrVal, Val: []byte("test1")}},
				&AliasedExpr{Expr: &SQLVal{Type: StrVal, Val: []byte("test2")}},
			}, // Two arguments - should fail for UPPER
		}

		result, err := engine.evaluateStringFunction(funcExpr, HybridScanResult{})
		if err == nil {
			t.Errorf("Expected error for wrong argument count, got nil")
		}
		if result != nil {
			t.Errorf("Expected nil result for wrong argument count, got %v", result)
		}

		expectedError := "function UPPER expects exactly 1 argument"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})
}

// Helper function to create a string value for testing
func testStringValue(s string) StringGetter {
	return &testStringValueImpl{value: s}
}

type testStringValueImpl struct {
	value string
}

func (s *testStringValueImpl) String() string {
	return s.value
}
