package engine

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestStringFunctions(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("LENGTH function tests", func(t *testing.T) {
		tests := []struct {
			name      string
			value     *schema_pb.Value
			expected  int64
			expectErr bool
		}{
			{
				name:      "Length of string",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}},
				expected:  11,
				expectErr: false,
			},
			{
				name:      "Length of empty string",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: ""}},
				expected:  0,
				expectErr: false,
			},
			{
				name:      "Length of number",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
				expected:  5,
				expectErr: false,
			},
			{
				name:      "Length of null value",
				value:     nil,
				expected:  0,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := engine.Length(tt.value)

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

				intVal, ok := result.Kind.(*schema_pb.Value_Int64Value)
				if !ok {
					t.Errorf("LENGTH should return int64 value, got %T", result.Kind)
					return
				}

				if intVal.Int64Value != tt.expected {
					t.Errorf("Expected %d, got %d", tt.expected, intVal.Int64Value)
				}
			})
		}
	})

	t.Run("UPPER/LOWER function tests", func(t *testing.T) {
		// Test UPPER
		result, err := engine.Upper(&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}})
		if err != nil {
			t.Errorf("UPPER failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "HELLO WORLD" {
			t.Errorf("Expected 'HELLO WORLD', got '%s'", stringVal.StringValue)
		}

		// Test LOWER
		result, err = engine.Lower(&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}})
		if err != nil {
			t.Errorf("LOWER failed: %v", err)
		}
		stringVal, _ = result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "hello world" {
			t.Errorf("Expected 'hello world', got '%s'", stringVal.StringValue)
		}
	})

	t.Run("TRIM function tests", func(t *testing.T) {
		tests := []struct {
			name     string
			function func(*schema_pb.Value) (*schema_pb.Value, error)
			input    string
			expected string
		}{
			{"TRIM whitespace", engine.Trim, "  Hello World  ", "Hello World"},
			{"LTRIM whitespace", engine.LTrim, "  Hello World  ", "Hello World  "},
			{"RTRIM whitespace", engine.RTrim, "  Hello World  ", "  Hello World"},
			{"TRIM with tabs and newlines", engine.Trim, "\t\nHello\t\n", "Hello"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := tt.function(&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: tt.input}})
				if err != nil {
					t.Errorf("Function failed: %v", err)
					return
				}

				stringVal, ok := result.Kind.(*schema_pb.Value_StringValue)
				if !ok {
					t.Errorf("Function should return string value, got %T", result.Kind)
					return
				}

				if stringVal.StringValue != tt.expected {
					t.Errorf("Expected '%s', got '%s'", tt.expected, stringVal.StringValue)
				}
			})
		}
	})

	t.Run("SUBSTRING function tests", func(t *testing.T) {
		testStr := &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}}

		// Test substring with start and length
		result, err := engine.Substring(testStr,
			&schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 7}},
			&schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}})
		if err != nil {
			t.Errorf("SUBSTRING failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "World" {
			t.Errorf("Expected 'World', got '%s'", stringVal.StringValue)
		}

		// Test substring with just start position
		result, err = engine.Substring(testStr,
			&schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 7}})
		if err != nil {
			t.Errorf("SUBSTRING failed: %v", err)
		}
		stringVal, _ = result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "World" {
			t.Errorf("Expected 'World', got '%s'", stringVal.StringValue)
		}
	})

	t.Run("CONCAT function tests", func(t *testing.T) {
		result, err := engine.Concat(
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello"}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: " "}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "World"}},
		)
		if err != nil {
			t.Errorf("CONCAT failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "Hello World" {
			t.Errorf("Expected 'Hello World', got '%s'", stringVal.StringValue)
		}

		// Test with mixed types
		result, err = engine.Concat(
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Number: "}},
			&schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 42}},
		)
		if err != nil {
			t.Errorf("CONCAT failed: %v", err)
		}
		stringVal, _ = result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "Number: 42" {
			t.Errorf("Expected 'Number: 42', got '%s'", stringVal.StringValue)
		}
	})

	t.Run("REPLACE function tests", func(t *testing.T) {
		result, err := engine.Replace(
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World World"}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "World"}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Universe"}},
		)
		if err != nil {
			t.Errorf("REPLACE failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "Hello Universe Universe" {
			t.Errorf("Expected 'Hello Universe Universe', got '%s'", stringVal.StringValue)
		}
	})

	t.Run("POSITION function tests", func(t *testing.T) {
		result, err := engine.Position(
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "World"}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}},
		)
		if err != nil {
			t.Errorf("POSITION failed: %v", err)
		}
		intVal, _ := result.Kind.(*schema_pb.Value_Int64Value)
		if intVal.Int64Value != 7 {
			t.Errorf("Expected 7, got %d", intVal.Int64Value)
		}

		// Test not found
		result, err = engine.Position(
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "NotFound"}},
			&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}},
		)
		if err != nil {
			t.Errorf("POSITION failed: %v", err)
		}
		intVal, _ = result.Kind.(*schema_pb.Value_Int64Value)
		if intVal.Int64Value != 0 {
			t.Errorf("Expected 0 for not found, got %d", intVal.Int64Value)
		}
	})

	t.Run("LEFT/RIGHT function tests", func(t *testing.T) {
		testStr := &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello World"}}

		// Test LEFT
		result, err := engine.Left(testStr, &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}})
		if err != nil {
			t.Errorf("LEFT failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "Hello" {
			t.Errorf("Expected 'Hello', got '%s'", stringVal.StringValue)
		}

		// Test RIGHT
		result, err = engine.Right(testStr, &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}})
		if err != nil {
			t.Errorf("RIGHT failed: %v", err)
		}
		stringVal, _ = result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "World" {
			t.Errorf("Expected 'World', got '%s'", stringVal.StringValue)
		}
	})

	t.Run("REVERSE function tests", func(t *testing.T) {
		result, err := engine.Reverse(&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "Hello"}})
		if err != nil {
			t.Errorf("REVERSE failed: %v", err)
		}
		stringVal, _ := result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "olleH" {
			t.Errorf("Expected 'olleH', got '%s'", stringVal.StringValue)
		}

		// Test with Unicode
		result, err = engine.Reverse(&schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "🙂👍"}})
		if err != nil {
			t.Errorf("REVERSE failed: %v", err)
		}
		stringVal, _ = result.Kind.(*schema_pb.Value_StringValue)
		if stringVal.StringValue != "👍🙂" {
			t.Errorf("Expected '👍🙂', got '%s'", stringVal.StringValue)
		}
	})
}

// TestStringFunctionsSQL tests string functions through SQL execution
func TestStringFunctionsSQL(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name        string
		sql         string
		expectError bool
		expectedVal string
	}{
		{
			name:        "UPPER function",
			sql:         "SELECT UPPER('hello world') AS upper_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "HELLO WORLD",
		},
		{
			name:        "LOWER function",
			sql:         "SELECT LOWER('HELLO WORLD') AS lower_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "hello world",
		},
		{
			name:        "LENGTH function",
			sql:         "SELECT LENGTH('hello') AS length_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "5",
		},
		{
			name:        "TRIM function",
			sql:         "SELECT TRIM('  hello world  ') AS trimmed_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "hello world",
		},
		{
			name:        "LTRIM function",
			sql:         "SELECT LTRIM('  hello world  ') AS ltrimmed_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "hello world  ",
		},
		{
			name:        "RTRIM function",
			sql:         "SELECT RTRIM('  hello world  ') AS rtrimmed_value FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "  hello world",
		},
		{
			name:        "Multiple string functions",
			sql:         "SELECT UPPER('hello') AS up, LOWER('WORLD') AS low, LENGTH('test') AS len FROM user_events LIMIT 1",
			expectError: false,
			expectedVal: "", // We'll check this separately
		},
		{
			name:        "String function with wrong argument count",
			sql:         "SELECT UPPER('hello', 'extra') FROM user_events LIMIT 1",
			expectError: true,
			expectedVal: "",
		},
		{
			name:        "String function with no arguments",
			sql:         "SELECT UPPER() FROM user_events LIMIT 1",
			expectError: true,
			expectedVal: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if tc.expectError {
				if err == nil && result.Error == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.Error != nil {
				t.Errorf("Query result has error: %v", result.Error)
				return
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected at least one row")
			}

			if tc.name == "Multiple string functions" {
				// Special case for multiple functions test
				if len(result.Rows[0]) != 3 {
					t.Fatalf("Expected 3 columns, got %d", len(result.Rows[0]))
				}

				// Check UPPER('hello') -> 'HELLO'
				if result.Rows[0][0].ToString() != "HELLO" {
					t.Errorf("Expected 'HELLO', got '%s'", result.Rows[0][0].ToString())
				}

				// Check LOWER('WORLD') -> 'world'
				if result.Rows[0][1].ToString() != "world" {
					t.Errorf("Expected 'world', got '%s'", result.Rows[0][1].ToString())
				}

				// Check LENGTH('test') -> '4'
				if result.Rows[0][2].ToString() != "4" {
					t.Errorf("Expected '4', got '%s'", result.Rows[0][2].ToString())
				}
			} else {
				actualVal := result.Rows[0][0].ToString()
				if actualVal != tc.expectedVal {
					t.Errorf("Expected '%s', got '%s'", tc.expectedVal, actualVal)
				}
			}
		})
	}
}
