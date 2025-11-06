package engine

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestArithmeticOperations(t *testing.T) {
	engine := NewTestSQLEngine()

	tests := []struct {
		name      string
		left      *schema_pb.Value
		right     *schema_pb.Value
		operator  ArithmeticOperator
		expected  *schema_pb.Value
		expectErr bool
	}{
		// Addition tests
		{
			name:      "Add two integers",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpAdd,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 15}},
			expectErr: false,
		},
		{
			name:      "Add integer and float",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 5.5}},
			operator:  OpAdd,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 15.5}},
			expectErr: false,
		},
		// Subtraction tests
		{
			name:      "Subtract two integers",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 3}},
			operator:  OpSub,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 7}},
			expectErr: false,
		},
		// Multiplication tests
		{
			name:      "Multiply two integers",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 6}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 7}},
			operator:  OpMul,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 42}},
			expectErr: false,
		},
		{
			name:      "Multiply with float",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: 2.5}},
			operator:  OpMul,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 12.5}},
			expectErr: false,
		},
		// Division tests
		{
			name:      "Divide two integers",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 20}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 4}},
			operator:  OpDiv,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 5.0}},
			expectErr: false,
		},
		{
			name:      "Division by zero",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 0}},
			operator:  OpDiv,
			expected:  nil,
			expectErr: true,
		},
		// Modulo tests
		{
			name:      "Modulo operation",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 17}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpMod,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 2}},
			expectErr: false,
		},
		{
			name:      "Modulo by zero",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 0}},
			operator:  OpMod,
			expected:  nil,
			expectErr: true,
		},
		// String conversion tests
		{
			name:      "Add string number to integer",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "15"}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpAdd,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 20.0}},
			expectErr: false,
		},
		{
			name:      "Invalid string conversion",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "not_a_number"}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpAdd,
			expected:  nil,
			expectErr: true,
		},
		// Boolean conversion tests
		{
			name:      "Add boolean to integer",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: true}},
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpAdd,
			expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 6.0}},
			expectErr: false,
		},
		// Null value tests
		{
			name:      "Add with null left operand",
			left:      nil,
			right:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			operator:  OpAdd,
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "Add with null right operand",
			left:      &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			right:     nil,
			operator:  OpAdd,
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.EvaluateArithmeticExpression(tt.left, tt.right, tt.operator)

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

			if !valuesEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIndividualArithmeticFunctions(t *testing.T) {
	engine := NewTestSQLEngine()

	left := &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 10}}
	right := &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 3}}

	// Test Add function
	result, err := engine.Add(left, right)
	if err != nil {
		t.Errorf("Add function failed: %v", err)
	}
	expected := &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 13}}
	if !valuesEqual(result, expected) {
		t.Errorf("Add: Expected %v, got %v", expected, result)
	}

	// Test Subtract function
	result, err = engine.Subtract(left, right)
	if err != nil {
		t.Errorf("Subtract function failed: %v", err)
	}
	expected = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 7}}
	if !valuesEqual(result, expected) {
		t.Errorf("Subtract: Expected %v, got %v", expected, result)
	}

	// Test Multiply function
	result, err = engine.Multiply(left, right)
	if err != nil {
		t.Errorf("Multiply function failed: %v", err)
	}
	expected = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 30}}
	if !valuesEqual(result, expected) {
		t.Errorf("Multiply: Expected %v, got %v", expected, result)
	}

	// Test Divide function
	result, err = engine.Divide(left, right)
	if err != nil {
		t.Errorf("Divide function failed: %v", err)
	}
	expected = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 10.0 / 3.0}}
	if !valuesEqual(result, expected) {
		t.Errorf("Divide: Expected %v, got %v", expected, result)
	}

	// Test Modulo function
	result, err = engine.Modulo(left, right)
	if err != nil {
		t.Errorf("Modulo function failed: %v", err)
	}
	expected = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1}}
	if !valuesEqual(result, expected) {
		t.Errorf("Modulo: Expected %v, got %v", expected, result)
	}
}

func TestMathematicalFunctions(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("ROUND function tests", func(t *testing.T) {
		tests := []struct {
			name      string
			value     *schema_pb.Value
			precision *schema_pb.Value
			expected  *schema_pb.Value
			expectErr bool
		}{
			{
				name:      "Round float to integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.7}},
				precision: nil,
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 4.0}},
				expectErr: false,
			},
			{
				name:      "Round integer stays integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				precision: nil,
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expectErr: false,
			},
			{
				name:      "Round with precision 2",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.14159}},
				precision: &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 2}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.14}},
				expectErr: false,
			},
			{
				name:      "Round negative number",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: -3.7}},
				precision: nil,
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: -4.0}},
				expectErr: false,
			},
			{
				name:      "Round null value",
				value:     nil,
				precision: nil,
				expected:  nil,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var result *schema_pb.Value
				var err error

				if tt.precision != nil {
					result, err = engine.Round(tt.value, tt.precision)
				} else {
					result, err = engine.Round(tt.value)
				}

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

				if !valuesEqual(result, tt.expected) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("CEIL function tests", func(t *testing.T) {
		tests := []struct {
			name      string
			value     *schema_pb.Value
			expected  *schema_pb.Value
			expectErr bool
		}{
			{
				name:      "Ceil positive decimal",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.2}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 4}},
				expectErr: false,
			},
			{
				name:      "Ceil negative decimal",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: -3.2}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: -3}},
				expectErr: false,
			},
			{
				name:      "Ceil integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expectErr: false,
			},
			{
				name:      "Ceil null value",
				value:     nil,
				expected:  nil,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := engine.Ceil(tt.value)

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

				if !valuesEqual(result, tt.expected) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("FLOOR function tests", func(t *testing.T) {
		tests := []struct {
			name      string
			value     *schema_pb.Value
			expected  *schema_pb.Value
			expectErr bool
		}{
			{
				name:      "Floor positive decimal",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.8}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 3}},
				expectErr: false,
			},
			{
				name:      "Floor negative decimal",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: -3.2}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: -4}},
				expectErr: false,
			},
			{
				name:      "Floor integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expectErr: false,
			},
			{
				name:      "Floor null value",
				value:     nil,
				expected:  nil,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := engine.Floor(tt.value)

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

				if !valuesEqual(result, tt.expected) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("ABS function tests", func(t *testing.T) {
		tests := []struct {
			name      string
			value     *schema_pb.Value
			expected  *schema_pb.Value
			expectErr bool
		}{
			{
				name:      "Abs positive integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expectErr: false,
			},
			{
				name:      "Abs negative integer",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: -5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
				expectErr: false,
			},
			{
				name:      "Abs positive double",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.14}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.14}},
				expectErr: false,
			},
			{
				name:      "Abs negative double",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: -3.14}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: 3.14}},
				expectErr: false,
			},
			{
				name:      "Abs positive float",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: 2.5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: 2.5}},
				expectErr: false,
			},
			{
				name:      "Abs negative float",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: -2.5}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: 2.5}},
				expectErr: false,
			},
			{
				name:      "Abs zero",
				value:     &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 0}},
				expected:  &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 0}},
				expectErr: false,
			},
			{
				name:      "Abs null value",
				value:     nil,
				expected:  nil,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := engine.Abs(tt.value)

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

				if !valuesEqual(result, tt.expected) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			})
		}
	})
}

// Helper function to compare two schema_pb.Value objects
func valuesEqual(v1, v2 *schema_pb.Value) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	switch v1Kind := v1.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_Int32Value); ok {
			return v1Kind.Int32Value == v2Kind.Int32Value
		}
	case *schema_pb.Value_Int64Value:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_Int64Value); ok {
			return v1Kind.Int64Value == v2Kind.Int64Value
		}
	case *schema_pb.Value_FloatValue:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_FloatValue); ok {
			return v1Kind.FloatValue == v2Kind.FloatValue
		}
	case *schema_pb.Value_DoubleValue:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_DoubleValue); ok {
			return v1Kind.DoubleValue == v2Kind.DoubleValue
		}
	case *schema_pb.Value_StringValue:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_StringValue); ok {
			return v1Kind.StringValue == v2Kind.StringValue
		}
	case *schema_pb.Value_BoolValue:
		if v2Kind, ok := v2.Kind.(*schema_pb.Value_BoolValue); ok {
			return v1Kind.BoolValue == v2Kind.BoolValue
		}
	}

	return false
}
