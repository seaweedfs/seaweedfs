package engine

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestArithmeticExpressionParsing(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		expectNil  bool
		leftCol    string
		rightCol   string
		operator   string
	}{
		{
			name:       "simple addition",
			expression: "id+user_id",
			expectNil:  false,
			leftCol:    "id",
			rightCol:   "user_id",
			operator:   "+",
		},
		{
			name:       "simple subtraction",
			expression: "col1-col2",
			expectNil:  false,
			leftCol:    "col1",
			rightCol:   "col2",
			operator:   "-",
		},
		{
			name:       "multiplication with spaces",
			expression: "a * b",
			expectNil:  false,
			leftCol:    "a",
			rightCol:   "b",
			operator:   "*",
		},
		{
			name:       "string concatenation",
			expression: "first_name||last_name",
			expectNil:  false,
			leftCol:    "first_name",
			rightCol:   "last_name",
			operator:   "||",
		},
		{
			name:       "string concatenation with spaces",
			expression: "prefix || suffix",
			expectNil:  false,
			leftCol:    "prefix",
			rightCol:   "suffix",
			operator:   "||",
		},
		{
			name:       "not arithmetic",
			expression: "simple_column",
			expectNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use CockroachDB parser to parse the expression
			cockroachParser := NewCockroachSQLParser()
			dummySelect := fmt.Sprintf("SELECT %s", tt.expression)
			stmt, err := cockroachParser.ParseSQL(dummySelect)

			var result *ArithmeticExpr
			if err == nil {
				if selectStmt, ok := stmt.(*SelectStatement); ok && len(selectStmt.SelectExprs) > 0 {
					if aliasedExpr, ok := selectStmt.SelectExprs[0].(*AliasedExpr); ok {
						if arithmeticExpr, ok := aliasedExpr.Expr.(*ArithmeticExpr); ok {
							result = arithmeticExpr
						}
					}
				}
			}

			if tt.expectNil {
				if result != nil {
					t.Errorf("Expected nil for %s, got %v", tt.expression, result)
				}
				return
			}

			if result == nil {
				t.Errorf("Expected arithmetic expression for %s, got nil", tt.expression)
				return
			}

			if result.Operator != tt.operator {
				t.Errorf("Expected operator %s, got %s", tt.operator, result.Operator)
			}

			// Check left operand
			if leftCol, ok := result.Left.(*ColName); ok {
				if leftCol.Name.String() != tt.leftCol {
					t.Errorf("Expected left column %s, got %s", tt.leftCol, leftCol.Name.String())
				}
			} else {
				t.Errorf("Expected left operand to be ColName, got %T", result.Left)
			}

			// Check right operand
			if rightCol, ok := result.Right.(*ColName); ok {
				if rightCol.Name.String() != tt.rightCol {
					t.Errorf("Expected right column %s, got %s", tt.rightCol, rightCol.Name.String())
				}
			} else {
				t.Errorf("Expected right operand to be ColName, got %T", result.Right)
			}
		})
	}
}

func TestArithmeticExpressionEvaluation(t *testing.T) {
	engine := NewSQLEngine("")

	// Create test data
	result := HybridScanResult{
		Values: map[string]*schema_pb.Value{
			"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 10}},
			"user_id":    {Kind: &schema_pb.Value_Int64Value{Int64Value: 5}},
			"price":      {Kind: &schema_pb.Value_DoubleValue{DoubleValue: 25.5}},
			"qty":        {Kind: &schema_pb.Value_Int64Value{Int64Value: 3}},
			"first_name": {Kind: &schema_pb.Value_StringValue{StringValue: "John"}},
			"last_name":  {Kind: &schema_pb.Value_StringValue{StringValue: "Doe"}},
			"prefix":     {Kind: &schema_pb.Value_StringValue{StringValue: "Hello"}},
			"suffix":     {Kind: &schema_pb.Value_StringValue{StringValue: "World"}},
		},
	}

	tests := []struct {
		name       string
		expression string
		expected   interface{}
	}{
		{
			name:       "integer addition",
			expression: "id+user_id",
			expected:   int64(15),
		},
		{
			name:       "integer subtraction",
			expression: "id-user_id",
			expected:   int64(5),
		},
		{
			name:       "mixed types multiplication",
			expression: "price*qty",
			expected:   float64(76.5),
		},
		{
			name:       "string concatenation",
			expression: "first_name||last_name",
			expected:   "JohnDoe",
		},
		{
			name:       "string concatenation with spaces",
			expression: "prefix || suffix",
			expected:   "HelloWorld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the arithmetic expression using CockroachDB parser
			cockroachParser := NewCockroachSQLParser()
			dummySelect := fmt.Sprintf("SELECT %s", tt.expression)
			stmt, err := cockroachParser.ParseSQL(dummySelect)
			if err != nil {
				t.Fatalf("Failed to parse expression %s: %v", tt.expression, err)
			}

			var arithmeticExpr *ArithmeticExpr
			if selectStmt, ok := stmt.(*SelectStatement); ok && len(selectStmt.SelectExprs) > 0 {
				if aliasedExpr, ok := selectStmt.SelectExprs[0].(*AliasedExpr); ok {
					if arithExpr, ok := aliasedExpr.Expr.(*ArithmeticExpr); ok {
						arithmeticExpr = arithExpr
					}
				}
			}

			if arithmeticExpr == nil {
				t.Fatalf("Failed to parse arithmetic expression: %s", tt.expression)
			}

			// Evaluate the expression
			value, err := engine.evaluateArithmeticExpression(arithmeticExpr, result)
			if err != nil {
				t.Fatalf("Failed to evaluate expression: %v", err)
			}

			if value == nil {
				t.Fatalf("Got nil value for expression: %s", tt.expression)
			}

			// Check the result
			switch expected := tt.expected.(type) {
			case int64:
				if intVal, ok := value.Kind.(*schema_pb.Value_Int64Value); ok {
					if intVal.Int64Value != expected {
						t.Errorf("Expected %d, got %d", expected, intVal.Int64Value)
					}
				} else {
					t.Errorf("Expected int64 result, got %T", value.Kind)
				}
			case float64:
				if doubleVal, ok := value.Kind.(*schema_pb.Value_DoubleValue); ok {
					if doubleVal.DoubleValue != expected {
						t.Errorf("Expected %f, got %f", expected, doubleVal.DoubleValue)
					}
				} else {
					t.Errorf("Expected double result, got %T", value.Kind)
				}
			case string:
				if stringVal, ok := value.Kind.(*schema_pb.Value_StringValue); ok {
					if stringVal.StringValue != expected {
						t.Errorf("Expected %s, got %s", expected, stringVal.StringValue)
					}
				} else {
					t.Errorf("Expected string result, got %T", value.Kind)
				}
			}
		})
	}
}

func TestSelectArithmeticExpression(t *testing.T) {
	// Test parsing a SELECT with arithmetic and string concatenation expressions
	stmt, err := ParseSQL("SELECT id+user_id, user_id*2, first_name||last_name FROM test_table")
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	selectStmt := stmt.(*SelectStatement)
	if len(selectStmt.SelectExprs) != 3 {
		t.Fatalf("Expected 3 select expressions, got %d", len(selectStmt.SelectExprs))
	}

	// Check first expression (id+user_id)
	aliasedExpr1 := selectStmt.SelectExprs[0].(*AliasedExpr)
	if arithmeticExpr1, ok := aliasedExpr1.Expr.(*ArithmeticExpr); ok {
		if arithmeticExpr1.Operator != "+" {
			t.Errorf("Expected + operator, got %s", arithmeticExpr1.Operator)
		}
	} else {
		t.Errorf("Expected arithmetic expression, got %T", aliasedExpr1.Expr)
	}

	// Check second expression (user_id*2)
	aliasedExpr2 := selectStmt.SelectExprs[1].(*AliasedExpr)
	if arithmeticExpr2, ok := aliasedExpr2.Expr.(*ArithmeticExpr); ok {
		if arithmeticExpr2.Operator != "*" {
			t.Errorf("Expected * operator, got %s", arithmeticExpr2.Operator)
		}
	} else {
		t.Errorf("Expected arithmetic expression, got %T", aliasedExpr2.Expr)
	}

	// Check third expression (first_name||last_name)
	aliasedExpr3 := selectStmt.SelectExprs[2].(*AliasedExpr)
	if arithmeticExpr3, ok := aliasedExpr3.Expr.(*ArithmeticExpr); ok {
		if arithmeticExpr3.Operator != "||" {
			t.Errorf("Expected || operator, got %s", arithmeticExpr3.Operator)
		}
	} else {
		t.Errorf("Expected string concatenation expression, got %T", aliasedExpr3.Expr)
	}
}
