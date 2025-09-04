package engine

import (
	"fmt"
	"math"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ArithmeticOperator represents basic arithmetic operations
type ArithmeticOperator string

const (
	OpAdd    ArithmeticOperator = "+"
	OpSub    ArithmeticOperator = "-"
	OpMul    ArithmeticOperator = "*"
	OpDiv    ArithmeticOperator = "/"
	OpMod    ArithmeticOperator = "%"
)

// EvaluateArithmeticExpression evaluates basic arithmetic operations between two values
func (e *SQLEngine) EvaluateArithmeticExpression(left, right *schema_pb.Value, operator ArithmeticOperator) (*schema_pb.Value, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("arithmetic operation requires non-null operands")
	}

	// Convert values to numeric types for calculation
	leftNum, err := e.valueToFloat64(left)
	if err != nil {
		return nil, fmt.Errorf("left operand conversion error: %v", err)
	}

	rightNum, err := e.valueToFloat64(right)
	if err != nil {
		return nil, fmt.Errorf("right operand conversion error: %v", err)
	}

	var result float64
	var resultErr error

	switch operator {
	case OpAdd:
		result = leftNum + rightNum
	case OpSub:
		result = leftNum - rightNum
	case OpMul:
		result = leftNum * rightNum
	case OpDiv:
		if rightNum == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		result = leftNum / rightNum
	case OpMod:
		if rightNum == 0 {
			return nil, fmt.Errorf("modulo by zero")
		}
		result = math.Mod(leftNum, rightNum)
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator: %s", operator)
	}

	if resultErr != nil {
		return nil, resultErr
	}

	// Convert result back to appropriate schema value type
	// If both operands were integers and operation doesn't produce decimal, return integer
	if e.isIntegerValue(left) && e.isIntegerValue(right) && 
		(operator == OpAdd || operator == OpSub || operator == OpMul || operator == OpMod) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: int64(result)},
		}, nil
	}

	// Otherwise return as double/float
	return &schema_pb.Value{
		Kind: &schema_pb.Value_DoubleValue{DoubleValue: result},
	}, nil
}

// Helper function to convert schema_pb.Value to float64
func (e *SQLEngine) valueToFloat64(value *schema_pb.Value) (float64, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return float64(v.Int32Value), nil
	case *schema_pb.Value_Int64Value:
		return float64(v.Int64Value), nil
	case *schema_pb.Value_FloatValue:
		return float64(v.FloatValue), nil
	case *schema_pb.Value_DoubleValue:
		return v.DoubleValue, nil
	case *schema_pb.Value_StringValue:
		// Try to parse string as number
		if f, err := strconv.ParseFloat(v.StringValue, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to number", v.StringValue)
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert value type to number")
	}
}

// Helper function to check if a value is an integer type
func (e *SQLEngine) isIntegerValue(value *schema_pb.Value) bool {
	switch value.Kind.(type) {
	case *schema_pb.Value_Int32Value, *schema_pb.Value_Int64Value:
		return true
	default:
		return false
	}
}

// Add evaluates addition (left + right)
func (e *SQLEngine) Add(left, right *schema_pb.Value) (*schema_pb.Value, error) {
	return e.EvaluateArithmeticExpression(left, right, OpAdd)
}

// Subtract evaluates subtraction (left - right)
func (e *SQLEngine) Subtract(left, right *schema_pb.Value) (*schema_pb.Value, error) {
	return e.EvaluateArithmeticExpression(left, right, OpSub)
}

// Multiply evaluates multiplication (left * right)
func (e *SQLEngine) Multiply(left, right *schema_pb.Value) (*schema_pb.Value, error) {
	return e.EvaluateArithmeticExpression(left, right, OpMul)
}

// Divide evaluates division (left / right)
func (e *SQLEngine) Divide(left, right *schema_pb.Value) (*schema_pb.Value, error) {
	return e.EvaluateArithmeticExpression(left, right, OpDiv)
}

// Modulo evaluates modulo operation (left % right)
func (e *SQLEngine) Modulo(left, right *schema_pb.Value) (*schema_pb.Value, error) {
	return e.EvaluateArithmeticExpression(left, right, OpMod)
}
