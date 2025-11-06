package engine

import (
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ===============================
// ARITHMETIC OPERATORS
// ===============================

// ArithmeticOperator represents basic arithmetic operations
type ArithmeticOperator string

const (
	OpAdd ArithmeticOperator = "+"
	OpSub ArithmeticOperator = "-"
	OpMul ArithmeticOperator = "*"
	OpDiv ArithmeticOperator = "/"
	OpMod ArithmeticOperator = "%"
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

// ===============================
// MATHEMATICAL FUNCTIONS
// ===============================

// Round rounds a numeric value to the nearest integer or specified decimal places
func (e *SQLEngine) Round(value *schema_pb.Value, precision ...*schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("ROUND function requires non-null value")
	}

	num, err := e.valueToFloat64(value)
	if err != nil {
		return nil, fmt.Errorf("ROUND function conversion error: %v", err)
	}

	// Default precision is 0 (round to integer)
	precisionValue := 0
	if len(precision) > 0 && precision[0] != nil {
		precFloat, err := e.valueToFloat64(precision[0])
		if err != nil {
			return nil, fmt.Errorf("ROUND precision conversion error: %v", err)
		}
		precisionValue = int(precFloat)
	}

	// Apply rounding
	multiplier := math.Pow(10, float64(precisionValue))
	rounded := math.Round(num*multiplier) / multiplier

	// Return as integer if precision is 0 and original was integer, otherwise as double
	if precisionValue == 0 && e.isIntegerValue(value) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: int64(rounded)},
		}, nil
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_DoubleValue{DoubleValue: rounded},
	}, nil
}

// Ceil returns the smallest integer greater than or equal to the value
func (e *SQLEngine) Ceil(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("CEIL function requires non-null value")
	}

	num, err := e.valueToFloat64(value)
	if err != nil {
		return nil, fmt.Errorf("CEIL function conversion error: %v", err)
	}

	result := math.Ceil(num)

	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: int64(result)},
	}, nil
}

// Floor returns the largest integer less than or equal to the value
func (e *SQLEngine) Floor(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("FLOOR function requires non-null value")
	}

	num, err := e.valueToFloat64(value)
	if err != nil {
		return nil, fmt.Errorf("FLOOR function conversion error: %v", err)
	}

	result := math.Floor(num)

	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: int64(result)},
	}, nil
}

// Abs returns the absolute value of a number
func (e *SQLEngine) Abs(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("ABS function requires non-null value")
	}

	num, err := e.valueToFloat64(value)
	if err != nil {
		return nil, fmt.Errorf("ABS function conversion error: %v", err)
	}

	result := math.Abs(num)

	// Return same type as input if possible
	if e.isIntegerValue(value) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: int64(result)},
		}, nil
	}

	// Check if original was float32
	if _, ok := value.Kind.(*schema_pb.Value_FloatValue); ok {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_FloatValue{FloatValue: float32(result)},
		}, nil
	}

	// Default to double
	return &schema_pb.Value{
		Kind: &schema_pb.Value_DoubleValue{DoubleValue: result},
	}, nil
}
