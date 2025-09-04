package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

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

// ===============================
// DATE/TIME CONSTANTS
// ===============================

// CurrentDate returns the current date as a string in YYYY-MM-DD format
func (e *SQLEngine) CurrentDate() (*schema_pb.Value, error) {
	now := time.Now()
	dateStr := now.Format("2006-01-02")
	
	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: dateStr},
	}, nil
}

// CurrentTimestamp returns the current timestamp
func (e *SQLEngine) CurrentTimestamp() (*schema_pb.Value, error) {
	now := time.Now()
	
	// Return as TimestampValue with microseconds
	timestampMicros := now.UnixMicro()
	
	return &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: timestampMicros,
			},
		},
	}, nil
}

// CurrentTime returns the current time as a string in HH:MM:SS format
func (e *SQLEngine) CurrentTime() (*schema_pb.Value, error) {
	now := time.Now()
	timeStr := now.Format("15:04:05")
	
	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: timeStr},
	}, nil
}

// Now is an alias for CurrentTimestamp (common SQL function name)
func (e *SQLEngine) Now() (*schema_pb.Value, error) {
	return e.CurrentTimestamp()
}

// ===============================
// EXTRACT FUNCTION
// ===============================

// DatePart represents the part of a date/time to extract
type DatePart string

const (
	PartYear     DatePart = "YEAR"
	PartMonth    DatePart = "MONTH"
	PartDay      DatePart = "DAY"
	PartHour     DatePart = "HOUR"
	PartMinute   DatePart = "MINUTE"
	PartSecond   DatePart = "SECOND"
	PartWeek     DatePart = "WEEK"
	PartDayOfYear DatePart = "DOY"
	PartDayOfWeek DatePart = "DOW"
	PartQuarter   DatePart = "QUARTER"
	PartEpoch     DatePart = "EPOCH"
)

// Extract extracts a specific part from a date/time value
func (e *SQLEngine) Extract(part DatePart, value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("EXTRACT function requires non-null value")
	}

	// Convert value to time
	t, err := e.valueToTime(value)
	if err != nil {
		return nil, fmt.Errorf("EXTRACT function time conversion error: %v", err)
	}

	var result int64

	switch strings.ToUpper(string(part)) {
	case string(PartYear):
		result = int64(t.Year())
	case string(PartMonth):
		result = int64(t.Month())
	case string(PartDay):
		result = int64(t.Day())
	case string(PartHour):
		result = int64(t.Hour())
	case string(PartMinute):
		result = int64(t.Minute())
	case string(PartSecond):
		result = int64(t.Second())
	case string(PartWeek):
		_, week := t.ISOWeek()
		result = int64(week)
	case string(PartDayOfYear):
		result = int64(t.YearDay())
	case string(PartDayOfWeek):
		result = int64(t.Weekday())
	case string(PartQuarter):
		month := t.Month()
		result = int64((month-1)/3 + 1)
	case string(PartEpoch):
		result = t.Unix()
	default:
		return nil, fmt.Errorf("unsupported date part: %s", part)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: result},
	}, nil
}

// Helper function to convert schema_pb.Value to time.Time
func (e *SQLEngine) valueToTime(value *schema_pb.Value) (time.Time, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_TimestampValue:
		if v.TimestampValue == nil {
			return time.Time{}, fmt.Errorf("null timestamp value")
		}
		return time.UnixMicro(v.TimestampValue.TimestampMicros), nil
	case *schema_pb.Value_StringValue:
		// Try to parse various date/time string formats
		dateFormats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
			"2006-01-02",
			"15:04:05",
		}
		
		for _, format := range dateFormats {
			if t, err := time.Parse(format, v.StringValue); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse date/time string: %s", v.StringValue)
	case *schema_pb.Value_Int64Value:
		// Assume Unix timestamp (seconds)
		return time.Unix(v.Int64Value, 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert value type to date/time")
	}
}
