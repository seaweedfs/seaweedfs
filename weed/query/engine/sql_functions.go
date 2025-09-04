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
		dateFormats := []struct {
			format   string
			useLocal bool
		}{
			{"2006-01-02 15:04:05", true},  // Local time assumed for non-timezone formats
			{"2006-01-02T15:04:05Z", false}, // UTC format
			{"2006-01-02T15:04:05", true},   // Local time assumed
			{"2006-01-02", true},            // Local time assumed for date only
			{"15:04:05", true},              // Local time assumed for time only
		}
		
		for _, formatSpec := range dateFormats {
			if t, err := time.Parse(formatSpec.format, v.StringValue); err == nil {
				if formatSpec.useLocal {
					// Convert to local timezone if no timezone was specified
					return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local), nil
				}
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

// ===============================
// DATE_TRUNC FUNCTION
// ===============================

// DateTrunc truncates a date/time to the specified precision
func (e *SQLEngine) DateTrunc(precision string, value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("DATE_TRUNC function requires non-null value")
	}

	// Convert value to time
	t, err := e.valueToTime(value)
	if err != nil {
		return nil, fmt.Errorf("DATE_TRUNC function time conversion error: %v", err)
	}

	var truncated time.Time

	switch strings.ToLower(precision) {
	case "microsecond", "microseconds":
		// No truncation needed for microsecond precision
		truncated = t
	case "millisecond", "milliseconds":
		truncated = t.Truncate(time.Millisecond)
	case "second", "seconds":
		truncated = t.Truncate(time.Second)
	case "minute", "minutes":
		truncated = t.Truncate(time.Minute)
	case "hour", "hours":
		truncated = t.Truncate(time.Hour)
	case "day", "days":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "week", "weeks":
		// Truncate to beginning of week (Monday)
		days := int(t.Weekday())
		if days == 0 { // Sunday = 0, adjust to make Monday = 0
			days = 6
		} else {
			days = days - 1
		}
		truncated = time.Date(t.Year(), t.Month(), t.Day()-days, 0, 0, 0, 0, t.Location())
	case "month", "months":
		truncated = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "quarter", "quarters":
		month := t.Month()
		quarterMonth := ((int(month)-1)/3)*3 + 1
		truncated = time.Date(t.Year(), time.Month(quarterMonth), 1, 0, 0, 0, 0, t.Location())
	case "year", "years":
		truncated = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "decade", "decades":
		year := (t.Year()/10) * 10
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	case "century", "centuries":
		year := ((t.Year()-1)/100)*100 + 1
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	case "millennium", "millennia":
		year := ((t.Year()-1)/1000)*1000 + 1
		truncated = time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
	default:
		return nil, fmt.Errorf("unsupported date truncation precision: %s", precision)
	}

	// Return as TimestampValue
	return &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: truncated.UnixMicro(),
			},
		},
	}, nil
}

// ===============================
// STRING FUNCTIONS
// ===============================

// Length returns the length of a string
func (e *SQLEngine) Length(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("LENGTH function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("LENGTH function conversion error: %v", err)
	}

	length := int64(len(str))
	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: length},
	}, nil
}

// Upper converts a string to uppercase
func (e *SQLEngine) Upper(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("UPPER function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("UPPER function conversion error: %v", err)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: strings.ToUpper(str)},
	}, nil
}

// Lower converts a string to lowercase
func (e *SQLEngine) Lower(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("LOWER function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("LOWER function conversion error: %v", err)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: strings.ToLower(str)},
	}, nil
}

// Trim removes leading and trailing whitespace from a string
func (e *SQLEngine) Trim(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("TRIM function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("TRIM function conversion error: %v", err)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: strings.TrimSpace(str)},
	}, nil
}

// LTrim removes leading whitespace from a string
func (e *SQLEngine) LTrim(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("LTRIM function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("LTRIM function conversion error: %v", err)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: strings.TrimLeft(str, " \t\n\r")},
	}, nil
}

// RTrim removes trailing whitespace from a string
func (e *SQLEngine) RTrim(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("RTRIM function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("RTRIM function conversion error: %v", err)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: strings.TrimRight(str, " \t\n\r")},
	}, nil
}

// Substring extracts a substring from a string
func (e *SQLEngine) Substring(value *schema_pb.Value, start *schema_pb.Value, length ...*schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil || start == nil {
		return nil, fmt.Errorf("SUBSTRING function requires non-null value and start position")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING function value conversion error: %v", err)
	}

	startPos, err := e.valueToInt64(start)
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING function start position conversion error: %v", err)
	}

	// Convert to 0-based indexing (SQL uses 1-based)
	if startPos < 1 {
		startPos = 1
	}
	startIdx := int(startPos - 1)

	if startIdx >= len(str) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}, nil
	}

	var result string
	if len(length) > 0 && length[0] != nil {
		lengthVal, err := e.valueToInt64(length[0])
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING function length conversion error: %v", err)
		}

		if lengthVal <= 0 {
			result = ""
		} else {
			endIdx := startIdx + int(lengthVal)
			if endIdx > len(str) {
				endIdx = len(str)
			}
			result = str[startIdx:endIdx]
		}
	} else {
		result = str[startIdx:]
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: result},
	}, nil
}

// Concat concatenates multiple strings
func (e *SQLEngine) Concat(values ...*schema_pb.Value) (*schema_pb.Value, error) {
	if len(values) == 0 {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}, nil
	}

	var result strings.Builder
	for i, value := range values {
		if value == nil {
			continue // Skip null values
		}

		str, err := e.valueToString(value)
		if err != nil {
			return nil, fmt.Errorf("CONCAT function value %d conversion error: %v", i, err)
		}
		result.WriteString(str)
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: result.String()},
	}, nil
}

// Replace replaces all occurrences of a substring with another substring
func (e *SQLEngine) Replace(value, oldStr, newStr *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil || oldStr == nil || newStr == nil {
		return nil, fmt.Errorf("REPLACE function requires non-null values")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("REPLACE function value conversion error: %v", err)
	}

	old, err := e.valueToString(oldStr)
	if err != nil {
		return nil, fmt.Errorf("REPLACE function old string conversion error: %v", err)
	}

	new, err := e.valueToString(newStr)
	if err != nil {
		return nil, fmt.Errorf("REPLACE function new string conversion error: %v", err)
	}

	result := strings.ReplaceAll(str, old, new)

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: result},
	}, nil
}

// Position returns the position of a substring in a string (1-based, 0 if not found)
func (e *SQLEngine) Position(substring, value *schema_pb.Value) (*schema_pb.Value, error) {
	if substring == nil || value == nil {
		return nil, fmt.Errorf("POSITION function requires non-null values")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("POSITION function string conversion error: %v", err)
	}

	substr, err := e.valueToString(substring)
	if err != nil {
		return nil, fmt.Errorf("POSITION function substring conversion error: %v", err)
	}

	pos := strings.Index(str, substr)
	if pos == -1 {
		pos = 0 // SQL returns 0 for not found
	} else {
		pos = pos + 1 // Convert to 1-based indexing
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: int64(pos)},
	}, nil
}

// Left returns the leftmost characters of a string
func (e *SQLEngine) Left(value *schema_pb.Value, length *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil || length == nil {
		return nil, fmt.Errorf("LEFT function requires non-null values")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("LEFT function string conversion error: %v", err)
	}

	lengthVal, err := e.valueToInt64(length)
	if err != nil {
		return nil, fmt.Errorf("LEFT function length conversion error: %v", err)
	}

	if lengthVal <= 0 {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}, nil
	}

	if int(lengthVal) >= len(str) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: str[:lengthVal]},
	}, nil
}

// Right returns the rightmost characters of a string
func (e *SQLEngine) Right(value *schema_pb.Value, length *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil || length == nil {
		return nil, fmt.Errorf("RIGHT function requires non-null values")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("RIGHT function string conversion error: %v", err)
	}

	lengthVal, err := e.valueToInt64(length)
	if err != nil {
		return nil, fmt.Errorf("RIGHT function length conversion error: %v", err)
	}

	if lengthVal <= 0 {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}, nil
	}

	if int(lengthVal) >= len(str) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	startPos := len(str) - int(lengthVal)
	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: str[startPos:]},
	}, nil
}

// Reverse reverses a string
func (e *SQLEngine) Reverse(value *schema_pb.Value) (*schema_pb.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("REVERSE function requires non-null value")
	}

	str, err := e.valueToString(value)
	if err != nil {
		return nil, fmt.Errorf("REVERSE function conversion error: %v", err)
	}

	// Reverse the string rune by rune to handle Unicode correctly
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: string(runes)},
	}, nil
}

// Helper function to convert schema_pb.Value to string
func (e *SQLEngine) valueToString(value *schema_pb.Value) (string, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_StringValue:
		return v.StringValue, nil
	case *schema_pb.Value_Int32Value:
		return strconv.FormatInt(int64(v.Int32Value), 10), nil
	case *schema_pb.Value_Int64Value:
		return strconv.FormatInt(v.Int64Value, 10), nil
	case *schema_pb.Value_FloatValue:
		return strconv.FormatFloat(float64(v.FloatValue), 'g', -1, 32), nil
	case *schema_pb.Value_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'g', -1, 64), nil
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return "true", nil
		}
		return "false", nil
	case *schema_pb.Value_BytesValue:
		return string(v.BytesValue), nil
	default:
		return "", fmt.Errorf("cannot convert value type to string")
	}
}

// Helper function to convert schema_pb.Value to int64
func (e *SQLEngine) valueToInt64(value *schema_pb.Value) (int64, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return int64(v.Int32Value), nil
	case *schema_pb.Value_Int64Value:
		return v.Int64Value, nil
	case *schema_pb.Value_FloatValue:
		return int64(v.FloatValue), nil
	case *schema_pb.Value_DoubleValue:
		return int64(v.DoubleValue), nil
	case *schema_pb.Value_StringValue:
		if i, err := strconv.ParseInt(v.StringValue, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to integer", v.StringValue)
	default:
		return 0, fmt.Errorf("cannot convert value type to integer")
	}
}
