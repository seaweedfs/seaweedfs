package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

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
			if lengthVal > int64(math.MaxInt) || lengthVal < int64(math.MinInt) {
				// If length is out-of-bounds for int, take substring from startIdx to end
				result = str[startIdx:]
			} else {
				// Safe conversion after bounds check
				endIdx := startIdx + int(lengthVal)
				if endIdx > len(str) {
					endIdx = len(str)
				}
				result = str[startIdx:endIdx]
			}
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

	if lengthVal > int64(len(str)) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	if lengthVal > int64(math.MaxInt) || lengthVal < int64(math.MinInt) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	// Safe conversion after bounds check
	return &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: str[:int(lengthVal)]},
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

	if lengthVal > int64(len(str)) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	if lengthVal > int64(math.MaxInt) || lengthVal < int64(math.MinInt) {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: str},
		}, nil
	}

	// Safe conversion after bounds check
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
