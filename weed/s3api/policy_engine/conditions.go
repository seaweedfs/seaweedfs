package policy_engine

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ConditionEvaluator evaluates policy conditions
type ConditionEvaluator interface {
	Evaluate(conditionValue interface{}, contextValues []string) bool
}

// StringEqualsEvaluator evaluates StringEquals conditions
type StringEqualsEvaluator struct{}

func (e *StringEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		for _, contextValue := range contextValues {
			if expected == contextValue {
				return true
			}
		}
	}
	return false
}

// StringNotEqualsEvaluator evaluates StringNotEquals conditions
type StringNotEqualsEvaluator struct{}

func (e *StringNotEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		for _, contextValue := range contextValues {
			if expected == contextValue {
				return false
			}
		}
	}
	return true
}

// StringLikeEvaluator evaluates StringLike conditions (supports wildcards)
type StringLikeEvaluator struct{}

func (e *StringLikeEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedPatterns := normalizeToStringSlice(conditionValue)
	for _, pattern := range expectedPatterns {
		for _, contextValue := range contextValues {
			if matchesWildcard(pattern, contextValue) {
				return true
			}
		}
	}
	return false
}

// StringNotLikeEvaluator evaluates StringNotLike conditions
type StringNotLikeEvaluator struct{}

func (e *StringNotLikeEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedPatterns := normalizeToStringSlice(conditionValue)
	for _, pattern := range expectedPatterns {
		for _, contextValue := range contextValues {
			if matchesWildcard(pattern, contextValue) {
				return false
			}
		}
	}
	return true
}

// NumericEqualsEvaluator evaluates NumericEquals conditions
type NumericEqualsEvaluator struct{}

func (e *NumericEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if expectedNum == contextNum {
				return true
			}
		}
	}
	return false
}

// NumericNotEqualsEvaluator evaluates NumericNotEquals conditions
type NumericNotEqualsEvaluator struct{}

func (e *NumericNotEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if expectedNum == contextNum {
				return false
			}
		}
	}
	return true
}

// NumericLessThanEvaluator evaluates NumericLessThan conditions
type NumericLessThanEvaluator struct{}

func (e *NumericLessThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextNum < expectedNum {
				return true
			}
		}
	}
	return false
}

// NumericLessThanEqualsEvaluator evaluates NumericLessThanEquals conditions
type NumericLessThanEqualsEvaluator struct{}

func (e *NumericLessThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextNum <= expectedNum {
				return true
			}
		}
	}
	return false
}

// NumericGreaterThanEvaluator evaluates NumericGreaterThan conditions
type NumericGreaterThanEvaluator struct{}

func (e *NumericGreaterThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextNum > expectedNum {
				return true
			}
		}
	}
	return false
}

// NumericGreaterThanEqualsEvaluator evaluates NumericGreaterThanEquals conditions
type NumericGreaterThanEqualsEvaluator struct{}

func (e *NumericGreaterThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedNum, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextNum, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextNum >= expectedNum {
				return true
			}
		}
	}
	return false
}

// DateEqualsEvaluator evaluates DateEquals conditions
type DateEqualsEvaluator struct{}

func (e *DateEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if expectedTime.Equal(contextTime) {
				return true
			}
		}
	}
	return false
}

// DateNotEqualsEvaluator evaluates DateNotEquals conditions
type DateNotEqualsEvaluator struct{}

func (e *DateNotEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if expectedTime.Equal(contextTime) {
				return false
			}
		}
	}
	return true
}

// DateLessThanEvaluator evaluates DateLessThan conditions
type DateLessThanEvaluator struct{}

func (e *DateLessThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if contextTime.Before(expectedTime) {
				return true
			}
		}
	}
	return false
}

// DateLessThanEqualsEvaluator evaluates DateLessThanEquals conditions
type DateLessThanEqualsEvaluator struct{}

func (e *DateLessThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if contextTime.Before(expectedTime) || contextTime.Equal(expectedTime) {
				return true
			}
		}
	}
	return false
}

// DateGreaterThanEvaluator evaluates DateGreaterThan conditions
type DateGreaterThanEvaluator struct{}

func (e *DateGreaterThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if contextTime.After(expectedTime) {
				return true
			}
		}
	}
	return false
}

// DateGreaterThanEqualsEvaluator evaluates DateGreaterThanEquals conditions
type DateGreaterThanEqualsEvaluator struct{}

func (e *DateGreaterThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedTime, err := time.Parse(time.RFC3339, expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextTime, err := time.Parse(time.RFC3339, contextValue)
			if err != nil {
				continue
			}
			if contextTime.After(expectedTime) || contextTime.Equal(expectedTime) {
				return true
			}
		}
	}
	return false
}

// BoolEvaluator evaluates Bool conditions
type BoolEvaluator struct{}

func (e *BoolEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedBool, err := strconv.ParseBool(expected)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextBool, err := strconv.ParseBool(contextValue)
			if err != nil {
				continue
			}
			if expectedBool == contextBool {
				return true
			}
		}
	}
	return false
}

// IpAddressEvaluator evaluates IpAddress conditions
type IpAddressEvaluator struct{}

func (e *IpAddressEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		_, expectedNet, err := net.ParseCIDR(expected)
		if err != nil {
			// Try parsing as single IP
			expectedIP := net.ParseIP(expected)
			if expectedIP == nil {
				continue
			}
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP != nil && contextIP.Equal(expectedIP) {
					return true
				}
			}
		} else {
			// CIDR network
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP != nil && expectedNet.Contains(contextIP) {
					return true
				}
			}
		}
	}
	return false
}

// NotIpAddressEvaluator evaluates NotIpAddress conditions
type NotIpAddressEvaluator struct{}

func (e *NotIpAddressEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		_, expectedNet, err := net.ParseCIDR(expected)
		if err != nil {
			// Try parsing as single IP
			expectedIP := net.ParseIP(expected)
			if expectedIP == nil {
				continue
			}
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP != nil && contextIP.Equal(expectedIP) {
					return false
				}
			}
		} else {
			// CIDR network
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP != nil && expectedNet.Contains(contextIP) {
					return false
				}
			}
		}
	}
	return true
}

// ArnEqualsEvaluator evaluates ArnEquals conditions
type ArnEqualsEvaluator struct{}

func (e *ArnEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		for _, contextValue := range contextValues {
			if expected == contextValue {
				return true
			}
		}
	}
	return false
}

// ArnLikeEvaluator evaluates ArnLike conditions
type ArnLikeEvaluator struct{}

func (e *ArnLikeEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedPatterns := normalizeToStringSlice(conditionValue)
	for _, pattern := range expectedPatterns {
		for _, contextValue := range contextValues {
			if matchesWildcard(pattern, contextValue) {
				return true
			}
		}
	}
	return false
}

// NullEvaluator evaluates Null conditions
type NullEvaluator struct{}

func (e *NullEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := normalizeToStringSlice(conditionValue)
	for _, expected := range expectedValues {
		expectedBool, err := strconv.ParseBool(expected)
		if err != nil {
			continue
		}

		isNull := len(contextValues) == 0 || (len(contextValues) == 1 && contextValues[0] == "")

		if expectedBool == isNull {
			return true
		}
	}
	return false
}

// GetConditionEvaluator returns the appropriate evaluator for a condition operator
func GetConditionEvaluator(operator string) (ConditionEvaluator, error) {
	switch operator {
	case "StringEquals":
		return &StringEqualsEvaluator{}, nil
	case "StringNotEquals":
		return &StringNotEqualsEvaluator{}, nil
	case "StringLike":
		return &StringLikeEvaluator{}, nil
	case "StringNotLike":
		return &StringNotLikeEvaluator{}, nil
	case "NumericEquals":
		return &NumericEqualsEvaluator{}, nil
	case "NumericNotEquals":
		return &NumericNotEqualsEvaluator{}, nil
	case "NumericLessThan":
		return &NumericLessThanEvaluator{}, nil
	case "NumericLessThanEquals":
		return &NumericLessThanEqualsEvaluator{}, nil
	case "NumericGreaterThan":
		return &NumericGreaterThanEvaluator{}, nil
	case "NumericGreaterThanEquals":
		return &NumericGreaterThanEqualsEvaluator{}, nil
	case "DateEquals":
		return &DateEqualsEvaluator{}, nil
	case "DateNotEquals":
		return &DateNotEqualsEvaluator{}, nil
	case "DateLessThan":
		return &DateLessThanEvaluator{}, nil
	case "DateLessThanEquals":
		return &DateLessThanEqualsEvaluator{}, nil
	case "DateGreaterThan":
		return &DateGreaterThanEvaluator{}, nil
	case "DateGreaterThanEquals":
		return &DateGreaterThanEqualsEvaluator{}, nil
	case "Bool":
		return &BoolEvaluator{}, nil
	case "IpAddress":
		return &IpAddressEvaluator{}, nil
	case "NotIpAddress":
		return &NotIpAddressEvaluator{}, nil
	case "ArnEquals":
		return &ArnEqualsEvaluator{}, nil
	case "ArnLike":
		return &ArnLikeEvaluator{}, nil
	case "Null":
		return &NullEvaluator{}, nil
	default:
		return nil, fmt.Errorf("unsupported condition operator: %s", operator)
	}
}

// matchesWildcard checks if a string matches a wildcard pattern
func matchesWildcard(pattern, str string) bool {
	// Convert pattern to a simple regex-like matching
	// This is a simplified implementation

	// Handle simple cases
	if pattern == "*" {
		return true
	}

	if pattern == str {
		return true
	}

	// Split pattern by wildcards
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		// No wildcards, exact match
		return pattern == str
	}

	// Check if string starts with first part
	if len(parts[0]) > 0 && !strings.HasPrefix(str, parts[0]) {
		return false
	}

	// Check if string ends with last part
	if len(parts[len(parts)-1]) > 0 && !strings.HasSuffix(str, parts[len(parts)-1]) {
		return false
	}

	// Check middle parts
	searchStr := str
	if len(parts[0]) > 0 {
		searchStr = searchStr[len(parts[0]):]
	}
	if len(parts[len(parts)-1]) > 0 {
		searchStr = searchStr[:len(searchStr)-len(parts[len(parts)-1])]
	}

	for i := 1; i < len(parts)-1; i++ {
		if len(parts[i]) > 0 {
			index := strings.Index(searchStr, parts[i])
			if index == -1 {
				return false
			}
			searchStr = searchStr[index+len(parts[i]):]
		}
	}

	return true
}

// EvaluateConditions evaluates all conditions in a policy statement
func EvaluateConditions(conditions PolicyConditions, contextValues map[string][]string) bool {
	if len(conditions) == 0 {
		return true // No conditions means always true
	}

	for operator, conditionMap := range conditions {
		conditionEvaluator, err := GetConditionEvaluator(operator)
		if err != nil {
			glog.Warningf("Unsupported condition operator: %s", operator)
			continue
		}

		for key, value := range conditionMap {
			contextVals, exists := contextValues[key]
			if !exists {
				contextVals = []string{}
			}

			if !conditionEvaluator.Evaluate(value.Strings(), contextVals) {
				return false // If any condition fails, the whole condition block fails
			}
		}
	}

	return true
}

// EvaluateConditionsLegacy evaluates conditions using the old interface{} format for backward compatibility
func EvaluateConditionsLegacy(conditions map[string]interface{}, contextValues map[string][]string) bool {
	if len(conditions) == 0 {
		return true // No conditions means always true
	}

	for operator, conditionMap := range conditions {
		conditionEvaluator, err := GetConditionEvaluator(operator)
		if err != nil {
			glog.Warningf("Unsupported condition operator: %s", operator)
			continue
		}

		conditionMapTyped, ok := conditionMap.(map[string]interface{})
		if !ok {
			glog.Warningf("Invalid condition format for operator: %s", operator)
			continue
		}

		for key, value := range conditionMapTyped {
			contextVals, exists := contextValues[key]
			if !exists {
				contextVals = []string{}
			}

			if !conditionEvaluator.Evaluate(value, contextVals) {
				return false // If any condition fails, the whole condition block fails
			}
		}
	}

	return true
}
