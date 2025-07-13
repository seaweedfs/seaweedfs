package policy_engine

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// NormalizedValueCache provides size-limited caching for normalized values with LRU eviction
type NormalizedValueCache struct {
	mu          sync.RWMutex
	cache       map[string][]string
	maxSize     int
	accessOrder []string // For LRU eviction
}

// NewNormalizedValueCache creates a new normalized value cache with configurable size
func NewNormalizedValueCache(maxSize int) *NormalizedValueCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default size
	}
	return &NormalizedValueCache{
		cache:   make(map[string][]string),
		maxSize: maxSize,
	}
}

// Get retrieves a cached value and updates access order
func (c *NormalizedValueCache) Get(key string) ([]string, bool) {
	c.mu.RLock()
	value, exists := c.cache[key]
	c.mu.RUnlock()

	if exists {
		c.updateAccessOrder(key)
	}
	return value, exists
}

// Set stores a value in the cache with size limit enforcement
func (c *NormalizedValueCache) Set(key string, value []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if _, exists := c.cache[key]; exists {
		c.cache[key] = value
		c.updateAccessOrderLocked(key)
		return
	}

	// If at max size, evict least recently used
	if len(c.cache) >= c.maxSize {
		c.evictLeastRecentlyUsed()
	}

	c.cache[key] = value
	c.updateAccessOrderLocked(key)
}

// updateAccessOrder updates the access order for LRU (thread-safe)
func (c *NormalizedValueCache) updateAccessOrder(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateAccessOrderLocked(key)
}

// updateAccessOrderLocked updates the access order for LRU (requires lock)
func (c *NormalizedValueCache) updateAccessOrderLocked(key string) {
	// Remove from current position
	for i, k := range c.accessOrder {
		if k == key {
			c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	c.accessOrder = append(c.accessOrder, key)
}

// evictLeastRecentlyUsed removes the least recently used item
func (c *NormalizedValueCache) evictLeastRecentlyUsed() {
	if len(c.accessOrder) > 0 {
		oldestKey := c.accessOrder[0]
		delete(c.cache, oldestKey)
		c.accessOrder = c.accessOrder[1:]
	}
}

// Clear clears all cached values
func (c *NormalizedValueCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string][]string)
	c.accessOrder = nil
}

// GetStats returns cache statistics
func (c *NormalizedValueCache) GetStats() (size int, maxSize int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache), c.maxSize
}

// Global cache instance with size limit
var normalizedValueCache = NewNormalizedValueCache(1000)

// getCachedNormalizedValues returns cached normalized values or caches new ones
func getCachedNormalizedValues(value interface{}) []string {
	// Create a string key for caching - more efficient than fmt.Sprintf
	typeStr := reflect.TypeOf(value).String()
	cacheKey := typeStr + ":" + fmt.Sprint(value)

	// Try to get from cache
	if cached, exists := normalizedValueCache.Get(cacheKey); exists {
		return cached
	}

	// Not in cache, normalize and store
	// Use the error-handling version for better error reporting
	normalized, err := normalizeToStringSliceWithError(value)
	if err != nil {
		glog.Warningf("Failed to normalize policy value %v: %v", value, err)
		// Fallback to string conversion for backward compatibility
		normalized = []string{fmt.Sprintf("%v", value)}
	}

	normalizedValueCache.Set(cacheKey, normalized)

	return normalized
}

// ConditionEvaluator evaluates policy conditions
type ConditionEvaluator interface {
	Evaluate(conditionValue interface{}, contextValues []string) bool
}

// StringEqualsEvaluator evaluates StringEquals conditions
type StringEqualsEvaluator struct{}

func (e *StringEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	patterns := getCachedNormalizedValues(conditionValue)
	for _, pattern := range patterns {
		for _, contextValue := range contextValues {
			if MatchesWildcard(pattern, contextValue) {
				return true
			}
		}
	}
	return false
}

// StringNotLikeEvaluator evaluates StringNotLike conditions
type StringNotLikeEvaluator struct{}

func (e *StringNotLikeEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	patterns := getCachedNormalizedValues(conditionValue)
	for _, pattern := range patterns {
		for _, contextValue := range contextValues {
			if MatchesWildcard(pattern, contextValue) {
				return false
			}
		}
	}
	return true
}

// NumericEqualsEvaluator evaluates NumericEquals conditions
type NumericEqualsEvaluator struct{}

func (e *NumericEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if expectedFloat == contextFloat {
				return true
			}
		}
	}
	return false
}

// NumericNotEqualsEvaluator evaluates NumericNotEquals conditions
type NumericNotEqualsEvaluator struct{}

func (e *NumericNotEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if expectedFloat == contextFloat {
				return false
			}
		}
	}
	return true
}

// NumericLessThanEvaluator evaluates NumericLessThan conditions
type NumericLessThanEvaluator struct{}

func (e *NumericLessThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextFloat < expectedFloat {
				return true
			}
		}
	}
	return false
}

// NumericLessThanEqualsEvaluator evaluates NumericLessThanEquals conditions
type NumericLessThanEqualsEvaluator struct{}

func (e *NumericLessThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextFloat <= expectedFloat {
				return true
			}
		}
	}
	return false
}

// NumericGreaterThanEvaluator evaluates NumericGreaterThan conditions
type NumericGreaterThanEvaluator struct{}

func (e *NumericGreaterThanEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextFloat > expectedFloat {
				return true
			}
		}
	}
	return false
}

// NumericGreaterThanEqualsEvaluator evaluates NumericGreaterThanEquals conditions
type NumericGreaterThanEqualsEvaluator struct{}

func (e *NumericGreaterThanEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			continue
		}
		for _, contextValue := range contextValues {
			contextFloat, err := strconv.ParseFloat(contextValue, 64)
			if err != nil {
				continue
			}
			if contextFloat >= expectedFloat {
				return true
			}
		}
	}
	return false
}

// DateEqualsEvaluator evaluates DateEquals conditions
type DateEqualsEvaluator struct{}

func (e *DateEqualsEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		for _, contextValue := range contextValues {
			if strings.ToLower(expected) == strings.ToLower(contextValue) {
				return true
			}
		}
	}
	return false
}

// IpAddressEvaluator evaluates IpAddress conditions
type IpAddressEvaluator struct{}

func (e *IpAddressEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		_, expectedNet, err := net.ParseCIDR(expected)
		if err != nil {
			// Try parsing as single IP
			expectedIP := net.ParseIP(expected)
			if expectedIP == nil {
				glog.V(3).Infof("Failed to parse expected IP address: %s", expected)
				continue
			}
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP == nil {
					glog.V(3).Infof("Failed to parse IP address: %s", contextValue)
					continue
				}
				if contextIP.Equal(expectedIP) {
					return true
				}
			}
		} else {
			// CIDR network
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP == nil {
					glog.V(3).Infof("Failed to parse IP address: %s", contextValue)
					continue
				}
				if expectedNet.Contains(contextIP) {
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
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		_, expectedNet, err := net.ParseCIDR(expected)
		if err != nil {
			// Try parsing as single IP
			expectedIP := net.ParseIP(expected)
			if expectedIP == nil {
				glog.V(3).Infof("Failed to parse expected IP address: %s", expected)
				continue
			}
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP == nil {
					glog.V(3).Infof("Failed to parse IP address: %s", contextValue)
					continue
				}
				if contextIP.Equal(expectedIP) {
					return false
				}
			}
		} else {
			// CIDR network
			for _, contextValue := range contextValues {
				contextIP := net.ParseIP(contextValue)
				if contextIP == nil {
					glog.V(3).Infof("Failed to parse IP address: %s", contextValue)
					continue
				}
				if expectedNet.Contains(contextIP) {
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
	expectedValues := getCachedNormalizedValues(conditionValue)
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
	patterns := getCachedNormalizedValues(conditionValue)
	for _, pattern := range patterns {
		for _, contextValue := range contextValues {
			if MatchesWildcard(pattern, contextValue) {
				return true
			}
		}
	}
	return false
}

// NullEvaluator evaluates Null conditions
type NullEvaluator struct{}

func (e *NullEvaluator) Evaluate(conditionValue interface{}, contextValues []string) bool {
	expectedValues := getCachedNormalizedValues(conditionValue)
	for _, expected := range expectedValues {
		expectedBool := strings.ToLower(expected) == "true"
		contextExists := len(contextValues) > 0
		if expectedBool && !contextExists {
			return true // Key should be null and it is
		}
		if !expectedBool && contextExists {
			return true // Key should not be null and it isn't
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
