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
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// LRUNode represents a node in the doubly-linked list for efficient LRU operations
type LRUNode struct {
	key   string
	value []string
	prev  *LRUNode
	next  *LRUNode
}

// NormalizedValueCache provides size-limited caching for normalized values with efficient LRU eviction
type NormalizedValueCache struct {
	mu      sync.RWMutex
	cache   map[string]*LRUNode
	maxSize int
	head    *LRUNode // Most recently used
	tail    *LRUNode // Least recently used
}

// NewNormalizedValueCache creates a new normalized value cache with configurable size
func NewNormalizedValueCache(maxSize int) *NormalizedValueCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default size
	}

	// Create dummy head and tail nodes for easier list manipulation
	head := &LRUNode{}
	tail := &LRUNode{}
	head.next = tail
	tail.prev = head

	return &NormalizedValueCache{
		cache:   make(map[string]*LRUNode),
		maxSize: maxSize,
		head:    head,
		tail:    tail,
	}
}

// Get retrieves a cached value and updates access order in O(1) time
func (c *NormalizedValueCache) Get(key string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.cache[key]; exists {
		// Move to head (most recently used) - O(1) operation
		c.moveToHead(node)
		return node.value, true
	}
	return nil, false
}

// Set stores a value in the cache with size limit enforcement in O(1) time
func (c *NormalizedValueCache) Set(key string, value []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.cache[key]; exists {
		// Update existing node and move to head
		node.value = value
		c.moveToHead(node)
		return
	}

	// Create new node
	newNode := &LRUNode{
		key:   key,
		value: value,
	}

	// If at max size, evict least recently used
	if len(c.cache) >= c.maxSize {
		c.evictLeastRecentlyUsed()
	}

	// Add to cache and move to head
	c.cache[key] = newNode
	c.addToHead(newNode)
}

// moveToHead moves a node to the head of the list (most recently used) - O(1)
func (c *NormalizedValueCache) moveToHead(node *LRUNode) {
	c.removeNode(node)
	c.addToHead(node)
}

// addToHead adds a node right after the head - O(1)
func (c *NormalizedValueCache) addToHead(node *LRUNode) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

// removeNode removes a node from the list - O(1)
func (c *NormalizedValueCache) removeNode(node *LRUNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

// removeTail removes the last node before tail (least recently used) - O(1)
func (c *NormalizedValueCache) removeTail() *LRUNode {
	lastNode := c.tail.prev
	c.removeNode(lastNode)
	return lastNode
}

// evictLeastRecentlyUsed removes the least recently used item in O(1) time
func (c *NormalizedValueCache) evictLeastRecentlyUsed() {
	tail := c.removeTail()
	delete(c.cache, tail.key)
}

// Clear clears all cached values
func (c *NormalizedValueCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*LRUNode)
	c.head.next = c.tail
	c.tail.prev = c.head
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

// ExistingObjectTagPrefix is the prefix for S3 policy condition keys
const ExistingObjectTagPrefix = "s3:ExistingObjectTag/"

// getConditionContextValue resolves the value(s) for a condition key.
// For s3:ExistingObjectTag/<key> conditions, it looks up the tag in objectEntry.
// For other condition keys, it looks up the value in contextValues.
func getConditionContextValue(key string, contextValues map[string][]string, objectEntry map[string][]byte) []string {
	if strings.HasPrefix(key, ExistingObjectTagPrefix) {
		tagKey := key[len(ExistingObjectTagPrefix):]
		if tagKey == "" {
			return []string{} // Invalid: empty tag key
		}
		metadataKey := s3_constants.AmzObjectTaggingPrefix + tagKey
		if objectEntry != nil {
			if tagValue, exists := objectEntry[metadataKey]; exists {
				return []string{string(tagValue)}
			}
		}
		return []string{}
	}

	if vals, exists := contextValues[key]; exists {
		return vals
	}
	return []string{}
}

// EvaluateConditions evaluates all conditions in a policy statement
// objectEntry is the object's metadata from entry.Extended (can be nil)
func EvaluateConditions(conditions PolicyConditions, contextValues map[string][]string, objectEntry map[string][]byte) bool {
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
			contextVals := getConditionContextValue(key, contextValues, objectEntry)
			if !conditionEvaluator.Evaluate(value.Strings(), contextVals) {
				return false // If any condition fails, the whole condition block fails
			}
		}
	}

	return true
}

// EvaluateConditionsLegacy evaluates conditions using the old interface{} format for backward compatibility
// objectEntry is the object's metadata from entry.Extended (can be nil)
func EvaluateConditionsLegacy(conditions map[string]interface{}, contextValues map[string][]string, objectEntry map[string][]byte) bool {
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
			contextVals := getConditionContextValue(key, contextValues, objectEntry)
			if !conditionEvaluator.Evaluate(value, contextVals) {
				return false // If any condition fails, the whole condition block fails
			}
		}
	}

	return true
}
