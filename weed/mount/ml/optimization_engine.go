package ml

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// OptimizationEngine provides a flexible, rule-based system for ML optimizations
type OptimizationEngine struct {
	sync.RWMutex

	// Rule-based system
	rules      map[string]*OptimizationRule
	templates  map[string]*OptimizationTemplate
	strategies map[string]OptimizationStrategy

	// Learning system
	usagePatterns map[string]*UsagePattern
	adaptiveRules map[string]*AdaptiveRule

	// Plugin system
	plugins map[string]OptimizationPlugin

	enabled bool
}

// OptimizationRule defines a single optimization rule
type OptimizationRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Priority    int                    `json:"priority"`
	Conditions  []RuleCondition        `json:"conditions"`
	Actions     []RuleAction           `json:"actions"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// RuleCondition defines when a rule should be applied
type RuleCondition struct {
	Type     string      `json:"type"`     // file_pattern, access_pattern, workload_type, etc.
	Property string      `json:"property"` // file_path, extension, size, frequency, etc.
	Operator string      `json:"operator"` // equals, contains, matches, greater_than, etc.
	Value    interface{} `json:"value"`    // The value to compare against
	Weight   float64     `json:"weight"`   // Weight for scoring (0.0 to 1.0)
}

// RuleAction defines what optimization to apply
type RuleAction struct {
	Type       string                 `json:"type"`       // prefetch, cache, coordinate, etc.
	Target     string                 `json:"target"`     // file, workload, gpu, etc.
	Parameters map[string]interface{} `json:"parameters"` // Action-specific parameters
}

// OptimizationTemplate provides reusable optimization configurations
type OptimizationTemplate struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Category    string                 `json:"category"`   // training, inference, preprocessing, etc.
	Rules       []string               `json:"rules"`      // Rule IDs to apply
	Parameters  map[string]interface{} `json:"parameters"` // Default parameters
}

// OptimizationStrategy interface for pluggable optimization strategies
type OptimizationStrategy interface {
	GetID() string
	GetName() string
	CanOptimize(context *OptimizationContext) bool
	Optimize(context *OptimizationContext) *OptimizationResult
	GetMetrics() map[string]interface{}
}

// OptimizationPlugin interface for framework-specific plugins
type OptimizationPlugin interface {
	GetFrameworkName() string
	DetectFramework(filePath string, content []byte) float64 // Confidence score 0.0-1.0
	GetOptimizationHints(context *OptimizationContext) []OptimizationHint
	GetDefaultRules() []*OptimizationRule
	GetDefaultTemplates() []*OptimizationTemplate
}

// OptimizationContext provides context for optimization decisions
type OptimizationContext struct {
	// File context
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
	FileType string `json:"file_type"`
	MimeType string `json:"mime_type"`

	// Access context
	AccessPattern   AccessPattern `json:"access_pattern"`
	AccessFrequency int64         `json:"access_frequency"`
	AccessHistory   []time.Time   `json:"access_history"`

	// Workload context
	WorkloadType string `json:"workload_type"`
	ProcessID    int    `json:"process_id"`
	Framework    string `json:"framework"`
	ModelSize    int64  `json:"model_size"`
	BatchSize    int    `json:"batch_size"`

	// System context
	AvailableMemory  uint64 `json:"available_memory"`
	AvailableGPUs    []int  `json:"available_gpus"`
	NetworkBandwidth int64  `json:"network_bandwidth"`
	StorageIOPS      int    `json:"storage_iops"`

	// ML-specific context
	TrainingPhase string  `json:"training_phase"`
	EpochNumber   int     `json:"epoch_number"`
	DatasetSize   int64   `json:"dataset_size"`
	ModelAccuracy float64 `json:"model_accuracy"`

	// Custom context
	CustomProperties map[string]interface{} `json:"custom_properties"`
}

// OptimizationResult contains the results of optimization
type OptimizationResult struct {
	Applied         bool                   `json:"applied"`
	Confidence      float64                `json:"confidence"`
	Optimizations   []AppliedOptimization  `json:"optimizations"`
	Recommendations []string               `json:"recommendations"`
	Metrics         map[string]interface{} `json:"metrics"`
	NextReview      time.Time              `json:"next_review"`
}

// AppliedOptimization represents a single optimization that was applied
type AppliedOptimization struct {
	Type       string                 `json:"type"`
	Target     string                 `json:"target"`
	Parameters map[string]interface{} `json:"parameters"`
	Expected   map[string]interface{} `json:"expected"` // Expected improvements
	Actual     map[string]interface{} `json:"actual"`   // Actual results (filled later)
}

// OptimizationHint provides hints for optimization
type OptimizationHint struct {
	Type        string                 `json:"type"`
	Description string                 `json:"description"`
	Priority    int                    `json:"priority"`
	Parameters  map[string]interface{} `json:"parameters"`
}

// UsagePattern tracks usage patterns for adaptive optimization
type UsagePattern struct {
	sync.RWMutex

	ID              string             `json:"id"`
	Pattern         string             `json:"pattern"`         // Pattern identifier
	Frequency       int64              `json:"frequency"`       // How often this pattern occurs
	SuccessRate     float64            `json:"success_rate"`    // Success rate of optimizations
	AvgImprovement  float64            `json:"avg_improvement"` // Average improvement achieved
	LastSeen        time.Time          `json:"last_seen"`
	Characteristics map[string]float64 `json:"characteristics"` // Pattern characteristics
}

// AdaptiveRule represents a rule that adapts based on learning
type AdaptiveRule struct {
	BaseRule    *OptimizationRule  `json:"base_rule"`
	Adaptations map[string]float64 `json:"adaptations"` // Parameter adaptations
	Performance PerformanceMetrics `json:"performance"`
	LastUpdate  time.Time          `json:"last_update"`
}

// PerformanceMetrics tracks rule performance
type PerformanceMetrics struct {
	Applications   int64   `json:"applications"`    // Number of times applied
	Successes      int64   `json:"successes"`       // Number of successful applications
	AvgImprovement float64 `json:"avg_improvement"` // Average improvement
	AvgLatency     float64 `json:"avg_latency"`     // Average optimization latency
	ErrorRate      float64 `json:"error_rate"`      // Error rate
}

// NewOptimizationEngine creates a new optimization engine
func NewOptimizationEngine(enabled bool) *OptimizationEngine {
	engine := &OptimizationEngine{
		rules:         make(map[string]*OptimizationRule),
		templates:     make(map[string]*OptimizationTemplate),
		strategies:    make(map[string]OptimizationStrategy),
		usagePatterns: make(map[string]*UsagePattern),
		adaptiveRules: make(map[string]*AdaptiveRule),
		plugins:       make(map[string]OptimizationPlugin),
		enabled:       enabled,
	}

	if enabled {
		// Load default rules and templates
		engine.loadDefaultConfiguration()

		// Initialize built-in strategies
		engine.initializeStrategies()

		glog.V(1).Infof("Optimization engine initialized with %d rules, %d templates",
			len(engine.rules), len(engine.templates))
	}

	return engine
}

// loadDefaultConfiguration loads default rules and templates
func (oe *OptimizationEngine) loadDefaultConfiguration() {
	// Load default ML optimization rules
	defaultRules := []*OptimizationRule{
		{
			ID:          "sequential_prefetch",
			Name:        "Sequential Access Prefetching",
			Description: "Enable aggressive prefetching for sequential access patterns",
			Priority:    100,
			Conditions: []RuleCondition{
				{
					Type:     "access_pattern",
					Property: "pattern_type",
					Operator: "equals",
					Value:    "sequential",
					Weight:   1.0,
				},
				{
					Type:     "file_context",
					Property: "size",
					Operator: "greater_than",
					Value:    1024 * 1024, // 1MB
					Weight:   0.8,
				},
			},
			Actions: []RuleAction{
				{
					Type:   "prefetch",
					Target: "file",
					Parameters: map[string]interface{}{
						"chunk_size":    64 * 1024, // 64KB chunks
						"prefetch_size": 8,         // 8 chunks ahead
						"queue_depth":   4,         // 4 parallel prefetch
					},
				},
			},
		},
		{
			ID:          "ml_model_cache",
			Name:        "ML Model Caching",
			Description: "Optimize caching for ML model files",
			Priority:    90,
			Conditions: []RuleCondition{
				{
					Type:     "file_pattern",
					Property: "extension",
					Operator: "in",
					Value:    []string{".pth", ".pt", ".ckpt", ".h5", ".pb", ".onnx"},
					Weight:   1.0,
				},
				{
					Type:     "workload_context",
					Property: "workload_type",
					Operator: "in",
					Value:    []string{"training", "inference"},
					Weight:   0.9,
				},
			},
			Actions: []RuleAction{
				{
					Type:   "cache",
					Target: "file",
					Parameters: map[string]interface{}{
						"cache_strategy":  "persistent",
						"priority":        "high",
						"eviction_policy": "lfu",
					},
				},
			},
		},
		{
			ID:          "dataset_batch_optimize",
			Name:        "Dataset Batch Optimization",
			Description: "Optimize access patterns for dataset files during batch processing",
			Priority:    85,
			Conditions: []RuleCondition{
				{
					Type:     "file_pattern",
					Property: "name_pattern",
					Operator: "matches",
					Value:    ".*\\.(csv|parquet|tfrecord|arrow)$",
					Weight:   0.9,
				},
				{
					Type:     "access_pattern",
					Property: "batch_size",
					Operator: "greater_than",
					Value:    10,
					Weight:   0.8,
				},
			},
			Actions: []RuleAction{
				{
					Type:   "batch_prefetch",
					Target: "dataset",
					Parameters: map[string]interface{}{
						"batch_aware":    true,
						"shuffle_buffer": 1000,
						"parallel_calls": 4,
					},
				},
			},
		},
	}

	// Register default rules
	for _, rule := range defaultRules {
		oe.rules[rule.ID] = rule
	}

	// Load default templates
	defaultTemplates := []*OptimizationTemplate{
		{
			ID:          "pytorch_training",
			Name:        "PyTorch Training Optimization",
			Description: "Optimized configuration for PyTorch training workloads",
			Category:    "training",
			Rules:       []string{"sequential_prefetch", "ml_model_cache", "dataset_batch_optimize"},
			Parameters: map[string]interface{}{
				"framework":       "pytorch",
				"prefetch_factor": 2.0,
				"cache_ratio":     0.3,
			},
		},
		{
			ID:          "tensorflow_inference",
			Name:        "TensorFlow Inference Optimization",
			Description: "Optimized configuration for TensorFlow inference workloads",
			Category:    "inference",
			Rules:       []string{"ml_model_cache"},
			Parameters: map[string]interface{}{
				"framework":       "tensorflow",
				"model_preload":   true,
				"batch_inference": true,
			},
		},
		{
			ID:          "generic_ml_training",
			Name:        "Generic ML Training",
			Description: "General-purpose optimization for ML training",
			Category:    "training",
			Rules:       []string{"sequential_prefetch", "dataset_batch_optimize"},
			Parameters: map[string]interface{}{
				"adaptive":      true,
				"learning_rate": 0.001,
			},
		},
	}

	// Register default templates
	for _, template := range defaultTemplates {
		oe.templates[template.ID] = template
	}
}

// initializeStrategies initializes built-in optimization strategies
func (oe *OptimizationEngine) initializeStrategies() {
	// Register built-in strategies
	oe.strategies["adaptive_prefetch"] = &AdaptivePrefetchStrategy{}
	oe.strategies["intelligent_cache"] = &IntelligentCacheStrategy{}
	oe.strategies["workload_coordination"] = &WorkloadCoordinationStrategy{}
}

// RegisterPlugin registers an optimization plugin
func (oe *OptimizationEngine) RegisterPlugin(plugin OptimizationPlugin) error {
	oe.Lock()
	defer oe.Unlock()

	frameworkName := plugin.GetFrameworkName()
	if _, exists := oe.plugins[frameworkName]; exists {
		return fmt.Errorf("plugin for framework '%s' already registered", frameworkName)
	}

	oe.plugins[frameworkName] = plugin

	// Load plugin's default rules and templates
	for _, rule := range plugin.GetDefaultRules() {
		oe.rules[rule.ID] = rule
	}

	for _, template := range plugin.GetDefaultTemplates() {
		oe.templates[template.ID] = template
	}

	glog.V(1).Infof("Registered optimization plugin for framework: %s", frameworkName)
	return nil
}

// OptimizeAccess applies optimization for file access
func (oe *OptimizationEngine) OptimizeAccess(context *OptimizationContext) *OptimizationResult {
	if !oe.enabled {
		return &OptimizationResult{Applied: false}
	}

	oe.RLock()
	defer oe.RUnlock()

	result := &OptimizationResult{
		Applied:         false,
		Confidence:      0.0,
		Optimizations:   make([]AppliedOptimization, 0),
		Recommendations: make([]string, 0),
		Metrics:         make(map[string]interface{}),
		NextReview:      time.Now().Add(5 * time.Minute),
	}

	// Enhance context with framework detection
	oe.enhanceContext(context)

	// Find applicable rules
	applicableRules := oe.findApplicableRules(context)
	if len(applicableRules) == 0 {
		glog.V(3).Infof("No applicable rules found for context: %+v", context)
		return result
	}

	// Sort rules by priority and confidence
	sortedRules := oe.sortRulesByPriority(applicableRules, context)

	// Apply top rules
	totalConfidence := 0.0
	appliedCount := 0

	for _, ruleMatch := range sortedRules {
		if appliedCount >= 5 { // Limit number of applied optimizations
			break
		}

		optimization := oe.applyRule(ruleMatch.Rule, context, ruleMatch.Confidence)
		if optimization != nil {
			result.Optimizations = append(result.Optimizations, *optimization)
			totalConfidence += ruleMatch.Confidence
			appliedCount++
			result.Applied = true
		}
	}

	// Calculate overall confidence
	if appliedCount > 0 {
		result.Confidence = totalConfidence / float64(appliedCount)
	}

	// Generate recommendations
	result.Recommendations = oe.generateRecommendations(context, sortedRules)

	// Update usage patterns for learning
	oe.updateUsagePatterns(context, result)

	glog.V(2).Infof("Applied %d optimizations with confidence %.2f",
		appliedCount, result.Confidence)

	return result
}

// enhanceContext enhances the optimization context with additional information
func (oe *OptimizationEngine) enhanceContext(context *OptimizationContext) {
	// Detect framework using plugins
	if context.Framework == "" {
		context.Framework = oe.detectFramework(context.FilePath, nil)
	}

	// Enhance with file type detection
	if context.FileType == "" {
		context.FileType = oe.detectFileType(context.FilePath)
	}

	// Add pattern-based enhancements
	if context.CustomProperties == nil {
		context.CustomProperties = make(map[string]interface{})
	}

	// Add file-based hints
	ext := strings.ToLower(filepath.Ext(context.FilePath))
	context.CustomProperties["file_extension"] = ext
	context.CustomProperties["is_model_file"] = oe.isModelFile(ext)
	context.CustomProperties["is_dataset_file"] = oe.isDatasetFile(ext)
}

// detectFramework detects ML framework from file path and content
func (oe *OptimizationEngine) detectFramework(filePath string, content []byte) string {
	bestFramework := ""
	bestScore := 0.0

	for _, plugin := range oe.plugins {
		score := plugin.DetectFramework(filePath, content)
		if score > bestScore {
			bestScore = score
			bestFramework = plugin.GetFrameworkName()
		}
	}

	// Fallback to simple pattern matching
	if bestFramework == "" {
		ext := strings.ToLower(filepath.Ext(filePath))
		switch ext {
		case ".pth", ".pt":
			return "pytorch"
		case ".h5", ".hdf5":
			return "tensorflow"
		case ".ckpt":
			return "tensorflow"
		case ".pb":
			return "tensorflow"
		case ".onnx":
			return "onnx"
		}
	}

	return bestFramework
}

// detectFileType detects the type of file for optimization purposes
func (oe *OptimizationEngine) detectFileType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))

	if oe.isModelFile(ext) {
		return "model"
	}
	if oe.isDatasetFile(ext) {
		return "dataset"
	}
	if oe.isConfigFile(ext) {
		return "config"
	}
	if oe.isLogFile(ext) {
		return "log"
	}

	return "unknown"
}

// Helper functions for file type detection
func (oe *OptimizationEngine) isModelFile(ext string) bool {
	modelExtensions := []string{".pth", ".pt", ".ckpt", ".h5", ".hdf5", ".pb", ".onnx", ".pkl", ".model"}
	for _, modelExt := range modelExtensions {
		if ext == modelExt {
			return true
		}
	}
	return false
}

func (oe *OptimizationEngine) isDatasetFile(ext string) bool {
	datasetExtensions := []string{".csv", ".json", ".parquet", ".arrow", ".tfrecord", ".hdf5", ".npy", ".npz"}
	for _, dataExt := range datasetExtensions {
		if ext == dataExt {
			return true
		}
	}
	return false
}

func (oe *OptimizationEngine) isConfigFile(ext string) bool {
	configExtensions := []string{".yaml", ".yml", ".json", ".toml", ".ini", ".conf", ".cfg"}
	for _, confExt := range configExtensions {
		if ext == confExt {
			return true
		}
	}
	return false
}

func (oe *OptimizationEngine) isLogFile(ext string) bool {
	return ext == ".log" || ext == ".txt"
}

// RuleMatch represents a rule with its confidence score
type RuleMatch struct {
	Rule       *OptimizationRule
	Confidence float64
}

// findApplicableRules finds rules that apply to the given context
func (oe *OptimizationEngine) findApplicableRules(context *OptimizationContext) []*RuleMatch {
	matches := make([]*RuleMatch, 0)

	for _, rule := range oe.rules {
		confidence := oe.evaluateRuleConditions(rule, context)
		if confidence > 0.5 { // Minimum confidence threshold
			matches = append(matches, &RuleMatch{
				Rule:       rule,
				Confidence: confidence,
			})
		}
	}

	return matches
}

// evaluateRuleConditions evaluates rule conditions against context
func (oe *OptimizationEngine) evaluateRuleConditions(rule *OptimizationRule, context *OptimizationContext) float64 {
	if len(rule.Conditions) == 0 {
		return 0.0
	}

	totalWeight := 0.0
	matchedWeight := 0.0

	for _, condition := range rule.Conditions {
		totalWeight += condition.Weight

		if oe.evaluateCondition(condition, context) {
			matchedWeight += condition.Weight
		}
	}

	if totalWeight == 0 {
		return 0.0
	}

	return matchedWeight / totalWeight
}

// evaluateCondition evaluates a single condition
func (oe *OptimizationEngine) evaluateCondition(condition RuleCondition, context *OptimizationContext) bool {
	var contextValue interface{}

	// Extract context value based on condition type and property
	switch condition.Type {
	case "file_pattern":
		contextValue = oe.getFileProperty(condition.Property, context)
	case "access_pattern":
		contextValue = oe.getAccessProperty(condition.Property, context)
	case "workload_context":
		contextValue = oe.getWorkloadProperty(condition.Property, context)
	case "system_context":
		contextValue = oe.getSystemProperty(condition.Property, context)
	default:
		return false
	}

	// Evaluate based on operator
	return oe.evaluateOperator(condition.Operator, contextValue, condition.Value)
}

// Property extraction methods
func (oe *OptimizationEngine) getFileProperty(property string, context *OptimizationContext) interface{} {
	switch property {
	case "path":
		return context.FilePath
	case "extension":
		return strings.ToLower(filepath.Ext(context.FilePath))
	case "size":
		return context.FileSize
	case "type":
		return context.FileType
	case "name_pattern":
		return filepath.Base(context.FilePath)
	default:
		return nil
	}
}

func (oe *OptimizationEngine) getAccessProperty(property string, context *OptimizationContext) interface{} {
	switch property {
	case "pattern_type":
		return context.AccessPattern.String()
	case "frequency":
		return context.AccessFrequency
	case "batch_size":
		return context.BatchSize
	default:
		return nil
	}
}

func (oe *OptimizationEngine) getWorkloadProperty(property string, context *OptimizationContext) interface{} {
	switch property {
	case "workload_type":
		return context.WorkloadType
	case "framework":
		return context.Framework
	case "training_phase":
		return context.TrainingPhase
	default:
		return nil
	}
}

func (oe *OptimizationEngine) getSystemProperty(property string, context *OptimizationContext) interface{} {
	switch property {
	case "available_memory":
		return context.AvailableMemory
	case "gpu_count":
		return len(context.AvailableGPUs)
	default:
		return nil
	}
}

// evaluateOperator evaluates comparison operators
func (oe *OptimizationEngine) evaluateOperator(operator string, contextValue, ruleValue interface{}) bool {
	switch operator {
	case "equals":
		return contextValue == ruleValue
	case "contains":
		if contextStr, ok := contextValue.(string); ok {
			if ruleStr, ok := ruleValue.(string); ok {
				return strings.Contains(contextStr, ruleStr)
			}
		}
	case "matches":
		if contextStr, ok := contextValue.(string); ok {
			if ruleStr, ok := ruleValue.(string); ok {
				matched, _ := regexp.MatchString(ruleStr, contextStr)
				return matched
			}
		}
	case "in":
		if ruleSlice, ok := ruleValue.([]interface{}); ok {
			for _, item := range ruleSlice {
				if contextValue == item {
					return true
				}
			}
		}
		if ruleSlice, ok := ruleValue.([]string); ok {
			if contextStr, ok := contextValue.(string); ok {
				for _, item := range ruleSlice {
					if contextStr == item {
						return true
					}
				}
			}
		}
	case "greater_than":
		return oe.compareNumbers(contextValue, ruleValue, ">")
	case "less_than":
		return oe.compareNumbers(contextValue, ruleValue, "<")
	case "greater_equal":
		return oe.compareNumbers(contextValue, ruleValue, ">=")
	case "less_equal":
		return oe.compareNumbers(contextValue, ruleValue, "<=")
	}

	return false
}

// compareNumbers compares numeric values
func (oe *OptimizationEngine) compareNumbers(a, b interface{}, op string) bool {
	aFloat, aOk := oe.toFloat64(a)
	bFloat, bOk := oe.toFloat64(b)

	if !aOk || !bOk {
		return false
	}

	switch op {
	case ">":
		return aFloat > bFloat
	case "<":
		return aFloat < bFloat
	case ">=":
		return aFloat >= bFloat
	case "<=":
		return aFloat <= bFloat
	default:
		return false
	}
}

// toFloat64 converts various numeric types to float64
func (oe *OptimizationEngine) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// sortRulesByPriority sorts rules by priority and confidence
func (oe *OptimizationEngine) sortRulesByPriority(matches []*RuleMatch, context *OptimizationContext) []*RuleMatch {
	// Simple sorting by combined score (priority * confidence)
	for i := 0; i < len(matches)-1; i++ {
		for j := i + 1; j < len(matches); j++ {
			scoreI := float64(matches[i].Rule.Priority) * matches[i].Confidence
			scoreJ := float64(matches[j].Rule.Priority) * matches[j].Confidence

			if scoreI < scoreJ {
				matches[i], matches[j] = matches[j], matches[i]
			}
		}
	}

	return matches
}

// applyRule applies a single optimization rule
func (oe *OptimizationEngine) applyRule(rule *OptimizationRule, context *OptimizationContext, confidence float64) *AppliedOptimization {
	if len(rule.Actions) == 0 {
		return nil
	}

	// For now, apply the first action (could be extended to handle multiple actions)
	action := rule.Actions[0]

	optimization := &AppliedOptimization{
		Type:       action.Type,
		Target:     action.Target,
		Parameters: make(map[string]interface{}),
		Expected:   make(map[string]interface{}),
		Actual:     make(map[string]interface{}),
	}

	// Copy parameters
	for k, v := range action.Parameters {
		optimization.Parameters[k] = v
	}

	// Set expected improvements based on rule type
	optimization.Expected["confidence"] = confidence
	optimization.Expected["rule_id"] = rule.ID
	optimization.Expected["improvement_estimate"] = confidence * 0.5 // Simple estimation

	glog.V(3).Infof("Applied rule '%s' with confidence %.2f", rule.ID, confidence)

	return optimization
}

// generateRecommendations generates optimization recommendations
func (oe *OptimizationEngine) generateRecommendations(context *OptimizationContext, matches []*RuleMatch) []string {
	recommendations := make([]string, 0)

	// Add general recommendations based on context
	if context.Framework != "" {
		recommendations = append(recommendations,
			fmt.Sprintf("Consider using %s-specific optimizations", context.Framework))
	}

	if context.FileType == "model" && context.FileSize > 100*1024*1024 {
		recommendations = append(recommendations,
			"Large model file detected - consider model compression or sharding")
	}

	if context.AccessPattern == SequentialAccess {
		recommendations = append(recommendations,
			"Sequential access pattern detected - increase prefetch buffer size")
	}

	return recommendations
}

// updateUsagePatterns updates usage patterns for adaptive learning
func (oe *OptimizationEngine) updateUsagePatterns(context *OptimizationContext, result *OptimizationResult) {
	patternKey := oe.generatePatternKey(context)

	oe.Lock()
	defer oe.Unlock()

	pattern, exists := oe.usagePatterns[patternKey]
	if !exists {
		pattern = &UsagePattern{
			ID:              patternKey,
			Pattern:         patternKey,
			Frequency:       0,
			SuccessRate:     0.0,
			AvgImprovement:  0.0,
			LastSeen:        time.Now(),
			Characteristics: make(map[string]float64),
		}
		oe.usagePatterns[patternKey] = pattern
	}

	pattern.Lock()
	pattern.Frequency++
	pattern.LastSeen = time.Now()

	// Update characteristics
	pattern.Characteristics["file_size"] = float64(context.FileSize)
	pattern.Characteristics["access_frequency"] = float64(context.AccessFrequency)
	pattern.Characteristics["confidence"] = result.Confidence

	pattern.Unlock()
}

// generatePatternKey generates a key for pattern identification
func (oe *OptimizationEngine) generatePatternKey(context *OptimizationContext) string {
	key := fmt.Sprintf("fw:%s|type:%s|pattern:%s|phase:%s",
		context.Framework,
		context.FileType,
		context.AccessPattern.String(),
		context.TrainingPhase)

	return key
}

// GetMetrics returns optimization engine metrics
func (oe *OptimizationEngine) GetMetrics() map[string]interface{} {
	oe.RLock()
	defer oe.RUnlock()

	metrics := map[string]interface{}{
		"enabled":          oe.enabled,
		"rules_count":      len(oe.rules),
		"templates_count":  len(oe.templates),
		"strategies_count": len(oe.strategies),
		"plugins_count":    len(oe.plugins),
		"patterns_learned": len(oe.usagePatterns),
	}

	// Add pattern statistics
	totalFrequency := int64(0)
	for _, pattern := range oe.usagePatterns {
		pattern.RLock()
		totalFrequency += pattern.Frequency
		pattern.RUnlock()
	}
	metrics["total_pattern_frequency"] = totalFrequency

	return metrics
}

// LoadConfiguration loads optimization rules and templates from configuration
func (oe *OptimizationEngine) LoadConfiguration(configData []byte) error {
	var config struct {
		Rules     []*OptimizationRule     `json:"rules"`
		Templates []*OptimizationTemplate `json:"templates"`
	}

	if err := json.Unmarshal(configData, &config); err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	oe.Lock()
	defer oe.Unlock()

	// Load rules
	for _, rule := range config.Rules {
		oe.rules[rule.ID] = rule
		glog.V(2).Infof("Loaded optimization rule: %s", rule.ID)
	}

	// Load templates
	for _, template := range config.Templates {
		oe.templates[template.ID] = template
		glog.V(2).Infof("Loaded optimization template: %s", template.ID)
	}

	return nil
}

// Shutdown gracefully shuts down the optimization engine
func (oe *OptimizationEngine) Shutdown() {
	oe.Lock()
	defer oe.Unlock()

	oe.enabled = false
	glog.V(1).Infof("Optimization engine shutdown complete")
}

// Built-in optimization strategies

// AdaptivePrefetchStrategy implements adaptive prefetching
type AdaptivePrefetchStrategy struct{}

func (s *AdaptivePrefetchStrategy) GetID() string   { return "adaptive_prefetch" }
func (s *AdaptivePrefetchStrategy) GetName() string { return "Adaptive Prefetch Strategy" }

func (s *AdaptivePrefetchStrategy) CanOptimize(context *OptimizationContext) bool {
	return context.AccessPattern == SequentialAccess || context.AccessPattern == StridedAccess
}

func (s *AdaptivePrefetchStrategy) Optimize(context *OptimizationContext) *OptimizationResult {
	return &OptimizationResult{
		Applied:    true,
		Confidence: 0.8,
		Optimizations: []AppliedOptimization{
			{
				Type:   "prefetch",
				Target: "file",
				Parameters: map[string]interface{}{
					"strategy":   "adaptive",
					"chunk_size": 64 * 1024,
				},
			},
		},
	}
}

func (s *AdaptivePrefetchStrategy) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"strategy":     "adaptive_prefetch",
		"applications": 0,
	}
}

// IntelligentCacheStrategy implements intelligent caching
type IntelligentCacheStrategy struct{}

func (s *IntelligentCacheStrategy) GetID() string   { return "intelligent_cache" }
func (s *IntelligentCacheStrategy) GetName() string { return "Intelligent Cache Strategy" }

func (s *IntelligentCacheStrategy) CanOptimize(context *OptimizationContext) bool {
	return context.FileType == "model" || context.AccessFrequency > 10
}

func (s *IntelligentCacheStrategy) Optimize(context *OptimizationContext) *OptimizationResult {
	return &OptimizationResult{
		Applied:    true,
		Confidence: 0.7,
		Optimizations: []AppliedOptimization{
			{
				Type:   "cache",
				Target: "file",
				Parameters: map[string]interface{}{
					"strategy": "intelligent",
					"priority": "high",
				},
			},
		},
	}
}

func (s *IntelligentCacheStrategy) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"strategy":     "intelligent_cache",
		"applications": 0,
	}
}

// WorkloadCoordinationStrategy implements workload coordination
type WorkloadCoordinationStrategy struct{}

func (s *WorkloadCoordinationStrategy) GetID() string   { return "workload_coordination" }
func (s *WorkloadCoordinationStrategy) GetName() string { return "Workload Coordination Strategy" }

func (s *WorkloadCoordinationStrategy) CanOptimize(context *OptimizationContext) bool {
	return context.WorkloadType != "" && context.ProcessID > 0
}

func (s *WorkloadCoordinationStrategy) Optimize(context *OptimizationContext) *OptimizationResult {
	return &OptimizationResult{
		Applied:    true,
		Confidence: 0.6,
		Optimizations: []AppliedOptimization{
			{
				Type:   "coordinate",
				Target: "workload",
				Parameters: map[string]interface{}{
					"strategy": "coordination",
					"priority": "normal",
				},
			},
		},
	}
}

func (s *WorkloadCoordinationStrategy) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"strategy":     "workload_coordination",
		"applications": 0,
	}
}
