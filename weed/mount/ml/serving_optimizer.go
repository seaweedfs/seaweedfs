package ml

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ServingPattern represents different model serving patterns
type ServingPattern int

const (
	ServingPatternUnknown ServingPattern = iota
	ServingPatternBatchInference         // Batch inference processing
	ServingPatternRealtimeInference      // Real-time inference requests
	ServingPatternStreamingInference     // Streaming inference
	ServingPatternMultiModalServing      // Multi-modal model serving
	ServingPatternEnsembleServing        // Ensemble model serving
	ServingPatternA_BServing             // A/B testing model serving
	ServingPatternCanaryServing          // Canary deployment serving
	ServingPatternAutoScalingServing     // Auto-scaling inference
)

// ModelServingInfo represents information about a serving model
type ModelServingInfo struct {
	sync.RWMutex
	
	// Model identity
	ModelID       string         `json:"model_id"`
	ModelPath     string         `json:"model_path"`
	ModelVersion  string         `json:"model_version"`
	ModelType     string         `json:"model_type"`     // tensorflow, pytorch, onnx, etc.
	Framework     string         `json:"framework"`      // serving framework (tensorflow-serving, torchserve, etc.)
	
	// Model characteristics
	ModelSize     uint64         `json:"model_size"`     // Model size in bytes
	InputShape    []int          `json:"input_shape"`    // Input tensor shape
	OutputShape   []int          `json:"output_shape"`   // Output tensor shape
	BatchSize     int            `json:"batch_size"`     // Optimal batch size
	Precision     string         `json:"precision"`      // fp32, fp16, int8, etc.
	
	// Serving configuration
	ServingPattern ServingPattern `json:"serving_pattern"`
	MinReplicas   int            `json:"min_replicas"`
	MaxReplicas   int            `json:"max_replicas"`
	TargetLatency time.Duration  `json:"target_latency"`
	TargetThroughput float64     `json:"target_throughput"` // requests per second
	
	// Performance metrics
	CurrentLatency    time.Duration `json:"current_latency"`
	CurrentThroughput float64       `json:"current_throughput"`
	CacheHitRate      float64       `json:"cache_hit_rate"`
	LoadTime          time.Duration `json:"load_time"`
	WarmupTime        time.Duration `json:"warmup_time"`
	
	// Resource usage
	CPUUsage      float64        `json:"cpu_usage"`      // CPU utilization percentage
	MemoryUsage   uint64         `json:"memory_usage"`   // Memory usage in bytes
	GPUUsage      float64        `json:"gpu_usage"`      // GPU utilization percentage
	GPUMemoryUsage uint64        `json:"gpu_memory_usage"` // GPU memory usage in bytes
	
	// Access patterns
	AccessFrequency map[string]int64 `json:"access_frequency"` // File -> access count
	HotFiles        []string         `json:"hot_files"`        // Frequently accessed files
	ColdFiles       []string         `json:"cold_files"`       // Rarely accessed files
	
	// Lifecycle
	DeployedAt    time.Time      `json:"deployed_at"`
	LastAccessed  time.Time      `json:"last_accessed"`
	RequestCount  int64          `json:"request_count"`
	ErrorCount    int64          `json:"error_count"`
}

// InferenceRequest represents an inference request
type InferenceRequest struct {
	RequestID    string                 `json:"request_id"`
	ModelID      string                 `json:"model_id"`
	InputData    []string               `json:"input_data"`    // File paths for input data
	BatchSize    int                    `json:"batch_size"`
	Priority     int                    `json:"priority"`
	Timestamp    time.Time              `json:"timestamp"`
	Deadline     time.Time              `json:"deadline"`      // SLA deadline
	Metadata     map[string]interface{} `json:"metadata"`
}

// ServingOptimizer optimizes model serving patterns
type ServingOptimizer struct {
	sync.RWMutex
	
	// Configuration
	enabled              bool                              // Whether serving optimization is enabled
	optimizationInterval time.Duration                     // How often to optimize
	cacheTTL             time.Duration                     // Cache time-to-live
	preloadThreshold     float64                           // Threshold to preload models
	
	// Model tracking
	activeModels         map[string]*ModelServingInfo      // Currently served models
	modelVersions        map[string][]string               // Model -> versions
	servingHistory       map[string]*ServingHistory        // Historical serving data
	
	// Request tracking
	requestQueue         []*InferenceRequest               // Pending inference requests
	completedRequests    map[string]*InferenceRequest      // Completed requests
	
	// Optimization state
	optimizationRules    []*ServingOptimizationRule        // Optimization rules
	cachingStrategy      *ServingCacheStrategy             // Caching strategy
	loadBalancer         *ModelLoadBalancer                // Load balancing
	
	// Performance tracking
	latencyHistogram     map[time.Duration]int64           // Latency distribution
	throughputHistory    []ThroughputSample                // Throughput over time
	errorRates           map[string]float64                // Error rates per model
	
	// Background tasks
	ctx                  context.Context
	cancel               context.CancelFunc
	
	// Metrics
	totalRequests        int64                             // Total inference requests
	cachedRequests       int64                             // Requests served from cache
	optimizationEvents   int64                             // Optimization events triggered
}

// ServingHistory tracks historical serving information
type ServingHistory struct {
	ModelID              string                 `json:"model_id"`
	AccessPatterns       []AccessPatternSample  `json:"access_patterns"`
	PerformanceMetrics   []PerformanceSample    `json:"performance_metrics"`
	ScalingEvents        []ScalingEvent         `json:"scaling_events"`
	ErrorEvents          []ErrorEvent           `json:"error_events"`
}

// AccessPatternSample represents a sample of access patterns
type AccessPatternSample struct {
	Timestamp     time.Time `json:"timestamp"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	AvgBatchSize  float64   `json:"avg_batch_size"`
	Pattern       ServingPattern `json:"pattern"`
}

// PerformanceSample represents a performance measurement
type PerformanceSample struct {
	Timestamp  time.Time     `json:"timestamp"`
	Latency    time.Duration `json:"latency"`
	Throughput float64       `json:"throughput"`
	CPUUsage   float64       `json:"cpu_usage"`
	MemoryUsage uint64       `json:"memory_usage"`
}

// ScalingEvent represents a scaling event
type ScalingEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	Action      string    `json:"action"`      // scale_up, scale_down, scale_out, scale_in
	Reason      string    `json:"reason"`      // latency_sla_breach, high_throughput, etc.
	OldReplicas int       `json:"old_replicas"`
	NewReplicas int       `json:"new_replicas"`
}

// ErrorEvent represents an error event
type ErrorEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	ErrorType   string                 `json:"error_type"`
	ErrorMsg    string                 `json:"error_msg"`
	RequestID   string                 `json:"request_id"`
	ModelID     string                 `json:"model_id"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// ThroughputSample represents a throughput measurement
type ThroughputSample struct {
	Timestamp  time.Time `json:"timestamp"`
	Throughput float64   `json:"throughput"` // requests per second
	ModelID    string    `json:"model_id"`
}

// ServingOptimizationRule defines rules for optimizing model serving
type ServingOptimizationRule struct {
	Name         string                 `json:"name"`
	Condition    string                 `json:"condition"`    // latency > 100ms, throughput < 10rps
	Action       string                 `json:"action"`       // preload, cache, scale_up, etc.
	Parameters   map[string]interface{} `json:"parameters"`
	ModelPattern string                 `json:"model_pattern"` // Model name pattern to match
	Priority     int                    `json:"priority"`
	Enabled      bool                   `json:"enabled"`
}

// ServingCacheStrategy defines caching strategies for model serving
type ServingCacheStrategy struct {
	ModelCaching     bool          `json:"model_caching"`      // Cache model files
	ResultCaching    bool          `json:"result_caching"`     // Cache inference results
	InputCaching     bool          `json:"input_caching"`      // Cache preprocessed inputs
	CacheSizeLimit   uint64        `json:"cache_size_limit"`   // Maximum cache size in bytes
	CacheTTL         time.Duration `json:"cache_ttl"`          // Cache time-to-live
	EvictionPolicy   string        `json:"eviction_policy"`    // LRU, LFU, TTL
	CacheWarmup      bool          `json:"cache_warmup"`       // Proactively warm cache
}

// ModelLoadBalancer handles load balancing between model replicas
type ModelLoadBalancer struct {
	Strategy      string                  `json:"strategy"`       // round_robin, least_connections, weighted
	HealthChecks  bool                    `json:"health_checks"`  // Enable health checking
	Weights       map[string]int          `json:"weights"`        // Replica -> weight
	ActiveReplicas map[string]bool        `json:"active_replicas"` // Replica -> healthy status
}

// NewServingOptimizer creates a new serving optimizer
func NewServingOptimizer(enabled bool) *ServingOptimizer {
	ctx, cancel := context.WithCancel(context.Background())
	
	so := &ServingOptimizer{
		enabled:              enabled,
		optimizationInterval: 30 * time.Second,  // Optimize every 30 seconds
		cacheTTL:            10 * time.Minute,    // 10-minute cache TTL
		preloadThreshold:    0.8,                 // Preload at 80% threshold
		
		activeModels:      make(map[string]*ModelServingInfo),
		modelVersions:     make(map[string][]string),
		servingHistory:    make(map[string]*ServingHistory),
		requestQueue:      make([]*InferenceRequest, 0),
		completedRequests: make(map[string]*InferenceRequest),
		optimizationRules: make([]*ServingOptimizationRule, 0),
		latencyHistogram:  make(map[time.Duration]int64),
		errorRates:        make(map[string]float64),
		
		ctx:    ctx,
		cancel: cancel,
	}
	
	// Initialize default optimization rules
	so.initializeServingRules()
	
	// Initialize caching strategy
	so.cachingStrategy = &ServingCacheStrategy{
		ModelCaching:   true,
		ResultCaching:  true,
		InputCaching:   false, // Disabled by default
		CacheSizeLimit: 1024 * 1024 * 1024, // 1GB cache limit
		CacheTTL:       10 * time.Minute,
		EvictionPolicy: "LRU",
		CacheWarmup:    true,
	}
	
	// Initialize load balancer
	so.loadBalancer = &ModelLoadBalancer{
		Strategy:       "least_connections",
		HealthChecks:   true,
		Weights:        make(map[string]int),
		ActiveReplicas: make(map[string]bool),
	}
	
	if enabled {
		// Start optimization loop
		go so.optimizationLoop()
		glog.V(1).Infof("Serving optimizer started with interval %v", so.optimizationInterval)
	}
	
	return so
}

// initializeServingRules sets up default serving optimization rules
func (so *ServingOptimizer) initializeServingRules() {
	// Rule 1: Preload frequently accessed models
	so.optimizationRules = append(so.optimizationRules, &ServingOptimizationRule{
		Name:         "preload_popular_models",
		Condition:    "access_frequency > 10 AND last_access < 300s",
		Action:       "preload",
		Parameters:   map[string]interface{}{"priority": 10},
		ModelPattern: "*",
		Priority:     10,
		Enabled:      true,
	})
	
	// Rule 2: Scale up when latency exceeds SLA
	so.optimizationRules = append(so.optimizationRules, &ServingOptimizationRule{
		Name:         "scale_up_on_latency",
		Condition:    "avg_latency > target_latency * 1.5",
		Action:       "scale_up",
		Parameters:   map[string]interface{}{"scale_factor": 1.5},
		ModelPattern: "*",
		Priority:     20,
		Enabled:      true,
	})
	
	// Rule 3: Cache inference results for batch patterns
	so.optimizationRules = append(so.optimizationRules, &ServingOptimizationRule{
		Name:         "cache_batch_results",
		Condition:    "serving_pattern == 'batch' AND cache_hit_rate < 0.3",
		Action:       "enable_result_caching",
		Parameters:   map[string]interface{}{"cache_size": "100MB"},
		ModelPattern: "*",
		Priority:     15,
		Enabled:      true,
	})
	
	// Rule 4: Optimize model format for inference
	so.optimizationRules = append(so.optimizationRules, &ServingOptimizationRule{
		Name:         "optimize_model_format",
		Condition:    "load_time > 10s AND model_format != 'optimized'",
		Action:       "convert_model_format",
		Parameters:   map[string]interface{}{"target_format": "tensorrt"},
		ModelPattern: "*.onnx,*.pb",
		Priority:     5,
		Enabled:      true,
	})
}

// RegisterModel registers a new model for serving optimization
func (so *ServingOptimizer) RegisterModel(model *ModelServingInfo) {
	so.Lock()
	defer so.Unlock()
	
	so.activeModels[model.ModelID] = model
	
	// Initialize serving history
	so.servingHistory[model.ModelID] = &ServingHistory{
		ModelID:            model.ModelID,
		AccessPatterns:     make([]AccessPatternSample, 0),
		PerformanceMetrics: make([]PerformanceSample, 0),
		ScalingEvents:      make([]ScalingEvent, 0),
		ErrorEvents:        make([]ErrorEvent, 0),
	}
	
	// Track model version
	versions := so.modelVersions[model.ModelPath]
	if versions == nil {
		versions = make([]string, 0)
	}
	versions = append(versions, model.ModelVersion)
	so.modelVersions[model.ModelPath] = versions
	
	glog.V(1).Infof("Registered model for serving optimization: %s (%s)", model.ModelID, model.ServingPattern)
}

// RecordInferenceRequest records an inference request for optimization analysis
func (so *ServingOptimizer) RecordInferenceRequest(request *InferenceRequest) {
	so.Lock()
	defer so.Unlock()
	
	// Update model access patterns
	if model, exists := so.activeModels[request.ModelID]; exists {
		model.Lock()
		model.RequestCount++
		model.LastAccessed = time.Now()
		if model.AccessFrequency == nil {
			model.AccessFrequency = make(map[string]int64)
		}
		for _, inputFile := range request.InputData {
			model.AccessFrequency[inputFile]++
		}
		model.Unlock()
	}
	
	so.totalRequests++
	
	// Add to request queue for processing
	so.requestQueue = append(so.requestQueue, request)
	
	// Record access pattern sample
	so.recordAccessPattern(request)
}

// recordAccessPattern records access pattern information
func (so *ServingOptimizer) recordAccessPattern(request *InferenceRequest) {
	if history, exists := so.servingHistory[request.ModelID]; exists {
		sample := AccessPatternSample{
			Timestamp:     time.Now(),
			AvgBatchSize:  float64(request.BatchSize),
			Pattern:       ServingPatternRealtimeInference, // Default pattern
		}
		
		// Detect serving pattern based on request characteristics
		if request.BatchSize > 32 {
			sample.Pattern = ServingPatternBatchInference
		} else if time.Until(request.Deadline) < 100*time.Millisecond {
			sample.Pattern = ServingPatternRealtimeInference
		}
		
		history.AccessPatterns = append(history.AccessPatterns, sample)
		
		// Keep only recent samples (last 1000)
		if len(history.AccessPatterns) > 1000 {
			history.AccessPatterns = history.AccessPatterns[len(history.AccessPatterns)-500:]
		}
	}
}

// OptimizeModelAccess provides optimization recommendations for model file access
func (so *ServingOptimizer) OptimizeModelAccess(modelID string, filePaths []string) *ModelAccessOptimization {
	so.RLock()
	model := so.activeModels[modelID]
	history := so.servingHistory[modelID]
	so.RUnlock()
	
	if model == nil {
		return &ModelAccessOptimization{
			ShouldPreload: false,
			CacheStrategy: "none",
			PrefetchSize:  64 * 1024,
		}
	}
	
	model.RLock()
	defer model.RUnlock()
	
	optimization := &ModelAccessOptimization{
		ModelID:       modelID,
		ShouldPreload: false,
		CacheStrategy: "default",
		PrefetchSize:  256 * 1024, // Default 256KB prefetch
		Priority:      10,
		FileOptimizations: make(map[string]*FileAccessOptimization),
	}
	
	// Determine if model should be preloaded based on access patterns and history
	hasHistory := history != nil
	if model.RequestCount > 100 && time.Since(model.LastAccessed) < 5*time.Minute {
		optimization.ShouldPreload = true
		optimization.Priority = 20
		
		// Boost priority if we have serving history
		if hasHistory {
			optimization.Priority = 25
		}
	}
	
	// Optimize based on serving pattern
	switch model.ServingPattern {
	case ServingPatternBatchInference:
		// Batch inference benefits from larger prefetch and caching
		optimization.PrefetchSize = int64(model.BatchSize) * 1024 * 64 // 64KB per batch item
		optimization.CacheStrategy = "aggressive"
		
	case ServingPatternRealtimeInference:
		// Real-time inference needs fast access
		optimization.ShouldPreload = true
		optimization.CacheStrategy = "memory"
		optimization.PrefetchSize = int64(model.ModelSize / 10) // 10% of model size
		if optimization.PrefetchSize > 10*1024*1024 {
			optimization.PrefetchSize = 10 * 1024 * 1024 // Cap at 10MB
		}
		
	case ServingPatternEnsembleServing:
		// Ensemble serving needs coordinated loading
		optimization.ShouldPreload = true
		optimization.CacheStrategy = "coordinated"
		optimization.Priority = 25
		
	case ServingPatternAutoScalingServing:
		// Auto-scaling benefits from quick startup
		optimization.ShouldPreload = false // Avoid preloading to save memory
		optimization.CacheStrategy = "lazy"
		optimization.PrefetchSize = 1024 * 1024 // 1MB for quick startup
	}
	
	// Analyze file-specific access patterns
	for _, filePath := range filePaths {
		fileOpt := &FileAccessOptimization{
			FilePath:     filePath,
			ShouldCache:  false,
			PrefetchSize: optimization.PrefetchSize,
			Priority:     optimization.Priority,
		}
		
		// Check if file is hot (frequently accessed)
		if accessCount, exists := model.AccessFrequency[filePath]; exists && accessCount > 50 {
			fileOpt.ShouldCache = true
			fileOpt.Priority += 10
			
			// Determine file category and optimize accordingly
			if strings.Contains(filePath, "model.pb") || strings.Contains(filePath, ".onnx") {
				// Model definition files - high priority caching
				fileOpt.Priority += 20
				fileOpt.PrefetchSize = fileOpt.PrefetchSize * 2
			} else if strings.Contains(filePath, "variables") || strings.Contains(filePath, "weights") {
				// Weight files - moderate priority, larger prefetch
				fileOpt.Priority += 15
				fileOpt.PrefetchSize = fileOpt.PrefetchSize * 3
			} else if strings.Contains(filePath, "config") || strings.Contains(filePath, "metadata") {
				// Config files - high priority, smaller prefetch
				fileOpt.Priority += 25
				fileOpt.PrefetchSize = 64 * 1024 // 64KB for config files
			}
		}
		
		optimization.FileOptimizations[filePath] = fileOpt
	}
	
	return optimization
}

// ModelAccessOptimization holds optimization recommendations for model access
type ModelAccessOptimization struct {
	ModelID           string                              `json:"model_id"`
	ShouldPreload     bool                                `json:"should_preload"`
	CacheStrategy     string                              `json:"cache_strategy"`
	PrefetchSize      int64                               `json:"prefetch_size"`
	Priority          int                                 `json:"priority"`
	FileOptimizations map[string]*FileAccessOptimization `json:"file_optimizations"`
}

// FileAccessOptimization holds optimization recommendations for individual files
type FileAccessOptimization struct {
	FilePath     string `json:"file_path"`
	ShouldCache  bool   `json:"should_cache"`
	PrefetchSize int64  `json:"prefetch_size"`
	Priority     int    `json:"priority"`
}

// optimizationLoop runs the main optimization loop
func (so *ServingOptimizer) optimizationLoop() {
	ticker := time.NewTicker(so.optimizationInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-so.ctx.Done():
			return
		case <-ticker.C:
			so.performOptimization()
		}
	}
}

// performOptimization performs serving optimizations
func (so *ServingOptimizer) performOptimization() {
	so.Lock()
	defer so.Unlock()
	
	// Process completed requests and update metrics
	so.updateMetrics()
	
	// Evaluate optimization rules
	for _, rule := range so.optimizationRules {
		if !rule.Enabled {
			continue
		}
		
		for modelID, model := range so.activeModels {
			if so.matchesPattern(model.ModelPath, rule.ModelPattern) && so.evaluateCondition(model, rule.Condition) {
				so.executeOptimizationAction(modelID, rule)
				so.optimizationEvents++
			}
		}
	}
	
	// Cleanup old data
	so.cleanupHistoricalData()
}

// updateMetrics updates performance metrics
func (so *ServingOptimizer) updateMetrics() {
	now := time.Now()
	
	for modelID, model := range so.activeModels {
		model.RLock()
		
		// Record performance sample
		if history, exists := so.servingHistory[modelID]; exists {
			sample := PerformanceSample{
				Timestamp:   now,
				Latency:     model.CurrentLatency,
				Throughput:  model.CurrentThroughput,
				CPUUsage:    model.CPUUsage,
				MemoryUsage: model.MemoryUsage,
			}
			
			history.PerformanceMetrics = append(history.PerformanceMetrics, sample)
			
			// Keep only recent samples
			if len(history.PerformanceMetrics) > 1000 {
				history.PerformanceMetrics = history.PerformanceMetrics[len(history.PerformanceMetrics)-500:]
			}
		}
		
		// Update hot/cold file lists
		so.updateHotColdFiles(model)
		
		model.RUnlock()
	}
}

// updateHotColdFiles updates the hot and cold file lists for a model
func (so *ServingOptimizer) updateHotColdFiles(model *ModelServingInfo) {
	// Sort files by access frequency
	type fileAccess struct {
		path  string
		count int64
	}
	
	accesses := make([]fileAccess, 0, len(model.AccessFrequency))
	for path, count := range model.AccessFrequency {
		accesses = append(accesses, fileAccess{path: path, count: count})
	}
	
	sort.Slice(accesses, func(i, j int) bool {
		return accesses[i].count > accesses[j].count
	})
	
	// Top 20% are hot files
	hotCount := len(accesses) / 5
	if hotCount == 0 && len(accesses) > 0 {
		hotCount = 1
	}
	
	model.HotFiles = make([]string, 0, hotCount)
	model.ColdFiles = make([]string, 0)
	
	for i, access := range accesses {
		if i < hotCount {
			model.HotFiles = append(model.HotFiles, access.path)
		} else {
			model.ColdFiles = append(model.ColdFiles, access.path)
		}
	}
}

// matchesPattern checks if a path matches a pattern
func (so *ServingOptimizer) matchesPattern(path, pattern string) bool {
	if pattern == "*" {
		return true
	}
	
	// Simple pattern matching - could be enhanced with proper glob matching
	patterns := strings.Split(pattern, ",")
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if strings.HasSuffix(path, strings.TrimPrefix(p, "*")) {
			return true
		}
	}
	
	return false
}

// evaluateCondition evaluates an optimization condition
func (so *ServingOptimizer) evaluateCondition(model *ModelServingInfo, condition string) bool {
	// Simple condition evaluation - in production, this could use a proper expression parser
	model.RLock()
	defer model.RUnlock()
	
	if strings.Contains(condition, "access_frequency >") {
		// Check if model is accessed frequently
		return model.RequestCount > 10
	}
	
	if strings.Contains(condition, "avg_latency > target_latency") {
		// Check latency SLA
		return model.CurrentLatency > model.TargetLatency
	}
	
	if strings.Contains(condition, "cache_hit_rate <") {
		// Check cache effectiveness
		return model.CacheHitRate < 0.3
	}
	
	if strings.Contains(condition, "load_time >") {
		// Check model load time
		return model.LoadTime > 10*time.Second
	}
	
	return false
}

// executeOptimizationAction executes an optimization action
func (so *ServingOptimizer) executeOptimizationAction(modelID string, rule *ServingOptimizationRule) {
	switch rule.Action {
	case "preload":
		so.preloadModel(modelID, rule.Parameters)
	case "scale_up":
		so.scaleUpModel(modelID, rule.Parameters)
	case "enable_result_caching":
		so.enableResultCaching(modelID, rule.Parameters)
	case "convert_model_format":
		so.convertModelFormat(modelID, rule.Parameters)
	default:
		glog.V(3).Infof("Unknown serving optimization action: %s", rule.Action)
	}
	
	glog.V(2).Infof("Executed serving optimization: %s -> %s for model %s", rule.Name, rule.Action, modelID)
}

// preloadModel marks a model for preloading
func (so *ServingOptimizer) preloadModel(modelID string, params map[string]interface{}) {
	glog.V(2).Infof("Preloading model %s due to access pattern", modelID)
	// Implementation would coordinate with model serving framework
}

// scaleUpModel triggers scaling up of model replicas
func (so *ServingOptimizer) scaleUpModel(modelID string, params map[string]interface{}) {
	if model, exists := so.activeModels[modelID]; exists {
		scaleFactor := 1.5
		if sf, ok := params["scale_factor"].(float64); ok {
			scaleFactor = sf
		}
		
		model.Lock()
		oldReplicas := model.MaxReplicas
		model.MaxReplicas = int(float64(model.MaxReplicas) * scaleFactor)
		model.Unlock()
		
		// Record scaling event
		if history, exists := so.servingHistory[modelID]; exists {
			event := ScalingEvent{
				Timestamp:   time.Now(),
				Action:      "scale_up",
				Reason:      "latency_sla_breach",
				OldReplicas: oldReplicas,
				NewReplicas: model.MaxReplicas,
			}
			history.ScalingEvents = append(history.ScalingEvents, event)
		}
		
		glog.V(2).Infof("Scaled up model %s from %d to %d replicas", modelID, oldReplicas, model.MaxReplicas)
	}
}

// enableResultCaching enables result caching for a model
func (so *ServingOptimizer) enableResultCaching(modelID string, params map[string]interface{}) {
	glog.V(2).Infof("Enabling result caching for model %s", modelID)
	so.cachingStrategy.ResultCaching = true
}

// convertModelFormat suggests converting model to optimized format
func (so *ServingOptimizer) convertModelFormat(modelID string, params map[string]interface{}) {
	targetFormat := "tensorrt"
	if tf, ok := params["target_format"].(string); ok {
		targetFormat = tf
	}
	
	glog.V(2).Infof("Recommending model format conversion: %s -> %s", modelID, targetFormat)
}

// cleanupHistoricalData cleans up old historical data
func (so *ServingOptimizer) cleanupHistoricalData() {
	cutoffTime := time.Now().Add(-24 * time.Hour) // Keep last 24 hours
	
	for _, history := range so.servingHistory {
		// Clean up old access patterns
		filteredPatterns := make([]AccessPatternSample, 0)
		for _, pattern := range history.AccessPatterns {
			if pattern.Timestamp.After(cutoffTime) {
				filteredPatterns = append(filteredPatterns, pattern)
			}
		}
		history.AccessPatterns = filteredPatterns
		
		// Clean up old performance metrics
		filteredMetrics := make([]PerformanceSample, 0)
		for _, metric := range history.PerformanceMetrics {
			if metric.Timestamp.After(cutoffTime) {
				filteredMetrics = append(filteredMetrics, metric)
			}
		}
		history.PerformanceMetrics = filteredMetrics
	}
}

// GetServingMetrics returns comprehensive serving metrics
func (so *ServingOptimizer) GetServingMetrics() ServingOptimizerMetrics {
	so.RLock()
	defer so.RUnlock()
	
	metrics := ServingOptimizerMetrics{
		ActiveModels:        int64(len(so.activeModels)),
		TotalRequests:       so.totalRequests,
		CachedRequests:      so.cachedRequests,
		OptimizationEvents:  so.optimizationEvents,
		AvgLatency:         so.calculateAverageLatency(),
		AvgThroughput:      so.calculateAverageThroughput(),
		CacheHitRate:       so.calculateCacheHitRate(),
		ModelsByPattern:    make(map[ServingPattern]int64),
	}
	
	// Count models by serving pattern
	for _, model := range so.activeModels {
		model.RLock()
		metrics.ModelsByPattern[model.ServingPattern]++
		model.RUnlock()
	}
	
	return metrics
}

// ServingOptimizerMetrics holds metrics for serving optimization
type ServingOptimizerMetrics struct {
	ActiveModels       int64                           `json:"active_models"`
	TotalRequests      int64                           `json:"total_requests"`
	CachedRequests     int64                           `json:"cached_requests"`
	OptimizationEvents int64                           `json:"optimization_events"`
	AvgLatency         time.Duration                   `json:"avg_latency"`
	AvgThroughput      float64                         `json:"avg_throughput"`
	CacheHitRate       float64                         `json:"cache_hit_rate"`
	ModelsByPattern    map[ServingPattern]int64        `json:"models_by_pattern"`
}

// Helper functions for metrics calculation

func (so *ServingOptimizer) calculateAverageLatency() time.Duration {
	totalLatency := time.Duration(0)
	count := 0
	
	for _, model := range so.activeModels {
		model.RLock()
		if model.CurrentLatency > 0 {
			totalLatency += model.CurrentLatency
			count++
		}
		model.RUnlock()
	}
	
	if count == 0 {
		return 0
	}
	
	return totalLatency / time.Duration(count)
}

func (so *ServingOptimizer) calculateAverageThroughput() float64 {
	totalThroughput := 0.0
	count := 0
	
	for _, model := range so.activeModels {
		model.RLock()
		if model.CurrentThroughput > 0 {
			totalThroughput += model.CurrentThroughput
			count++
		}
		model.RUnlock()
	}
	
	if count == 0 {
		return 0
	}
	
	return totalThroughput / float64(count)
}

func (so *ServingOptimizer) calculateCacheHitRate() float64 {
	if so.totalRequests == 0 {
		return 0
	}
	
	return float64(so.cachedRequests) / float64(so.totalRequests)
}

// Shutdown gracefully shuts down the serving optimizer
func (so *ServingOptimizer) Shutdown() {
	if so.cancel != nil {
		so.cancel()
	}
	
	glog.V(1).Infof("Serving optimizer shutdown complete")
}

// String methods for enums

func (sp ServingPattern) String() string {
	switch sp {
	case ServingPatternBatchInference:
		return "BatchInference"
	case ServingPatternRealtimeInference:
		return "RealtimeInference"
	case ServingPatternStreamingInference:
		return "StreamingInference"
	case ServingPatternMultiModalServing:
		return "MultiModalServing"
	case ServingPatternEnsembleServing:
		return "EnsembleServing"
	case ServingPatternA_BServing:
		return "A_BServing"
	case ServingPatternCanaryServing:
		return "CanaryServing"
	case ServingPatternAutoScalingServing:
		return "AutoScalingServing"
	default:
		return "Unknown"
	}
}
