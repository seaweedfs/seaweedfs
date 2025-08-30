package ml

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// MLOptimization provides ML-aware optimizations for FUSE mounting
type MLOptimization struct {
	// Core optimization components
	ReaderCache         *MLReaderCache
	PrefetchManager     *PrefetchManager
	PatternDetector     *AccessPatternDetector
	
	// New flexible optimization system
	OptimizationEngine  *OptimizationEngine
	ConfigManager       *OptimizationConfigManager
	
	// Legacy components (kept for backward compatibility)
	DatasetDetector     *DatasetPatternDetector
	TrainingOptimizer   *TrainingOptimizer
	BatchOptimizer      *BatchOptimizer
	WorkloadCoordinator *WorkloadCoordinator
	GPUCoordinator      *GPUCoordinator
	DistributedCoordinator *DistributedCoordinator
	ServingOptimizer    *ServingOptimizer
	TensorOptimizer     *TensorOptimizer
	
	enabled             bool
	useOptimizationEngine bool
}

// MLConfig holds configuration for ML optimizations
type MLConfig struct {
	// Prefetch configuration
	PrefetchWorkers   int           // Number of prefetch workers
	PrefetchQueueSize int           // Size of prefetch queue
	PrefetchTimeout   time.Duration // Timeout for prefetch operations

	// Pattern detection configuration
	EnableMLHeuristics    bool    // Enable ML-specific pattern detection
	SequentialThreshold   int     // Minimum consecutive reads for sequential detection
	ConfidenceThreshold   float64 // Minimum confidence to trigger prefetch

	// Cache configuration
	MaxPrefetchAhead  int // Maximum chunks to prefetch ahead
	PrefetchBatchSize int // Number of chunks to prefetch in one batch
	
	// Advanced Phase 4 configuration (Legacy)
	EnableWorkloadCoordination bool // Enable cross-process workload coordination
	EnableGPUCoordination     bool // Enable GPU memory coordination
	EnableDistributedTraining bool // Enable distributed training optimizations
	EnableModelServing        bool // Enable model serving optimizations
	EnableTensorOptimization  bool // Enable tensor file optimizations
	
	// New optimization engine configuration
	UseOptimizationEngine     bool   // Use new flexible optimization engine
	ConfigurationPath         string // Path to optimization configuration files
	EnableAdaptiveLearning    bool   // Enable adaptive learning from usage patterns
	EnablePluginSystem        bool   // Enable plugin system for frameworks
}

// DefaultMLConfig returns default configuration optimized for ML workloads
func DefaultMLConfig() *MLConfig {
	return &MLConfig{
		// Prefetch settings
		PrefetchWorkers:   8,
		PrefetchQueueSize: 100,
		PrefetchTimeout:   30 * time.Second,

		// Pattern detection settings
		EnableMLHeuristics:  true,
		SequentialThreshold: 3,
		ConfidenceThreshold: 0.6,

		// Cache settings
		MaxPrefetchAhead:  8,
		PrefetchBatchSize: 3,
		
		// Advanced Phase 4 features (disabled by default for stability)
		EnableWorkloadCoordination: false,
		EnableGPUCoordination:     false,
		EnableDistributedTraining: false,
		EnableModelServing:        false,
		EnableTensorOptimization:  false,
		
		// New optimization engine (enabled by default for flexibility)
		UseOptimizationEngine:     true,
		ConfigurationPath:         "",    // Use built-in configuration
		EnableAdaptiveLearning:    true,
		EnablePluginSystem:        true,
	}
}

// NewMLOptimization creates a new ML optimization instance
func NewMLOptimization(config *MLConfig, chunkCache chunk_cache.ChunkCache, lookupFn wdclient.LookupFileIdFunctionType) *MLOptimization {
	if config == nil {
		config = DefaultMLConfig()
	}

	// Create dataset pattern detector
	datasetDetector := NewDatasetPatternDetector()

	// Create training optimizer
	trainingOptimizer := NewTrainingOptimizer(datasetDetector)

	// Create batch optimizer
	batchOptimizer := NewBatchOptimizer()

	// Create ML reader cache with embedded prefetch manager and pattern detector
	mlReaderCache := NewMLReaderCache(10, chunkCache, lookupFn)

	// Configure the ML reader cache with provided settings
	mlReaderCache.SetPrefetchConfiguration(config.MaxPrefetchAhead, config.PrefetchBatchSize)

	opt := &MLOptimization{
		ReaderCache:         mlReaderCache,
		PrefetchManager:     mlReaderCache.prefetchManager,
		PatternDetector:     mlReaderCache.patternDetector,
		DatasetDetector:     datasetDetector,
		TrainingOptimizer:   trainingOptimizer,
		BatchOptimizer:      batchOptimizer,
		enabled:             true,
		useOptimizationEngine: config.UseOptimizationEngine,
	}
	
	// Initialize new optimization engine if enabled
	if config.UseOptimizationEngine {
		// Create optimization engine
		opt.OptimizationEngine = NewOptimizationEngine(true)
		
		// Create configuration manager
		configPath := config.ConfigurationPath
		if configPath == "" {
			configPath = "/tmp/ml_optimization_configs" // Default path
		}
		opt.ConfigManager = NewOptimizationConfigManager(configPath)
		
		// Register built-in plugins if enabled
		if config.EnablePluginSystem {
			// Import and register plugins - would be done dynamically in real implementation
			opt.initializeBuiltinPlugins()
		}
		
		// Load configuration
		if err := opt.loadOptimizationConfiguration(config); err != nil {
			glog.Warningf("Failed to load optimization configuration: %v", err)
		}
		
		glog.V(1).Infof("Optimization engine initialized with adaptive learning: %v", 
			config.EnableAdaptiveLearning)
	}

	// Initialize Phase 4 advanced components if enabled
	if config.EnableWorkloadCoordination {
		opt.WorkloadCoordinator = NewWorkloadCoordinator(true)
		glog.V(1).Infof("Workload coordinator enabled")
	}

	if config.EnableGPUCoordination {
		opt.GPUCoordinator = NewGPUCoordinator(true)
		glog.V(1).Infof("GPU coordinator enabled")
	}

	if config.EnableDistributedTraining {
		opt.DistributedCoordinator = NewDistributedCoordinator("ml-node-1", true)
		glog.V(1).Infof("Distributed training coordinator enabled")
	}

	if config.EnableModelServing {
		opt.ServingOptimizer = NewServingOptimizer(true)
		glog.V(1).Infof("Model serving optimizer enabled")
	}

	if config.EnableTensorOptimization {
		opt.TensorOptimizer = NewTensorOptimizer(true)
		glog.V(1).Infof("Tensor optimizer enabled")
	}

	glog.V(1).Infof("ML optimization enabled with config: workers=%d, queue=%d, confidence=%.2f",
		config.PrefetchWorkers, config.PrefetchQueueSize, config.ConfidenceThreshold)

	return opt
}

// Enable enables or disables ML optimization
func (opt *MLOptimization) Enable(enabled bool) {
	opt.enabled = enabled
	if opt.ReaderCache != nil {
		opt.ReaderCache.EnableMLPrefetch(enabled)
	}
	glog.V(2).Infof("ML optimization %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// IsEnabled returns whether ML optimization is enabled
func (opt *MLOptimization) IsEnabled() bool {
	return opt.enabled
}

// GetMetrics returns comprehensive ML optimization metrics
func (opt *MLOptimization) GetMetrics() *MLOptimizationMetrics {
	if opt.ReaderCache == nil {
		return &MLOptimizationMetrics{}
	}
	
	mlMetrics := opt.ReaderCache.GetMLMetrics()
	
	return &MLOptimizationMetrics{
		Enabled:              opt.enabled,
		PrefetchHits:         mlMetrics.PrefetchHits,
		PrefetchMisses:       mlMetrics.PrefetchMisses,
		MLPrefetchTriggered:  mlMetrics.MLPrefetchTriggered,
		TotalAccesses:        mlMetrics.PatternMetrics.TotalAccesses,
		SequentialReads:      mlMetrics.PatternMetrics.SequentialReads,
		RandomReads:          mlMetrics.PatternMetrics.RandomReads,
		PatternCounts:        mlMetrics.PatternMetrics.PatternCounts,
		ActivePrefetchJobs:   mlMetrics.PrefetchMetrics.ActiveJobs,
		PrefetchWorkers:      mlMetrics.PrefetchMetrics.Workers,
	}
}

// MLOptimizationMetrics holds comprehensive metrics for ML optimization
type MLOptimizationMetrics struct {
	Enabled              bool                     `json:"enabled"`
	PrefetchHits         int64                    `json:"prefetch_hits"`
	PrefetchMisses       int64                    `json:"prefetch_misses"`
	MLPrefetchTriggered  int64                    `json:"ml_prefetch_triggered"`
	TotalAccesses        int64                    `json:"total_accesses"`
	SequentialReads      int64                    `json:"sequential_reads"`
	RandomReads          int64                    `json:"random_reads"`
	PatternCounts        map[AccessPattern]int    `json:"pattern_counts"`
	ActivePrefetchJobs   int64                    `json:"active_prefetch_jobs"`
	PrefetchWorkers      int64                    `json:"prefetch_workers"`
}

// Shutdown gracefully shuts down all ML optimization components
func (opt *MLOptimization) Shutdown() {
	if opt.ReaderCache != nil {
		opt.ReaderCache.Shutdown()
	}

	if opt.DatasetDetector != nil {
		opt.DatasetDetector.Cleanup()
	}

	if opt.BatchOptimizer != nil {
		opt.BatchOptimizer.Shutdown()
	}

	// Shutdown Phase 4 components
	if opt.WorkloadCoordinator != nil {
		opt.WorkloadCoordinator.Shutdown()
	}

	if opt.GPUCoordinator != nil {
		opt.GPUCoordinator.Shutdown()
	}

	if opt.DistributedCoordinator != nil {
		opt.DistributedCoordinator.Shutdown()
	}

	if opt.ServingOptimizer != nil {
		opt.ServingOptimizer.Shutdown()
	}

	if opt.TensorOptimizer != nil {
		opt.TensorOptimizer.Shutdown()
	}

	// Shutdown new optimization engine
	if opt.OptimizationEngine != nil {
		opt.OptimizationEngine.Shutdown()
	}

	glog.V(1).Infof("ML optimization shutdown complete")
}

// initializeBuiltinPlugins initializes built-in optimization plugins
func (opt *MLOptimization) initializeBuiltinPlugins() {
	// Create and register PyTorch plugin
	pytorchPlugin := NewPyTorchPlugin()
	if err := opt.OptimizationEngine.RegisterPlugin(pytorchPlugin); err != nil {
		glog.Warningf("Failed to register PyTorch plugin: %v", err)
	}
	
	// Create and register TensorFlow plugin  
	tensorflowPlugin := NewTensorFlowPlugin()
	if err := opt.OptimizationEngine.RegisterPlugin(tensorflowPlugin); err != nil {
		glog.Warningf("Failed to register TensorFlow plugin: %v", err)
	}
	
	// Additional plugins would be registered here
	glog.V(1).Infof("Initialized %d built-in optimization plugins", 2)
}

// loadOptimizationConfiguration loads optimization configuration
func (opt *MLOptimization) loadOptimizationConfiguration(config *MLConfig) error {
	if config.ConfigurationPath != "" && config.ConfigurationPath != "/tmp/ml_optimization_configs" {
		// Load from specified path
		configs, err := opt.ConfigManager.LoadConfigurationDirectory(config.ConfigurationPath)
		if err != nil {
			return fmt.Errorf("failed to load configurations from %s: %w", config.ConfigurationPath, err)
		}
		
		// Apply configurations to engine
		for _, cfg := range configs {
			for _, rule := range cfg.Rules {
				opt.OptimizationEngine.rules[rule.ID] = rule
			}
			for _, template := range cfg.Templates {
				opt.OptimizationEngine.templates[template.ID] = template
			}
		}
		
		glog.V(1).Infof("Loaded %d optimization configurations", len(configs))
	} else {
		// Use default configuration
		defaultConfig := opt.ConfigManager.GenerateDefaultConfiguration()
		
		// Apply default configuration
		for _, rule := range defaultConfig.Rules {
			opt.OptimizationEngine.rules[rule.ID] = rule
		}
		for _, template := range defaultConfig.Templates {
			opt.OptimizationEngine.templates[template.ID] = template
		}
		
		glog.V(1).Infof("Loaded default optimization configuration")
	}
	
	return nil
}

// OptimizeFileAccess provides intelligent file access optimization using the new engine
func (opt *MLOptimization) OptimizeFileAccess(filePath string, accessPattern AccessPattern, 
	workloadType string, fileSize int64) *OptimizationResult {
	
	if !opt.enabled || !opt.useOptimizationEngine || opt.OptimizationEngine == nil {
		return &OptimizationResult{Applied: false}
	}
	
	// Create optimization context
	context := &OptimizationContext{
		FilePath:      filePath,
		FileSize:      fileSize,
		AccessPattern: accessPattern,
		WorkloadType:  workloadType,
		// Add more context fields as needed
	}
	
	// Get optimization recommendations
	result := opt.OptimizationEngine.OptimizeAccess(context)
	
	return result
}

// NewPyTorchPlugin creates a PyTorch optimization plugin
func NewPyTorchPlugin() OptimizationPlugin {
	return &BasicMLPlugin{
		frameworkName: "pytorch",
		extensions:    []string{".pth", ".pt"},
		patterns:      []string{"torch", "pytorch"},
	}
}

// NewTensorFlowPlugin creates a TensorFlow optimization plugin
func NewTensorFlowPlugin() OptimizationPlugin {
	return &BasicMLPlugin{
		frameworkName: "tensorflow", 
		extensions:    []string{".pb", ".h5", ".ckpt", ".tfrecord"},
		patterns:      []string{"tensorflow", "keras", "savedmodel"},
	}
}

// BasicMLPlugin provides a simple plugin implementation
type BasicMLPlugin struct {
	frameworkName string
	extensions    []string
	patterns      []string
}

func (p *BasicMLPlugin) GetFrameworkName() string {
	return p.frameworkName
}

func (p *BasicMLPlugin) DetectFramework(filePath string, content []byte) float64 {
	// Simple detection based on file extensions and patterns
	for _, ext := range p.extensions {
		if strings.HasSuffix(strings.ToLower(filePath), ext) {
			return 0.8
		}
	}
	
	lowerPath := strings.ToLower(filePath)
	for _, pattern := range p.patterns {
		if strings.Contains(lowerPath, pattern) {
			return 0.6
		}
	}
	
	return 0.0
}

func (p *BasicMLPlugin) GetOptimizationHints(context *OptimizationContext) []OptimizationHint {
	return []OptimizationHint{
		{
			Type:        "framework_hint",
			Description: fmt.Sprintf("Detected %s framework", p.frameworkName),
			Priority:    50,
			Parameters: map[string]interface{}{
				"framework": p.frameworkName,
				"confidence": "medium",
			},
		},
	}
}

func (p *BasicMLPlugin) GetDefaultRules() []*OptimizationRule {
	return []*OptimizationRule{
		{
			ID:          fmt.Sprintf("%s_basic_optimization", p.frameworkName),
			Name:        fmt.Sprintf("%s Basic Optimization", strings.Title(p.frameworkName)),
			Description: fmt.Sprintf("Basic optimizations for %s files", p.frameworkName),
			Priority:    75,
			Conditions: []RuleCondition{
				{
					Type:     "workload_context",
					Property: "framework",
					Operator: "equals",
					Value:    p.frameworkName,
					Weight:   1.0,
				},
			},
			Actions: []RuleAction{
				{
					Type:   "cache",
					Target: "file",
					Parameters: map[string]interface{}{
						"strategy":   "framework_aware",
						"framework":  p.frameworkName,
						"priority":   "normal",
					},
				},
			},
		},
	}
}

func (p *BasicMLPlugin) GetDefaultTemplates() []*OptimizationTemplate {
	return []*OptimizationTemplate{
		{
			ID:          fmt.Sprintf("%s_default_template", p.frameworkName),
			Name:        fmt.Sprintf("%s Default Template", strings.Title(p.frameworkName)),
			Description: fmt.Sprintf("Default optimization template for %s", p.frameworkName),
			Category:    "framework_default",
			Rules:       []string{fmt.Sprintf("%s_basic_optimization", p.frameworkName)},
			Parameters: map[string]interface{}{
				"framework": p.frameworkName,
				"mode":      "balanced",
			},
		},
	}
}

// RecordAccess records a file access for pattern detection (convenience method)
func (opt *MLOptimization) RecordAccess(inode uint64, offset int64, size int) *AccessInfo {
	if !opt.enabled || opt.PatternDetector == nil {
		return nil
	}
	return opt.PatternDetector.RecordAccess(inode, offset, size)
}

// ShouldPrefetch determines if prefetching should be triggered (convenience method)
func (opt *MLOptimization) ShouldPrefetch(inode uint64) (bool, int64) {
	if !opt.enabled || opt.PatternDetector == nil {
		return false, 0
	}
	return opt.PatternDetector.ShouldPrefetch(inode)
}
