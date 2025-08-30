package ml

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// TrainingPhase represents different phases of ML training
type TrainingPhase int

const (
	PhaseUnknown TrainingPhase = iota
	PhaseInitialization      // Model initialization and warmup
	PhaseTraining           // Active training phase
	PhaseValidation        // Validation phase
	PhaseSaveCheckpoint    // Saving model checkpoints
	PhaseEvaluation       // Model evaluation
	PhaseInference        // Inference/prediction phase
	PhaseHyperparamTuning // Hyperparameter tuning
)

// TrainingWorkloadInfo tracks information about a training workload
type TrainingWorkloadInfo struct {
	sync.RWMutex
	
	// Workload identification
	WorkloadID        string               // Unique identifier for this training session
	StartTime         time.Time            // When training started
	CurrentPhase      TrainingPhase        // Current training phase
	PhaseStartTime    time.Time            // When current phase started
	
	// Dataset information
	TrainingDatasets  map[uint64]*DatasetTraversalInfo // Training datasets by inode
	ValidationDatasets map[uint64]*DatasetTraversalInfo // Validation datasets by inode
	
	// Model information
	ModelFiles        map[uint64]*ModelFileInfo         // Model files by inode
	CheckpointFreq    time.Duration                     // How often checkpoints are saved
	LastCheckpoint    time.Time                         // When last checkpoint was saved
	
	// Training statistics
	EpochsCompleted   int                               // Number of training epochs completed
	BatchesProcessed  int64                             // Total batches processed
	CurrentLearningRate float64                         // Current learning rate
	LossHistory       []float64                         // Recent loss values
	
	// Performance metrics
	BatchProcessingTime time.Duration                   // Average time per batch
	IOWaitTime        time.Duration                     // Time waiting for I/O
	ComputeTime       time.Duration                     // Time spent computing
	ThroughputItems   float64                           // Items processed per second
	
	// Optimization state
	OptimizationLevel OptimizationLevel                 // Current optimization level
	PrefetchStrategy  PrefetchStrategy                  // Current prefetching strategy
	CachePolicy       CachePolicy                       // Current caching policy
}

// ModelFileInfo tracks information about model files
type ModelFileInfo struct {
	sync.RWMutex
	
	FileType          ModelFileType        // Type of model file
	Size              int64                // File size
	LastModified      time.Time            // Last modification time
	AccessPattern     AccessPattern        // How the file is accessed
	IsCheckpoint      bool                 // Whether this is a checkpoint file
	CheckpointEpoch   int                  // Epoch number if checkpoint
	LoadFrequency     time.Duration        // How often file is loaded
	SaveFrequency     time.Duration        // How often file is saved
}

// ModelFileType represents different types of model files
type ModelFileType int

const (
	ModelFileUnknown ModelFileType = iota
	ModelWeights                   // Model weights/parameters
	ModelArchitecture             // Model architecture definition
	ModelOptimizer                // Optimizer state
	ModelCheckpoint               // Full model checkpoint
	ModelMetadata                 // Model metadata
)

// OptimizationLevel represents different levels of ML optimization
type OptimizationLevel int

const (
	OptimizationBasic OptimizationLevel = iota
	OptimizationBalanced
	OptimizationAggressive
	OptimizationMaximum
)

// PrefetchStrategy represents different prefetching strategies for training
type PrefetchStrategy int

const (
	PrefetchConservative PrefetchStrategy = iota
	PrefetchBalanced
	PrefetchAggressive
	PrefetchAdaptive
)

// CachePolicy represents different caching policies for training data
type CachePolicy int

const (
	CachePolicyNone CachePolicy = iota
	CachePolicyLRU
	CachePolicyTrainingAware
	CachePolicyML
)

// TrainingOptimizer optimizes file access patterns for ML training workloads
type TrainingOptimizer struct {
	sync.RWMutex
	
	// Configuration
	maxWorkloads       int                              // Maximum concurrent workloads to track
	phaseDetectionWindowSize int                        // Number of accesses to analyze for phase detection
	
	// Active workloads
	workloads          map[string]*TrainingWorkloadInfo // workload ID -> info
	inodeToWorkload    map[uint64]string                // inode -> workload ID mapping
	
	// Pattern detection
	datasetDetector    *DatasetPatternDetector          // Dataset pattern detector
	
	// Optimization policies
	defaultOptLevel    OptimizationLevel                // Default optimization level
	adaptiveOptimization bool                           // Whether to automatically adjust optimization
	
	// Statistics
	totalWorkloads     int64                            // Total workloads seen
	activeWorkloads    int64                            // Currently active workloads
	optimizationEvents int64                            // Number of optimization events
}

// NewTrainingOptimizer creates a new training optimizer
func NewTrainingOptimizer(datasetDetector *DatasetPatternDetector) *TrainingOptimizer {
	return &TrainingOptimizer{
		maxWorkloads:             10,   // Track up to 10 concurrent training workloads
		phaseDetectionWindowSize: 100,  // Analyze last 100 accesses for phase detection
		
		workloads:               make(map[string]*TrainingWorkloadInfo),
		inodeToWorkload:         make(map[uint64]string),
		datasetDetector:         datasetDetector,
		
		defaultOptLevel:         OptimizationBalanced,
		adaptiveOptimization:    true,
	}
}

// RegisterTrainingWorkload registers a new training workload
func (to *TrainingOptimizer) RegisterTrainingWorkload(workloadID string) *TrainingWorkloadInfo {
	to.Lock()
	defer to.Unlock()
	
	workload := &TrainingWorkloadInfo{
		WorkloadID:           workloadID,
		StartTime:            time.Now(),
		CurrentPhase:         PhaseInitialization,
		PhaseStartTime:       time.Now(),
		TrainingDatasets:     make(map[uint64]*DatasetTraversalInfo),
		ValidationDatasets:   make(map[uint64]*DatasetTraversalInfo),
		ModelFiles:           make(map[uint64]*ModelFileInfo),
		CheckpointFreq:       30 * time.Minute, // Default checkpoint frequency
		OptimizationLevel:    to.defaultOptLevel,
		PrefetchStrategy:     PrefetchBalanced,
		CachePolicy:          CachePolicyTrainingAware,
		LossHistory:          make([]float64, 0, 100),
	}
	
	to.workloads[workloadID] = workload
	to.totalWorkloads++
	to.activeWorkloads++
	
	glog.V(1).Infof("Registered training workload: %s", workloadID)
	return workload
}

// RecordFileAccess records a file access and associates it with training workload
func (to *TrainingOptimizer) RecordFileAccess(inode uint64, fileType MLFileType, offset int64, size int, isRead bool) {
	to.RLock()
	workloadID := to.inodeToWorkload[inode]
	to.RUnlock()
	
	if workloadID == "" {
		// Try to detect workload based on file access patterns
		workloadID = to.detectWorkloadFromAccess(inode, fileType, offset, size)
	}
	
	if workloadID == "" {
		return // No associated workload
	}
	
	to.RLock()
	workload := to.workloads[workloadID]
	to.RUnlock()
	
	if workload == nil {
		return
	}
	
	workload.Lock()
	defer workload.Unlock()
	
	// Update workload statistics based on file type
	switch fileType {
	case MLFileDataset:
		to.handleDatasetAccess(workload, inode, offset, size, isRead)
	case MLFileModel:
		to.handleModelAccess(workload, inode, offset, size, isRead)
	default:
		// General file access
		to.handleGeneralAccess(workload, inode, offset, size, isRead)
	}
	
	// Detect training phase changes
	to.detectPhaseChange(workload)
	
	// Apply adaptive optimizations if enabled
	if to.adaptiveOptimization {
		to.applyAdaptiveOptimizations(workload)
	}
}

// detectWorkloadFromAccess attempts to detect which workload a file access belongs to
func (to *TrainingOptimizer) detectWorkloadFromAccess(inode uint64, fileType MLFileType, offset int64, size int) string {
	// Simple heuristic: assign to the most recently active workload
	// In a more sophisticated implementation, this could use process tracking,
	// directory structure analysis, or other heuristics
	
	to.RLock()
	defer to.RUnlock()
	
	var latestWorkloadID string
	latestTime := time.Time{}
	
	for workloadID, workload := range to.workloads {
		workload.RLock()
		if workload.PhaseStartTime.After(latestTime) {
			latestTime = workload.PhaseStartTime
			latestWorkloadID = workloadID
		}
		workload.RUnlock()
	}
	
	if latestWorkloadID != "" {
		to.Lock()
		to.inodeToWorkload[inode] = latestWorkloadID
		to.Unlock()
		
		glog.V(4).Infof("Associated inode %d with workload %s", inode, latestWorkloadID)
	}
	
	return latestWorkloadID
}

// handleDatasetAccess processes dataset file access
func (to *TrainingOptimizer) handleDatasetAccess(workload *TrainingWorkloadInfo, inode uint64, offset int64, size int, isRead bool) {
	if !isRead {
		return // Dataset files are typically read-only during training
	}
	
	// Use dataset pattern detector to analyze access
	if to.datasetDetector != nil {
		datasetInfo := to.datasetDetector.RecordDatasetAccess(inode, offset, size, 0, false)
		if datasetInfo != nil {
			// Store dataset info in workload
			if datasetInfo.ValidationAccess {
				workload.ValidationDatasets[inode] = datasetInfo
			} else {
				workload.TrainingDatasets[inode] = datasetInfo
			}
			
			// Update workload metrics
			if datasetInfo.EpochCount > workload.EpochsCompleted {
				workload.EpochsCompleted = datasetInfo.EpochCount
			}
			
			if datasetInfo.ItemsPerSecond > 0 {
				workload.ThroughputItems = datasetInfo.ItemsPerSecond
			}
		}
	}
	
	workload.BatchesProcessed++
}

// handleModelAccess processes model file access
func (to *TrainingOptimizer) handleModelAccess(workload *TrainingWorkloadInfo, inode uint64, offset int64, size int, isRead bool) {
	modelInfo := workload.ModelFiles[inode]
	if modelInfo == nil {
		modelInfo = &ModelFileInfo{
			FileType:     to.detectModelFileType(inode, offset, size, isRead),
			Size:         int64(size),
			LastModified: time.Now(),
		}
		workload.ModelFiles[inode] = modelInfo
	}
	
	modelInfo.Lock()
	defer modelInfo.Unlock()
	
	now := time.Now()
	
	if isRead {
		// Model loading
		if modelInfo.LoadFrequency == 0 {
			modelInfo.LoadFrequency = now.Sub(modelInfo.LastModified)
		} else {
			// Running average
			freq := now.Sub(modelInfo.LastModified)
			modelInfo.LoadFrequency = (modelInfo.LoadFrequency + freq) / 2
		}
	} else {
		// Model saving (checkpoint)
		if modelInfo.SaveFrequency == 0 {
			modelInfo.SaveFrequency = now.Sub(modelInfo.LastModified)
		} else {
			freq := now.Sub(modelInfo.LastModified)
			modelInfo.SaveFrequency = (modelInfo.SaveFrequency + freq) / 2
		}
		
		// Update checkpoint information
		if modelInfo.IsCheckpoint {
			workload.LastCheckpoint = now
			if modelInfo.SaveFrequency > 0 {
				workload.CheckpointFreq = modelInfo.SaveFrequency
			}
		}
	}
	
	modelInfo.LastModified = now
}

// handleGeneralAccess processes general file access
func (to *TrainingOptimizer) handleGeneralAccess(workload *TrainingWorkloadInfo, inode uint64, offset int64, size int, isRead bool) {
	// For config files, logs, etc.
	// This can be extended with specific handling for different file types
}

// detectModelFileType attempts to determine the type of model file
func (to *TrainingOptimizer) detectModelFileType(inode uint64, offset int64, size int, isRead bool) ModelFileType {
	// Simple heuristics based on access patterns
	// This could be enhanced with filename analysis, content analysis, etc.
	
	if size > 100*1024*1024 { // Large files likely to be model weights or checkpoints
		if isRead {
			return ModelWeights
		} else {
			return ModelCheckpoint
		}
	}
	
	if size < 1024 { // Small files likely to be metadata or config
		return ModelMetadata
	}
	
	return ModelFileUnknown
}

// detectPhaseChange detects changes in training phase
func (to *TrainingOptimizer) detectPhaseChange(workload *TrainingWorkloadInfo) {
	now := time.Now()
	currentPhase := workload.CurrentPhase
	
	// Simple phase detection heuristics
	// In practice, this could be much more sophisticated
	
	timeSincePhaseStart := now.Sub(workload.PhaseStartTime)
	
	switch currentPhase {
	case PhaseInitialization:
		// Transition to training after initial period
		if timeSincePhaseStart > 5*time.Minute && workload.BatchesProcessed > 10 {
			to.transitionPhase(workload, PhaseTraining)
		}
		
	case PhaseTraining:
		// Look for validation phase indicators
		hasValidationActivity := len(workload.ValidationDatasets) > 0
		for _, datasetInfo := range workload.ValidationDatasets {
			datasetInfo.RLock()
			recentActivity := now.Sub(datasetInfo.LastEpochStart) < 10*time.Minute
			datasetInfo.RUnlock()
			if recentActivity {
				hasValidationActivity = true
				break
			}
		}
		
		if hasValidationActivity {
			to.transitionPhase(workload, PhaseValidation)
		}
		
		// Check for checkpoint saving
		if now.Sub(workload.LastCheckpoint) < 5*time.Minute {
			to.transitionPhase(workload, PhaseSaveCheckpoint)
		}
		
	case PhaseValidation:
		// Return to training after validation
		if timeSincePhaseStart > 2*time.Minute {
			to.transitionPhase(workload, PhaseTraining)
		}
		
	case PhaseSaveCheckpoint:
		// Return to training after checkpoint
		if timeSincePhaseStart > 1*time.Minute {
			to.transitionPhase(workload, PhaseTraining)
		}
	}
}

// transitionPhase transitions workload to a new training phase
func (to *TrainingOptimizer) transitionPhase(workload *TrainingWorkloadInfo, newPhase TrainingPhase) {
	oldPhase := workload.CurrentPhase
	workload.CurrentPhase = newPhase
	workload.PhaseStartTime = time.Now()
	
	glog.V(2).Infof("Training phase transition: workload=%s, %v -> %v", 
		workload.WorkloadID, oldPhase, newPhase)
}

// applyAdaptiveOptimizations applies optimizations based on current workload state
func (to *TrainingOptimizer) applyAdaptiveOptimizations(workload *TrainingWorkloadInfo) {
	// Adjust optimization level based on training phase and performance
	switch workload.CurrentPhase {
	case PhaseInitialization:
		// Conservative during initialization
		workload.OptimizationLevel = OptimizationBasic
		workload.PrefetchStrategy = PrefetchConservative
		
	case PhaseTraining:
		// Aggressive optimization during training
		workload.OptimizationLevel = OptimizationAggressive
		workload.PrefetchStrategy = PrefetchAggressive
		
		// If throughput is low, try maximum optimization
		if workload.ThroughputItems > 0 && workload.ThroughputItems < 10 {
			workload.OptimizationLevel = OptimizationMaximum
			workload.PrefetchStrategy = PrefetchAdaptive
		}
		
	case PhaseValidation:
		// Balanced optimization for validation
		workload.OptimizationLevel = OptimizationBalanced
		workload.PrefetchStrategy = PrefetchBalanced
		
	case PhaseSaveCheckpoint:
		// Focus on write optimization during checkpoints
		workload.CachePolicy = CachePolicyML
		workload.PrefetchStrategy = PrefetchConservative
	}
	
	to.optimizationEvents++
}

// GetWorkloadInfo returns information about a training workload
func (to *TrainingOptimizer) GetWorkloadInfo(workloadID string) *TrainingWorkloadInfo {
	to.RLock()
	defer to.RUnlock()
	
	return to.workloads[workloadID]
}

// GetRecommendations returns optimization recommendations for a file
func (to *TrainingOptimizer) GetRecommendations(inode uint64) *OptimizationRecommendations {
	to.RLock()
	workloadID := to.inodeToWorkload[inode]
	workload := to.workloads[workloadID]
	to.RUnlock()
	
	if workload == nil {
		return &OptimizationRecommendations{}
	}
	
	workload.RLock()
	defer workload.RUnlock()
	
	recommendations := &OptimizationRecommendations{
		PrefetchSize:      64 * 1024,        // Default 64KB
		ShouldCache:       true,
		CachePriority:     CachePriorityNormal,
		OptimizationLevel: workload.OptimizationLevel,
	}
	
	// Adjust recommendations based on file type and training phase
	switch workload.CurrentPhase {
	case PhaseTraining:
		// Aggressive prefetching for training data
		recommendations.PrefetchSize = 1024 * 1024 // 1MB
		recommendations.ShouldCache = true
		recommendations.CachePriority = CachePriorityHigh
		
	case PhaseValidation:
		// Conservative prefetching for validation
		recommendations.PrefetchSize = 256 * 1024 // 256KB
		recommendations.ShouldCache = true
		recommendations.CachePriority = CachePriorityNormal
		
	case PhaseSaveCheckpoint:
		// Focus on write performance
		recommendations.PrefetchSize = 0 // No prefetching during writes
		recommendations.ShouldCache = false
		recommendations.CachePriority = CachePriorityLow
	}
	
	// Check if this is a dataset file with specific patterns
	if datasetInfo := workload.TrainingDatasets[inode]; datasetInfo != nil {
		datasetInfo.RLock()
		if datasetInfo.OptimalPrefetchSize > 0 {
			recommendations.PrefetchSize = int(datasetInfo.OptimalPrefetchSize)
		}
		recommendations.ShouldCache = datasetInfo.ShouldCache
		datasetInfo.RUnlock()
	}
	
	return recommendations
}

// OptimizationRecommendations holds recommendations for file access optimization
type OptimizationRecommendations struct {
	PrefetchSize      int                    `json:"prefetch_size"`
	ShouldCache       bool                   `json:"should_cache"`
	CachePriority     CachePriority          `json:"cache_priority"`
	OptimizationLevel OptimizationLevel      `json:"optimization_level"`
}

// CachePriority represents priority levels for caching
type CachePriority int

const (
	CachePriorityLow CachePriority = iota
	CachePriorityNormal
	CachePriorityHigh
	CachePriorityUrgent
)

// GetTrainingMetrics returns comprehensive training optimization metrics
func (to *TrainingOptimizer) GetTrainingMetrics() TrainingOptimizerMetrics {
	to.RLock()
	defer to.RUnlock()
	
	metrics := TrainingOptimizerMetrics{
		TotalWorkloads:      to.totalWorkloads,
		ActiveWorkloads:     to.activeWorkloads,
		OptimizationEvents:  to.optimizationEvents,
		WorkloadPhases:      make(map[TrainingPhase]int64),
	}
	
	// Aggregate workload statistics
	for _, workload := range to.workloads {
		workload.RLock()
		metrics.WorkloadPhases[workload.CurrentPhase]++
		metrics.TotalEpochs += int64(workload.EpochsCompleted)
		metrics.TotalBatches += workload.BatchesProcessed
		workload.RUnlock()
	}
	
	return metrics
}

// TrainingOptimizerMetrics holds metrics for training optimization
type TrainingOptimizerMetrics struct {
	TotalWorkloads     int64                     `json:"total_workloads"`
	ActiveWorkloads    int64                     `json:"active_workloads"`
	TotalEpochs        int64                     `json:"total_epochs"`
	TotalBatches       int64                     `json:"total_batches"`
	OptimizationEvents int64                     `json:"optimization_events"`
	WorkloadPhases     map[TrainingPhase]int64   `json:"workload_phases"`
}

// String methods for enums

func (tp TrainingPhase) String() string {
	switch tp {
	case PhaseInitialization:
		return "Initialization"
	case PhaseTraining:
		return "Training"
	case PhaseValidation:
		return "Validation"
	case PhaseSaveCheckpoint:
		return "SaveCheckpoint"
	case PhaseEvaluation:
		return "Evaluation"
	case PhaseInference:
		return "Inference"
	case PhaseHyperparamTuning:
		return "HyperparamTuning"
	default:
		return "Unknown"
	}
}

func (mft ModelFileType) String() string {
	switch mft {
	case ModelWeights:
		return "Weights"
	case ModelArchitecture:
		return "Architecture"
	case ModelOptimizer:
		return "Optimizer"
	case ModelCheckpoint:
		return "Checkpoint"
	case ModelMetadata:
		return "Metadata"
	default:
		return "Unknown"
	}
}

func (ol OptimizationLevel) String() string {
	switch ol {
	case OptimizationBasic:
		return "Basic"
	case OptimizationBalanced:
		return "Balanced"
	case OptimizationAggressive:
		return "Aggressive"
	case OptimizationMaximum:
		return "Maximum"
	default:
		return "Basic"
	}
}

func (ps PrefetchStrategy) String() string {
	switch ps {
	case PrefetchConservative:
		return "Conservative"
	case PrefetchBalanced:
		return "Balanced"
	case PrefetchAggressive:
		return "Aggressive"
	case PrefetchAdaptive:
		return "Adaptive"
	default:
		return "Conservative"
	}
}
