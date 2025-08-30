package ml

import (
	"math"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// CacheEntry represents a cached item with ML-aware metadata
type CacheEntry struct {
	Inode       uint64        // File inode
	Size        uint64        // Size of cached data
	LastAccess  time.Time     // Last access time
	AccessCount int64         // Total access count
	CacheLevel  int           // Cache level (0=memory, 1=disk, etc.)
	Pattern     AccessPattern // Detected access pattern
	FileType    MLFileType    // Type of ML file
	IsHot       bool          // Whether this is a hot chunk
	
	// ML-specific metadata
	IsTrainingData    bool    // Whether this is training data
	IsModel          bool    // Whether this is a model file
	PredictedReuse   float64 // Predicted reuse probability (0.0-1.0)
	EpochRelevance   float64 // Relevance for current training epoch
}

// MLCachePolicy implements ML-aware cache eviction policy
type MLCachePolicy struct {
	// Weights for different factors (sum should be 1.0)
	accessFrequencyWeight float64 // Weight for access frequency
	recencyWeight         float64 // Weight for access recency  
	sizeWeight            float64 // Weight for item size
	mlWeight              float64 // Weight for ML-specific factors
	
	// ML-specific parameters
	trainingDataBoost     float64 // Boost factor for training data
	modelFileBoost        float64 // Boost factor for model files
	sequentialBoost       float64 // Boost factor for sequential access
	epochRelevanceBoost   float64 // Boost factor for epoch-relevant data
	
	// Time-based parameters
	hotThreshold          time.Duration // Threshold for considering item "hot"
	coldThreshold         time.Duration // Threshold for considering item "cold"
	
	// Size-based parameters
	largeFileThreshold    uint64  // Threshold for large files
	smallFilePreference   float64 // Preference for keeping small files
	
	// Statistics
	totalEvictions        int64
	mlFileEvictions       int64
	trainingDataEvictions int64
	modelFileEvictions    int64
}

// NewMLCachePolicy creates a new ML-aware cache eviction policy
func NewMLCachePolicy() *MLCachePolicy {
	return &MLCachePolicy{
		// Balanced weights
		accessFrequencyWeight: 0.3,
		recencyWeight:         0.3,
		sizeWeight:           0.2,
		mlWeight:             0.2,
		
		// ML-specific boosts
		trainingDataBoost:   1.5, // 50% boost for training data
		modelFileBoost:      2.0, // 100% boost for model files
		sequentialBoost:     1.3, // 30% boost for sequential access
		epochRelevanceBoost: 1.4, // 40% boost for epoch-relevant data
		
		// Time thresholds
		hotThreshold:  1 * time.Minute,
		coldThreshold: 10 * time.Minute,
		
		// Size parameters
		largeFileThreshold:  10 * 1024 * 1024, // 10MB
		smallFilePreference: 1.2,              // 20% preference for small files
	}
}

// CalculateEvictionScore calculates an eviction score for a cache entry
// Lower scores indicate higher priority for eviction
func (policy *MLCachePolicy) CalculateEvictionScore(entry *CacheEntry) float64 {
	now := time.Now()
	timeSinceAccess := now.Sub(entry.LastAccess)
	
	// Base factors
	accessFrequencyScore := policy.calculateAccessFrequencyScore(entry)
	recencyScore := policy.calculateRecencyScore(timeSinceAccess)
	sizeScore := policy.calculateSizeScore(entry.Size)
	mlScore := policy.calculateMLScore(entry)
	
	// Weighted combination
	totalScore := policy.accessFrequencyWeight*accessFrequencyScore +
		policy.recencyWeight*recencyScore +
		policy.sizeWeight*sizeScore +
		policy.mlWeight*mlScore
	
	glog.V(4).Infof("Eviction score for inode=%d: total=%.3f (freq=%.3f, recency=%.3f, size=%.3f, ml=%.3f)", 
		entry.Inode, totalScore, accessFrequencyScore, recencyScore, sizeScore, mlScore)
	
	return totalScore
}

// ShouldEvict determines if a cache entry should be evicted
func (policy *MLCachePolicy) ShouldEvict(entry *CacheEntry) bool {
	score := policy.CalculateEvictionScore(entry)
	
	// Different thresholds based on ML file type
	threshold := 0.3 // Default threshold
	
	switch entry.FileType {
	case MLFileModel:
		threshold = 0.1 // Very low threshold - keep models cached longer
	case MLFileDataset:
		if entry.Pattern == SequentialAccess || entry.Pattern == EpochAccess {
			threshold = 0.2 // Lower threshold for sequential dataset access
		} else {
			threshold = 0.4 // Higher threshold for random dataset access
		}
	case MLFileTensor:
		threshold = 0.25 // Medium threshold for tensor files
	case MLFileConfig:
		threshold = 0.5 // Higher threshold for config files (less critical)
	default:
		threshold = 0.3 // Default for unknown files
	}
	
	shouldEvict := score < threshold
	
	if shouldEvict {
		policy.totalEvictions++
		if entry.IsTrainingData {
			policy.trainingDataEvictions++
		}
		if entry.IsModel {
			policy.modelFileEvictions++
		}
		if entry.FileType != MLFileUnknown {
			policy.mlFileEvictions++
		}
		
		glog.V(4).Infof("Evicting: inode=%d, score=%.3f < threshold=%.3f, type=%v", 
			entry.Inode, score, threshold, entry.FileType)
	}
	
	return shouldEvict
}

// calculateAccessFrequencyScore calculates score based on access frequency
func (policy *MLCachePolicy) calculateAccessFrequencyScore(entry *CacheEntry) float64 {
	if entry.AccessCount == 0 {
		return 0.0
	}
	
	// Logarithmic scaling for access count
	base := math.Log(float64(entry.AccessCount) + 1)
	
	// Apply ML-specific boosts
	boost := 1.0
	if entry.IsTrainingData {
		boost *= policy.trainingDataBoost
	}
	if entry.IsModel {
		boost *= policy.modelFileBoost
	}
	if entry.Pattern == SequentialAccess {
		boost *= policy.sequentialBoost
	}
	if entry.EpochRelevance > 0.5 {
		boost *= policy.epochRelevanceBoost
	}
	
	return base * boost
}

// calculateRecencyScore calculates score based on access recency
func (policy *MLCachePolicy) calculateRecencyScore(timeSinceAccess time.Duration) float64 {
	if timeSinceAccess <= policy.hotThreshold {
		return 1.0 // Very recent access
	}
	
	if timeSinceAccess >= policy.coldThreshold {
		return 0.1 // Very old access
	}
	
	// Linear decay between hot and cold thresholds
	ratio := float64(timeSinceAccess-policy.hotThreshold) / float64(policy.coldThreshold-policy.hotThreshold)
	return 1.0 - ratio*0.9 // Decay from 1.0 to 0.1
}

// calculateSizeScore calculates score based on item size
func (policy *MLCachePolicy) calculateSizeScore(size uint64) float64 {
	if size < policy.largeFileThreshold {
		// Prefer keeping smaller files (higher score)
		return policy.smallFilePreference
	}
	
	// Larger files get lower score (more likely to be evicted)
	// But not too low since they might be important model files
	ratio := float64(size) / float64(policy.largeFileThreshold)
	return math.Max(0.3, 1.0/math.Sqrt(ratio))
}

// calculateMLScore calculates ML-specific factors
func (policy *MLCachePolicy) calculateMLScore(entry *CacheEntry) float64 {
	score := 0.5 // Base score for non-ML files
	
	// File type bonuses
	switch entry.FileType {
	case MLFileModel:
		score = 1.0 // Highest priority for model files
	case MLFileDataset:
		score = 0.8 // High priority for datasets
	case MLFileTensor:
		score = 0.7 // Good priority for tensor files
	case MLFileConfig:
		score = 0.4 // Lower priority for config files
	case MLFileLog:
		score = 0.3 // Lowest priority for log files
	default:
		score = 0.5 // Default for unknown files
	}
	
	// Access pattern bonuses
	switch entry.Pattern {
	case SequentialAccess:
		score *= 1.2 // Boost for sequential access
	case ModelAccess:
		score *= 1.5 // Strong boost for model access
	case EpochAccess:
		score *= 1.3 // Boost for epoch access
	case BatchAccess:
		score *= 1.1 // Small boost for batch access
	}
	
	// Predicted reuse bonus
	if entry.PredictedReuse > 0.7 {
		score *= 1.2 // Boost for high predicted reuse
	}
	
	// Epoch relevance bonus
	if entry.EpochRelevance > 0.5 {
		score *= (1.0 + entry.EpochRelevance*0.3) // Up to 30% boost for epoch relevance
	}
	
	// Hot chunk bonus
	if entry.IsHot {
		score *= 1.1
	}
	
	return score
}

// GetEvictionMetrics returns eviction policy metrics
func (policy *MLCachePolicy) GetEvictionMetrics() MLCachePolicyMetrics {
	return MLCachePolicyMetrics{
		TotalEvictions:        policy.totalEvictions,
		MLFileEvictions:       policy.mlFileEvictions,
		TrainingDataEvictions: policy.trainingDataEvictions,
		ModelFileEvictions:    policy.modelFileEvictions,
		
		// Configuration
		AccessFrequencyWeight: policy.accessFrequencyWeight,
		RecencyWeight:         policy.recencyWeight,
		SizeWeight:           policy.sizeWeight,
		MLWeight:             policy.mlWeight,
	}
}

// MLCachePolicyMetrics holds metrics for the ML cache policy
type MLCachePolicyMetrics struct {
	TotalEvictions        int64   `json:"total_evictions"`
	MLFileEvictions       int64   `json:"ml_file_evictions"`
	TrainingDataEvictions int64   `json:"training_data_evictions"`
	ModelFileEvictions    int64   `json:"model_file_evictions"`
	
	// Configuration weights
	AccessFrequencyWeight float64 `json:"access_frequency_weight"`
	RecencyWeight         float64 `json:"recency_weight"`
	SizeWeight           float64 `json:"size_weight"`
	MLWeight             float64 `json:"ml_weight"`
}

// SetWeights updates the eviction policy weights
func (policy *MLCachePolicy) SetWeights(frequency, recency, size, ml float64) {
	total := frequency + recency + size + ml
	if total == 0 {
		glog.Warningf("Invalid weights provided, using defaults")
		return
	}
	
	// Normalize weights to sum to 1.0
	policy.accessFrequencyWeight = frequency / total
	policy.recencyWeight = recency / total
	policy.sizeWeight = size / total
	policy.mlWeight = ml / total
	
	glog.V(2).Infof("Updated eviction policy weights: freq=%.2f, recency=%.2f, size=%.2f, ml=%.2f", 
		policy.accessFrequencyWeight, policy.recencyWeight, policy.sizeWeight, policy.mlWeight)
}

// SetMLBoosts updates the ML-specific boost factors
func (policy *MLCachePolicy) SetMLBoosts(trainingData, model, sequential, epochRelevance float64) {
	policy.trainingDataBoost = trainingData
	policy.modelFileBoost = model
	policy.sequentialBoost = sequential
	policy.epochRelevanceBoost = epochRelevance
	
	glog.V(2).Infof("Updated ML boost factors: training=%.2f, model=%.2f, sequential=%.2f, epoch=%.2f", 
		trainingData, model, sequential, epochRelevance)
}
