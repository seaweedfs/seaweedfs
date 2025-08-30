package ml

import (
	"testing"
	"time"
)

func TestMLCachePolicy_Basic(t *testing.T) {
	policy := NewMLCachePolicy()

	// Test basic eviction score calculation
	entry := &CacheEntry{
		Inode:       1,
		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		CacheLevel:  0,
		Pattern:     RandomAccess,
		FileType:    MLFileUnknown,
		IsHot:       false,
	}

	score := policy.CalculateEvictionScore(entry)
	if score <= 0 {
		t.Error("Eviction score should be positive")
	}

	shouldEvict := policy.ShouldEvict(entry)
	t.Logf("Basic entry eviction: score=%.3f, shouldEvict=%v", score, shouldEvict)
}

func TestMLCachePolicy_ModelFileBoost(t *testing.T) {
	policy := NewMLCachePolicy()

	// Create two identical entries, one is a model file
	baseEntry := &CacheEntry{
		Inode:       1,
		Size:        10 * 1024 * 1024, // 10MB
		LastAccess:  time.Now().Add(-5 * time.Minute),
		AccessCount: 3,
		CacheLevel:  0,
		Pattern:     SequentialAccess,
		FileType:    MLFileUnknown,
		IsModel:     false,
	}

	modelEntry := &CacheEntry{
		Inode:       2,
		Size:        10 * 1024 * 1024, // 10MB
		LastAccess:  time.Now().Add(-5 * time.Minute),
		AccessCount: 3,
		CacheLevel:  0,
		Pattern:     SequentialAccess,
		FileType:    MLFileModel,
		IsModel:     true,
	}

	baseScore := policy.CalculateEvictionScore(baseEntry)
	modelScore := policy.CalculateEvictionScore(modelEntry)

	if modelScore <= baseScore {
		t.Errorf("Model file should have higher score than regular file: model=%.3f, base=%.3f",
			modelScore, baseScore)
	}

	// Model files should be less likely to be evicted
	baseShouldEvict := policy.ShouldEvict(baseEntry)
	modelShouldEvict := policy.ShouldEvict(modelEntry)

	if modelShouldEvict && !baseShouldEvict {
		t.Error("Model file should not be evicted if regular file is not evicted")
	}

	t.Logf("Model vs Base eviction: model=%.3f (evict=%v), base=%.3f (evict=%v)",
		modelScore, modelShouldEvict, baseScore, baseShouldEvict)
}

func TestMLCachePolicy_TrainingDataBoost(t *testing.T) {
	policy := NewMLCachePolicy()

	regularEntry := &CacheEntry{
		Inode:          1,
		Size:           1024,
		LastAccess:     time.Now().Add(-2 * time.Minute),
		AccessCount:    10,
		FileType:       MLFileUnknown,
		IsTrainingData: false,
	}

	trainingEntry := &CacheEntry{
		Inode:          2,
		Size:           1024,
		LastAccess:     time.Now().Add(-2 * time.Minute),
		AccessCount:    10,
		FileType:       MLFileDataset,
		IsTrainingData: true,
	}

	regularScore := policy.CalculateEvictionScore(regularEntry)
	trainingScore := policy.CalculateEvictionScore(trainingEntry)

	if trainingScore <= regularScore {
		t.Errorf("Training data should have higher score: training=%.3f, regular=%.3f",
			trainingScore, regularScore)
	}
}

func TestMLCachePolicy_AccessPatternBoost(t *testing.T) {
	policy := NewMLCachePolicy()

	randomEntry := &CacheEntry{
		Inode:       1,
		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		Pattern:     RandomAccess,
		FileType:    MLFileDataset,
	}

	sequentialEntry := &CacheEntry{
		Inode:       2,
		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		Pattern:     SequentialAccess,
		FileType:    MLFileDataset,
	}

	modelAccessEntry := &CacheEntry{
		Inode:       3,
		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		Pattern:     ModelAccess,
		FileType:    MLFileModel,
	}

	randomScore := policy.CalculateEvictionScore(randomEntry)
	sequentialScore := policy.CalculateEvictionScore(sequentialEntry)
	modelScore := policy.CalculateEvictionScore(modelAccessEntry)

	if sequentialScore <= randomScore {
		t.Errorf("Sequential access should have higher score than random: seq=%.3f, random=%.3f",
			sequentialScore, randomScore)
	}

	if modelScore <= sequentialScore {
		t.Errorf("Model access should have highest score: model=%.3f, seq=%.3f",
			modelScore, sequentialScore)
	}

	t.Logf("Pattern comparison: random=%.3f, sequential=%.3f, model=%.3f",
		randomScore, sequentialScore, modelScore)
}

func TestMLCachePolicy_SizePreference(t *testing.T) {
	policy := NewMLCachePolicy()

	smallEntry := &CacheEntry{
		Inode:       1,
		Size:        1024, // 1KB
		LastAccess:  time.Now().Add(-5 * time.Minute),
		AccessCount: 3,
		FileType:    MLFileUnknown,
	}

	largeEntry := &CacheEntry{
		Inode:       2,
		Size:        50 * 1024 * 1024, // 50MB
		LastAccess:  time.Now().Add(-5 * time.Minute),
		AccessCount: 3,
		FileType:    MLFileUnknown,
	}

	smallScore := policy.CalculateEvictionScore(smallEntry)
	largeScore := policy.CalculateEvictionScore(largeEntry)

	if smallScore <= largeScore {
		t.Errorf("Small files should have higher score than large files: small=%.3f, large=%.3f",
			smallScore, largeScore)
	}
}

func TestMLCachePolicy_RecencyDecay(t *testing.T) {
	policy := NewMLCachePolicy()

	// Create entries with different access times
	recentEntry := &CacheEntry{
		Inode: 1,

		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		FileType:    MLFileUnknown,
	}

	oldEntry := &CacheEntry{
		Inode: 2,

		Size:        1024,
		LastAccess:  time.Now().Add(-20 * time.Minute),
		AccessCount: 5,
		FileType:    MLFileUnknown,
	}

	recentScore := policy.CalculateEvictionScore(recentEntry)
	oldScore := policy.CalculateEvictionScore(oldEntry)

	if recentScore <= oldScore {
		t.Errorf("Recent access should have higher score: recent=%.3f, old=%.3f",
			recentScore, oldScore)
	}
}

func TestMLCachePolicy_EpochRelevance(t *testing.T) {
	policy := NewMLCachePolicy()

	lowRelevanceEntry := &CacheEntry{
		Inode: 1,

		Size:           1024,
		LastAccess:     time.Now(),
		AccessCount:    5,
		FileType:       MLFileDataset,
		EpochRelevance: 0.2,
	}

	highRelevanceEntry := &CacheEntry{
		Inode: 2,

		Size:           1024,
		LastAccess:     time.Now(),
		AccessCount:    5,
		FileType:       MLFileDataset,
		EpochRelevance: 0.9,
	}

	lowScore := policy.CalculateEvictionScore(lowRelevanceEntry)
	highScore := policy.CalculateEvictionScore(highRelevanceEntry)

	if highScore <= lowScore {
		t.Errorf("High epoch relevance should have higher score: high=%.3f, low=%.3f",
			highScore, lowScore)
	}
}

func TestMLCachePolicy_DifferentThresholds(t *testing.T) {
	policy := NewMLCachePolicy()

	// Create entries for different file types with same base score
	unknownEntry := &CacheEntry{
		Inode: 1,

		Size:        1024,
		LastAccess:  time.Now().Add(-15 * time.Minute), // Old enough to potentially evict
		AccessCount: 2,
		FileType:    MLFileUnknown,
	}

	modelEntry := &CacheEntry{
		Inode: 2,

		Size:        1024,
		LastAccess:  time.Now().Add(-15 * time.Minute),
		AccessCount: 2,
		FileType:    MLFileModel,
		IsModel:     true,
	}

	datasetEntry := &CacheEntry{
		Inode: 3,

		Size:        1024,
		LastAccess:  time.Now().Add(-15 * time.Minute),
		AccessCount: 2,
		FileType:    MLFileDataset,
		Pattern:     SequentialAccess,
	}

	unknownShouldEvict := policy.ShouldEvict(unknownEntry)
	modelShouldEvict := policy.ShouldEvict(modelEntry)
	datasetShouldEvict := policy.ShouldEvict(datasetEntry)

	// Models should be least likely to be evicted
	if modelShouldEvict && (!unknownShouldEvict || !datasetShouldEvict) {
		t.Error("Model files should be least likely to be evicted")
	}

	t.Logf("Eviction by type: unknown=%v, model=%v, dataset=%v",
		unknownShouldEvict, modelShouldEvict, datasetShouldEvict)
}

func TestMLCachePolicy_SetWeights(t *testing.T) {
	policy := NewMLCachePolicy()

	// Test setting custom weights
	policy.SetWeights(0.4, 0.3, 0.1, 0.2)

	if policy.accessFrequencyWeight != 0.4 {
		t.Errorf("Expected frequency weight 0.4, got %.2f", policy.accessFrequencyWeight)
	}

	if policy.recencyWeight != 0.3 {
		t.Errorf("Expected recency weight 0.3, got %.2f", policy.recencyWeight)
	}

	if policy.sizeWeight != 0.1 {
		t.Errorf("Expected size weight 0.1, got %.2f", policy.sizeWeight)
	}

	if policy.mlWeight != 0.2 {
		t.Errorf("Expected ML weight 0.2, got %.2f", policy.mlWeight)
	}

	// Test weight normalization
	policy.SetWeights(2.0, 2.0, 1.0, 1.0) // Total = 6.0

	expectedFreq := 2.0 / 6.0
	if abs(policy.accessFrequencyWeight-expectedFreq) > 0.001 {
		t.Errorf("Expected normalized frequency weight %.3f, got %.3f",
			expectedFreq, policy.accessFrequencyWeight)
	}
}

func TestMLCachePolicy_SetMLBoosts(t *testing.T) {
	policy := NewMLCachePolicy()

	// Test setting custom boost factors
	policy.SetMLBoosts(2.0, 3.0, 1.5, 1.8)

	if policy.trainingDataBoost != 2.0 {
		t.Errorf("Expected training data boost 2.0, got %.2f", policy.trainingDataBoost)
	}

	if policy.modelFileBoost != 3.0 {
		t.Errorf("Expected model file boost 3.0, got %.2f", policy.modelFileBoost)
	}

	if policy.sequentialBoost != 1.5 {
		t.Errorf("Expected sequential boost 1.5, got %.2f", policy.sequentialBoost)
	}

	if policy.epochRelevanceBoost != 1.8 {
		t.Errorf("Expected epoch relevance boost 1.8, got %.2f", policy.epochRelevanceBoost)
	}
}

func TestMLCachePolicy_Metrics(t *testing.T) {
	policy := NewMLCachePolicy()

	// Simulate some evictions
	entries := []*CacheEntry{
		{FileType: MLFileModel, IsModel: true},
		{FileType: MLFileDataset, IsTrainingData: true},
		{FileType: MLFileUnknown},
	}

	for _, entry := range entries {
		entry.LastAccess = time.Now().Add(-30 * time.Minute) // Old enough to evict
		entry.AccessCount = 1
		entry.Size = 1024

		if policy.ShouldEvict(entry) {
			// Eviction counters are updated in ShouldEvict
		}
	}

	metrics := policy.GetEvictionMetrics()

	if metrics.TotalEvictions == 0 {
		t.Error("Should have some total evictions")
	}

	// Verify weight configuration in metrics
	if metrics.AccessFrequencyWeight != policy.accessFrequencyWeight {
		t.Error("Metrics should reflect current weight configuration")
	}
}

func TestMLCachePolicy_HotChunkPreference(t *testing.T) {
	policy := NewMLCachePolicy()

	coldEntry := &CacheEntry{
		Inode: 1,

		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		IsHot:       false,
		FileType:    MLFileDataset,
	}

	hotEntry := &CacheEntry{
		Inode: 2,

		Size:        1024,
		LastAccess:  time.Now(),
		AccessCount: 5,
		IsHot:       true,
		FileType:    MLFileDataset,
	}

	coldScore := policy.CalculateEvictionScore(coldEntry)
	hotScore := policy.CalculateEvictionScore(hotEntry)

	if hotScore <= coldScore {
		t.Errorf("Hot chunk should have higher score: hot=%.3f, cold=%.3f", hotScore, coldScore)
	}
}

func TestMLCachePolicy_RecencyThresholds(t *testing.T) {
	policy := NewMLCachePolicy()

	// Test hot threshold
	hotEntry := &CacheEntry{
		Inode:       1,
		Size:        1024,
		LastAccess:  time.Now().Add(-30 * time.Second), // Within hot threshold
		AccessCount: 1,
	}

	// Test cold threshold
	coldEntry := &CacheEntry{
		Inode:       2,
		Size:        1024,
		LastAccess:  time.Now().Add(-15 * time.Minute), // Beyond cold threshold
		AccessCount: 1,
	}

	// Test middle
	middleEntry := &CacheEntry{
		Inode:       3,
		Size:        1024,
		LastAccess:  time.Now().Add(-5 * time.Minute), // Between thresholds
		AccessCount: 1,
	}

	hotScore := policy.calculateRecencyScore(time.Since(hotEntry.LastAccess))
	coldScore := policy.calculateRecencyScore(time.Since(coldEntry.LastAccess))
	middleScore := policy.calculateRecencyScore(time.Since(middleEntry.LastAccess))

	if hotScore != 1.0 {
		t.Errorf("Hot entry should have score 1.0, got %.3f", hotScore)
	}

	if coldScore != 0.1 {
		t.Errorf("Cold entry should have score 0.1, got %.3f", coldScore)
	}

	if middleScore <= coldScore || middleScore >= hotScore {
		t.Errorf("Middle entry should have score between hot and cold: %.3f not in (%.3f, %.3f)",
			middleScore, coldScore, hotScore)
	}
}

func TestMLCachePolicy_SizeScore(t *testing.T) {
	policy := NewMLCachePolicy()

	smallSize := uint64(1024)              // 1KB
	largeSize := uint64(100 * 1024 * 1024) // 100MB

	smallScore := policy.calculateSizeScore(smallSize)
	largeScore := policy.calculateSizeScore(largeSize)

	if smallScore <= largeScore {
		t.Errorf("Small files should have higher size score: small=%.3f, large=%.3f",
			smallScore, largeScore)
	}

	// Large files should still have reasonable score (not too low)
	if largeScore < 0.2 {
		t.Errorf("Large files should have reasonable score, got %.3f", largeScore)
	}
}

func TestMLCachePolicy_AccessFrequencyScore(t *testing.T) {
	policy := NewMLCachePolicy()

	lowAccessEntry := &CacheEntry{
		AccessCount: 1,
		FileType:    MLFileUnknown,
		Pattern:     RandomAccess,
	}

	highAccessEntry := &CacheEntry{
		AccessCount: 100,
		FileType:    MLFileUnknown,
		Pattern:     RandomAccess,
	}

	lowScore := policy.calculateAccessFrequencyScore(lowAccessEntry)
	highScore := policy.calculateAccessFrequencyScore(highAccessEntry)

	if highScore <= lowScore {
		t.Errorf("High access count should have higher score: high=%.3f, low=%.3f",
			highScore, lowScore)
	}
}

// Helper function
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// Benchmark tests

func BenchmarkMLCachePolicy_CalculateEvictionScore(b *testing.B) {
	policy := NewMLCachePolicy()

	entry := &CacheEntry{
		Inode: 1,

		Size:           1024,
		LastAccess:     time.Now().Add(-5 * time.Minute),
		AccessCount:    10,
		FileType:       MLFileDataset,
		Pattern:        SequentialAccess,
		IsTrainingData: true,
		EpochRelevance: 0.8,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		policy.CalculateEvictionScore(entry)
	}
}

func BenchmarkMLCachePolicy_ShouldEvict(b *testing.B) {
	policy := NewMLCachePolicy()

	entry := &CacheEntry{
		Inode: 1,

		Size:        1024,
		LastAccess:  time.Now().Add(-5 * time.Minute),
		AccessCount: 10,
		FileType:    MLFileDataset,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		policy.ShouldEvict(entry)
	}
}
