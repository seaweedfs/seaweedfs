package ec_vacuum

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// EcVacuumLogic contains the core business logic for EC vacuum operations
// This is extracted from EcVacuumTask to make it easily testable
type EcVacuumLogic struct{}

// NewEcVacuumLogic creates a new instance of the core logic
func NewEcVacuumLogic() *EcVacuumLogic {
	return &EcVacuumLogic{}
}

// GenerationPlan represents a plan for generation transitions during vacuum
type GenerationPlan struct {
	VolumeID         uint32
	SourceGeneration uint32
	TargetGeneration uint32
	SourceNodes      map[pb.ServerAddress]erasure_coding.ShardBits
	CleanupPlan      []uint32 // Generations to be cleaned up
}

// ShardDistribution represents how shards are distributed across nodes
type ShardDistribution struct {
	Generation uint32
	Nodes      map[pb.ServerAddress]erasure_coding.ShardBits
}

// VacuumPlan represents the complete plan for an EC vacuum operation
type VacuumPlan struct {
	VolumeID             uint32
	Collection           string
	CurrentGeneration    uint32
	TargetGeneration     uint32
	SourceDistribution   ShardDistribution
	ExpectedDistribution ShardDistribution
	GenerationsToCleanup []uint32
	SafetyChecks         []string
}

// DetermineGenerationsFromParams extracts generation information from task parameters
// Now supports multiple generations and finds the most complete one for vacuum
func (logic *EcVacuumLogic) DetermineGenerationsFromParams(params *worker_pb.TaskParams) (sourceGen, targetGen uint32, err error) {
	if params == nil {
		return 0, 0, fmt.Errorf("task parameters cannot be nil")
	}

	if len(params.Sources) == 0 {
		// Fallback to safe defaults for backward compatibility
		return 0, 1, nil
	}

	// Group sources by generation and analyze completeness
	generationAnalysis, err := logic.AnalyzeGenerationCompleteness(params)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to analyze generation completeness: %w", err)
	}

	// Find the most complete generation that can be used for reconstruction
	mostCompleteGen, found := logic.FindMostCompleteGeneration(generationAnalysis)
	if !found {
		return 0, 0, fmt.Errorf("no generation has sufficient shards for reconstruction")
	}

	// Target generation is max(all generations) + 1
	maxGen := logic.FindMaxGeneration(generationAnalysis)
	targetGen = maxGen + 1

	return mostCompleteGen, targetGen, nil
}

// ParseSourceNodes extracts source node information from task parameters for a specific generation
func (logic *EcVacuumLogic) ParseSourceNodes(params *worker_pb.TaskParams, targetGeneration uint32) (map[pb.ServerAddress]erasure_coding.ShardBits, error) {
	if params == nil {
		return nil, fmt.Errorf("task parameters cannot be nil")
	}

	sourceNodes := make(map[pb.ServerAddress]erasure_coding.ShardBits)

	for _, source := range params.Sources {
		if source.Node == "" || source.Generation != targetGeneration {
			continue
		}

		serverAddr := pb.ServerAddress(source.Node)
		var shardBits erasure_coding.ShardBits

		// Convert shard IDs to ShardBits
		for _, shardId := range source.ShardIds {
			if shardId < erasure_coding.TotalShardsCount {
				shardBits = shardBits.AddShardId(erasure_coding.ShardId(shardId))
			}
		}

		if shardBits.ShardIdCount() > 0 {
			sourceNodes[serverAddr] = shardBits
		}
	}

	if len(sourceNodes) == 0 {
		return nil, fmt.Errorf("no valid source nodes found for generation %d: sources=%d", targetGeneration, len(params.Sources))
	}

	return sourceNodes, nil
}

// CreateVacuumPlan creates a comprehensive plan for the EC vacuum operation
func (logic *EcVacuumLogic) CreateVacuumPlan(volumeID uint32, collection string, params *worker_pb.TaskParams) (*VacuumPlan, error) {
	// Extract generations and analyze completeness
	sourceGen, targetGen, err := logic.DetermineGenerationsFromParams(params)
	if err != nil {
		return nil, fmt.Errorf("failed to determine generations: %w", err)
	}

	// Parse source nodes from the selected generation
	sourceNodes, err := logic.ParseSourceNodes(params, sourceGen)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source nodes: %w", err)
	}

	// Create source distribution
	sourceDistribution := ShardDistribution{
		Generation: sourceGen,
		Nodes:      sourceNodes,
	}

	// Expected distribution is same nodes but with target generation
	expectedDistribution := ShardDistribution{
		Generation: targetGen,
		Nodes:      sourceNodes, // Same nodes, new generation
	}

	// Get all available generations for cleanup calculation
	generationAnalysis, err := logic.AnalyzeGenerationCompleteness(params)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze generations for cleanup: %w", err)
	}

	// All generations except target should be cleaned up
	var allGenerations []uint32
	for generation := range generationAnalysis {
		allGenerations = append(allGenerations, generation)
	}
	generationsToCleanup := logic.CalculateCleanupGenerations(sourceGen, targetGen, allGenerations)

	// Generate safety checks
	safetyChecks := logic.generateSafetyChecks(sourceDistribution, targetGen)

	return &VacuumPlan{
		VolumeID:             volumeID,
		Collection:           collection,
		CurrentGeneration:    sourceGen,
		TargetGeneration:     targetGen,
		SourceDistribution:   sourceDistribution,
		ExpectedDistribution: expectedDistribution,
		GenerationsToCleanup: generationsToCleanup,
		SafetyChecks:         safetyChecks,
	}, nil
}

// ValidateShardDistribution validates that the shard distribution is sufficient for vacuum
func (logic *EcVacuumLogic) ValidateShardDistribution(distribution ShardDistribution) error {
	totalShards := erasure_coding.ShardBits(0)

	for _, shardBits := range distribution.Nodes {
		totalShards = totalShards.Plus(shardBits)
	}

	shardCount := totalShards.ShardIdCount()
	if shardCount < erasure_coding.DataShardsCount {
		return fmt.Errorf("insufficient shards for reconstruction: have %d, need at least %d",
			shardCount, erasure_coding.DataShardsCount)
	}

	return nil
}

// CalculateCleanupGenerations determines which generations should be cleaned up
func (logic *EcVacuumLogic) CalculateCleanupGenerations(currentGen, targetGen uint32, availableGenerations []uint32) []uint32 {
	var toCleanup []uint32

	for _, gen := range availableGenerations {
		// Don't clean up the target generation
		if gen != targetGen {
			toCleanup = append(toCleanup, gen)
		}
	}

	return toCleanup
}

// generateSafetyChecks creates a list of safety checks for the vacuum plan
func (logic *EcVacuumLogic) generateSafetyChecks(distribution ShardDistribution, targetGen uint32) []string {
	var checks []string

	// Check 1: Sufficient shards
	totalShards := erasure_coding.ShardBits(0)
	for _, shardBits := range distribution.Nodes {
		totalShards = totalShards.Plus(shardBits)
	}

	checks = append(checks, fmt.Sprintf("Total shards available: %d/%d",
		totalShards.ShardIdCount(), erasure_coding.TotalShardsCount))

	// Check 2: Minimum data shards
	if totalShards.ShardIdCount() >= erasure_coding.DataShardsCount {
		checks = append(checks, "✅ Sufficient data shards for reconstruction")
	} else {
		checks = append(checks, "❌ INSUFFICIENT data shards for reconstruction")
	}

	// Check 3: Node distribution
	checks = append(checks, fmt.Sprintf("Shard distribution across %d nodes", len(distribution.Nodes)))

	// Check 4: Generation safety
	checks = append(checks, fmt.Sprintf("Target generation %d != source generation %d",
		targetGen, distribution.Generation))

	return checks
}

// EstimateCleanupImpact estimates the storage impact of cleanup operations
func (logic *EcVacuumLogic) EstimateCleanupImpact(plan *VacuumPlan, volumeSize uint64) CleanupImpact {
	// Estimate size per generation
	sizePerGeneration := volumeSize

	// Calculate total cleanup impact
	var totalCleanupSize uint64
	for range plan.GenerationsToCleanup {
		totalCleanupSize += sizePerGeneration
	}

	return CleanupImpact{
		GenerationsToCleanup: len(plan.GenerationsToCleanup),
		EstimatedSizeFreed:   totalCleanupSize,
		NodesAffected:        len(plan.SourceDistribution.Nodes),
		ShardsToDelete:       logic.countShardsToDelete(plan),
	}
}

// CleanupImpact represents the estimated impact of cleanup operations
type CleanupImpact struct {
	GenerationsToCleanup int
	EstimatedSizeFreed   uint64
	NodesAffected        int
	ShardsToDelete       int
}

// GenerationAnalysis represents the analysis of shard completeness per generation
type GenerationAnalysis struct {
	Generation     uint32
	ShardBits      erasure_coding.ShardBits
	ShardCount     int
	Nodes          map[pb.ServerAddress]erasure_coding.ShardBits
	CanReconstruct bool // Whether this generation has enough shards for reconstruction
}

// AnalyzeGenerationCompleteness analyzes each generation's shard completeness
func (logic *EcVacuumLogic) AnalyzeGenerationCompleteness(params *worker_pb.TaskParams) (map[uint32]*GenerationAnalysis, error) {
	if params == nil {
		return nil, fmt.Errorf("task parameters cannot be nil")
	}

	generationMap := make(map[uint32]*GenerationAnalysis)

	// Group sources by generation
	for _, source := range params.Sources {
		if source.Node == "" {
			continue
		}

		generation := source.Generation
		if _, exists := generationMap[generation]; !exists {
			generationMap[generation] = &GenerationAnalysis{
				Generation: generation,
				ShardBits:  erasure_coding.ShardBits(0),
				Nodes:      make(map[pb.ServerAddress]erasure_coding.ShardBits),
			}
		}

		analysis := generationMap[generation]
		serverAddr := pb.ServerAddress(source.Node)
		var shardBits erasure_coding.ShardBits

		// Convert shard IDs to ShardBits
		for _, shardId := range source.ShardIds {
			if shardId < erasure_coding.TotalShardsCount {
				shardBits = shardBits.AddShardId(erasure_coding.ShardId(shardId))
			}
		}

		if shardBits.ShardIdCount() > 0 {
			analysis.Nodes[serverAddr] = shardBits
			analysis.ShardBits = analysis.ShardBits.Plus(shardBits)
		}
	}

	// Calculate completeness for each generation
	for _, analysis := range generationMap {
		analysis.ShardCount = analysis.ShardBits.ShardIdCount()
		analysis.CanReconstruct = analysis.ShardCount >= erasure_coding.DataShardsCount
	}

	return generationMap, nil
}

// FindMostCompleteGeneration finds the generation with the most complete set of shards
// that can be used for reconstruction
func (logic *EcVacuumLogic) FindMostCompleteGeneration(generationMap map[uint32]*GenerationAnalysis) (uint32, bool) {
	var bestGeneration uint32
	var bestShardCount int
	found := false

	for generation, analysis := range generationMap {
		// Only consider generations that can reconstruct
		if !analysis.CanReconstruct {
			continue
		}

		// Prefer the generation with the most shards, or if tied, the highest generation number
		if !found || analysis.ShardCount > bestShardCount ||
			(analysis.ShardCount == bestShardCount && generation > bestGeneration) {
			bestGeneration = generation
			bestShardCount = analysis.ShardCount
			found = true
		}
	}

	return bestGeneration, found
}

// FindMaxGeneration finds the highest generation number among all available generations
func (logic *EcVacuumLogic) FindMaxGeneration(generationMap map[uint32]*GenerationAnalysis) uint32 {
	var maxGen uint32
	for generation := range generationMap {
		if generation > maxGen {
			maxGen = generation
		}
	}
	return maxGen
}

// countShardsToDelete counts how many shard files will be deleted
func (logic *EcVacuumLogic) countShardsToDelete(plan *VacuumPlan) int {
	totalShards := 0
	for _, shardBits := range plan.SourceDistribution.Nodes {
		totalShards += shardBits.ShardIdCount()
	}
	return totalShards * len(plan.GenerationsToCleanup)
}
