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
func (logic *EcVacuumLogic) DetermineGenerationsFromParams(params *worker_pb.TaskParams) (sourceGen, targetGen uint32, err error) {
	if params == nil {
		return 0, 0, fmt.Errorf("task parameters cannot be nil")
	}

	if len(params.Sources) == 0 {
		// Fallback to safe defaults for backward compatibility
		return 0, 1, nil
	}

	// Use generation from first source (all sources should have same generation)
	if params.Sources[0].Generation > 0 {
		sourceGen = params.Sources[0].Generation
		targetGen = sourceGen + 1
	} else {
		// Generation 0 case
		sourceGen = 0
		targetGen = 1
	}

	// Validate consistency - all sources should have the same generation
	for i, source := range params.Sources {
		if source.Generation != sourceGen {
			return 0, 0, fmt.Errorf("inconsistent generations in sources: source[0]=%d, source[%d]=%d",
				sourceGen, i, source.Generation)
		}
	}

	return sourceGen, targetGen, nil
}

// ParseSourceNodes extracts source node information from task parameters
func (logic *EcVacuumLogic) ParseSourceNodes(params *worker_pb.TaskParams) (map[pb.ServerAddress]erasure_coding.ShardBits, error) {
	if params == nil {
		return nil, fmt.Errorf("task parameters cannot be nil")
	}

	sourceNodes := make(map[pb.ServerAddress]erasure_coding.ShardBits)

	for _, source := range params.Sources {
		if source.Node == "" {
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
		return nil, fmt.Errorf("no valid source nodes found: sources=%d", len(params.Sources))
	}

	return sourceNodes, nil
}

// CreateVacuumPlan creates a comprehensive plan for the EC vacuum operation
func (logic *EcVacuumLogic) CreateVacuumPlan(volumeID uint32, collection string, params *worker_pb.TaskParams) (*VacuumPlan, error) {
	// Extract generations
	sourceGen, targetGen, err := logic.DetermineGenerationsFromParams(params)
	if err != nil {
		return nil, fmt.Errorf("failed to determine generations: %w", err)
	}

	// Parse source nodes
	sourceNodes, err := logic.ParseSourceNodes(params)
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

	// Determine what to cleanup (simplified: just source generation)
	generationsToCleanup := []uint32{sourceGen}

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

// countShardsToDelete counts how many shard files will be deleted
func (logic *EcVacuumLogic) countShardsToDelete(plan *VacuumPlan) int {
	totalShards := 0
	for _, shardBits := range plan.SourceDistribution.Nodes {
		totalShards += shardBits.ShardIdCount()
	}
	return totalShards * len(plan.GenerationsToCleanup)
}
