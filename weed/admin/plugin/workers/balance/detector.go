package balance

import (
	"math"
	"sort"
)

// NodeDiskMetric contains disk usage information for a data node
type NodeDiskMetric struct {
	NodeID      string
	TotalSpace  uint64
	UsedSpace   uint64
	FreeSpace   uint64
	VolumeCount int
}

// RebalanceCandidate represents a candidate volume for rebalancing
type RebalanceCandidate struct {
	VolumeID          uint32
	SourceNodeID      string
	DestinationNodeID string
	VolumeSize        uint64
	CurrentNodeUsage  float64
	DestinationUsage  float64
	ExpectedBenefit   float64
	ImbalanceScore    float64
	Priority          int
	CanRelocate       bool
	Reason            string
}

// DetectionOptions contains options for rebalancing detection
type DetectionOptions struct {
	MinVolumeSize              uint64
	MaxVolumeSize              uint64
	DiskUsageThreshold         float64
	AcceptableImbalancePercent float64
	PreferBalancedDistribution bool
	DataNodeCount              int
}

// Detector identifies imbalanced data distribution
type Detector struct {
	config DetectionOptions
}

// NewDetector creates a new balance detector
func NewDetector(opts DetectionOptions) *Detector {
	return &Detector{
		config: opts,
	}
}

// DetectJobs analyzes disk usage across nodes and identifies rebalance opportunities
func (d *Detector) DetectJobs(nodeMetrics map[string]*NodeDiskMetric) ([]*RebalanceCandidate, error) {
	candidates := make([]*RebalanceCandidate, 0)

	if len(nodeMetrics) == 0 {
		return candidates, nil
	}

	// Calculate statistics
	avgUsage := d.calculateAverageUsage(nodeMetrics)
	stdDev := d.calculateUsageStdDev(nodeMetrics, avgUsage)
	imbalanceScore := stdDev / avgUsage

	// Check if imbalance exceeds threshold
	threshold := d.config.AcceptableImbalancePercent / 100.0
	if imbalanceScore < threshold {
		return candidates, nil
	}

	// Find source and destination nodes
	sourceNodes := d.findSourceNodes(nodeMetrics, avgUsage)
	destNodes := d.findDestinationNodes(nodeMetrics, avgUsage)

	// Generate rebalance candidates
	for _, sourceNode := range sourceNodes {
		for _, destNode := range destNodes {
			candidate := d.evaluateRebalanceOpportunity(sourceNode, destNode, nodeMetrics, imbalanceScore)
			if candidate.CanRelocate {
				candidates = append(candidates, candidate)
			}
		}
	}

	// Sort by priority
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Priority > candidates[j].Priority
	})

	return candidates, nil
}

// calculateAverageUsage calculates average disk usage across nodes
func (d *Detector) calculateAverageUsage(nodeMetrics map[string]*NodeDiskMetric) float64 {
	if len(nodeMetrics) == 0 {
		return 0
	}

	var totalUsage float64
	for _, node := range nodeMetrics {
		if node.TotalSpace > 0 {
			totalUsage += float64(node.UsedSpace) / float64(node.TotalSpace)
		}
	}

	return totalUsage / float64(len(nodeMetrics))
}

// calculateUsageStdDev calculates standard deviation of disk usage
func (d *Detector) calculateUsageStdDev(nodeMetrics map[string]*NodeDiskMetric, avgUsage float64) float64 {
	if len(nodeMetrics) <= 1 {
		return 0
	}

	var sumSquaredDiff float64
	for _, node := range nodeMetrics {
		var nodeUsage float64
		if node.TotalSpace > 0 {
			nodeUsage = float64(node.UsedSpace) / float64(node.TotalSpace)
		}
		diff := nodeUsage - avgUsage
		sumSquaredDiff += diff * diff
	}

	variance := sumSquaredDiff / float64(len(nodeMetrics))
	return math.Sqrt(variance)
}

// findSourceNodes identifies nodes with high disk usage
func (d *Detector) findSourceNodes(nodeMetrics map[string]*NodeDiskMetric, avgUsage float64) []*NodeDiskMetric {
	sources := make([]*NodeDiskMetric, 0)

	threshold := avgUsage * 1.2 // 20% above average
	for _, node := range nodeMetrics {
		if node.TotalSpace == 0 {
			continue
		}

		nodeUsage := float64(node.UsedSpace) / float64(node.TotalSpace)
		if nodeUsage > threshold && float64(node.UsedSpace) > 0 {
			sources = append(sources, node)
		}
	}

	// Sort by usage (highest first)
	sort.Slice(sources, func(i, j int) bool {
		usageI := float64(sources[i].UsedSpace) / float64(sources[i].TotalSpace)
		usageJ := float64(sources[j].UsedSpace) / float64(sources[j].TotalSpace)
		return usageI > usageJ
	})

	return sources
}

// findDestinationNodes identifies nodes with low disk usage
func (d *Detector) findDestinationNodes(nodeMetrics map[string]*NodeDiskMetric, avgUsage float64) []*NodeDiskMetric {
	destinations := make([]*NodeDiskMetric, 0)

	threshold := avgUsage * 0.8 // 20% below average
	for _, node := range nodeMetrics {
		if node.TotalSpace == 0 {
			continue
		}

		nodeUsage := float64(node.UsedSpace) / float64(node.TotalSpace)
		if nodeUsage < threshold && node.FreeSpace > 0 {
			destinations = append(destinations, node)
		}
	}

	// Sort by free space (most available first)
	sort.Slice(destinations, func(i, j int) bool {
		return destinations[i].FreeSpace > destinations[j].FreeSpace
	})

	return destinations
}

// evaluateRebalanceOpportunity evaluates if rebalancing between two nodes is beneficial
func (d *Detector) evaluateRebalanceOpportunity(
	sourceNode, destNode *NodeDiskMetric,
	allNodes map[string]*NodeDiskMetric,
	currentImbalanceScore float64,
) *RebalanceCandidate {
	candidate := &RebalanceCandidate{
		SourceNodeID:      sourceNode.NodeID,
		DestinationNodeID: destNode.NodeID,
		CanRelocate:       false,
	}

	// Check if destination node has sufficient capacity
	if !d.checkNodeCapacity(destNode) {
		candidate.Reason = "destination node insufficient capacity"
		return candidate
	}

	// Calculate current usage
	sourceUsage := float64(sourceNode.UsedSpace) / float64(sourceNode.TotalSpace)
	destUsage := float64(destNode.UsedSpace) / float64(destNode.TotalSpace)

	candidate.CurrentNodeUsage = sourceUsage
	candidate.DestinationUsage = destUsage
	candidate.VolumeSize = 1000 // Default volume size

	// Estimate benefit
	benefit := d.estimateRebalanceBenefit(sourceUsage, destUsage)
	candidate.ExpectedBenefit = benefit

	// Calculate imbalance score for this candidate
	candidate.ImbalanceScore = currentImbalanceScore

	// Determine priority
	candidate.Priority = int(benefit * 100)
	if candidate.Priority < 0 {
		candidate.Priority = 0
	}

	// Check if rebalancing is worthwhile
	if benefit > 0.01 { // 1% improvement threshold
		candidate.CanRelocate = true
		candidate.Reason = "beneficial rebalancing opportunity"
	} else {
		candidate.Reason = "insufficient benefit from rebalancing"
	}

	return candidate
}

// checkNodeCapacity validates if destination node can accept data
func (d *Detector) checkNodeCapacity(node *NodeDiskMetric) bool {
	if node.TotalSpace == 0 {
		return false
	}

	// Check if node has at least 10% free space
	freePercentage := float64(node.FreeSpace) / float64(node.TotalSpace)
	if freePercentage < 0.1 {
		return false
	}

	// Check if node doesn't exceed disk usage threshold
	usagePercentage := float64(node.UsedSpace) / float64(node.TotalSpace)
	if usagePercentage > d.config.DiskUsageThreshold/100.0 {
		return false
	}

	return true
}

// estimateRebalanceBenefit estimates the benefit of moving data from source to destination
func (d *Detector) estimateRebalanceBenefit(sourceUsage, destUsage float64) float64 {
	// Simple calculation: difference between source and destination usage
	return sourceUsage - destUsage
}

// SortByImbalance sorts candidates by imbalance impact
func SortByImbalance(candidates []*RebalanceCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ExpectedBenefit != candidates[j].ExpectedBenefit {
			return candidates[i].ExpectedBenefit > candidates[j].ExpectedBenefit
		}
		return candidates[i].Priority > candidates[j].Priority
	})
}

// VolumeMetric contains volume statistics
type VolumeMetric struct {
	VolumeID     uint32
	DataNodeID   string
	Size         uint64
	FreeSpace    uint64
	ReplicaCount int
	RackID       string
	DataCenterID string
	FileCount    int64
	LastModified int64
	Collection   string
}

// FilterByCriteria filters rebalance candidates by specific criteria
func FilterByCriteria(candidates []*RebalanceCandidate, criteria map[string]string) []*RebalanceCandidate {
	filtered := make([]*RebalanceCandidate, 0)

	for _, candidate := range candidates {
		if !candidate.CanRelocate {
			continue
		}

		// Apply source node filter if specified
		if sourceNode, ok := criteria["source_node"]; ok && sourceNode != "" && candidate.SourceNodeID != sourceNode {
			continue
		}

		// Apply destination node filter if specified
		if destNode, ok := criteria["dest_node"]; ok && destNode != "" && candidate.DestinationNodeID != destNode {
			continue
		}

		// Apply minimum benefit filter if specified
		if minBenefit, ok := criteria["min_benefit"]; ok && minBenefit != "" {
			// Would parse minBenefit and filter
		}

		filtered = append(filtered, candidate)
	}

	return filtered
}

// GroupBySourceNode groups candidates by source node for parallel execution
func GroupBySourceNode(candidates []*RebalanceCandidate) map[string][]*RebalanceCandidate {
	grouped := make(map[string][]*RebalanceCandidate)

	for _, candidate := range candidates {
		sourceID := candidate.SourceNodeID
		if sourceID == "" {
			sourceID = "unknown"
		}
		grouped[sourceID] = append(grouped[sourceID], candidate)
	}

	return grouped
}
