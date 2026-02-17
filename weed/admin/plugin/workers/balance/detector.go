package balance

import (
"fmt"
"math"
)

// RebalanceCandidate represents a rebalance opportunity
type RebalanceCandidate struct {
VolumeID              uint32
SourceNodeID          string
DestinationNodeID     string
SourceUsagePercent    float32
DestinationUsagePercent float32
ImbalanceScore        float32
DataToMove            uint64
ExpectedBenefit       float32
Priority              int
CanExecute            bool
Reason                string
}

// DetectionOptions contains options for detection
type DetectionOptions struct {
AcceptableImbalance     float32
DiskUsageThreshold      float32
MinVolumeSize           uint64
MaxVolumeSize           uint64
PreferBalancedDist      bool
PreferredNodes          []string
ExcludeNodes            []string
}

// Detector scans for rebalance opportunities
type Detector struct {
config DetectionOptions
}

// NewDetector creates a new balance detector
func NewDetector(opts DetectionOptions) *Detector {
return &Detector{
config: opts,
}
}

// DetectJobs analyzes disk usage and identifies rebalance opportunities
func (d *Detector) DetectJobs(nodeMetrics map[string]*NodeMetric) ([]*RebalanceCandidate, error) {
candidates := make([]*RebalanceCandidate, 0)

// Calculate cluster statistics
avgUsage, stdDev := d.calculateClusterStats(nodeMetrics)

// Find imbalanced nodes
for sourceID, sourceMetric := range nodeMetrics {
if d.isNodeExcluded(sourceID) {
continue
}

if sourceMetric.UsagePercent > avgUsage+stdDev {
// Source node is above average
for destID, destMetric := range nodeMetrics {
if sourceID == destID || d.isNodeExcluded(destID) {
continue
}

if destMetric.UsagePercent < avgUsage-stdDev {
// Found a destination below average
candidate := d.evaluateRebalanceOpportunity(
sourceID, sourceMetric,
destID, destMetric,
)
if candidate.CanExecute {
candidates = append(candidates, candidate)
}
}
}
}
}

SortByImbalance(candidates)
return candidates, nil
}

// evaluateRebalanceOpportunity evaluates a single rebalance opportunity
func (d *Detector) evaluateRebalanceOpportunity(
sourceID string, sourceMetric *NodeMetric,
destID string, destMetric *NodeMetric,
) *RebalanceCandidate {
candidate := &RebalanceCandidate{
SourceNodeID:            sourceID,
DestinationNodeID:       destID,
SourceUsagePercent:      sourceMetric.UsagePercent,
DestinationUsagePercent: destMetric.UsagePercent,
}

// Check destination capacity
if !d.checkNodeCapacity(destMetric) {
candidate.CanExecute = false
candidate.Reason = "destination node insufficient free space"
return candidate
}

// Calculate imbalance score
imbalance := math.Abs(float64(sourceMetric.UsagePercent - destMetric.UsagePercent))
candidate.ImbalanceScore = float32(imbalance)

// Check if imbalance exceeds acceptable level
if candidate.ImbalanceScore < d.config.AcceptableImbalance {
candidate.CanExecute = false
candidate.Reason = fmt.Sprintf("imbalance below threshold: %.2f < %.2f", candidate.ImbalanceScore, d.config.AcceptableImbalance)
return candidate
}

// Calculate data to move (simplified)
candidate.DataToMove = uint64(sourceMetric.UsedSpace / 10)
candidate.ExpectedBenefit = candidate.ImbalanceScore / 2

candidate.CanExecute = true
candidate.Priority = int(candidate.ImbalanceScore)
candidate.Reason = "eligible for rebalancing"

return candidate
}

// checkNodeCapacity checks if destination node has sufficient capacity
func (d *Detector) checkNodeCapacity(metric *NodeMetric) bool {
freeSpacePercent := 100 - metric.UsagePercent
return freeSpacePercent > 20 // Need at least 20% free
}

// calculateClusterStats calculates average usage and standard deviation
func (d *Detector) calculateClusterStats(nodeMetrics map[string]*NodeMetric) (float32, float32) {
if len(nodeMetrics) == 0 {
return 0, 0
}

var sum float32
for _, metric := range nodeMetrics {
sum += metric.UsagePercent
}

avg := sum / float32(len(nodeMetrics))

var sumDiffSq float32
for _, metric := range nodeMetrics {
diff := metric.UsagePercent - avg
sumDiffSq += diff * diff
}

variance := sumDiffSq / float32(len(nodeMetrics))
stdDev := float32(math.Sqrt(float64(variance)))

return avg, stdDev
}

// isNodeExcluded checks if a node is in the exclusion list
func (d *Detector) isNodeExcluded(nodeID string) bool {
for _, excluded := range d.config.ExcludeNodes {
if excluded == nodeID {
return true
}
}
return false
}

// NodeMetric contains node statistics
type NodeMetric struct {
NodeID        string
TotalSpace    uint64
UsedSpace     uint64
FreeSpace     uint64
UsagePercent  float32
VolumeCount   int
LastUpdated   int64
IsHealthy     bool
}

// SortByImbalance sorts candidates by imbalance score
func SortByImbalance(candidates []*RebalanceCandidate) {
for i := 0; i < len(candidates); i++ {
for j := i + 1; j < len(candidates); j++ {
if candidates[j].ImbalanceScore > candidates[i].ImbalanceScore {
candidates[i], candidates[j] = candidates[j], candidates[i]
}
}
}
}
