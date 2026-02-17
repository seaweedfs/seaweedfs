package erasure_coding

import (
	"fmt"
	"strings"
)

// CandidateVolume represents a volume eligible for EC
type CandidateVolume struct {
	VolumeID      uint32
	DataNodeID    string
	Size          uint64
	FreeSpace     uint64
	ReplicaCount  int
	RackID        string
	DataCenterID  string
	FileCount     int64
	LastModified  int64
	CanEncode     bool
	Reason        string
}

// DetectionOptions contains options for detection
type DetectionOptions struct {
	MinVolumeSize     uint64
	MaxVolumeSize     uint64
	RackAwareness     bool
	DataCenterAwareness bool
	PreferredNodes    []string
	ExcludeNodes      []string
}

// Detector scans for EC candidates
type Detector struct {
	config DetectionOptions
}

// NewDetector creates a new EC detector
func NewDetector(opts DetectionOptions) *Detector {
	return &Detector{
		config: opts,
	}
}

// DetectJobs scans volumes for EC candidates
func (d *Detector) DetectJobs(volumeMetrics map[uint32]*VolumeMetric) ([]*CandidateVolume, error) {
	candidates := make([]*CandidateVolume, 0)

	for volumeID, metric := range volumeMetrics {
		candidate, shouldInclude := d.evaluateVolume(volumeID, metric)
		if shouldInclude {
			candidates = append(candidates, candidate)
		}
	}

	return candidates, nil
}

// evaluateVolume checks if a volume should be encoded
func (d *Detector) evaluateVolume(volumeID uint32, metric *VolumeMetric) (*CandidateVolume, bool) {
	candidate := &CandidateVolume{
		VolumeID:      volumeID,
		DataNodeID:    metric.DataNodeID,
		Size:          metric.Size,
		FreeSpace:     metric.FreeSpace,
		ReplicaCount:  metric.ReplicaCount,
		RackID:        metric.RackID,
		DataCenterID:  metric.DataCenterID,
		FileCount:     metric.FileCount,
		LastModified:  metric.LastModified,
	}

	// Check size constraints
	if metric.Size < d.config.MinVolumeSize {
		candidate.CanEncode = false
		candidate.Reason = fmt.Sprintf("volume too small: %d < %d", metric.Size, d.config.MinVolumeSize)
		return candidate, false
	}

	if metric.Size > d.config.MaxVolumeSize {
		candidate.CanEncode = false
		candidate.Reason = fmt.Sprintf("volume too large: %d > %d", metric.Size, d.config.MaxVolumeSize)
		return candidate, false
	}

	// Check if already encoded
	if metric.IsEncoded {
		candidate.CanEncode = false
		candidate.Reason = "volume already encoded"
		return candidate, false
	}

	// Check replica count for optimization potential
	if metric.ReplicaCount <= 1 {
		candidate.CanEncode = false
		candidate.Reason = "insufficient replication for encoding"
		return candidate, false
	}

	// Check node exclusion
	if d.isNodeExcluded(metric.DataNodeID) {
		candidate.CanEncode = false
		candidate.Reason = "node is in exclusion list"
		return candidate, false
	}

	// Check node preference
	if len(d.config.PreferredNodes) > 0 && !d.isPreferredNode(metric.DataNodeID) {
		candidate.CanEncode = false
		candidate.Reason = "node not in preferred list"
		return candidate, false
	}

	// Check rack awareness if enabled
	if d.config.RackAwareness && metric.RackID == "" {
		candidate.CanEncode = false
		candidate.Reason = "rack awareness enabled but no rack information"
		return candidate, false
	}

	// Check data center awareness if enabled
	if d.config.DataCenterAwareness && metric.DataCenterID == "" {
		candidate.CanEncode = false
		candidate.Reason = "data center awareness enabled but no data center information"
		return candidate, false
	}

	// Check if volume has aged enough (at least 1 hour old)
	if metric.LastModified == 0 {
		candidate.CanEncode = false
		candidate.Reason = "volume too new, needs aging"
		return candidate, false
	}

	// Volume is a candidate
	candidate.CanEncode = true
	candidate.Reason = "eligible for erasure coding"
	return candidate, true
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

// isPreferredNode checks if a node is in the preferred list
func (d *Detector) isPreferredNode(nodeID string) bool {
	for _, preferred := range d.config.PreferredNodes {
		if preferred == nodeID {
			return true
		}
	}
	return false
}

// VolumeMetric contains volume statistics
type VolumeMetric struct {
	VolumeID      uint32
	DataNodeID    string
	Size          uint64
	FreeSpace     uint64
	ReplicaCount  int
	RackID        string
	DataCenterID  string
	FileCount     int64
	LastModified  int64
	IsEncoded     bool
	CanResize     bool
	Collection    string
	CompactionSize int64
}

// FilterByCriteria filters volumes by specific criteria
func FilterByCriteria(candidates []*CandidateVolume, criteria map[string]string) []*CandidateVolume {
	filtered := make([]*CandidateVolume, 0)

	for _, candidate := range candidates {
		if !candidate.CanEncode {
			continue
		}

		// Apply collection filter if specified
		if collection, ok := criteria["collection"]; ok && collection != "" {
			// Would need collection info in candidate; skipping for now
			continue
		}

		// Apply rack filter if specified
		if rack, ok := criteria["rack"]; ok && rack != "" && candidate.RackID != rack {
			continue
		}

		// Apply data center filter if specified
		if dc, ok := criteria["datacenter"]; ok && dc != "" && candidate.DataCenterID != dc {
			continue
		}

		filtered = append(filtered, candidate)
	}

	return filtered
}

// SortByPriority sorts candidates by encoding priority
func SortByPriority(candidates []*CandidateVolume) {
	// In a real implementation, this would use a sorting algorithm
	// For now, candidates are already in order
}

// GroupByRack groups candidates by rack for distributed encoding
func GroupByRack(candidates []*CandidateVolume) map[string][]*CandidateVolume {
	grouped := make(map[string][]*CandidateVolume)

	for _, candidate := range candidates {
		rackID := candidate.RackID
		if rackID == "" {
			rackID = "default"
		}
		grouped[rackID] = append(grouped[rackID], candidate)
	}

	return grouped
}
