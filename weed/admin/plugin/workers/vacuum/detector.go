package vacuum

import (
	"fmt"
)

// VacuumCandidate represents a volume eligible for vacuum
type VacuumCandidate struct {
	VolumeID        uint32
	DataNodeID      string
	Size            uint64
	UsedSpace       uint64
	DeadSpace       uint64
	DeadSpacePercent float32
	FragmentationScore float32
	RackID          string
	CanVacuum       bool
	Reason          string
}

// DetectionOptions contains options for detection
type DetectionOptions struct {
	MinVolumeSize      uint64
	MaxVolumeSize      uint64
	DeadSpaceThreshold float32
	PreferredNodes    []string
	ExcludeNodes      []string
}

// Detector scans for vacuum candidates
type Detector struct {
	config DetectionOptions
}

// NewDetector creates a new vacuum detector
func NewDetector(opts DetectionOptions) *Detector {
	return &Detector{
		config: opts,
	}
}

// DetectJobs scans volumes for vacuum candidates
func (d *Detector) DetectJobs(volumeMetrics map[uint32]*VolumeMetric) ([]*VacuumCandidate, error) {
	candidates := make([]*VacuumCandidate, 0)

	for volumeID, metric := range volumeMetrics {
		candidate, shouldInclude := d.evaluateVolume(volumeID, metric)
		if shouldInclude {
			candidates = append(candidates, candidate)
		}
	}

	SortByFragmentation(candidates)
	return candidates, nil
}

// evaluateVolume checks if a volume should be vacuumed
func (d *Detector) evaluateVolume(volumeID uint32, metric *VolumeMetric) (*VacuumCandidate, bool) {
	candidate := &VacuumCandidate{
		VolumeID:   volumeID,
		DataNodeID: metric.DataNodeID,
		Size:       metric.Size,
		UsedSpace:  metric.UsedSpace,
		RackID:     metric.RackID,
	}

	// Check size constraints
	if metric.Size < d.config.MinVolumeSize {
		candidate.CanVacuum = false
		candidate.Reason = fmt.Sprintf("volume too small: %d < %d", metric.Size, d.config.MinVolumeSize)
		return candidate, false
	}

	if metric.Size > d.config.MaxVolumeSize {
		candidate.CanVacuum = false
		candidate.Reason = fmt.Sprintf("volume too large: %d > %d", metric.Size, d.config.MaxVolumeSize)
		return candidate, false
	}

	// Calculate dead space
	deadSpace := metric.Size - metric.UsedSpace
	deadSpacePercent := float32(deadSpace) * 100 / float32(metric.Size)
	candidate.DeadSpace = deadSpace
	candidate.DeadSpacePercent = deadSpacePercent

	// Check dead space threshold
	if deadSpacePercent < d.config.DeadSpaceThreshold {
		candidate.CanVacuum = false
		candidate.Reason = fmt.Sprintf("insufficient dead space: %.2f%% < %.2f%%", deadSpacePercent, d.config.DeadSpaceThreshold)
		return candidate, false
	}

	// Check node exclusion
	if d.isNodeExcluded(metric.DataNodeID) {
		candidate.CanVacuum = false
		candidate.Reason = "node is in exclusion list"
		return candidate, false
	}

	// Check node preference
	if len(d.config.PreferredNodes) > 0 && !d.isPreferredNode(metric.DataNodeID) {
		candidate.CanVacuum = false
		candidate.Reason = "node not in preferred list"
		return candidate, false
	}

	// Calculate fragmentation score
	candidate.FragmentationScore = d.calculateFragmentationScore(metric)

	candidate.CanVacuum = true
	candidate.Reason = "eligible for vacuum"
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

// calculateFragmentationScore calculates how fragmented a volume is
func (d *Detector) calculateFragmentationScore(metric *VolumeMetric) float32 {
	if metric.Size == 0 {
		return 0
	}
	deadSpace := metric.Size - metric.UsedSpace
	return float32(deadSpace) * 100 / float32(metric.Size)
}

// VolumeMetric contains volume statistics
type VolumeMetric struct {
	VolumeID   uint32
	DataNodeID string
	Size       uint64
	UsedSpace  uint64
	FileCount  int64
	LastVacuumTime int64
	RackID     string
	Collection string
}

// SortByFragmentation sorts candidates by fragmentation score
func SortByFragmentation(candidates []*VacuumCandidate) {
	// Simple bubble sort for demonstration
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].FragmentationScore > candidates[i].FragmentationScore {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}
}
