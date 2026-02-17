package vacuum

import (
	"sort"
)

// VacuumCandidate represents a volume eligible for vacuum
type VacuumCandidate struct {
	VolumeID           uint32
	DataNodeID         string
	Size               uint64
	UsedSpace          uint64
	DeadSpace          uint64
	DeadSpacePercent   float64
	ReplicaCount       int
	RackID             string
	DataCenterID       string
	FileCount          int64
	LastModified       int64
	CanVacuum          bool
	FragmentationScore float64
	Reason             string
}

// DetectionOptions contains options for detection
type DetectionOptions struct {
	MinVolumeSize      uint64
	MaxVolumeSize      uint64
	DeadSpaceThreshold int
	TargetUtilization  int
	ExcludeNodes       []string
	PreferredNodes     []string
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

	d.SortByFragmentation(candidates)
	return candidates, nil
}

// evaluateVolume checks if a volume should be vacuumed
func (d *Detector) evaluateVolume(volumeID uint32, metric *VolumeMetric) (*VacuumCandidate, bool) {
	deadSpace := metric.Size - metric.UsedSpace
	deadSpacePercent := 0.0
	if metric.Size > 0 {
		deadSpacePercent = float64(deadSpace) * 100.0 / float64(metric.Size)
	}

	candidate := &VacuumCandidate{
		VolumeID:         volumeID,
		DataNodeID:       metric.DataNodeID,
		Size:             metric.Size,
		UsedSpace:        metric.UsedSpace,
		DeadSpace:        deadSpace,
		DeadSpacePercent: deadSpacePercent,
		ReplicaCount:     metric.ReplicaCount,
		RackID:           metric.RackID,
		DataCenterID:     metric.DataCenterID,
		FileCount:        metric.FileCount,
		LastModified:     metric.LastModified,
	}

	if metric.Size < d.config.MinVolumeSize {
		candidate.CanVacuum = false
		candidate.Reason = "volume too small"
		return candidate, false
	}

	if metric.Size > d.config.MaxVolumeSize {
		candidate.CanVacuum = false
		candidate.Reason = "volume too large"
		return candidate, false
	}

	if int(deadSpacePercent) < d.config.DeadSpaceThreshold {
		candidate.CanVacuum = false
		candidate.Reason = "insufficient dead space"
		return candidate, false
	}

	if d.isNodeExcluded(metric.DataNodeID) {
		candidate.CanVacuum = false
		candidate.Reason = "node excluded"
		return candidate, false
	}

	if len(d.config.PreferredNodes) > 0 && !d.isPreferredNode(metric.DataNodeID) {
		candidate.CanVacuum = false
		candidate.Reason = "node not preferred"
		return candidate, false
	}

	if metric.IsRebalancing {
		candidate.CanVacuum = false
		candidate.Reason = "volume rebalancing"
		return candidate, false
	}

	utilization := 0
	if metric.Size > 0 {
		utilization = int(float64(metric.UsedSpace) * 100.0 / float64(metric.Size))
	}

	candidate.CanVacuum = true
	candidate.FragmentationScore = calculateFragmentationScore(deadSpacePercent, float64(utilization))
	return candidate, true
}

// isNodeExcluded checks if a node is excluded
func (d *Detector) isNodeExcluded(nodeID string) bool {
	for _, excluded := range d.config.ExcludeNodes {
		if excluded == nodeID {
			return true
		}
	}
	return false
}

// isPreferredNode checks if a node is preferred
func (d *Detector) isPreferredNode(nodeID string) bool {
	for _, preferred := range d.config.PreferredNodes {
		if preferred == nodeID {
			return true
		}
	}
