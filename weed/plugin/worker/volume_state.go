package pluginworker

import (
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VolumeState controls which volumes participate in detection (e.g. balance,
// vacuum). It is passed to FilterMetricsByVolumeState by the various plugin
// handlers.
type VolumeState string

const (
	VolumeStateAll    VolumeState = "ALL"
	VolumeStateActive VolumeState = "ACTIVE"
	VolumeStateFull   VolumeState = "FULL"
)

// FilterMetricsByVolumeState filters volume metrics by state.
// "ACTIVE" keeps volumes with FullnessRatio < 1.01 (writable, below size limit).
// "FULL" keeps volumes with FullnessRatio >= 1.01 (read-only, above size limit).
// "ALL" or any other value returns all metrics unfiltered.
func FilterMetricsByVolumeState(metrics []*workertypes.VolumeHealthMetrics, state VolumeState) []*workertypes.VolumeHealthMetrics {
	const fullnessThreshold = 1.01

	var predicate func(m *workertypes.VolumeHealthMetrics) bool
	switch state {
	case VolumeStateActive:
		predicate = func(m *workertypes.VolumeHealthMetrics) bool {
			return m.FullnessRatio < fullnessThreshold
		}
	case VolumeStateFull:
		predicate = func(m *workertypes.VolumeHealthMetrics) bool {
			return m.FullnessRatio >= fullnessThreshold
		}
	default:
		return metrics
	}

	filtered := make([]*workertypes.VolumeHealthMetrics, 0, len(metrics))
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if predicate(m) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

// FilterMetricsByLocation filters volume metrics by data center, rack, and node
// wildcards. Empty filters match anything in their respective dimension.
func FilterMetricsByLocation(metrics []*workertypes.VolumeHealthMetrics, dcFilter, rackFilter, nodeFilter string) []*workertypes.VolumeHealthMetrics {
	dcMatchers := wildcard.CompileWildcardMatchers(dcFilter)
	rackMatchers := wildcard.CompileWildcardMatchers(rackFilter)
	nodeMatchers := wildcard.CompileWildcardMatchers(nodeFilter)

	filtered := make([]*workertypes.VolumeHealthMetrics, 0, len(metrics))
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if !wildcard.MatchesAnyWildcard(dcMatchers, m.DataCenter) {
			continue
		}
		if !wildcard.MatchesAnyWildcard(rackMatchers, m.Rack) {
			continue
		}
		if !wildcard.MatchesAnyWildcard(nodeMatchers, m.Server) {
			continue
		}
		filtered = append(filtered, m)
	}
	return filtered
}
