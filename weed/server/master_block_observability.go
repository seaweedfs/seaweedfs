package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Health-state constants. Priority: unsafe > rebuilding > degraded > healthy.
const (
	HealthStateHealthy    = "healthy"
	HealthStateDegraded   = "degraded"
	HealthStateRebuilding = "rebuilding"
	HealthStateUnsafe     = "unsafe"
)

// deriveHealthState determines the operator-facing health state for a volume
// from registry facts. This is the shared derivation used by both per-volume
// responses and cluster-level summaries.
//
// Priority: unsafe > rebuilding > degraded > healthy.
func deriveHealthState(entry *BlockVolumeEntry) string {
	role := blockvol.RoleFromWire(entry.Role)

	// unsafe: no primary, primary not alive is handled at cluster level
	// (server liveness is not available on the entry itself).
	// Entry-level unsafe: role is not primary, or volume has failed control state.
	if role != blockvol.RolePrimary {
		return HealthStateUnsafe
	}

	// unsafe: strict durability below required replica count.
	if entry.DurabilityMode == "sync_all" || entry.DurabilityMode == "sync_quorum" {
		durMode, _ := blockvol.ParseDurabilityMode(entry.DurabilityMode)
		rf := entry.ReplicaFactor
		if rf == 0 {
			rf = 2
		}
		required := durMode.RequiredReplicas(rf)
		if len(entry.Replicas) < required {
			return HealthStateUnsafe
		}
	}

	// rebuilding: any replica in rebuild state.
	for _, ri := range entry.Replicas {
		riRole := blockvol.RoleFromWire(ri.Role)
		if riRole == blockvol.RoleRebuilding {
			return HealthStateRebuilding
		}
	}

	// degraded: actual replicas below desired.
	rf := entry.ReplicaFactor
	if rf == 0 {
		rf = 2
	}
	desiredReplicas := rf - 1 // RF includes primary
	if desiredReplicas > 0 && len(entry.Replicas) < desiredReplicas {
		return HealthStateDegraded
	}

	// degraded: replica degraded flag set.
	if entry.ReplicaDegraded {
		return HealthStateDegraded
	}

	return HealthStateHealthy
}

// deriveHealthStateWithLiveness adds server-liveness awareness.
// primaryAlive comes from the registry's blockServers check.
func deriveHealthStateWithLiveness(entry *BlockVolumeEntry, primaryAlive bool) string {
	if !primaryAlive {
		return HealthStateUnsafe
	}
	return deriveHealthState(entry)
}

// clusterHealthSummary holds aggregated health counts for the cluster summary.
type clusterHealthSummary struct {
	Healthy    int
	Degraded   int
	Rebuilding int
	Unsafe     int
}

// computeClusterHealthSummary iterates all volumes and computes health counts.
// Uses server liveness from the registry for accurate unsafe detection.
func (r *BlockVolumeRegistry) ComputeClusterHealthSummary() clusterHealthSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var summary clusterHealthSummary
	for _, entry := range r.volumes {
		primaryAlive := r.blockServers[entry.VolumeServer] != nil
		state := deriveHealthStateWithLiveness(entry, primaryAlive)
		switch state {
		case HealthStateHealthy:
			summary.Healthy++
		case HealthStateDegraded:
			summary.Degraded++
		case HealthStateRebuilding:
			summary.Rebuilding++
		case HealthStateUnsafe:
			summary.Unsafe++
		}
	}
	return summary
}

// NvmeCapableServerCount returns the number of servers with NVMe enabled.
func (r *BlockVolumeRegistry) NvmeCapableServerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, info := range r.blockServers {
		if info != nil && info.NvmeAddr != "" {
			count++
		}
	}
	return count
}
