package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// hostFromAddress returns the physical machine (host/IP) of a server address,
// falling back to the node id when no address is known.
func hostFromAddress(address, nodeID string) string {
	return balancer.HostFromAddress(address, nodeID)
}

// IsGoodMove checks whether moving a volume from sourceNodeID to target would
// satisfy the volume's replica placement policy. It is a thin adapter over the
// shared placement logic in weed/topology/balancer so the shell command and this
// worker cannot drift.
func IsGoodMove(rp *super_block.ReplicaPlacement, existingReplicas []types.ReplicaLocation, sourceNodeID string, target types.ReplicaLocation) bool {
	locs := make([]balancer.Location, len(existingReplicas))
	for i, r := range existingReplicas {
		locs[i] = toBalancerLocation(r)
	}
	return balancer.IsGoodMove(rp, locs, sourceNodeID, toBalancerLocation(target))
}

// toBalancerLocation copies a worker replica location to the shared placement
// type field-by-field, so a future change to either struct is a compile error
// here rather than a silent mismatch.
func toBalancerLocation(r types.ReplicaLocation) balancer.Location {
	return balancer.Location{
		DataCenter: r.DataCenter,
		Rack:       r.Rack,
		NodeID:     r.NodeID,
		Host:       r.Host,
	}
}
