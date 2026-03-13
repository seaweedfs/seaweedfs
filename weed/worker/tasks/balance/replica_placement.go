package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// IsGoodMove checks whether moving a volume from sourceNodeID to target
// would satisfy the volume's replica placement policy, given the current
// set of replica locations.
func IsGoodMove(rp *super_block.ReplicaPlacement, existingReplicas []types.ReplicaLocation, sourceNodeID string, target types.ReplicaLocation) bool {
	if rp == nil || !rp.HasReplication() {
		return true // no replication constraint
	}

	// Build the replica set after the move: remove source, add target
	afterMove := make([]types.ReplicaLocation, 0, len(existingReplicas))
	for _, r := range existingReplicas {
		if r.NodeID != sourceNodeID {
			afterMove = append(afterMove, r)
		}
	}

	return satisfyReplicaPlacement(rp, afterMove, target)
}

// satisfyReplicaPlacement checks whether placing a replica at target
// is consistent with the replication policy, given the existing replicas.
// Ported from weed/shell/command_volume_fix_replication.go
func satisfyReplicaPlacement(rp *super_block.ReplicaPlacement, replicas []types.ReplicaLocation, target types.ReplicaLocation) bool {
	existingDCs, _, existingNodes := countReplicas(replicas)

	targetNodeKey := target.DataCenter + " " + target.Rack + " " + target.NodeID
	if _, found := existingNodes[targetNodeKey]; found {
		// avoid duplicated volume on the same data node
		return false
	}

	primaryDCs, _ := findTopKeys(existingDCs)

	// ensure data center count is within limit
	if _, found := existingDCs[target.DataCenter]; !found {
		// different from existing dcs
		if len(existingDCs) < rp.DiffDataCenterCount+1 {
			return true
		}
		return false
	}
	// now same as one of existing data centers
	if !isAmong(target.DataCenter, primaryDCs) {
		return false
	}

	// now on a primary dc - check racks within this DC
	primaryDcRacks := make(map[string]int)
	for _, r := range replicas {
		if r.DataCenter != target.DataCenter {
			continue
		}
		rackKey := r.DataCenter + " " + r.Rack
		primaryDcRacks[rackKey]++
	}

	targetRackKey := target.DataCenter + " " + target.Rack
	primaryRacks, _ := findTopKeys(primaryDcRacks)
	sameRackCount := primaryDcRacks[targetRackKey]

	if _, found := primaryDcRacks[targetRackKey]; !found {
		// different from existing racks
		if len(primaryDcRacks) < rp.DiffRackCount+1 {
			return true
		}
		return false
	}
	// same as one of existing racks
	if !isAmong(targetRackKey, primaryRacks) {
		return false
	}

	// on primary rack - check same-rack count
	if sameRackCount < rp.SameRackCount+1 {
		return true
	}
	return false
}

func countReplicas(replicas []types.ReplicaLocation) (dcCounts, rackCounts, nodeCounts map[string]int) {
	dcCounts = make(map[string]int)
	rackCounts = make(map[string]int)
	nodeCounts = make(map[string]int)
	for _, r := range replicas {
		dcCounts[r.DataCenter]++
		rackKey := r.DataCenter + " " + r.Rack
		rackCounts[rackKey]++
		nodeKey := r.DataCenter + " " + r.Rack + " " + r.NodeID
		nodeCounts[nodeKey]++
	}
	return
}

func findTopKeys(m map[string]int) (topKeys []string, max int) {
	for k, c := range m {
		if max < c {
			topKeys = topKeys[:0]
			topKeys = append(topKeys, k)
			max = c
		} else if max == c {
			topKeys = append(topKeys, k)
		}
	}
	return
}

func isAmong(key string, keys []string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
