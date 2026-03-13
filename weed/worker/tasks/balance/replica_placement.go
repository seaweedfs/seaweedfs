package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// rackKey uniquely identifies a rack within a data center.
type rackKey struct {
	DataCenter string
	Rack       string
}

// nodeKey uniquely identifies a node within a rack.
type nodeKey struct {
	DataCenter string
	Rack       string
	NodeID     string
}

// IsGoodMove checks whether moving a volume from sourceNodeID to target
// would satisfy the volume's replica placement policy, given the current
// set of replica locations.
func IsGoodMove(rp *super_block.ReplicaPlacement, existingReplicas []types.ReplicaLocation, sourceNodeID string, target types.ReplicaLocation) bool {
	if rp == nil || !rp.HasReplication() {
		return true // no replication constraint
	}

	// Build the replica set after the move: remove source, add target
	afterMove := make([]types.ReplicaLocation, 0, len(existingReplicas))
	sourceFound := false
	for _, r := range existingReplicas {
		if r.NodeID == sourceNodeID {
			sourceFound = true
		} else {
			afterMove = append(afterMove, r)
		}
	}
	if !sourceFound {
		// Source not in replica list — cluster state may be inconsistent.
		// Treat as unsafe to avoid incorrect placement decisions.
		return false
	}

	return satisfyReplicaPlacement(rp, afterMove, target)
}

// satisfyReplicaPlacement checks whether placing a replica at target
// is consistent with the replication policy, given the existing replicas.
// Ported from weed/shell/command_volume_fix_replication.go
func satisfyReplicaPlacement(rp *super_block.ReplicaPlacement, replicas []types.ReplicaLocation, target types.ReplicaLocation) bool {
	existingDCs, _, existingNodes := countReplicas(replicas)

	targetNK := nodeKey{DataCenter: target.DataCenter, Rack: target.Rack, NodeID: target.NodeID}
	if _, found := existingNodes[targetNK]; found {
		// avoid duplicated volume on the same data node
		return false
	}

	primaryDCs, _ := findTopDCKeys(existingDCs)

	// ensure data center count is within limit
	if _, found := existingDCs[target.DataCenter]; !found {
		// different from existing dcs
		if len(existingDCs) < rp.DiffDataCenterCount+1 {
			return true
		}
		return false
	}
	// now same as one of existing data centers
	if !isAmongDC(target.DataCenter, primaryDCs) {
		return false
	}

	// now on a primary dc - check racks within this DC
	primaryDcRacks := make(map[rackKey]int)
	for _, r := range replicas {
		if r.DataCenter != target.DataCenter {
			continue
		}
		primaryDcRacks[rackKey{DataCenter: r.DataCenter, Rack: r.Rack}]++
	}

	targetRK := rackKey{DataCenter: target.DataCenter, Rack: target.Rack}
	primaryRacks, _ := findTopRackKeys(primaryDcRacks)
	sameRackCount := primaryDcRacks[targetRK]

	if _, found := primaryDcRacks[targetRK]; !found {
		// different from existing racks
		if len(primaryDcRacks) < rp.DiffRackCount+1 {
			return true
		}
		return false
	}
	// same as one of existing racks
	if !isAmongRack(targetRK, primaryRacks) {
		return false
	}

	// on primary rack - check same-rack count
	if sameRackCount < rp.SameRackCount+1 {
		return true
	}
	return false
}

func countReplicas(replicas []types.ReplicaLocation) (dcCounts map[string]int, rackCounts map[rackKey]int, nodeCounts map[nodeKey]int) {
	dcCounts = make(map[string]int)
	rackCounts = make(map[rackKey]int)
	nodeCounts = make(map[nodeKey]int)
	for _, r := range replicas {
		dcCounts[r.DataCenter]++
		rackCounts[rackKey{DataCenter: r.DataCenter, Rack: r.Rack}]++
		nodeCounts[nodeKey{DataCenter: r.DataCenter, Rack: r.Rack, NodeID: r.NodeID}]++
	}
	return
}

func findTopDCKeys(m map[string]int) (topKeys []string, max int) {
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

func findTopRackKeys(m map[rackKey]int) (topKeys []rackKey, max int) {
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

func isAmongDC(key string, keys []string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

func isAmongRack(key rackKey, keys []rackKey) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
