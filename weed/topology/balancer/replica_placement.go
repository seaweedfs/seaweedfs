package balancer

import (
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Location identifies where a volume replica lives, at the granularity replica
// placement reasons about: data center, rack, data node, and physical host
// (machine). Servers sharing a host are one fault domain. It is the shared shape
// the shell (from master_pb) and the worker (from ActiveTopology) both adapt to.
type Location struct {
	DataCenter string
	Rack       string
	NodeID     string
	Host       string
}

// HostFromAddress returns the physical machine (host/IP) of a server address,
// falling back to the node id (== ip:port in the common case) when no address is
// known.
func HostFromAddress(address, nodeID string) string {
	if address == "" {
		address = nodeID
	}
	return pb.ServerAddress(address).ToHost()
}

type rackKey struct {
	DataCenter string
	Rack       string
}

type nodeKey struct {
	DataCenter string
	Rack       string
	NodeID     string
}

// IsGoodMove reports whether moving a volume from sourceNodeID onto target keeps
// the volume's replica placement policy satisfied, given the current replica
// locations. It refuses moving onto the source itself, onto a node that already
// holds a replica, and onto a host that already holds another replica (best-effort
// machine anti-affinity), then defers to SatisfyReplicaPlacement.
func IsGoodMove(rp *super_block.ReplicaPlacement, existingReplicas []Location, sourceNodeID string, target Location) bool {
	if rp == nil || !rp.HasReplication() {
		return true // no replication constraint
	}

	// Never move onto the source node itself.
	if target.NodeID == sourceNodeID {
		return false
	}

	// Build the replica set after the move: remove the one replica being moved off
	// the source, keeping any others (an over-replicated node may hold more than
	// one replica of this volume, and only one is moving).
	afterMove := make([]Location, 0, len(existingReplicas))
	sourceFound := false
	for _, r := range existingReplicas {
		if r.NodeID == sourceNodeID && !sourceFound {
			sourceFound = true
		} else {
			afterMove = append(afterMove, r)
		}
	}
	if !sourceFound {
		// Source not in the replica list — cluster state may be inconsistent.
		// Treat as unsafe to avoid an incorrect placement decision.
		return false
	}

	// Best-effort machine anti-affinity: don't move a replica onto a host that
	// already holds another replica of this volume, so a single machine failure
	// can't take out two replicas.
	if target.Host != "" {
		for _, r := range afterMove {
			if r.Host == target.Host {
				return false
			}
		}
	}

	return SatisfyReplicaPlacement(rp, afterMove, target)
}

// SatisfyReplicaPlacement reports whether placing a replica at target is
// consistent with the replication policy given the existing replicas. It is the
// shared implementation for the shell (volume.balance / fix.replication /
// tier.move) and the maintenance balance worker.
func SatisfyReplicaPlacement(rp *super_block.ReplicaPlacement, replicas []Location, target Location) bool {
	existingDCs, _, existingNodes := countReplicas(replicas)

	targetNK := nodeKey{DataCenter: target.DataCenter, Rack: target.Rack, NodeID: target.NodeID}
	if _, found := existingNodes[targetNK]; found {
		// avoid a duplicated volume on the same data node
		return false
	}

	primaryDCs, _ := findTopDCKeys(existingDCs)

	// ensure the data center count is within limit
	if _, found := existingDCs[target.DataCenter]; !found {
		// different from existing dcs
		return len(existingDCs) < rp.DiffDataCenterCount+1
	}
	// same as one of the existing data centers
	if !slices.Contains(primaryDCs, target.DataCenter) {
		return false
	}

	// on a primary dc — check racks within this DC
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
		return len(primaryDcRacks) < rp.DiffRackCount+1
	}
	// same as one of the existing racks
	if !slices.Contains(primaryRacks, targetRK) {
		return false
	}

	// on the primary rack — check the same-rack count
	return sameRackCount < rp.SameRackCount+1
}

func countReplicas(replicas []Location) (dcCounts map[string]int, rackCounts map[rackKey]int, nodeCounts map[nodeKey]int) {
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
