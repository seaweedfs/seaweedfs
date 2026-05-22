package ecbalancer

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// FromActiveTopology builds a Topology snapshot from the cluster's ActiveTopology
// using the reservation-aware effective-capacity view that EC encode and repair
// rely on. It collects ALL EC-eligible disks with no hard disk-type filter;
// disk-type preference is applied later by callers (Place). Per-disk free EC shard
// slots come from GetEffectiveAvailableEcShardSlots (shard-granular, so in-flight
// task reservations that are not whole-volume multiples are not lost) minus the EC
// shards already persisted on the disk. Rack keys are composite "dc:rack".
//
// dataShards is the target collection's data-shard count, used to size free EC
// shard slots correctly for custom ratios (a 4+2 volume's shards are larger, so
// fewer fit per volume slot). Pass <= 0 for the default scheme. Because of this,
// the snapshot is ratio-specific; build one per collection ratio being placed.
//
// This is the encode/repair-side constructor; balance keeps its own raw-topology
// builder (buildBalancerTopology) until the snapshot sources are reconciled.
func FromActiveTopology(at *topology.ActiveTopology, dataShards int) *Topology {
	topo := NewTopology()
	if at == nil {
		return topo
	}

	disks := at.GetDisksWithEffectiveCapacity(topology.TaskTypeErasureCoding, "", 0)

	// Accumulate node-level free slots and group the node's disks together.
	nodeFree := make(map[string]int)
	nodeDC := make(map[string]string)
	nodeRack := make(map[string]string)
	byNode := make(map[string][]*topology.DiskInfo)
	for _, d := range disks {
		if d == nil || d.DiskInfo == nil {
			continue
		}
		if free := perDiskFreeECSlots(at, d, dataShards); free > 0 {
			nodeFree[d.NodeID] += free
		}
		nodeDC[d.NodeID] = d.DataCenter
		nodeRack[d.NodeID] = d.DataCenter + ":" + d.Rack
		byNode[d.NodeID] = append(byNode[d.NodeID], d)
	}

	for nodeID, ds := range byNode {
		node := topo.AddNode(nodeID, nodeDC[nodeID], nodeRack[nodeID], nodeFree[nodeID])
		for _, d := range ds {
			free := perDiskFreeECSlots(at, d, dataShards)
			if free < 0 {
				free = 0
			}
			node.AddDisk(d.DiskID, d.DiskType, free, ecShardCountOnDisk(d))
			for _, eci := range d.DiskInfo.EcShardInfos {
				if eci.DiskId != d.DiskID {
					continue
				}
				node.AddShards(eci.Id, eci.Collection, d.DiskID, erasure_coding.ShardBits(eci.EcIndexBits))
			}
		}
	}

	return topo
}

// perDiskFreeECSlots returns the disk's free EC shard slots for a volume with
// dataShards data shards: the topology's shard-granular effective availability
// (accounting for in-flight task reservations without truncating sub-volume
// reservations, and sized by the target ratio) minus the EC shards already
// persisted on the disk.
func perDiskFreeECSlots(at *topology.ActiveTopology, d *topology.DiskInfo, dataShards int) int {
	return at.GetEffectiveAvailableEcShardSlots(d.NodeID, d.DiskID, dataShards) - ecShardCountOnDisk(d)
}

// ecShardCountOnDisk counts the EC shards physically on this disk (matching
// eci.DiskId), across all volumes.
func ecShardCountOnDisk(d *topology.DiskInfo) int {
	count := 0
	for _, eci := range d.DiskInfo.EcShardInfos {
		if eci.DiskId == d.DiskID {
			count += erasure_coding.GetShardCount(eci)
		}
	}
	return count
}
