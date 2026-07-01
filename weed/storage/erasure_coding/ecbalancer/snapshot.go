package ecbalancer

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
)

// diskTooFull reports whether a disk's physical filesystem is at/above the
// balancing high-water mark, making it ineligible as an EC-shard target even
// when slot math says it has room. Statfs free bytes already include EC shard
// files. No-opinion when the disk reports no bytes (slot-only fallback).
func diskTooFull(d *topology.DiskInfo) bool {
	return balancer.DiskTooFullAfter(d.DiskInfo.DiskTotalBytes, d.DiskInfo.DiskFreeBytes, 0, balancer.DefaultMaxDiskUsagePercent)
}

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
	nodeAddr := make(map[string]string)
	byNode := make(map[string][]*topology.DiskInfo)
	for _, d := range disks {
		if d == nil || d.DiskInfo == nil {
			continue
		}
		if free := perDiskFreeECSlots(at, d, dataShards); free > 0 && !diskTooFull(d) {
			nodeFree[d.NodeID] += free
		}
		nodeDC[d.NodeID] = d.DataCenter
		nodeRack[d.NodeID] = d.DataCenter + ":" + d.Rack
		nodeAddr[d.NodeID] = d.Address
		byNode[d.NodeID] = append(byNode[d.NodeID], d)
	}

	for nodeID, ds := range byNode {
		node := topo.AddNode(nodeID, nodeDC[nodeID], nodeRack[nodeID], nodeFree[nodeID])
		// Group by physical machine: derive the host from the address (NodeID may be
		// an opaque id), falling back to the id when no address is recorded.
		addr := nodeAddr[nodeID]
		if addr == "" {
			addr = nodeID
		}
		node.SetHost(pb.ServerAddress(addr).ToHost())
		for _, d := range ds {
			free := perDiskFreeECSlots(at, d, dataShards)
			if free < 0 || diskTooFull(d) {
				free = 0
			}
			node.AddDisk(d.DiskID, d.DiskType, free, ecShardCountOnDisk(d))
			node.AddDiskTags(d.DiskID, d.DiskInfo.Tags)
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
// (sized by the target ratio) minus the EC shards already on the disk, also
// expressed in the target ratio's shard slots so mixed-ratio disks are charged by
// size rather than raw count.
func perDiskFreeECSlots(at *topology.ActiveTopology, d *topology.DiskInfo, dataShards int) int {
	return at.GetEffectiveAvailableEcShardSlots(d.NodeID, d.DiskID, dataShards) - ecShardSlotsOnDisk(d, dataShards)
}

// ecShardCountOnDisk counts the EC shards physically on this disk (matching
// eci.DiskId), across all volumes. Used as a per-disk load metric for scoring.
func ecShardCountOnDisk(d *topology.DiskInfo) int {
	count := 0
	for _, eci := range d.DiskInfo.EcShardInfos {
		if eci.DiskId == d.DiskID {
			count += erasure_coding.GetShardCount(eci)
		}
	}
	return count
}

// ecShardSlotsOnDisk returns the EC shards already on the disk expressed in the
// TARGET collection's shard slots. A shard of a collection with d data shards
// occupies ~1/d of a volume, i.e. targetDataShards/d target slots, so a 2+1 shard
// counted against a 10+4 snapshot consumes ~5 slots, not 1.
func ecShardSlotsOnDisk(d *topology.DiskInfo, targetDataShards int) int {
	if targetDataShards <= 0 {
		targetDataShards = erasure_coding.DataShardsCount
	}
	total := 0
	for _, eci := range d.DiskInfo.EcShardInfos {
		if eci.DiskId != d.DiskID {
			continue
		}
		ds := shardDataShards(eci)
		if ds <= 0 {
			ds = erasure_coding.DataShardsCount
		}
		// Round up so an existing shard always consumes at least its fractional
		// footprint; flooring lets a low-data-shard volume (targetDataShards < ds)
		// count as zero target slots and overstate the disk's free capacity.
		total += (erasure_coding.GetShardCount(eci)*targetDataShards + ds - 1) / ds
	}
	return total
}
