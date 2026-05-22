package ecbalancer

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// FromActiveTopology builds a Topology snapshot from the cluster's ActiveTopology
// using the reservation-aware effective-capacity view that EC encode and repair
// rely on (GetDisksWithEffectiveCapacity already accounts for in-flight tasks).
//
// It collects ALL EC-eligible disks with no hard disk-type filter; disk-type
// preference is applied later as a soft tier by callers. Per-disk free EC shard
// slots are free-volume-slots * DataShardsCount minus the EC shards already on the
// disk, matching buildBalancerTopology's accounting — but kept per physical disk,
// since GetDisksWithEffectiveCapacity separates disks (no even-split needed). Rack
// keys are composite "dc:rack" to avoid cross-DC name collisions.
//
// This is the encode/repair-side constructor; balance keeps its own raw-topology
// builder (buildBalancerTopology) until the snapshot sources are reconciled.
func FromActiveTopology(at *topology.ActiveTopology) *Topology {
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
		if free := perDiskFreeECSlots(d); free > 0 {
			nodeFree[d.NodeID] += free
		}
		nodeDC[d.NodeID] = d.DataCenter
		nodeRack[d.NodeID] = d.DataCenter + ":" + d.Rack
		byNode[d.NodeID] = append(byNode[d.NodeID], d)
	}

	for nodeID, ds := range byNode {
		node := topo.AddNode(nodeID, nodeDC[nodeID], nodeRack[nodeID], nodeFree[nodeID])
		for _, d := range ds {
			free := perDiskFreeECSlots(d)
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

// perDiskFreeECSlots returns the disk's free EC shard slots from the
// effective-capacity view: free volume slots * DataShardsCount minus the EC shards
// already on the disk.
func perDiskFreeECSlots(d *topology.DiskInfo) int {
	info := d.DiskInfo
	freeVol := int(info.MaxVolumeCount - info.VolumeCount)
	if freeVol < 0 {
		freeVol = 0
	}
	return freeVol*erasure_coding.DataShardsCount - ecShardCountOnDisk(d)
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
