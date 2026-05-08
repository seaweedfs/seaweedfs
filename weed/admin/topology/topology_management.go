package topology

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// splitDiskInfoByPhysicalDisk returns one master_pb.DiskInfo per physical
// disk_id observed in VolumeInfos / EcShardInfos. Multiple same-type physical
// disks collapse to one DiskInfo at the master; per-volume/per-shard records
// keep the original disk_id and are the authoritative signal here. Capacity
// is split evenly — the wire format doesn't carry per-disk capacity yet.
func splitDiskInfoByPhysicalDisk(diskInfo *master_pb.DiskInfo) []*master_pb.DiskInfo {
	if diskInfo == nil {
		return nil
	}

	// Records with DiskId=0 and a non-zero outer DiskId belong to the outer
	// disk — handles older payloads / fixtures that omit the per-record id.
	normalize := func(id uint32) uint32 {
		if id == 0 && diskInfo.DiskId != 0 {
			return diskInfo.DiskId
		}
		return id
	}

	diskIDs := make(map[uint32]struct{})
	for _, vi := range diskInfo.VolumeInfos {
		diskIDs[normalize(vi.DiskId)] = struct{}{}
	}
	for _, eci := range diskInfo.EcShardInfos {
		diskIDs[normalize(eci.DiskId)] = struct{}{}
	}
	if len(diskIDs) == 0 {
		diskIDs[diskInfo.DiskId] = struct{}{}
	}

	if len(diskIDs) == 1 {
		for diskID := range diskIDs {
			if diskID == diskInfo.DiskId {
				return []*master_pb.DiskInfo{diskInfo}
			}
		}
	}

	perDiskVolumes := make(map[uint32][]*master_pb.VolumeInformationMessage)
	for _, vi := range diskInfo.VolumeInfos {
		perDiskVolumes[normalize(vi.DiskId)] = append(perDiskVolumes[normalize(vi.DiskId)], vi)
	}
	perDiskShards := make(map[uint32][]*master_pb.VolumeEcShardInformationMessage)
	for _, eci := range diskInfo.EcShardInfos {
		perDiskShards[normalize(eci.DiskId)] = append(perDiskShards[normalize(eci.DiskId)], eci)
	}

	count := int64(len(diskIDs))
	share := func(total int64) int64 { return total / count }

	result := make([]*master_pb.DiskInfo, 0, len(diskIDs))
	for diskID := range diskIDs {
		result = append(result, &master_pb.DiskInfo{
			Type:              diskInfo.Type,
			MaxVolumeCount:    share(diskInfo.MaxVolumeCount),
			VolumeCount:       int64(len(perDiskVolumes[diskID])),
			FreeVolumeCount:   share(diskInfo.FreeVolumeCount),
			ActiveVolumeCount: share(diskInfo.ActiveVolumeCount),
			RemoteVolumeCount: share(diskInfo.RemoteVolumeCount),
			VolumeInfos:       perDiskVolumes[diskID],
			EcShardInfos:      perDiskShards[diskID],
			DiskId:            diskID,
			Tags:              append([]string(nil), diskInfo.Tags...),
		})
	}
	return result
}

// CountTopologyResources counts datacenters, nodes, and disks in topology info
func CountTopologyResources(topologyInfo *master_pb.TopologyInfo) (dcCount, nodeCount, diskCount int) {
	if topologyInfo == nil {
		return 0, 0, 0
	}
	dcCount = len(topologyInfo.DataCenterInfos)
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			nodeCount += len(rack.DataNodeInfos)
			for _, node := range rack.DataNodeInfos {
				diskCount += len(node.DiskInfos)
			}
		}
	}
	return
}

// UpdateTopology updates the topology information from master
func (at *ActiveTopology) UpdateTopology(topologyInfo *master_pb.TopologyInfo) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	// Validate topology updates to prevent clearing disk maps with invalid data
	if topologyInfo == nil {
		glog.Warningf("UpdateTopology received nil topologyInfo, preserving last-known-good topology")
		return fmt.Errorf("rejected invalid topology update: nil topologyInfo")
	}

	if len(topologyInfo.DataCenterInfos) == 0 {
		glog.Warningf("UpdateTopology received empty DataCenterInfos, preserving last-known-good topology (had %d nodes, %d disks)",
			len(at.nodes), len(at.disks))
		return fmt.Errorf("rejected invalid topology update: empty DataCenterInfos (had %d nodes, %d disks)", len(at.nodes), len(at.disks))
	}

	// Count incoming topology for validation logging
	dcCount, incomingNodes, incomingDisks := CountTopologyResources(topologyInfo)

	// Reject updates that would wipe out a valid topology with an empty one (e.g. during master restart)
	if incomingNodes == 0 && len(at.nodes) > 0 {
		glog.Warningf("UpdateTopology received topology with 0 nodes, preserving last-known-good topology (had %d nodes, %d disks)",
			len(at.nodes), len(at.disks))
		return fmt.Errorf("rejected invalid topology update: 0 nodes (had %d nodes, %d disks)", len(at.nodes), len(at.disks))
	}

	glog.V(2).Infof("UpdateTopology: validating update with %d datacenters, %d nodes, %d disks (current: %d nodes, %d disks)",
		dcCount, incomingNodes, incomingDisks, len(at.nodes), len(at.disks))

	at.topologyInfo = topologyInfo
	at.lastUpdated = time.Now()

	// Rebuild structured topology
	at.nodes = make(map[string]*activeNode)
	at.disks = make(map[string]*activeDisk)

	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				node := &activeNode{
					nodeID:     nodeInfo.Id,
					dataCenter: dc.Id,
					rack:       rack.Id,
					nodeInfo:   nodeInfo,
					disks:      make(map[uint32]*activeDisk),
				}

				// One activeDisk per physical disk_id (#9369): the master keys
				// DiskInfos by disk type, so same-type disks must be split out.
				for diskType, diskInfo := range nodeInfo.DiskInfos {
					perDiskInfos := splitDiskInfoByPhysicalDisk(diskInfo)
					for _, perDisk := range perDiskInfos {
						disk := &activeDisk{
							DiskInfo: &DiskInfo{
								NodeID:     nodeInfo.Id,
								DiskID:     perDisk.DiskId,
								DiskType:   diskType,
								DataCenter: dc.Id,
								Rack:       rack.Id,
								DiskInfo:   perDisk,
							},
						}

						diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, perDisk.DiskId)
						glog.V(3).Infof("UpdateTopology: adding disk key=%q nodeId=%q diskId=%d diskType=%q address=%q grpcPort=%d volumes=%d maxVolumes=%d",
							diskKey, nodeInfo.Id, perDisk.DiskId, diskType, nodeInfo.Address, nodeInfo.GrpcPort, perDisk.VolumeCount, perDisk.MaxVolumeCount)
						node.disks[perDisk.DiskId] = disk
						at.disks[diskKey] = disk
					}
				}

				at.nodes[nodeInfo.Id] = node
			}
		}
	}

	// Rebuild performance indexes for O(1) lookups
	at.rebuildIndexes()

	// Reassign task states to updated topology
	at.reassignTaskStates()

	glog.V(1).Infof("ActiveTopology updated: %d nodes, %d disks, %d volume entries, %d EC shard entries",
		len(at.nodes), len(at.disks), len(at.volumeIndex), len(at.ecShardIndex))
	return nil
}

// GetAvailableDisks returns disks that can accept new tasks of the given type
// NOTE: For capacity-aware operations, prefer GetDisksWithEffectiveCapacity
func (at *ActiveTopology) GetAvailableDisks(taskType TaskType, excludeNodeID string) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	var available []*DiskInfo

	for _, disk := range at.disks {
		if disk.NodeID == excludeNodeID {
			continue // Skip excluded node
		}

		if at.isDiskAvailable(disk, taskType) {
			// Create a copy with current load count and effective capacity
			diskCopy := *disk.DiskInfo
			diskCopy.LoadCount = len(disk.pendingTasks) + len(disk.assignedTasks)
			available = append(available, &diskCopy)
		}
	}

	return available
}

// HasRecentTaskForVolume checks if a volume had a recent task (to avoid immediate re-detection)
func (at *ActiveTopology) HasRecentTaskForVolume(volumeID uint32, taskType TaskType) bool {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	for _, task := range at.recentTasks {
		if task.VolumeID == volumeID && task.TaskType == taskType {
			return true
		}
	}

	return false
}

// GetAllNodes returns information about all nodes (public interface)
func (at *ActiveTopology) GetAllNodes() map[string]*master_pb.DataNodeInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	result := make(map[string]*master_pb.DataNodeInfo)
	for nodeID, node := range at.nodes {
		result[nodeID] = node.nodeInfo
	}
	return result
}

// GetTopologyInfo returns the current topology information (read-only access)
func (at *ActiveTopology) GetTopologyInfo() *master_pb.TopologyInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()
	return at.topologyInfo
}

// GetNodeDisks returns all disks for a specific node
func (at *ActiveTopology) GetNodeDisks(nodeID string) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	node, exists := at.nodes[nodeID]
	if !exists {
		return nil
	}

	var disks []*DiskInfo
	for _, disk := range node.disks {
		diskCopy := *disk.DiskInfo
		diskCopy.LoadCount = len(disk.pendingTasks) + len(disk.assignedTasks)
		disks = append(disks, &diskCopy)
	}

	return disks
}

// GetDiskCount returns the total number of disks in the active topology
func (at *ActiveTopology) GetDiskCount() int {
	at.mutex.RLock()
	defer at.mutex.RUnlock()
	return len(at.disks)
}

// rebuildIndexes rebuilds the volume and EC shard indexes for O(1) lookups
func (at *ActiveTopology) rebuildIndexes() {
	// Nil-safety guard: return early if topology is not valid
	if at.topologyInfo == nil || at.topologyInfo.DataCenterInfos == nil {
		glog.V(1).Infof("rebuildIndexes: skipping rebuild due to nil topology or DataCenterInfos")
		return
	}

	// Clear existing indexes
	at.volumeIndex = make(map[uint32][]string)
	at.ecShardIndex = make(map[uint32][]string)

	// Index by the per-record DiskId (not the outer DiskInfo.DiskId) so the
	// keys match the per-physical-disk activeDisk entries — see #9369.
	for _, dc := range at.topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				for _, diskInfo := range nodeInfo.DiskInfos {
					for _, volumeInfo := range diskInfo.VolumeInfos {
						diskID := volumeInfo.DiskId
						if diskID == 0 && diskInfo.DiskId != 0 {
							diskID = diskInfo.DiskId
						}
						diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, diskID)
						at.volumeIndex[volumeInfo.Id] = append(at.volumeIndex[volumeInfo.Id], diskKey)
					}
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						diskID := ecShardInfo.DiskId
						if diskID == 0 && diskInfo.DiskId != 0 {
							diskID = diskInfo.DiskId
						}
						diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, diskID)
						at.ecShardIndex[ecShardInfo.Id] = append(at.ecShardIndex[ecShardInfo.Id], diskKey)
					}
				}
			}
		}
	}
}

// GetVolumeLocations returns the disk locations for a volume using O(1) lookup
func (at *ActiveTopology) GetVolumeLocations(volumeID uint32, collection string) []VolumeReplica {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKeys, exists := at.volumeIndex[volumeID]
	if !exists {
		return []VolumeReplica{}
	}

	var replicas []VolumeReplica
	for _, diskKey := range diskKeys {
		if disk, diskExists := at.disks[diskKey]; diskExists {
			// Verify collection matches (since index doesn't include collection)
			if at.volumeMatchesCollection(disk, volumeID, collection) {
				replicas = append(replicas, VolumeReplica{
					ServerID:   disk.NodeID,
					DiskID:     disk.DiskID,
					DataCenter: disk.DataCenter,
					Rack:       disk.Rack,
				})
			}
		}
	}

	return replicas
}

// GetECShardLocations returns the disk locations for EC shards using O(1) lookup
func (at *ActiveTopology) GetECShardLocations(volumeID uint32, collection string) []VolumeReplica {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKeys, exists := at.ecShardIndex[volumeID]
	if !exists {
		return []VolumeReplica{}
	}

	var ecShards []VolumeReplica
	for _, diskKey := range diskKeys {
		if disk, diskExists := at.disks[diskKey]; diskExists {
			// Verify collection matches (since index doesn't include collection)
			if at.ecShardMatchesCollection(disk, volumeID, collection) {
				ecShards = append(ecShards, VolumeReplica{
					ServerID:   disk.NodeID,
					DiskID:     disk.DiskID,
					DataCenter: disk.DataCenter,
					Rack:       disk.Rack,
				})
			}
		}
	}

	return ecShards
}

// volumeMatchesCollection checks if a volume on a disk matches the given collection
func (at *ActiveTopology) volumeMatchesCollection(disk *activeDisk, volumeID uint32, collection string) bool {
	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return false
	}

	for _, volumeInfo := range disk.DiskInfo.DiskInfo.VolumeInfos {
		if volumeInfo.Id == volumeID && volumeInfo.Collection == collection {
			return true
		}
	}
	return false
}

// ecShardMatchesCollection checks if EC shards on a disk match the given collection
func (at *ActiveTopology) ecShardMatchesCollection(disk *activeDisk, volumeID uint32, collection string) bool {
	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return false
	}

	for _, ecShardInfo := range disk.DiskInfo.DiskInfo.EcShardInfos {
		if ecShardInfo.Id == volumeID && ecShardInfo.Collection == collection {
			return true
		}
	}
	return false
}
