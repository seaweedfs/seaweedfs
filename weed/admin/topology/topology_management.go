package topology

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// UpdateTopology updates the topology information from master
func (at *ActiveTopology) UpdateTopology(topologyInfo *master_pb.TopologyInfo) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

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

				// Add disks for this node
				for diskType, diskInfo := range nodeInfo.DiskInfos {
					disk := &activeDisk{
						DiskInfo: &DiskInfo{
							NodeID:     nodeInfo.Id,
							DiskID:     diskInfo.DiskId,
							DiskType:   diskType,
							DataCenter: dc.Id,
							Rack:       rack.Id,
							DiskInfo:   diskInfo,
						},
					}

					diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, diskInfo.DiskId)
					node.disks[diskInfo.DiskId] = disk
					at.disks[diskKey] = disk
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

// rebuildIndexes rebuilds the volume and EC shard indexes for O(1) lookups
func (at *ActiveTopology) rebuildIndexes() {
	// Clear existing indexes
	at.volumeIndex = make(map[uint32][]string)
	at.ecShardIndex = make(map[uint32][]string)

	// Rebuild indexes from current topology
	for _, dc := range at.topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				for _, diskInfo := range nodeInfo.DiskInfos {
					diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, diskInfo.DiskId)

					// Index volumes
					for _, volumeInfo := range diskInfo.VolumeInfos {
						volumeID := volumeInfo.Id
						at.volumeIndex[volumeID] = append(at.volumeIndex[volumeID], diskKey)
					}

					// Index EC shards
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						volumeID := ecShardInfo.Id
						at.ecShardIndex[volumeID] = append(at.ecShardIndex[volumeID], diskKey)
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
