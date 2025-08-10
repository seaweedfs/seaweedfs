package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// EcVolumeGenerationKey represents a unique key for EC volume with generation
type EcVolumeGenerationKey struct {
	VolumeId   needle.VolumeId
	Generation uint32
}

func (k EcVolumeGenerationKey) String() string {
	return fmt.Sprintf("v%d-g%d", k.VolumeId, k.Generation)
}

type EcShardLocations struct {
	Collection string
	Generation uint32 // generation of this set of shard locations
	Locations  [erasure_coding.TotalShardsCount][]*DataNode
}

func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var shards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			ShardSizes:  shardInfo.ShardSizes,
			Generation:  shardInfo.Generation, // extract generation from heartbeat
		}

		shards = append(shards, ecVolumeInfo)
	}
	// find out the delta volumes
	newShards, deletedShards = dn.UpdateEcShards(shards)
	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
	return
}

func (t *Topology) IncrementalSyncDataNodeEcShards(newEcShards, deletedEcShards []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) {
	// convert into in memory struct storage.VolumeInfo
	var newShards, deletedShards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range newEcShards {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			ShardSizes:  shardInfo.ShardSizes,
			Generation:  shardInfo.Generation, // extract generation from incremental heartbeat
		}

		newShards = append(newShards, ecVolumeInfo)
	}
	for _, shardInfo := range deletedEcShards {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			ShardSizes:  shardInfo.ShardSizes,
			Generation:  shardInfo.Generation, // extract generation from incremental heartbeat
		}

		deletedShards = append(deletedShards, ecVolumeInfo)
	}

	dn.DeltaUpdateEcShards(newShards, deletedShards)

	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
}

func NewEcShardLocations(collection string, generation uint32) *EcShardLocations {
	return &EcShardLocations{
		Collection: collection,
		Generation: generation,
	}
}

func (loc *EcShardLocations) AddShard(shardId erasure_coding.ShardId, dn *DataNode) (added bool) {
	dataNodes := loc.Locations[shardId]
	for _, n := range dataNodes {
		if n.Id() == dn.Id() {
			return false
		}
	}
	loc.Locations[shardId] = append(dataNodes, dn)
	return true
}

func (loc *EcShardLocations) DeleteShard(shardId erasure_coding.ShardId, dn *DataNode) (deleted bool) {
	dataNodes := loc.Locations[shardId]
	foundIndex := -1
	for index, n := range dataNodes {
		if n.Id() == dn.Id() {
			foundIndex = index
		}
	}
	if foundIndex < 0 {
		return false
	}
	loc.Locations[shardId] = append(dataNodes[:foundIndex], dataNodes[foundIndex+1:]...)
	return true
}

func (t *Topology) RegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {

	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	key := EcVolumeGenerationKey{
		VolumeId:   ecShardInfos.VolumeId,
		Generation: ecShardInfos.Generation,
	}
	locations, found := t.ecShardMap[key]
	if !found {
		locations = NewEcShardLocations(ecShardInfos.Collection, ecShardInfos.Generation)
		t.ecShardMap[key] = locations
	}
	for _, shardId := range ecShardInfos.ShardIds() {
		locations.AddShard(shardId, dn)
	}

	// Update active generation if this is newer or first time seeing this volume
	t.ecActiveGenerationMapLock.Lock()
	currentActive, exists := t.ecActiveGenerationMap[ecShardInfos.VolumeId]
	if !exists || ecShardInfos.Generation >= currentActive {
		t.ecActiveGenerationMap[ecShardInfos.VolumeId] = ecShardInfos.Generation
		glog.V(2).Infof("Updated active generation for EC volume %d to %d", ecShardInfos.VolumeId, ecShardInfos.Generation)
	}
	t.ecActiveGenerationMapLock.Unlock()
}

func (t *Topology) UnRegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
	glog.Infof("removing ec shard info:%+v", ecShardInfos)
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	key := EcVolumeGenerationKey{
		VolumeId:   ecShardInfos.VolumeId,
		Generation: ecShardInfos.Generation,
	}
	locations, found := t.ecShardMap[key]
	if !found {
		return
	}
	for _, shardId := range ecShardInfos.ShardIds() {
		locations.DeleteShard(shardId, dn)
	}

	// Check if this generation is now empty and clean up if needed
	isEmpty := true
	for _, shardLocations := range locations.Locations {
		if len(shardLocations) > 0 {
			isEmpty = false
			break
		}
	}

	if isEmpty {
		// Remove empty generation from map
		delete(t.ecShardMap, key)
		glog.V(2).Infof("Removed empty EC volume generation %d:%d", ecShardInfos.VolumeId, ecShardInfos.Generation)

		// Check if this was the active generation and update if needed
		t.ecActiveGenerationMapLock.Lock()
		if activeGen, exists := t.ecActiveGenerationMap[ecShardInfos.VolumeId]; exists && activeGen == ecShardInfos.Generation {
			// Find the highest remaining generation for this volume
			maxGeneration := uint32(0)
			hasRemaining := false
			for otherKey := range t.ecShardMap {
				if otherKey.VolumeId == ecShardInfos.VolumeId && otherKey.Generation > maxGeneration {
					maxGeneration = otherKey.Generation
					hasRemaining = true
				}
			}

			if hasRemaining {
				t.ecActiveGenerationMap[ecShardInfos.VolumeId] = maxGeneration
				glog.V(1).Infof("Updated active generation for EC volume %d to %d after cleanup", ecShardInfos.VolumeId, maxGeneration)
			} else {
				delete(t.ecActiveGenerationMap, ecShardInfos.VolumeId)
				glog.V(1).Infof("Removed active generation tracking for EC volume %d (no generations remain)", ecShardInfos.VolumeId)
			}
		}
		t.ecActiveGenerationMapLock.Unlock()
	}
}

func (t *Topology) LookupEcShards(vid needle.VolumeId, generation uint32) (locations *EcShardLocations, found bool) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	key := EcVolumeGenerationKey{
		VolumeId:   vid,
		Generation: generation,
	}
	locations, found = t.ecShardMap[key]

	return
}

func (t *Topology) ListEcServersByCollection(collection string) (dataNodes []pb.ServerAddress) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	dateNodeMap := make(map[pb.ServerAddress]bool)
	for _, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			for _, locations := range ecVolumeLocation.Locations {
				for _, loc := range locations {
					dateNodeMap[loc.ServerAddress()] = true
				}
			}
		}
	}

	for k, _ := range dateNodeMap {
		dataNodes = append(dataNodes, k)
	}

	return
}

func (t *Topology) DeleteEcCollection(collection string) {
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	var keysToDelete []EcVolumeGenerationKey
	var volumeIdsToDelete []needle.VolumeId
	for key, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			keysToDelete = append(keysToDelete, key)
			volumeIdsToDelete = append(volumeIdsToDelete, key.VolumeId)
		}
	}

	for _, key := range keysToDelete {
		delete(t.ecShardMap, key)
	}

	// Also clean up active generation tracking
	t.ecActiveGenerationMapLock.Lock()
	for _, vid := range volumeIdsToDelete {
		delete(t.ecActiveGenerationMap, vid)
	}
	t.ecActiveGenerationMapLock.Unlock()
}

// GetEcActiveGeneration returns the current active generation for an EC volume
func (t *Topology) GetEcActiveGeneration(vid needle.VolumeId) (uint32, bool) {
	t.ecActiveGenerationMapLock.RLock()
	defer t.ecActiveGenerationMapLock.RUnlock()

	generation, found := t.ecActiveGenerationMap[vid]
	return generation, found
}

// SetEcActiveGeneration sets the active generation for an EC volume
func (t *Topology) SetEcActiveGeneration(vid needle.VolumeId, generation uint32) {
	t.ecActiveGenerationMapLock.Lock()
	defer t.ecActiveGenerationMapLock.Unlock()

	t.ecActiveGenerationMap[vid] = generation
	glog.V(1).Infof("Set active generation for EC volume %d to %d", vid, generation)
}

// ListEcVolumesWithActiveGeneration returns all EC volumes and their active generations
func (t *Topology) ListEcVolumesWithActiveGeneration() map[needle.VolumeId]uint32 {
	t.ecActiveGenerationMapLock.RLock()
	defer t.ecActiveGenerationMapLock.RUnlock()

	result := make(map[needle.VolumeId]uint32)
	for vid, generation := range t.ecActiveGenerationMap {
		result[vid] = generation
	}
	return result
}
