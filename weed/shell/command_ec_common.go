package shell

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

type DataCenterId string
type EcNodeId string
type RackId string

// EcDisk represents a single disk on a volume server
type EcDisk struct {
	diskId       uint32
	diskType     string
	freeEcSlots  int
	ecShardCount int // Total EC shards on this disk
	// Map of volumeId -> ShardsInfo for shards on this disk
	ecShards map[needle.VolumeId]*erasure_coding.ShardsInfo
}

type EcNode struct {
	info       *master_pb.DataNodeInfo
	dc         DataCenterId
	rack       RackId
	freeEcSlot int
	// disks maps diskId -> EcDisk for disk-level balancing
	disks map[uint32]*EcDisk
}

type CandidateEcNode struct {
	ecNode     *EcNode
	shardCount int
}

type EcRack struct {
	ecNodes    map[EcNodeId]*EcNode
	freeEcSlot int
}

var (
	ecBalanceAlgorithmDescription = `
	func EcBalance() {
		for each collection:
			balanceEcVolumes(collectionName)
		for each rack:
			balanceEcRack(rack)
	}

	func balanceEcVolumes(collectionName){
		for each volume:
			doDeduplicateEcShards(volumeId)

		tracks rack~shardCount mapping
		for each volume:
			doBalanceEcShardsAcrossRacks(volumeId)

		for each volume:
			doBalanceEcShardsWithinRacks(volumeId)
	}

	// spread ec shards into more racks
	func doBalanceEcShardsAcrossRacks(volumeId){
		tracks rack~volumeIdShardCount mapping
		averageShardsPerEcRack = totalShardNumber / numRacks  // totalShardNumber is 14 for now, later could varies for each dc
		ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
		for each ecShardsToMove {
			destRack = pickOneRack(rack~shardCount, rack~volumeIdShardCount, ecShardReplicaPlacement)
			destVolumeServers = volume servers on the destRack
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	func doBalanceEcShardsWithinRacks(volumeId){
		racks = collect all racks that the volume id is on
		for rack, shards := range racks
			doBalanceEcShardsWithinOneRack(volumeId, shards, rack)
	}

	// move ec shards
	func doBalanceEcShardsWithinOneRack(volumeId, shards, rackId){
		tracks volumeServer~volumeIdShardCount mapping
		averageShardCount = len(shards) / numVolumeServers
		volumeServersOverAverage = volume servers with volumeId's ec shard counts > averageShardsPerEcRack
		ecShardsToMove = select overflown ec shards from volumeServersOverAverage
		for each ecShardsToMove {
			destVolumeServer = pickOneVolumeServer(volumeServer~shardCount, volumeServer~volumeIdShardCount, ecShardReplicaPlacement)
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	// move ec shards while keeping shard distribution for the same volume unchanged or more even
	func balanceEcRack(rack){
		averageShardCount = total shards / numVolumeServers
		for hasMovedOneEcShard {
			sort all volume servers ordered by the number of local ec shards
			pick the volume server A with the lowest number of ec shards x
			pick the volume server B with the highest number of ec shards y
			if y > averageShardCount and x +1 <= averageShardCount {
				if B has a ec shard with volume id v that A does not have {
					move one ec shard v from B to A
					hasMovedOneEcShard = true
				}
			}
		}
	}
	`
	// Overridable functions for testing.
	getDefaultReplicaPlacement = _getDefaultReplicaPlacement
)

func _getDefaultReplicaPlacement(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
	var resp *master_pb.GetMasterConfigurationResponse
	var err error

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		return err
	})
	if err != nil {
		return nil, err
	}

	return super_block.NewReplicaPlacementFromString(resp.DefaultReplication)
}

func parseReplicaPlacementArg(commandEnv *CommandEnv, replicaStr string) (*super_block.ReplicaPlacement, error) {
	var rp *super_block.ReplicaPlacement
	var err error

	if replicaStr != "" {
		rp, err = super_block.NewReplicaPlacementFromString(replicaStr)
		if err != nil {
			return rp, err
		}
		glog.V(1).Infof("using replica placement %q for EC volumes\n", rp.String())
	} else {
		// No replica placement argument provided, resolve from master default settings.
		rp, err = getDefaultReplicaPlacement(commandEnv)
		if err != nil {
			return rp, err
		}
		glog.V(1).Infof("using master default replica placement %q for EC volumes\n", rp.String())
	}

	return rp, nil
}

func collectTopologyInfo(commandEnv *CommandEnv, delayBeforeCollecting time.Duration) (topoInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, err error) {
	if delayBeforeCollecting > 0 {
		time.Sleep(delayBeforeCollecting)
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	return resp.TopologyInfo, resp.VolumeSizeLimitMb, nil
}

func collectDataNodes(commandEnv *CommandEnv, delayBeforeCollecting time.Duration) ([]*master_pb.DataNodeInfo, error) {
	dataNodes := []*master_pb.DataNodeInfo{}

	topo, _, err := collectTopologyInfo(commandEnv, delayBeforeCollecting)
	if err != nil {
		return nil, err
	}

	for _, dci := range topo.GetDataCenterInfos() {
		for _, r := range dci.GetRackInfos() {
			for _, dn := range r.GetDataNodeInfos() {
				dataNodes = append(dataNodes, dn)
			}
		}
	}

	return dataNodes, nil
}

func collectEcNodesForDC(commandEnv *CommandEnv, selectedDataCenter string, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	// list all possible locations
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots = collectEcVolumeServersByDc(topologyInfo, selectedDataCenter, diskType)

	sortEcNodesByFreeslotsDescending(ecNodes)

	return
}

func collectEcNodes(commandEnv *CommandEnv, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	return collectEcNodesForDC(commandEnv, "", diskType)
}

// collectVolumeIdToCollection returns a map from volume ID to its collection name
func collectVolumeIdToCollection(t *master_pb.TopologyInfo, vids []needle.VolumeId) map[needle.VolumeId]string {
	result := make(map[needle.VolumeId]string)
	if len(vids) == 0 {
		return result
	}

	vidSet := make(map[needle.VolumeId]bool)
	for _, vid := range vids {
		vidSet[vid] = true
	}

	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						vid := needle.VolumeId(vi.Id)
						if vidSet[vid] {
							result[vid] = vi.Collection
						}
					}
				}
			}
		}
	}
	return result
}

func collectCollectionsForVolumeIds(t *master_pb.TopologyInfo, vids []needle.VolumeId) []string {
	if len(vids) == 0 {
		return nil
	}

	found := map[string]bool{}
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						for _, vid := range vids {
							if needle.VolumeId(vi.Id) == vid {
								found[vi.Collection] = true
							}
						}
					}
					for _, ecs := range diskInfo.EcShardInfos {
						for _, vid := range vids {
							if needle.VolumeId(ecs.Id) == vid {
								found[ecs.Collection] = true
							}
						}
					}
				}
			}
		}
	}
	if len(found) == 0 {
		return nil
	}

	collections := []string{}
	for k, _ := range found {
		collections = append(collections, k)
	}
	sort.Strings(collections)
	return collections
}

func moveMountedShardToEcNode(commandEnv *CommandEnv, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, destDiskId uint32, applyBalancing bool, diskType types.DiskType) (err error) {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	copiedShardIds := []erasure_coding.ShardId{shardId}

	if applyBalancing {

		existingServerAddress := pb.NewServerAddressFromDataNode(existingLocation.info)

		// ask destination node to copy shard and the ecx file from source node, and mount it
		copiedShardIds, err = oneServerCopyAndMountEcShardsFromSource(commandEnv.option.GrpcDialOption, destinationEcNode, []erasure_coding.ShardId{shardId}, vid, collection, existingServerAddress, destDiskId)
		if err != nil {
			return err
		}

		// unmount the to be deleted shards
		err = unmountEcShards(commandEnv.option.GrpcDialOption, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		// ask source node to delete the shard, and maybe the ecx file
		err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		if destDiskId > 0 {
			fmt.Printf("moved ec shard %d.%d %s => %s (disk %d)\n", vid, shardId, existingLocation.info.Id, destinationEcNode.info.Id, destDiskId)
		} else {
			fmt.Printf("moved ec shard %d.%d %s => %s\n", vid, shardId, existingLocation.info.Id, destinationEcNode.info.Id)
		}

	}

	destinationEcNode.addEcVolumeShards(vid, collection, copiedShardIds, diskType)
	existingLocation.deleteEcVolumeShards(vid, copiedShardIds, diskType)

	return nil

}

func oneServerCopyAndMountEcShardsFromSource(grpcDialOption grpc.DialOption,
	targetServer *EcNode, shardIdsToCopy []erasure_coding.ShardId,
	volumeId needle.VolumeId, collection string, existingLocation pb.ServerAddress, destDiskId uint32) (copiedShardIds []erasure_coding.ShardId, err error) {

	fmt.Printf("allocate %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)

	targetAddress := pb.NewServerAddressFromDataNode(targetServer.info)
	err = operation.WithVolumeServerClient(false, targetAddress, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		if targetAddress != existingLocation {
			fmt.Printf("copy %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)
			_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:       erasure_coding.ShardIdsToUint32(shardIdsToCopy),
				CopyEcxFile:    true,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				CopyEcsumFile:  true, // propagate the bitrot sidecar with the shards (no-op if the source has none)
				SourceDataNode: string(existingLocation),
				DiskId:         destDiskId,
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s : %v\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id, copyErr)
			}
		}

		fmt.Printf("mount %d.%v on %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(shardIdsToCopy),
		})
		if mountErr != nil {
			return fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, shardIdsToCopy, targetServer.info.Id, mountErr)
		}

		if targetAddress != existingLocation {
			copiedShardIds = shardIdsToCopy
			glog.V(0).Infof("%s ec volume %d deletes shards %+v", existingLocation, volumeId, copiedShardIds)
		}

		return nil
	})

	if err != nil {
		return
	}

	return
}

func eachDataNode(topo *master_pb.TopologyInfo, fn func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(DataCenterId(dc.Id), RackId(rack.Id), dn)
			}
		}
	}
}

func sortEcNodesByFreeslotsDescending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return b.freeEcSlot - a.freeEcSlot
	})
}

func sortEcNodesByFreeslotsAscending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return a.freeEcSlot - b.freeEcSlot
	})
}

func countShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, eci := range ecShardInfos {
		count += erasure_coding.GetShardCount(eci)
	}
	return
}

func countFreeShardSlots(dn *master_pb.DataNodeInfo, diskType types.DiskType) (count int) {
	if dn.DiskInfos == nil {
		return 0
	}
	diskInfo := dn.DiskInfos[string(diskType)]
	if diskInfo == nil {
		return 0
	}

	slots := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countShards(diskInfo.EcShardInfos)
	if slots < 0 {
		return 0
	}

	return slots
}

func (ecNode *EcNode) localShardIdCount(vid uint32) int {
	for _, diskInfo := range ecNode.info.DiskInfos {
		for _, eci := range diskInfo.EcShardInfos {
			if vid == eci.Id {
				return erasure_coding.GetShardCount(eci)
			}
		}
	}
	return 0
}

func collectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int) {
	eachDataNode(topo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != string(dc) {
			return
		}

		freeEcSlots := countFreeShardSlots(dn, diskType)
		ecNode := &EcNode{
			info:       dn,
			dc:         dc,
			rack:       rack,
			freeEcSlot: int(freeEcSlots),
			disks:      make(map[uint32]*EcDisk),
		}

		// Build disk-level information from volumes and EC shards
		// First, discover all unique disk IDs from VolumeInfos (includes empty disks)
		allDiskIds := make(map[uint32]string) // diskId -> diskType
		for diskTypeKey, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			// Get all disk IDs from volumes
			for _, vi := range diskInfo.VolumeInfos {
				allDiskIds[vi.DiskId] = diskTypeKey
			}
			// Also get disk IDs from EC shards
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				allDiskIds[ecShardInfo.DiskId] = diskTypeKey
			}
		}

		// Group EC shards by disk_id
		diskShards := make(map[uint32]map[needle.VolumeId]*erasure_coding.ShardsInfo)
		for _, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, eci := range diskInfo.EcShardInfos {
				diskId := eci.DiskId
				if diskShards[diskId] == nil {
					diskShards[diskId] = make(map[needle.VolumeId]*erasure_coding.ShardsInfo)
				}
				vid := needle.VolumeId(eci.Id)
				diskShards[diskId][vid] = erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
			}
		}

		// Create EcDisk for each discovered disk
		diskCount := len(allDiskIds)
		if diskCount == 0 {
			diskCount = 1
		}
		freePerDisk := int(freeEcSlots) / diskCount

		for diskId, diskTypeStr := range allDiskIds {
			shards := diskShards[diskId]
			if shards == nil {
				shards = make(map[needle.VolumeId]*erasure_coding.ShardsInfo)
			}
			totalShardCount := 0
			for _, shardsInfo := range shards {
				totalShardCount += shardsInfo.Count()
			}

			ecNode.disks[diskId] = &EcDisk{
				diskId:       diskId,
				diskType:     diskTypeStr,
				freeEcSlots:  freePerDisk,
				ecShardCount: totalShardCount,
				ecShards:     shards,
			}
		}

		ecNodes = append(ecNodes, ecNode)
		totalFreeEcSlots += freeEcSlots
	})
	return
}

func sourceServerDeleteEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeDeletedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("delete %d.%v from %s\n", volumeId, toBeDeletedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(toBeDeletedShardIds),
		})
		return deleteErr
	})

}

// unmountAndDeleteEcShardsQuiet unmounts then deletes shards on one server in a
// single connection, without the per-call logging the interactive helpers emit.
// Used by the orphan sweep, which fans out to every node x volume and would
// otherwise flood the shell with no-op lines.
func unmountAndDeleteEcShardsQuiet(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, location pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	ids := erasure_coding.ShardIdsToUint32(shardIds)
	return operation.WithVolumeServerClient(false, location, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		if _, err := volumeServerClient.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: ids,
		}); err != nil {
			return fmt.Errorf("unmount: %w", err)
		}
		resp, err := volumeServerClient.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:     uint32(volumeId),
			Collection:   collection,
			ShardIds:     ids,
			FullTeardown: true,
		})
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		if !resp.GetFullTeardownDone() {
			return fmt.Errorf("delete on %s did not perform full teardown (pre-upgrade volume server?); a stale EC generation may remain", location)
		}
		return nil
	})
}

func unmountEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeUnmountedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: erasure_coding.ShardIdsToUint32(toBeUnmountedShardIds),
		})
		return deleteErr
	})
}

func mountEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeMountedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(toBeMountedShardIds),
		})
		return mountErr
	})
}

func ceilDivide(a, b int) int {
	var r int
	if (a % b) != 0 {
		r = 1
	}
	return (a / b) + r
}

func findEcVolumeShardsInfo(ecNode *EcNode, vid needle.VolumeId, diskType types.DiskType) *erasure_coding.ShardsInfo {
	if diskInfo, found := ecNode.info.DiskInfos[string(diskType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				return erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo)
			}
		}
	}

	// Returns an empty ShardsInfo struct on failure, to avoid potential nil dereferences.
	return erasure_coding.NewShardsInfo()
}

// TODO: simplify me
func (ecNode *EcNode) addEcVolumeShards(vid needle.VolumeId, collection string, shardIds []erasure_coding.ShardId, diskType types.DiskType) *EcNode {

	foundVolume := false
	diskInfo, found := ecNode.info.DiskInfos[string(diskType)]
	if found {
		for _, ecsi := range diskInfo.EcShardInfos {
			if needle.VolumeId(ecsi.Id) == vid {
				si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecsi)
				oldShardCount := si.Count()
				for _, shardId := range shardIds {
					si.Set(erasure_coding.NewShardInfo(shardId, 0))
				}
				ecsi.EcIndexBits = si.Bitmap()
				ecsi.ShardSizes = si.SizesInt64()
				ecNode.freeEcSlot -= si.Count() - oldShardCount
				foundVolume = true
				break
			}
		}
	} else {
		diskInfo = &master_pb.DiskInfo{
			Type: string(diskType),
		}
		ecNode.info.DiskInfos[string(diskType)] = diskInfo
	}

	if !foundVolume {
		si := erasure_coding.NewShardsInfo()
		for _, id := range shardIds {
			si.Set(erasure_coding.NewShardInfo(id, 0))
		}
		diskInfo.EcShardInfos = append(diskInfo.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          uint32(vid),
			Collection:  collection,
			EcIndexBits: si.Bitmap(),
			ShardSizes:  si.SizesInt64(),
			DiskType:    string(diskType),
		})
		ecNode.freeEcSlot -= si.Count()
	}

	return ecNode
}

func (ecNode *EcNode) deleteEcVolumeShards(vid needle.VolumeId, shardIds []erasure_coding.ShardId, diskType types.DiskType) *EcNode {

	if diskInfo, found := ecNode.info.DiskInfos[string(diskType)]; found {
		for _, eci := range diskInfo.EcShardInfos {
			if needle.VolumeId(eci.Id) == vid {
				si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
				oldCount := si.Count()
				for _, shardId := range shardIds {
					si.Delete(shardId)
				}
				eci.EcIndexBits = si.Bitmap()
				eci.ShardSizes = si.SizesInt64()
				ecNode.freeEcSlot -= si.Count() - oldCount
			}
		}
	}

	return ecNode
}

// pickBestDiskOnNode selects the best disk on a node for placing a new EC shard
// It prefers disks of the specified type with fewer shards and more free slots
// When shardId is provided and dataShardCount > 0, it applies anti-affinity:
// - For data shards (shardId < dataShardCount): prefer disks without parity shards
// - For parity shards (shardId >= dataShardCount): prefer disks without data shards
// If strictDiskType is false, it will fall back to other disk types if no matching disk is found
func pickBestDiskOnNode(ecNode *EcNode, vid needle.VolumeId, diskType types.DiskType, strictDiskType bool, shardId erasure_coding.ShardId, dataShardCount int) uint32 {
	if len(ecNode.disks) == 0 {
		return 0 // No disk info available, let the server decide
	}

	var bestDiskId uint32
	bestScore := -1
	var fallbackDiskId uint32
	fallbackScore := -1

	// Determine if we're placing a data or parity shard
	isDataShard := dataShardCount > 0 && int(shardId) < dataShardCount

	for diskId, disk := range ecNode.disks {
		if disk.freeEcSlots <= 0 {
			continue
		}

		// Check existing shards on this disk for this volume
		existingShards := 0
		hasDataShards := false
		hasParityShards := false
		if si, ok := disk.ecShards[vid]; ok {
			existingShards = si.Count()
			// Check what type of shards are on this disk
			if dataShardCount > 0 {
				for _, existingShardId := range si.Ids() {
					if int(existingShardId) < dataShardCount {
						hasDataShards = true
					} else {
						hasParityShards = true
					}
				}
			}
		}

		// Score: prefer disks with fewer total shards and fewer shards of this volume
		// Lower score is better
		score := disk.ecShardCount*10 + existingShards*100

		// Apply anti-affinity penalty if applicable
		if dataShardCount > 0 {
			if isDataShard && hasParityShards {
				// Penalize placing data shard on disk with parity shards
				score += 1000
			} else if !isDataShard && hasDataShards {
				// Penalize placing parity shard on disk with data shards
				score += 1000
			}
		}

		if disk.diskType == string(diskType) {
			// Matching disk type - this is preferred
			if bestScore == -1 || score < bestScore {
				bestScore = score
				bestDiskId = diskId
			}
		} else if !strictDiskType {
			// Non-matching disk type - use as fallback if allowed
			if fallbackScore == -1 || score < fallbackScore {
				fallbackScore = score
				fallbackDiskId = diskId
			}
		}
	}

	// Return matching disk type if found, otherwise fallback
	if bestDiskId != 0 {
		return bestDiskId
	}
	return fallbackDiskId
}

// ecBalancer drives an EC balance run: it collects the cluster's EC nodes, hands
// them to the shared ecbalancer planner, and executes the planned shard moves.
// The balancing policy lives in weed/storage/erasure_coding/ecbalancer, shared
// with the EC balance worker so the two cannot drift.
type ecBalancer struct {
	commandEnv         *CommandEnv
	ecNodes            []*EcNode
	replicaPlacement   *super_block.ReplicaPlacement
	applyBalancing     bool
	maxParallelization int
	diskType           types.DiskType
}

func EcBalance(commandEnv *CommandEnv, collections []string, dc string, ecReplicaPlacement *super_block.ReplicaPlacement, diskType types.DiskType, maxParallelization int, applyBalancing bool) (err error) {
	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := collectEcNodesForDC(commandEnv, dc, diskType)
	if err != nil {
		return err
	}
	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	ecb := &ecBalancer{
		commandEnv:         commandEnv,
		ecNodes:            allEcNodes,
		replicaPlacement:   ecReplicaPlacement,
		applyBalancing:     applyBalancing,
		maxParallelization: maxParallelization,
		diskType:           diskType,
	}

	if len(collections) == 0 {
		glog.V(1).Infof("WARNING: No collections to balance EC volumes across.\n")
	}
	return ecb.balance(collections)
}

// shellECRatio resolves a collection's EC data/parity counts, defaulting to the
// standard scheme. This is the shell's plug-in point for custom ratios.
func shellECRatio(_ string) (int, int) {
	// Custom EC ratios are an enterprise feature; OSS uses the standard scheme.
	return erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
}

// balance plans EC shard moves with the shared planner and executes them. When
// collections is empty all collections present are balanced.
func (ecb *ecBalancer) balance(collections []string) error {
	topo := toBalancerTopology(ecb.ecNodes, collections, ecb.diskType)
	moves := ecbalancer.Plan(topo, ecbalancer.Options{
		DiskType:           string(ecb.diskType),
		ImbalanceThreshold: 0, // the shell balances to an even distribution
		ReplicaPlacement:   ecb.replicaPlacement,
		Ratio:              shellECRatio,
		// Balance the global phase by fractional fullness so heterogeneous-capacity
		// nodes fill proportionally (matching the worker). This is identical to raw
		// shard count when capacities are uniform.
		GlobalUtilizationBased: true,
	})
	return ecb.executeMoves(moves)
}

// toBalancerTopology builds an ecbalancer.Topology from the shell's EcNode model,
// including the shards of the requested collections (all collections when empty).
func toBalancerTopology(ecNodes []*EcNode, collections []string, diskType types.DiskType) *ecbalancer.Topology {
	allowed := make(map[string]bool, len(collections))
	for _, c := range collections {
		allowed[c] = true
	}

	topo := ecbalancer.NewTopology()
	for _, en := range ecNodes {
		rackKey := string(en.dc) + ":" + string(en.rack)
		node := topo.AddNode(en.info.Id, string(en.dc), rackKey, en.freeEcSlot)
		// Group by physical machine (host) so shards spread across machines, not just
		// nodes; the id stays the node identity used for moves.
		node.SetHost(pb.NewServerAddressFromDataNode(en.info).ToHost())
		for diskId, d := range en.disks {
			node.AddDisk(diskId, d.diskType, d.freeEcSlots, d.ecShardCount)
		}
		diskInfo, found := en.info.DiskInfos[string(diskType)]
		if !found {
			continue
		}
		for _, eci := range diskInfo.EcShardInfos {
			if len(allowed) > 0 && !allowed[eci.Collection] {
				continue
			}
			node.AddShards(eci.Id, eci.Collection, eci.DiskId, erasure_coding.ShardBits(eci.EcIndexBits))
		}
	}
	return topo
}

// executeMoves carries out the planned moves. Phases run in order (a within-rack
// move can depend on a cross-rack move's result), and the independent moves
// within a phase run with up to maxParallelization concurrency. Apply mode does
// only the RPCs; dry-run mode runs sequentially and mutates the in-memory EcNode
// model so callers/tests can inspect the planned end state.
func (ecb *ecBalancer) executeMoves(moves []ecbalancer.Move) error {
	byID := make(map[string]*EcNode, len(ecb.ecNodes))
	for _, en := range ecb.ecNodes {
		byID[en.info.Id] = en
	}

	// Plan emits moves grouped by phase; run each contiguous same-phase group
	// together, waiting before the next so cross-phase dependencies hold.
	for i := 0; i < len(moves); {
		j := i
		for j < len(moves) && moves[j].Phase == moves[i].Phase {
			j++
		}
		if err := ecb.executePhase(byID, moves[i:j]); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func (ecb *ecBalancer) executePhase(byID map[string]*EcNode, moves []ecbalancer.Move) error {
	if !ecb.applyBalancing {
		// Dry-run: sequential so the in-memory model updates are race-free and
		// reflect the full plan for inspection.
		for _, m := range moves {
			if err := ecb.executeMove(byID, m); err != nil {
				return err
			}
		}
		return nil
	}
	// Apply mode: parallelize across volumes, but run one volume's moves within a
	// phase sequentially. Concurrent moves of the same volume to a node can race
	// on its shared .ecx/.ecj/.vif sidecar files.
	var order []uint32
	byVol := make(map[uint32][]ecbalancer.Move)
	for _, m := range moves {
		if _, ok := byVol[m.VolumeID]; !ok {
			order = append(order, m.VolumeID)
		}
		byVol[m.VolumeID] = append(byVol[m.VolumeID], m)
	}
	ewg := NewErrorWaitGroup(ecb.maxParallelization)
	for _, vid := range order {
		group := byVol[vid]
		ewg.Add(func() error {
			for _, m := range group {
				if err := ecb.executeMove(byID, m); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return ewg.Wait()
}

func (ecb *ecBalancer) executeMove(byID map[string]*EcNode, m ecbalancer.Move) error {
	src := byID[m.SourceNode]
	if src == nil {
		return nil
	}
	vid := needle.VolumeId(m.VolumeID)
	shardId := erasure_coding.ShardId(m.ShardID)
	shardIds := []erasure_coding.ShardId{shardId}

	if m.Phase == "dedup" {
		fmt.Printf("dedup: delete ec shard %d.%d on %s\n", vid, shardId, m.SourceNode)
		if !ecb.applyBalancing {
			src.deleteEcVolumeShards(vid, shardIds, ecb.diskType)
			return nil
		}
		grpcDialOption := ecb.commandEnv.option.GrpcDialOption
		addr := pb.NewServerAddressFromDataNode(src.info)
		if err := unmountEcShards(grpcDialOption, vid, addr, shardIds); err != nil {
			return err
		}
		return sourceServerDeleteEcShards(grpcDialOption, m.Collection, vid, addr, shardIds)
	}

	dst := byID[m.TargetNode]
	if dst == nil {
		return nil
	}
	if m.TargetDisk > 0 {
		fmt.Printf("%s moves ec shard %d.%d to %s (disk %d)\n", m.SourceNode, vid, shardId, m.TargetNode, m.TargetDisk)
	} else {
		fmt.Printf("%s moves ec shard %d.%d to %s\n", m.SourceNode, vid, shardId, m.TargetNode)
	}
	if !ecb.applyBalancing {
		// Dry-run: update the in-memory model only.
		return moveMountedShardToEcNode(ecb.commandEnv, src, m.Collection, vid, shardId, dst, m.TargetDisk, false, ecb.diskType)
	}
	return ecb.applyShardMoveRPC(src, dst, m.Collection, vid, shardId, m.TargetDisk)
}

// applyShardMoveRPC copies a shard to the destination disk, then unmounts and
// deletes it on the source. It does not touch the in-memory model, so it is safe
// to run concurrently across the moves of a phase.
func (ecb *ecBalancer) applyShardMoveRPC(src, dst *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destDiskId uint32) error {
	grpcDialOption := ecb.commandEnv.option.GrpcDialOption
	srcAddr := pb.NewServerAddressFromDataNode(src.info)
	copiedShardIds, err := oneServerCopyAndMountEcShardsFromSource(grpcDialOption, dst, []erasure_coding.ShardId{shardId}, vid, collection, srcAddr, destDiskId)
	if err != nil {
		return err
	}
	if len(copiedShardIds) == 0 {
		return nil
	}
	if err := unmountEcShards(grpcDialOption, vid, srcAddr, copiedShardIds); err != nil {
		return err
	}
	return sourceServerDeleteEcShards(grpcDialOption, collection, vid, srcAddr, copiedShardIds)
}

// compileCollectionPattern compiles a regex pattern for collection matching.
// Empty patterns match empty collections only.
// The special keyword CollectionDefault ("_default") matches empty collections.
func compileCollectionPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		// empty pattern matches empty collection
		return regexp.Compile("^$")
	}
	if pattern == CollectionDefault {
		// CollectionDefault keyword matches empty collection
		return regexp.Compile("^$")
	}
	return regexp.Compile(pattern)
}
