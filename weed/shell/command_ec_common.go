package shell

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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
		fmt.Printf("using replica placement %q for EC volumes\n", rp.String())
	} else {
		// No replica placement argument provided, resolve from master default settings.
		rp, err = getDefaultReplicaPlacement(commandEnv)
		if err != nil {
			return rp, err
		}
		fmt.Printf("using master default replica placement %q for EC volumes\n", rp.String())
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

// if the index node changed the freeEcSlot, need to keep every EcNode still sorted
func ensureSortedEcNodes(data []*CandidateEcNode, index int, lessThan func(i, j int) bool) {
	for i := index - 1; i >= 0; i-- {
		if lessThan(i+1, i) {
			swap(data, i, i+1)
		} else {
			break
		}
	}
	for i := index + 1; i < len(data); i++ {
		if lessThan(i, i-1) {
			swap(data, i, i-1)
		} else {
			break
		}
	}
}

func swap(data []*CandidateEcNode, i, j int) {
	t := data[i]
	data[i] = data[j]
	data[j] = t
}

func countShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, eci := range ecShardInfos {
		count += erasure_coding.ShardsCountFromVolumeEcShardInformationMessage(eci)
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
				return erasure_coding.ShardsCountFromVolumeEcShardInformationMessage(eci)
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

func unmountEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeUnmountedhardIds []erasure_coding.ShardId) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: erasure_coding.ShardIdsToUint32(toBeUnmountedhardIds),
		})
		return deleteErr
	})
}

func mountEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeMountedhardIds []erasure_coding.ShardId) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(toBeMountedhardIds),
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
					si.Set(shardId, 0)
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
			si.Set(id, 0)
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

func groupByCount(data []*EcNode, identifierFn func(*EcNode) (id string, count int)) map[string]int {
	countMap := make(map[string]int)
	for _, d := range data {
		id, count := identifierFn(d)
		countMap[id] += count
	}
	return countMap
}

func groupBy(data []*EcNode, identifierFn func(*EcNode) (id string)) map[string][]*EcNode {
	groupMap := make(map[string][]*EcNode)
	for _, d := range data {
		id := identifierFn(d)
		groupMap[id] = append(groupMap[id], d)
	}
	return groupMap
}

type ecBalancer struct {
	commandEnv         *CommandEnv
	ecNodes            []*EcNode
	replicaPlacement   *super_block.ReplicaPlacement
	applyBalancing     bool
	maxParallelization int
	diskType           types.DiskType // target disk type for EC shards (default: HardDriveType)
	// EC configuration for shard distribution (defaults to 10+4)
	dataShardCount   int
	parityShardCount int
}

// getDataShardCount returns the configured data shard count, defaulting to standard 10
func (ecb *ecBalancer) getDataShardCount() int {
	if ecb.dataShardCount > 0 {
		return ecb.dataShardCount
	}
	return erasure_coding.DataShardsCount
}

// getParityShardCount returns the configured parity shard count, defaulting to standard 4
func (ecb *ecBalancer) getParityShardCount() int {
	if ecb.parityShardCount > 0 {
		return ecb.parityShardCount
	}
	return erasure_coding.ParityShardsCount
}

func (ecb *ecBalancer) errorWaitGroup() *ErrorWaitGroup {
	return NewErrorWaitGroup(ecb.maxParallelization)
}

func (ecb *ecBalancer) racks() map[RackId]*EcRack {
	racks := make(map[RackId]*EcRack)
	for _, ecNode := range ecb.ecNodes {
		if racks[ecNode.rack] == nil {
			racks[ecNode.rack] = &EcRack{
				ecNodes: make(map[EcNodeId]*EcNode),
			}
		}
		racks[ecNode.rack].ecNodes[EcNodeId(ecNode.info.Id)] = ecNode
		racks[ecNode.rack].freeEcSlot += ecNode.freeEcSlot
	}
	return racks
}

func (ecb *ecBalancer) balanceEcVolumes(collection string) error {

	fmt.Printf("balanceEcVolumes %s\n", collection)

	if err := ecb.deleteDuplicatedEcShards(collection); err != nil {
		return fmt.Errorf("delete duplicated collection %s ec shards: %v", collection, err)
	}

	if err := ecb.balanceEcShardsAcrossRacks(collection); err != nil {
		return fmt.Errorf("balance across racks collection %s ec shards: %v", collection, err)
	}

	if err := ecb.balanceEcShardsWithinRacks(collection); err != nil {
		return fmt.Errorf("balance within racks collection %s ec shards: %v", collection, err)
	}

	return nil
}

func (ecb *ecBalancer) deleteDuplicatedEcShards(collection string) error {
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)

	ewg := ecb.errorWaitGroup()
	for vid, locations := range vidLocations {
		ewg.Add(func() error {
			return ecb.doDeduplicateEcShards(collection, vid, locations)
		})
	}
	return ewg.Wait()
}

func (ecb *ecBalancer) doDeduplicateEcShards(collection string, vid needle.VolumeId, locations []*EcNode) error {
	// check whether this volume has ecNodes that are over average
	// Use MaxShardCount (32) to support custom EC ratios
	shardToLocations := make([][]*EcNode, erasure_coding.MaxShardCount)
	for _, ecNode := range locations {
		si := findEcVolumeShardsInfo(ecNode, vid, ecb.diskType)
		for _, shardId := range si.Ids() {
			shardToLocations[shardId] = append(shardToLocations[shardId], ecNode)
		}
	}
	for shardId, ecNodes := range shardToLocations {
		if len(ecNodes) <= 1 {
			continue
		}
		sortEcNodesByFreeslotsAscending(ecNodes)
		fmt.Printf("ec shard %d.%d has %d copies, keeping %v\n", vid, shardId, len(ecNodes), ecNodes[0].info.Id)
		if !ecb.applyBalancing {
			continue
		}

		duplicatedShardIds := []erasure_coding.ShardId{erasure_coding.ShardId(shardId)}
		for _, ecNode := range ecNodes[1:] {
			if err := unmountEcShards(ecb.commandEnv.option.GrpcDialOption, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			if err := sourceServerDeleteEcShards(ecb.commandEnv.option.GrpcDialOption, collection, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			ecNode.deleteEcVolumeShards(vid, duplicatedShardIds, ecb.diskType)
		}
	}
	return nil
}

func (ecb *ecBalancer) balanceEcShardsAcrossRacks(collection string) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)

	// spread the ec shards evenly
	ewg := ecb.errorWaitGroup()
	for vid, locations := range vidLocations {
		ewg.Add(func() error {
			return ecb.doBalanceEcShardsAcrossRacks(collection, vid, locations)
		})
	}
	return ewg.Wait()
}

func countShardsByRack(vid needle.VolumeId, locations []*EcNode, diskType types.DiskType) map[string]int {
	return groupByCount(locations, func(ecNode *EcNode) (id string, count int) {
		id = string(ecNode.rack)
		if si := findEcVolumeShardsInfo(ecNode, vid, diskType); si != nil {
			count = si.Count()
		}
		return
	})
}

// shardsByTypePerRack counts data shards (< dataShards) and parity shards (>= dataShards) per rack
func shardsByTypePerRack(vid needle.VolumeId, locations []*EcNode, diskType types.DiskType, dataShards int) (dataPerRack, parityPerRack map[string][]erasure_coding.ShardId) {
	dataPerRack = make(map[string][]erasure_coding.ShardId)
	parityPerRack = make(map[string][]erasure_coding.ShardId)
	for _, ecNode := range locations {
		si := findEcVolumeShardsInfo(ecNode, vid, diskType)
		rackId := string(ecNode.rack)
		for _, shardId := range si.Ids() {
			if int(shardId) < dataShards {
				dataPerRack[rackId] = append(dataPerRack[rackId], shardId)
			} else {
				parityPerRack[rackId] = append(parityPerRack[rackId], shardId)
			}
		}
	}
	return
}

func (ecb *ecBalancer) doBalanceEcShardsAcrossRacks(collection string, vid needle.VolumeId, locations []*EcNode) error {
	racks := ecb.racks()
	numRacks := len(racks)

	// Use configured EC scheme for shard type classification (defaults to 10+4)
	dataShardCount := ecb.getDataShardCount()
	parityShardCount := ecb.getParityShardCount()

	// Get current distribution of data shards per rack (parity computed after data balancing)
	dataPerRack, _ := shardsByTypePerRack(vid, locations, ecb.diskType, dataShardCount)

	// Calculate max shards per rack for each type to ensure even spread
	// Data: 10 shards / 6 racks = max 2 per rack
	// Parity: 4 shards / 6 racks = max 1 per rack (with 2 racks having 0)
	maxDataPerRack := ceilDivide(dataShardCount, numRacks)
	maxParityPerRack := ceilDivide(parityShardCount, numRacks)

	rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
		return string(ecNode.rack)
	})

	// Track total shard count per rack for slot management
	rackToShardCount := countShardsByRack(vid, locations, ecb.diskType)

	// First pass: Balance data shards across racks
	if err := ecb.balanceShardTypeAcrossRacks(collection, vid, racks, rackEcNodesWithVid, dataPerRack, rackToShardCount, maxDataPerRack, "data"); err != nil {
		return err
	}

	// Refresh locations after data shard moves and get parity distribution
	locations = ecb.collectVolumeIdToEcNodes(collection)[vid]
	_, parityPerRack := shardsByTypePerRack(vid, locations, ecb.diskType, dataShardCount)
	rackEcNodesWithVid = groupBy(locations, func(ecNode *EcNode) string {
		return string(ecNode.rack)
	})
	rackToShardCount = countShardsByRack(vid, locations, ecb.diskType)

	// Second pass: Balance parity shards across racks
	if err := ecb.balanceShardTypeAcrossRacks(collection, vid, racks, rackEcNodesWithVid, parityPerRack, rackToShardCount, maxParityPerRack, "parity"); err != nil {
		return err
	}

	return nil
}

// balanceShardTypeAcrossRacks spreads shards of a specific type (data or parity) evenly across racks
func (ecb *ecBalancer) balanceShardTypeAcrossRacks(
	collection string,
	vid needle.VolumeId,
	racks map[RackId]*EcRack,
	rackEcNodesWithVid map[string][]*EcNode,
	shardsPerRack map[string][]erasure_coding.ShardId,
	rackToShardCount map[string]int,
	maxPerRack int,
	shardType string,
) error {
	// Find racks with too many shards of this type
	shardsToMove := make(map[erasure_coding.ShardId]*EcNode)
	for rackId, shards := range shardsPerRack {
		if len(shards) <= maxPerRack {
			continue
		}
		// Pick excess shards to move
		excess := len(shards) - maxPerRack
		ecNodes := rackEcNodesWithVid[rackId]
		for i := 0; i < excess && i < len(shards); i++ {
			shardId := shards[i]
			// Find which node has this shard
			for _, ecNode := range ecNodes {
				si := findEcVolumeShardsInfo(ecNode, vid, ecb.diskType)
				if si.Has(shardId) {
					shardsToMove[shardId] = ecNode
					break
				}
			}
		}
	}

	// Move shards to racks that have fewer than maxPerRack of this type
	for shardId, ecNode := range shardsToMove {
		// Find destination rack with room for this shard type
		destRackId, err := ecb.pickRackForShardType(racks, shardsPerRack, maxPerRack, rackToShardCount)
		if err != nil {
			fmt.Printf("ec %s shard %d.%d at %s can not find a destination rack:\n%s\n", shardType, vid, shardId, ecNode.info.Id, err.Error())
			continue
		}

		var possibleDestinationEcNodes []*EcNode
		for _, n := range racks[destRackId].ecNodes {
			possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
		}
		err = ecb.pickOneEcNodeAndMoveOneShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes)
		if err != nil {
			return err
		}

		// Update tracking
		shardsPerRack[string(destRackId)] = append(shardsPerRack[string(destRackId)], shardId)
		// Remove from source rack
		srcRack := string(ecNode.rack)
		for i, s := range shardsPerRack[srcRack] {
			if s == shardId {
				shardsPerRack[srcRack] = append(shardsPerRack[srcRack][:i], shardsPerRack[srcRack][i+1:]...)
				break
			}
		}
		rackToShardCount[string(destRackId)] += 1
		rackToShardCount[srcRack] -= 1
		racks[destRackId].freeEcSlot -= 1
		racks[RackId(srcRack)].freeEcSlot += 1
	}

	return nil
}

// pickRackForShardType selects a rack that has room for more shards of a specific type
func (ecb *ecBalancer) pickRackForShardType(
	rackToEcNodes map[RackId]*EcRack,
	shardsPerRack map[string][]erasure_coding.ShardId,
	maxPerRack int,
	rackToShardCount map[string]int,
) (RackId, error) {
	var candidates []RackId
	minShards := maxPerRack + 1

	for rackId, rack := range rackToEcNodes {
		if rack.freeEcSlot <= 0 {
			continue
		}
		currentCount := len(shardsPerRack[string(rackId)])
		if currentCount >= maxPerRack {
			continue
		}
		// For EC shards, replica placement constraint only applies when DiffRackCount > 0.
		if ecb.replicaPlacement != nil && ecb.replicaPlacement.DiffRackCount > 0 && rackToShardCount[string(rackId)] >= ecb.replicaPlacement.DiffRackCount {
			continue
		}
		if currentCount < minShards {
			candidates = nil
			minShards = currentCount
		}
		if currentCount == minShards {
			candidates = append(candidates, rackId)
		}
	}

	if len(candidates) == 0 {
		return "", errors.New("no rack available for shard type balancing")
	}
	return candidates[rand.IntN(len(candidates))], nil
}

func (ecb *ecBalancer) pickRackToBalanceShardsInto(rackToEcNodes map[RackId]*EcRack, rackToShardCount map[string]int) (RackId, error) {
	targets := []RackId{}
	targetShards := -1
	for _, shards := range rackToShardCount {
		if shards > targetShards {
			targetShards = shards
		}
	}

	details := ""
	for rackId, rack := range rackToEcNodes {
		shards := rackToShardCount[string(rackId)]

		if rack.freeEcSlot <= 0 {
			details += fmt.Sprintf("  Skipped %s because it has no free slots\n", rackId)
			continue
		}
		// For EC shards, replica placement constraint only applies when DiffRackCount > 0.
		// When DiffRackCount = 0 (e.g., replica placement "000"), EC shards should be
		// distributed freely across racks for fault tolerance - the "000" means
		// "no volume replication needed" because erasure coding provides redundancy.
		if ecb.replicaPlacement != nil && ecb.replicaPlacement.DiffRackCount > 0 && shards > ecb.replicaPlacement.DiffRackCount {
			details += fmt.Sprintf("  Skipped %s because shards %d > replica placement limit for other racks (%d)\n", rackId, shards, ecb.replicaPlacement.DiffRackCount)
			continue
		}

		if shards < targetShards {
			// Favor racks with less shards, to ensure an uniform distribution.
			targets = nil
			targetShards = shards
		}
		if shards == targetShards {
			targets = append(targets, rackId)
		}
	}

	if len(targets) == 0 {
		return "", errors.New(details)
	}
	return targets[rand.IntN(len(targets))], nil
}

func (ecb *ecBalancer) balanceEcShardsWithinRacks(collection string) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)
	racks := ecb.racks()

	// spread the ec shards evenly
	ewg := ecb.errorWaitGroup()
	for vid, locations := range vidLocations {

		// see the volume's shards are in how many racks, and how many in each rack
		rackToShardCount := countShardsByRack(vid, locations, ecb.diskType)
		rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
			return string(ecNode.rack)
		})

		for rackId, _ := range rackToShardCount {

			var possibleDestinationEcNodes []*EcNode
			for _, n := range racks[RackId(rackId)].ecNodes {
				if _, found := n.info.DiskInfos[string(ecb.diskType)]; found {
					possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
				}
			}
			sourceEcNodes := rackEcNodesWithVid[rackId]
			averageShardsPerEcNode := ceilDivide(rackToShardCount[rackId], len(possibleDestinationEcNodes))
			ewg.Add(func() error {
				return ecb.doBalanceEcShardsWithinOneRack(averageShardsPerEcNode, collection, vid, sourceEcNodes, possibleDestinationEcNodes)
			})
		}
	}
	return ewg.Wait()
}

func (ecb *ecBalancer) doBalanceEcShardsWithinOneRack(averageShardsPerEcNode int, collection string, vid needle.VolumeId, existingLocations, possibleDestinationEcNodes []*EcNode) error {
	for _, ecNode := range existingLocations {

		si := findEcVolumeShardsInfo(ecNode, vid, ecb.diskType)
		overLimitCount := si.Count() - averageShardsPerEcNode

		for _, shardId := range si.Ids() {

			if overLimitCount <= 0 {
				break
			}

			fmt.Printf("%s has %d overlimit, moving ec shard %d.%d\n", ecNode.info.Id, overLimitCount, vid, shardId)

			err := ecb.pickOneEcNodeAndMoveOneShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes)
			if err != nil {
				return err
			}

			overLimitCount--
		}
	}

	return nil
}

func (ecb *ecBalancer) balanceEcRacks() error {
	// balance one rack for all ec shards
	ewg := ecb.errorWaitGroup()
	for _, ecRack := range ecb.racks() {
		ewg.Add(func() error {
			return ecb.doBalanceEcRack(ecRack)
		})
	}
	return ewg.Wait()
}

func (ecb *ecBalancer) doBalanceEcRack(ecRack *EcRack) error {
	if len(ecRack.ecNodes) <= 1 {
		return nil
	}

	var rackEcNodes []*EcNode
	for _, node := range ecRack.ecNodes {
		rackEcNodes = append(rackEcNodes, node)
	}

	ecNodeIdToShardCount := groupByCount(rackEcNodes, func(ecNode *EcNode) (id string, count int) {
		diskInfo, found := ecNode.info.DiskInfos[string(ecb.diskType)]
		if !found {
			return
		}
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			count += erasure_coding.ShardsCountFromVolumeEcShardInformationMessage(ecShardInfo)
		}
		return ecNode.info.Id, count
	})

	var totalShardCount int
	for _, count := range ecNodeIdToShardCount {
		totalShardCount += count
	}

	averageShardCount := ceilDivide(totalShardCount, len(rackEcNodes))

	hasMove := true
	for hasMove {
		hasMove = false
		slices.SortFunc(rackEcNodes, func(a, b *EcNode) int {
			return b.freeEcSlot - a.freeEcSlot
		})
		emptyNode, fullNode := rackEcNodes[0], rackEcNodes[len(rackEcNodes)-1]
		emptyNodeShardCount, fullNodeShardCount := ecNodeIdToShardCount[emptyNode.info.Id], ecNodeIdToShardCount[fullNode.info.Id]
		if fullNodeShardCount > averageShardCount && emptyNodeShardCount+1 <= averageShardCount {

			emptyNodeIds := make(map[uint32]bool)
			if emptyDiskInfo, found := emptyNode.info.DiskInfos[string(ecb.diskType)]; found {
				for _, shards := range emptyDiskInfo.EcShardInfos {
					emptyNodeIds[shards.Id] = true
				}
			}
			if fullDiskInfo, found := fullNode.info.DiskInfos[string(ecb.diskType)]; found {
				for _, shards := range fullDiskInfo.EcShardInfos {
					if _, found := emptyNodeIds[shards.Id]; found {
						continue
					}
					si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shards)
					for _, shardId := range si.Ids() {
						vid := needle.VolumeId(shards.Id)
						// For balancing, strictly require matching disk type
						destDiskId := pickBestDiskOnNode(emptyNode, vid, ecb.diskType, true)

						if destDiskId > 0 {
							fmt.Printf("%s moves ec shards %d.%d to %s (disk %d)\n", fullNode.info.Id, shards.Id, shardId, emptyNode.info.Id, destDiskId)
						} else {
							fmt.Printf("%s moves ec shards %d.%d to %s\n", fullNode.info.Id, shards.Id, shardId, emptyNode.info.Id)
						}

						err := moveMountedShardToEcNode(ecb.commandEnv, fullNode, shards.Collection, vid, shardId, emptyNode, destDiskId, ecb.applyBalancing, ecb.diskType)
						if err != nil {
							return err
						}

						ecNodeIdToShardCount[emptyNode.info.Id]++
						ecNodeIdToShardCount[fullNode.info.Id]--
						hasMove = true
						break
					}
					break
				}
			}
		}
	}

	return nil
}

func (ecb *ecBalancer) pickEcNodeToBalanceShardsInto(vid needle.VolumeId, existingLocation *EcNode, possibleDestinations []*EcNode) (*EcNode, error) {
	if existingLocation == nil {
		return nil, fmt.Errorf("INTERNAL: missing source nodes")
	}
	if len(possibleDestinations) == 0 {
		return nil, fmt.Errorf("INTERNAL: missing destination nodes")
	}

	nodeShards := map[*EcNode]int{}
	for _, node := range possibleDestinations {
		count := 0
		if si := findEcVolumeShardsInfo(node, vid, ecb.diskType); si != nil {
			count = si.Count()
		}
		nodeShards[node] = count
	}

	targets := []*EcNode{}
	targetShards := -1
	for _, shards := range nodeShards {
		if shards > targetShards {
			targetShards = shards
		}
	}

	details := ""
	for _, node := range possibleDestinations {
		if node.info.Id == existingLocation.info.Id {
			continue
		}
		if node.freeEcSlot <= 0 {
			details += fmt.Sprintf("  Skipped %s because it has no free slots\n", node.info.Id)
			continue
		}

		shards := nodeShards[node]
		// For EC shards, replica placement constraint only applies when SameRackCount > 0.
		// When SameRackCount = 0 (e.g., replica placement "000"), EC shards should be
		// distributed freely within racks - the "000" means "no volume replication needed"
		// because erasure coding provides redundancy.
		if ecb.replicaPlacement != nil && ecb.replicaPlacement.SameRackCount > 0 && shards > ecb.replicaPlacement.SameRackCount+1 {
			details += fmt.Sprintf("  Skipped %s because shards %d > replica placement limit for the rack (%d + 1)\n", node.info.Id, shards, ecb.replicaPlacement.SameRackCount)
			continue
		}

		if shards < targetShards {
			// Favor nodes with less shards, to ensure an uniform distribution.
			targets = nil
			targetShards = shards
		}
		if shards == targetShards {
			targets = append(targets, node)
		}
	}

	if len(targets) == 0 {
		return nil, errors.New(details)
	}

	// When multiple nodes have the same shard count, prefer nodes with better disk distribution
	// (i.e., nodes with more disks that have fewer shards of this volume)
	if len(targets) > 1 {
		slices.SortFunc(targets, func(a, b *EcNode) int {
			aScore := diskDistributionScore(a, vid)
			bScore := diskDistributionScore(b, vid)
			return aScore - bScore // Lower score is better
		})
		return targets[0], nil
	}

	return targets[rand.IntN(len(targets))], nil
}

// diskDistributionScore calculates a score for how well-distributed shards are on the node's disks
// Lower score is better (means more room for balanced distribution)
func diskDistributionScore(ecNode *EcNode, vid needle.VolumeId) int {
	if len(ecNode.disks) == 0 {
		return 0
	}

	// Sum the existing shard count for this volume on each disk
	// Lower total means more room for new shards
	score := 0
	for _, disk := range ecNode.disks {
		if si, ok := disk.ecShards[vid]; ok {
			score += si.Count() * 10 // Weight shards of this volume heavily
		}
		score += disk.ecShardCount // Also consider total shards on disk
	}
	return score
}

// pickBestDiskOnNode selects the best disk on a node for placing a new EC shard
// It prefers disks of the specified type with fewer shards and more free slots
// If strictDiskType is false, it will fall back to other disk types if no matching disk is found
func pickBestDiskOnNode(ecNode *EcNode, vid needle.VolumeId, diskType types.DiskType, strictDiskType bool) uint32 {
	if len(ecNode.disks) == 0 {
		return 0 // No disk info available, let the server decide
	}

	var bestDiskId uint32
	bestScore := -1
	var fallbackDiskId uint32
	fallbackScore := -1

	for diskId, disk := range ecNode.disks {
		if disk.freeEcSlots <= 0 {
			continue
		}

		// Check if this volume already has shards on this disk
		existingShards := 0
		if si, ok := disk.ecShards[vid]; ok {
			existingShards = si.Count()
		}

		// Score: prefer disks with fewer total shards and fewer shards of this volume
		// Lower score is better
		score := disk.ecShardCount*10 + existingShards*100

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

// pickEcNodeAndDiskToBalanceShardsInto picks both a destination node and specific disk
func (ecb *ecBalancer) pickEcNodeAndDiskToBalanceShardsInto(vid needle.VolumeId, existingLocation *EcNode, possibleDestinations []*EcNode) (*EcNode, uint32, error) {
	node, err := ecb.pickEcNodeToBalanceShardsInto(vid, existingLocation, possibleDestinations)
	if err != nil {
		return nil, 0, err
	}

	// For balancing, strictly require matching disk type
	diskId := pickBestDiskOnNode(node, vid, ecb.diskType, true)
	return node, diskId, nil
}

func (ecb *ecBalancer) pickOneEcNodeAndMoveOneShard(existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, possibleDestinationEcNodes []*EcNode) error {
	destNode, destDiskId, err := ecb.pickEcNodeAndDiskToBalanceShardsInto(vid, existingLocation, possibleDestinationEcNodes)
	if err != nil {
		fmt.Printf("WARNING: Could not find suitable target node for %d.%d:\n%s", vid, shardId, err.Error())
		return nil
	}

	if destDiskId > 0 {
		fmt.Printf("%s moves ec shard %d.%d to %s (disk %d)\n", existingLocation.info.Id, vid, shardId, destNode.info.Id, destDiskId)
	} else {
		fmt.Printf("%s moves ec shard %d.%d to %s\n", existingLocation.info.Id, vid, shardId, destNode.info.Id)
	}
	return moveMountedShardToEcNode(ecb.commandEnv, existingLocation, collection, vid, shardId, destNode, destDiskId, ecb.applyBalancing, ecb.diskType)
}

func pickNEcShardsToMoveFrom(ecNodes []*EcNode, vid needle.VolumeId, n int, diskType types.DiskType) map[erasure_coding.ShardId]*EcNode {
	picked := make(map[erasure_coding.ShardId]*EcNode)
	var candidateEcNodes []*CandidateEcNode
	for _, ecNode := range ecNodes {
		si := findEcVolumeShardsInfo(ecNode, vid, diskType)
		if si.Count() > 0 {
			candidateEcNodes = append(candidateEcNodes, &CandidateEcNode{
				ecNode:     ecNode,
				shardCount: si.Count(),
			})
		}
	}
	slices.SortFunc(candidateEcNodes, func(a, b *CandidateEcNode) int {
		return b.shardCount - a.shardCount
	})
	for i := 0; i < n; i++ {
		selectedEcNodeIndex := -1
		for i, candidateEcNode := range candidateEcNodes {
			si := findEcVolumeShardsInfo(candidateEcNode.ecNode, vid, diskType)
			if si.Count() > 0 {
				selectedEcNodeIndex = i
				for _, shardId := range si.Ids() {
					candidateEcNode.shardCount--
					picked[shardId] = candidateEcNode.ecNode
					candidateEcNode.ecNode.deleteEcVolumeShards(vid, []erasure_coding.ShardId{shardId}, diskType)
					break
				}
				break
			}
		}
		if selectedEcNodeIndex >= 0 {
			ensureSortedEcNodes(candidateEcNodes, selectedEcNodeIndex, func(i, j int) bool {
				return candidateEcNodes[i].shardCount > candidateEcNodes[j].shardCount
			})
		}

	}
	return picked
}

func (ecb *ecBalancer) collectVolumeIdToEcNodes(collection string) map[needle.VolumeId][]*EcNode {
	vidLocations := make(map[needle.VolumeId][]*EcNode)
	for _, ecNode := range ecb.ecNodes {
		diskInfo, found := ecNode.info.DiskInfos[string(ecb.diskType)]
		if !found {
			continue
		}
		for _, shardInfo := range diskInfo.EcShardInfos {
			// ignore if not in current collection
			if shardInfo.Collection == collection {
				vidLocations[needle.VolumeId(shardInfo.Id)] = append(vidLocations[needle.VolumeId(shardInfo.Id)], ecNode)
			}
		}
	}
	return vidLocations
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
		fmt.Printf("WARNING: No collections to balance EC volumes across.\n")
	}
	for _, c := range collections {
		if err = ecb.balanceEcVolumes(c); err != nil {
			return err
		}
	}

	if err := ecb.balanceEcRacks(); err != nil {
		return fmt.Errorf("balance ec racks: %w", err)
	}

	return nil
}

// compileCollectionPattern compiles a regex pattern for collection matching.
// Empty patterns match empty collections only.
func compileCollectionPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		// empty pattern matches empty collection
		return regexp.Compile("^$")
	}
	return regexp.Compile(pattern)
}
