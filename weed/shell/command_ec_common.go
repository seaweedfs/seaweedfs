package shell

import (
	"context"
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

func moveMountedShardToEcNode(commandEnv *CommandEnv, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, applyBalancing bool) (err error) {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	copiedShardIds := []uint32{uint32(shardId)}

	if applyBalancing {

		existingServerAddress := pb.NewServerAddressFromDataNode(existingLocation.info)

		// ask destination node to copy shard and the ecx file from source node, and mount it
		copiedShardIds, err = oneServerCopyAndMountEcShardsFromSource(commandEnv.option.GrpcDialOption, destinationEcNode, []uint32{uint32(shardId)}, vid, collection, existingServerAddress)
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

		fmt.Printf("moved ec shard %d.%d %s => %s\n", vid, shardId, existingLocation.info.Id, destinationEcNode.info.Id)

	}

	destinationEcNode.addEcVolumeShards(vid, collection, copiedShardIds)
	existingLocation.deleteEcVolumeShards(vid, copiedShardIds)

	return nil

}

func oneServerCopyAndMountEcShardsFromSource(grpcDialOption grpc.DialOption,
	targetServer *EcNode, shardIdsToCopy []uint32,
	volumeId needle.VolumeId, collection string, existingLocation pb.ServerAddress) (copiedShardIds []uint32, err error) {

	fmt.Printf("allocate %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)

	targetAddress := pb.NewServerAddressFromDataNode(targetServer.info)
	err = operation.WithVolumeServerClient(false, targetAddress, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		if targetAddress != existingLocation {

			fmt.Printf("copy %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)
			_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:       shardIdsToCopy,
				CopyEcxFile:    true,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				SourceDataNode: string(existingLocation),
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s : %v\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id, copyErr)
			}
		}

		fmt.Printf("mount %d.%v on %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   shardIdsToCopy,
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

func eachDataNode(topo *master_pb.TopologyInfo, fn func(dc string, rack RackId, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(dc.Id, RackId(rack.Id), dn)
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

type CandidateEcNode struct {
	ecNode     *EcNode
	shardCount int
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
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += shardBits.ShardIdCount()
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
	return int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countShards(diskInfo.EcShardInfos)
}

type RackId string
type EcNodeId string

type EcNode struct {
	info       *master_pb.DataNodeInfo
	dc         string
	rack       RackId
	freeEcSlot int
}

func (ecNode *EcNode) localShardIdCount(vid uint32) int {
	for _, diskInfo := range ecNode.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if vid == ecShardInfo.Id {
				shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
				return shardBits.ShardIdCount()
			}
		}
	}
	return 0
}

type EcRack struct {
	ecNodes    map[EcNodeId]*EcNode
	freeEcSlot int
}

func collectEcNodes(commandEnv *CommandEnv, selectedDataCenter string) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {

	// list all possible locations
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots = collectEcVolumeServersByDc(topologyInfo, selectedDataCenter)

	sortEcNodesByFreeslotsDescending(ecNodes)

	return
}

func collectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string) (ecNodes []*EcNode, totalFreeEcSlots int) {
	eachDataNode(topo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != dc {
			return
		}

		freeEcSlots := countFreeShardSlots(dn, types.HardDriveType)
		ecNodes = append(ecNodes, &EcNode{
			info:       dn,
			dc:         dc,
			rack:       rack,
			freeEcSlot: int(freeEcSlots),
		})
		totalFreeEcSlots += freeEcSlots
	})
	return
}

func sourceServerDeleteEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeDeletedShardIds []uint32) error {

	fmt.Printf("delete %d.%v from %s\n", volumeId, toBeDeletedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeDeletedShardIds,
		})
		return deleteErr
	})

}

func unmountEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeUnmountedhardIds []uint32) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: toBeUnmountedhardIds,
		})
		return deleteErr
	})
}

func mountEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeMountedhardIds []uint32) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeMountedhardIds,
		})
		return mountErr
	})
}

func ceilDivide(total, n int) int {
	return int(math.Ceil(float64(total) / float64(n)))
}

func findEcVolumeShards(ecNode *EcNode, vid needle.VolumeId) erasure_coding.ShardBits {

	if diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				return erasure_coding.ShardBits(shardInfo.EcIndexBits)
			}
		}
	}

	return 0
}

func (ecNode *EcNode) addEcVolumeShards(vid needle.VolumeId, collection string, shardIds []uint32) *EcNode {

	foundVolume := false
	diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
	if found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				oldShardBits := erasure_coding.ShardBits(shardInfo.EcIndexBits)
				newShardBits := oldShardBits
				for _, shardId := range shardIds {
					newShardBits = newShardBits.AddShardId(erasure_coding.ShardId(shardId))
				}
				shardInfo.EcIndexBits = uint32(newShardBits)
				ecNode.freeEcSlot -= newShardBits.ShardIdCount() - oldShardBits.ShardIdCount()
				foundVolume = true
				break
			}
		}
	} else {
		diskInfo = &master_pb.DiskInfo{
			Type: string(types.HardDriveType),
		}
		ecNode.info.DiskInfos[string(types.HardDriveType)] = diskInfo
	}

	if !foundVolume {
		var newShardBits erasure_coding.ShardBits
		for _, shardId := range shardIds {
			newShardBits = newShardBits.AddShardId(erasure_coding.ShardId(shardId))
		}
		diskInfo.EcShardInfos = append(diskInfo.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          uint32(vid),
			Collection:  collection,
			EcIndexBits: uint32(newShardBits),
			DiskType:    string(types.HardDriveType),
		})
		ecNode.freeEcSlot -= len(shardIds)
	}

	return ecNode
}

func (ecNode *EcNode) deleteEcVolumeShards(vid needle.VolumeId, shardIds []uint32) *EcNode {

	if diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				oldShardBits := erasure_coding.ShardBits(shardInfo.EcIndexBits)
				newShardBits := oldShardBits
				for _, shardId := range shardIds {
					newShardBits = newShardBits.RemoveShardId(erasure_coding.ShardId(shardId))
				}
				shardInfo.EcIndexBits = uint32(newShardBits)
				ecNode.freeEcSlot -= newShardBits.ShardIdCount() - oldShardBits.ShardIdCount()
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

func collectRacks(allEcNodes []*EcNode) map[RackId]*EcRack {
	// collect racks info
	racks := make(map[RackId]*EcRack)
	for _, ecNode := range allEcNodes {
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

func balanceEcVolumes(commandEnv *CommandEnv, collection string, allEcNodes []*EcNode, racks map[RackId]*EcRack, applyBalancing bool) error {

	fmt.Printf("balanceEcVolumes %s\n", collection)

	if err := deleteDuplicatedEcShards(commandEnv, allEcNodes, collection, applyBalancing); err != nil {
		return fmt.Errorf("delete duplicated collection %s ec shards: %v", collection, err)
	}

	if err := balanceEcShardsAcrossRacks(commandEnv, allEcNodes, racks, collection, applyBalancing); err != nil {
		return fmt.Errorf("balance across racks collection %s ec shards: %v", collection, err)
	}

	if err := balanceEcShardsWithinRacks(commandEnv, allEcNodes, racks, collection, applyBalancing); err != nil {
		return fmt.Errorf("balance within racks collection %s ec shards: %v", collection, err)
	}

	return nil
}

func deleteDuplicatedEcShards(commandEnv *CommandEnv, allEcNodes []*EcNode, collection string, applyBalancing bool) error {
	// vid => []ecNode
	vidLocations := collectVolumeIdToEcNodes(allEcNodes, collection)
	// deduplicate ec shards
	for vid, locations := range vidLocations {
		if err := doDeduplicateEcShards(commandEnv, collection, vid, locations, applyBalancing); err != nil {
			return err
		}
	}
	return nil
}

func doDeduplicateEcShards(commandEnv *CommandEnv, collection string, vid needle.VolumeId, locations []*EcNode, applyBalancing bool) error {

	// check whether this volume has ecNodes that are over average
	shardToLocations := make([][]*EcNode, erasure_coding.TotalShardsCount)
	for _, ecNode := range locations {
		shardBits := findEcVolumeShards(ecNode, vid)
		for _, shardId := range shardBits.ShardIds() {
			shardToLocations[shardId] = append(shardToLocations[shardId], ecNode)
		}
	}
	for shardId, ecNodes := range shardToLocations {
		if len(ecNodes) <= 1 {
			continue
		}
		sortEcNodesByFreeslotsAscending(ecNodes)
		fmt.Printf("ec shard %d.%d has %d copies, keeping %v\n", vid, shardId, len(ecNodes), ecNodes[0].info.Id)
		if !applyBalancing {
			continue
		}

		duplicatedShardIds := []uint32{uint32(shardId)}
		for _, ecNode := range ecNodes[1:] {
			if err := unmountEcShards(commandEnv.option.GrpcDialOption, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			if err := sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			ecNode.deleteEcVolumeShards(vid, duplicatedShardIds)
		}
	}
	return nil
}

func balanceEcShardsAcrossRacks(commandEnv *CommandEnv, allEcNodes []*EcNode, racks map[RackId]*EcRack, collection string, applyBalancing bool) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := collectVolumeIdToEcNodes(allEcNodes, collection)
	// spread the ec shards evenly
	for vid, locations := range vidLocations {
		if err := doBalanceEcShardsAcrossRacks(commandEnv, collection, vid, locations, racks, applyBalancing); err != nil {
			return err
		}
	}
	return nil
}

func doBalanceEcShardsAcrossRacks(commandEnv *CommandEnv, collection string, vid needle.VolumeId, locations []*EcNode, racks map[RackId]*EcRack, applyBalancing bool) error {

	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))

	// see the volume's shards are in how many racks, and how many in each rack
	rackToShardCount := groupByCount(locations, func(ecNode *EcNode) (id string, count int) {
		shardBits := findEcVolumeShards(ecNode, vid)
		return string(ecNode.rack), shardBits.ShardIdCount()
	})
	rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
		return string(ecNode.rack)
	})

	// ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
	ecShardsToMove := make(map[erasure_coding.ShardId]*EcNode)
	for rackId, count := range rackToShardCount {
		if count > averageShardsPerEcRack {
			possibleEcNodes := rackEcNodesWithVid[rackId]
			for shardId, ecNode := range pickNEcShardsToMoveFrom(possibleEcNodes, vid, count-averageShardsPerEcRack) {
				ecShardsToMove[shardId] = ecNode
			}
		}
	}

	for shardId, ecNode := range ecShardsToMove {
		rackId := pickOneRack(racks, rackToShardCount, averageShardsPerEcRack)
		if rackId == "" {
			fmt.Printf("ec shard %d.%d at %s can not find a destination rack\n", vid, shardId, ecNode.info.Id)
			continue
		}
		var possibleDestinationEcNodes []*EcNode
		for _, n := range racks[rackId].ecNodes {
			possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
		}
		err := pickOneEcNodeAndMoveOneShard(commandEnv, averageShardsPerEcRack, ecNode, collection, vid, shardId, possibleDestinationEcNodes, applyBalancing)
		if err != nil {
			return err
		}
		rackToShardCount[string(rackId)] += 1
		rackToShardCount[string(ecNode.rack)] -= 1
		racks[rackId].freeEcSlot -= 1
		racks[ecNode.rack].freeEcSlot += 1
	}

	return nil
}

func pickOneRack(rackToEcNodes map[RackId]*EcRack, rackToShardCount map[string]int, averageShardsPerEcRack int) RackId {

	// TODO later may need to add some randomness

	for rackId, rack := range rackToEcNodes {
		if rackToShardCount[string(rackId)] >= averageShardsPerEcRack {
			continue
		}

		if rack.freeEcSlot <= 0 {
			continue
		}

		return rackId
	}

	return ""
}

func balanceEcShardsWithinRacks(commandEnv *CommandEnv, allEcNodes []*EcNode, racks map[RackId]*EcRack, collection string, applyBalancing bool) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := collectVolumeIdToEcNodes(allEcNodes, collection)

	// spread the ec shards evenly
	for vid, locations := range vidLocations {

		// see the volume's shards are in how many racks, and how many in each rack
		rackToShardCount := groupByCount(locations, func(ecNode *EcNode) (id string, count int) {
			shardBits := findEcVolumeShards(ecNode, vid)
			return string(ecNode.rack), shardBits.ShardIdCount()
		})
		rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
			return string(ecNode.rack)
		})

		for rackId, _ := range rackToShardCount {

			var possibleDestinationEcNodes []*EcNode
			for _, n := range racks[RackId(rackId)].ecNodes {
				if _, found := n.info.DiskInfos[string(types.HardDriveType)]; found {
					possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
				}
			}
			sourceEcNodes := rackEcNodesWithVid[rackId]
			averageShardsPerEcNode := ceilDivide(rackToShardCount[rackId], len(possibleDestinationEcNodes))
			if err := doBalanceEcShardsWithinOneRack(commandEnv, averageShardsPerEcNode, collection, vid, sourceEcNodes, possibleDestinationEcNodes, applyBalancing); err != nil {
				return err
			}
		}
	}
	return nil
}

func doBalanceEcShardsWithinOneRack(commandEnv *CommandEnv, averageShardsPerEcNode int, collection string, vid needle.VolumeId, existingLocations, possibleDestinationEcNodes []*EcNode, applyBalancing bool) error {

	for _, ecNode := range existingLocations {

		shardBits := findEcVolumeShards(ecNode, vid)
		overLimitCount := shardBits.ShardIdCount() - averageShardsPerEcNode

		for _, shardId := range shardBits.ShardIds() {

			if overLimitCount <= 0 {
				break
			}

			fmt.Printf("%s has %d overlimit, moving ec shard %d.%d\n", ecNode.info.Id, overLimitCount, vid, shardId)

			err := pickOneEcNodeAndMoveOneShard(commandEnv, averageShardsPerEcNode, ecNode, collection, vid, shardId, possibleDestinationEcNodes, applyBalancing)
			if err != nil {
				return err
			}

			overLimitCount--
		}
	}

	return nil
}

func balanceEcRacks(commandEnv *CommandEnv, racks map[RackId]*EcRack, applyBalancing bool) error {

	// balance one rack for all ec shards
	for _, ecRack := range racks {
		if err := doBalanceEcRack(commandEnv, ecRack, applyBalancing); err != nil {
			return err
		}
	}
	return nil
}

func doBalanceEcRack(commandEnv *CommandEnv, ecRack *EcRack, applyBalancing bool) error {

	if len(ecRack.ecNodes) <= 1 {
		return nil
	}

	var rackEcNodes []*EcNode
	for _, node := range ecRack.ecNodes {
		rackEcNodes = append(rackEcNodes, node)
	}

	ecNodeIdToShardCount := groupByCount(rackEcNodes, func(ecNode *EcNode) (id string, count int) {
		diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
		if !found {
			return
		}
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			count += erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIdCount()
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
			if emptyDiskInfo, found := emptyNode.info.DiskInfos[string(types.HardDriveType)]; found {
				for _, shards := range emptyDiskInfo.EcShardInfos {
					emptyNodeIds[shards.Id] = true
				}
			}
			if fullDiskInfo, found := fullNode.info.DiskInfos[string(types.HardDriveType)]; found {
				for _, shards := range fullDiskInfo.EcShardInfos {
					if _, found := emptyNodeIds[shards.Id]; !found {
						for _, shardId := range erasure_coding.ShardBits(shards.EcIndexBits).ShardIds() {

							fmt.Printf("%s moves ec shards %d.%d to %s\n", fullNode.info.Id, shards.Id, shardId, emptyNode.info.Id)

							err := moveMountedShardToEcNode(commandEnv, fullNode, shards.Collection, needle.VolumeId(shards.Id), shardId, emptyNode, applyBalancing)
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
	}

	return nil
}

func pickOneEcNodeAndMoveOneShard(commandEnv *CommandEnv, averageShardsPerEcNode int, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, possibleDestinationEcNodes []*EcNode, applyBalancing bool) error {

	sortEcNodesByFreeslotsDescending(possibleDestinationEcNodes)
	skipReason := ""
	for _, destEcNode := range possibleDestinationEcNodes {

		if destEcNode.info.Id == existingLocation.info.Id {
			continue
		}

		if destEcNode.freeEcSlot <= 0 {
			skipReason += fmt.Sprintf("  Skipping %s because it has no free slots\n", destEcNode.info.Id)
			continue
		}
		if findEcVolumeShards(destEcNode, vid).ShardIdCount() >= averageShardsPerEcNode {
			skipReason += fmt.Sprintf("  Skipping %s because it %d >= avernageShards (%d)\n",
				destEcNode.info.Id, findEcVolumeShards(destEcNode, vid).ShardIdCount(), averageShardsPerEcNode)
			continue
		}

		fmt.Printf("%s moves ec shard %d.%d to %s\n", existingLocation.info.Id, vid, shardId, destEcNode.info.Id)

		err := moveMountedShardToEcNode(commandEnv, existingLocation, collection, vid, shardId, destEcNode, applyBalancing)
		if err != nil {
			return err
		}

		return nil
	}
	fmt.Printf("WARNING: Could not find suitable taget node for %d.%d:\n%s", vid, shardId, skipReason)
	return nil
}

func pickNEcShardsToMoveFrom(ecNodes []*EcNode, vid needle.VolumeId, n int) map[erasure_coding.ShardId]*EcNode {
	picked := make(map[erasure_coding.ShardId]*EcNode)
	var candidateEcNodes []*CandidateEcNode
	for _, ecNode := range ecNodes {
		shardBits := findEcVolumeShards(ecNode, vid)
		if shardBits.ShardIdCount() > 0 {
			candidateEcNodes = append(candidateEcNodes, &CandidateEcNode{
				ecNode:     ecNode,
				shardCount: shardBits.ShardIdCount(),
			})
		}
	}
	slices.SortFunc(candidateEcNodes, func(a, b *CandidateEcNode) int {
		return b.shardCount - a.shardCount
	})
	for i := 0; i < n; i++ {
		selectedEcNodeIndex := -1
		for i, candidateEcNode := range candidateEcNodes {
			shardBits := findEcVolumeShards(candidateEcNode.ecNode, vid)
			if shardBits > 0 {
				selectedEcNodeIndex = i
				for _, shardId := range shardBits.ShardIds() {
					candidateEcNode.shardCount--
					picked[shardId] = candidateEcNode.ecNode
					candidateEcNode.ecNode.deleteEcVolumeShards(vid, []uint32{uint32(shardId)})
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

func collectVolumeIdToEcNodes(allEcNodes []*EcNode, collection string) map[needle.VolumeId][]*EcNode {
	vidLocations := make(map[needle.VolumeId][]*EcNode)
	for _, ecNode := range allEcNodes {
		diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
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

func EcBalance(commandEnv *CommandEnv, collections []string, dc string, applyBalancing bool) (err error) {
	if len(collections) == 0 {
		return fmt.Errorf("no collections to balance")
	}

	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, dc)
	if err != nil {
		return err
	}
	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	racks := collectRacks(allEcNodes)
	for _, c := range collections {
		if err = balanceEcVolumes(commandEnv, c, allEcNodes, racks, applyBalancing); err != nil {
			return err
		}
	}

	if err := balanceEcRacks(commandEnv, racks, applyBalancing); err != nil {
		return fmt.Errorf("balance ec racks: %v", err)
	}

	return nil
}
