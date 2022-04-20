package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"golang.org/x/exp/slices"
	"io"
)

func init() {
	Commands = append(Commands, &commandEcBalance{})
}

type commandEcBalance struct {
}

func (c *commandEcBalance) Name() string {
	return "ec.balance"
}

func (c *commandEcBalance) Help() string {
	return `balance all ec shards among all racks and volume servers

	ec.balance [-c EACH_COLLECTION|<collection_name>] [-force] [-dataCenter <data_center>]

	Algorithm:

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
			destRack = pickOneRack(rack~shardCount, rack~volumeIdShardCount, averageShardsPerEcRack)
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
			destVolumeServer = pickOneVolumeServer(volumeServer~shardCount, volumeServer~volumeIdShardCount, averageShardCount)
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
}

func (c *commandEcBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, *dc)
	if err != nil {
		return err
	}
	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	racks := collectRacks(allEcNodes)

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
		fmt.Printf("balanceEcVolumes collections %+v\n", len(collections))
		for _, c := range collections {
			fmt.Printf("balanceEcVolumes collection %+v\n", c)
			if err = balanceEcVolumes(commandEnv, c, allEcNodes, racks, *applyBalancing); err != nil {
				return err
			}
		}
	} else {
		if err = balanceEcVolumes(commandEnv, *collection, allEcNodes, racks, *applyBalancing); err != nil {
			return err
		}
	}

	if err := balanceEcRacks(commandEnv, racks, *applyBalancing); err != nil {
		return fmt.Errorf("balance ec racks: %v", err)
	}

	return nil
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
	vidLocations := collectVolumeIdToEcNodes(allEcNodes)
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
	vidLocations := collectVolumeIdToEcNodes(allEcNodes)
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
	vidLocations := collectVolumeIdToEcNodes(allEcNodes)

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
		slices.SortFunc(rackEcNodes, func(a, b *EcNode) bool {
			return a.freeEcSlot > b.freeEcSlot
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

	sortEcNodesByFreeslotsDecending(possibleDestinationEcNodes)

	for _, destEcNode := range possibleDestinationEcNodes {
		if destEcNode.info.Id == existingLocation.info.Id {
			continue
		}

		if destEcNode.freeEcSlot <= 0 {
			continue
		}
		if findEcVolumeShards(destEcNode, vid).ShardIdCount() >= averageShardsPerEcNode {
			continue
		}

		fmt.Printf("%s moves ec shard %d.%d to %s\n", existingLocation.info.Id, vid, shardId, destEcNode.info.Id)

		err := moveMountedShardToEcNode(commandEnv, existingLocation, collection, vid, shardId, destEcNode, applyBalancing)
		if err != nil {
			return err
		}

		return nil
	}

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
	slices.SortFunc(candidateEcNodes, func(a, b *CandidateEcNode) bool {
		return a.shardCount > b.shardCount
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

func collectVolumeIdToEcNodes(allEcNodes []*EcNode) map[needle.VolumeId][]*EcNode {
	vidLocations := make(map[needle.VolumeId][]*EcNode)
	for _, ecNode := range allEcNodes {
		diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
		if !found {
			continue
		}
		for _, shardInfo := range diskInfo.EcShardInfos {
			vidLocations[needle.VolumeId(shardInfo.Id)] = append(vidLocations[needle.VolumeId(shardInfo.Id)], ecNode)
		}
	}
	return vidLocations
}
