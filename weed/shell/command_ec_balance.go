package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	commands = append(commands, &commandEcBalance{})
}

type commandEcBalance struct {
}

func (c *commandEcBalance) Name() string {
	return "ec.balance"
}

func (c *commandEcBalance) Help() string {
	return `balance all ec shards among volume servers

	ec.balance [-c EACH_COLLECTION|<collection_name>] [-force] [-dataCenter <data_center>]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			balanceEcVolumes()
		}
	}

	func balanceEcVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}

`
}

func (c *commandEcBalance) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	var resp *master_pb.VolumeListResponse
	ctx := context.Background()
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	typeToNodes := collectVolumeServersByType(resp.TopologyInfo, *dc)
	for _, volumeServers := range typeToNodes {

		fmt.Printf("balanceEcVolumes servers %d\n", len(volumeServers))

		if len(volumeServers) < 2 {
			continue
		}

		if *collection == "EACH_COLLECTION" {
			collections, err := ListCollectionNames(commandEnv, false, true)
			if err != nil {
				return err
			}
			fmt.Printf("balanceEcVolumes collections %+v\n", len(collections))
			for _, c := range collections {
				fmt.Printf("balanceEcVolumes collection %+v\n", c)
				if err = balanceEcVolumes(commandEnv, c, *applyBalancing); err != nil {
					return err
				}
			}
		} else {
			if err = balanceEcVolumes(commandEnv, *collection, *applyBalancing); err != nil {
				return err
			}
		}

	}
	return nil
}

func balanceEcVolumes(commandEnv *commandEnv, collection string, applyBalancing bool) error {

	ctx := context.Background()

	fmt.Printf("balanceEcVolumes %s\n", collection)

	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := collectEcNodes(ctx, commandEnv)
	if err != nil {
		return err
	}
	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	// vid => []ecNode
	vidLocations := make(map[needle.VolumeId][]*EcNode)
	for _, ecNode := range allEcNodes {
		for _, shardInfo := range ecNode.info.EcShardInfos {
			vidLocations[needle.VolumeId(shardInfo.Id)] = append(vidLocations[needle.VolumeId(shardInfo.Id)], ecNode)
		}
	}

	for vid, locations := range vidLocations {

		if err := doDeduplicateEcShards(ctx, commandEnv, collection, vid, locations, applyBalancing); err != nil {
			return err
		}

		if err := doBalanceEcShards(ctx, commandEnv, collection, vid, locations, allEcNodes, applyBalancing); err != nil {
			return err
		}

	}

	return nil
}

func doBalanceEcShards(ctx context.Context, commandEnv *commandEnv, collection string, vid needle.VolumeId, locations []*EcNode, allEcNodes []*EcNode, applyBalancing bool) error {
	// collect all ec nodes with at least one free slot
	var possibleDestinationEcNodes []*EcNode
	for _, ecNode := range allEcNodes {
		if ecNode.freeEcSlot > 0 {
			possibleDestinationEcNodes = append(possibleDestinationEcNodes, ecNode)
		}
	}
	// calculate average number of shards an ec node should have for one volume
	averageShardsPerEcNode := int(math.Ceil(float64(erasure_coding.TotalShardsCount) / float64(len(possibleDestinationEcNodes))))
	fmt.Printf("vid %d averageShardsPerEcNode %+v\n", vid, averageShardsPerEcNode)
	// check whether this volume has ecNodes that are over average
	isOverLimit := false
	for _, ecNode := range locations {
		shardBits := findEcVolumeShards(ecNode, vid)
		if shardBits.ShardIdCount() > averageShardsPerEcNode {
			isOverLimit = true
			fmt.Printf("vid %d %s has %d shards, isOverLimit %+v\n", vid, ecNode.info.Id, shardBits.ShardIdCount(), isOverLimit)
			break
		}
	}
	if isOverLimit {
		if err := spreadShardsIntoMoreDataNodes(ctx, commandEnv, averageShardsPerEcNode, collection, vid, locations, possibleDestinationEcNodes, applyBalancing); err != nil {
			return err
		}
	}
	return nil
}

func doDeduplicateEcShards(ctx context.Context, commandEnv *commandEnv, collection string, vid needle.VolumeId, locations []*EcNode, applyBalancing bool) error {

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
		sortEcNodes(ecNodes)
		fmt.Printf("ec shard %d.%d has %d copies, keeping %v\n", vid, shardId, len(ecNodes), ecNodes[0].info.Id)
		if !applyBalancing {
			continue
		}

		duplicatedShardIds := []uint32{uint32(shardId)}
		for _, ecNode := range ecNodes[1:] {
			if err := unmountEcShards(ctx, commandEnv.option.GrpcDialOption, vid, ecNode.info.Id, duplicatedShardIds); err != nil {
				return err
			}
			if err := sourceServerDeleteEcShards(ctx, commandEnv.option.GrpcDialOption, collection, vid, ecNode.info.Id, duplicatedShardIds); err != nil {
				return err
			}
			ecNode.freeEcSlot++
		}
	}
	return nil
}

func spreadShardsIntoMoreDataNodes(ctx context.Context, commandEnv *commandEnv, averageShardsPerEcNode int, collection string, vid needle.VolumeId, existingLocations, possibleDestinationEcNodes []*EcNode, applyBalancing bool) error {

	for _, ecNode := range existingLocations {

		shardBits := findEcVolumeShards(ecNode, vid)
		overLimitCount := shardBits.ShardIdCount() - averageShardsPerEcNode

		for _, shardId := range shardBits.ShardIds() {

			if overLimitCount <= 0 {
				break
			}

			fmt.Printf("%s has %d overlimit, moving ec shard %d.%d\n", ecNode.info.Id, overLimitCount, vid, shardId)

			err := pickOneEcNodeAndMoveOneShard(ctx, commandEnv, averageShardsPerEcNode, ecNode, collection, vid, shardId, possibleDestinationEcNodes, applyBalancing)
			if err != nil {
				return err
			}

			overLimitCount--
		}
	}

	return nil
}

func pickOneEcNodeAndMoveOneShard(ctx context.Context, commandEnv *commandEnv, averageShardsPerEcNode int, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, possibleDestinationEcNodes []*EcNode, applyBalancing bool) error {

	sortEcNodes(possibleDestinationEcNodes)

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

		err := moveMountedShardToEcNode(ctx, commandEnv, existingLocation, collection, vid, shardId, destEcNode, applyBalancing)
		if err != nil {
			return err
		}

		destEcNode.freeEcSlot--
		existingLocation.freeEcSlot++
		return nil
	}

	return nil
}

func findEcVolumeShards(ecNode *EcNode, vid needle.VolumeId) erasure_coding.ShardBits {

	for _, shardInfo := range ecNode.info.EcShardInfos {
		if needle.VolumeId(shardInfo.Id) == vid {
			return erasure_coding.ShardBits(shardInfo.EcIndexBits)
		}
	}

	return 0
}
