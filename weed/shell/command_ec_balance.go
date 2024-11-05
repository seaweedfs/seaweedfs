package shell

import (
	"flag"
	"fmt"
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

func (c *commandEcBalance) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyBalancing, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var collections []string
	if *collection == "EACH_COLLECTION" {
		collections, err = ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
	} else {
		collections = append(collections, *collection)
	}
	fmt.Printf("balanceEcVolumes collections %+v\n", len(collections))

	return EcBalance(commandEnv, collections, *dc, *applyBalancing)
}
