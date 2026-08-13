package shell

import (
	"flag"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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

	ec.balance [-c EACH_COLLECTION|<collection_name>] [-apply] [-dataCenter <data_center>] [-shardReplicaPlacement <replica_placement>] [-diskType <disk_type>] [-volumeIds <id>[,<id>...]]

	Options:
	  -diskType: the disk type for EC shards (hdd, ssd, or empty for default hdd)
	  -volumeIds: only balance these ec volume ids, e.g. -volumeIds 123,456. The plan is
	    built as if no other ec volume existed, so nothing else is moved or deduplicated.
	    Ids without any ec shard in the selected collection, dataCenter and disk type are
	    an error.

	Algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcBalance) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	shardReplicaPlacement := balanceCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	diskTypeStr := balanceCommand.String("diskType", "", "the disk type for EC shards (hdd, ssd, or empty for default hdd)")
	volumeIdsStr := balanceCommand.String("volumeIds", "", "optional comma-separated list of ec volume ids to balance; defaults to all")
	maxParallelization := balanceCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	ioBytePerSecond := balanceCommand.Int64("ioBytePerSecond", 0, "limit the speed of each shard copy; 0 falls back to the volume server's maintenance rate")
	applyBalancing := balanceCommand.Bool("apply", false, "apply the balancing plan")
	// TODO: remove this alias
	applyBalancingAlias := balanceCommand.Bool("force", false, "apply the balancing plan (alias for -apply)")

	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, balanceCommand, applyBalancingAlias, applyBalancing)
	infoAboutSimulationMode(writer, *applyBalancing, "-apply")

	var volumeIds []needle.VolumeId
	if *volumeIdsStr != "" {
		if volumeIds, err = parseVolumeIdsFlag(*volumeIdsStr); err != nil {
			return err
		}
	}

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
	glog.V(1).Infof("balanceEcVolumes collections %+v\n", len(collections))

	rp, err := parseReplicaPlacementArg(commandEnv, *shardReplicaPlacement)
	if err != nil {
		return err
	}

	diskType := types.ToDiskType(*diskTypeStr)

	return EcBalance(commandEnv, collections, *dc, rp, diskType, *maxParallelization, *ioBytePerSecond, *applyBalancing, nil, volumeIds)
}
