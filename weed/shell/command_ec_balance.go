package shell

import (
	"flag"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
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

	ec.balance [-collection EACH_COLLECTION|<collection_name>] [-apply] [-dataCenter <data_center>] [-shardReplicaPlacement <replica_placement>] [-diskType <disk_type>] [-volumeIds <id>[,<id>...]]

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
	balanceCommand, f := newEcBalanceFlagSet(c.Name())

	if err = balanceCommand.Parse(args); err != nil {
		return err
	}

	handleDeprecatedForceFlag(writer, balanceCommand, f.force, f.apply)
	infoAboutSimulationMode(writer, *f.apply, "-apply")

	var volumeIds []needle.VolumeId
	if *f.volumeIds != "" {
		if volumeIds, err = parseVolumeIdsFlag(*f.volumeIds); err != nil {
			return err
		}
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	collections, err := resolveEcCollections(commandEnv, *f.collection)
	if err != nil {
		return err
	}
	glog.V(1).Infof("balanceEcVolumes collections %+v\n", len(collections))

	rp, err := parseReplicaPlacementArg(commandEnv, *f.shardReplicaPlacement)
	if err != nil {
		return err
	}

	diskType := types.ToDiskType(*f.diskType)

	return EcBalance(commandEnv, collections, *f.dc, rp, diskType, *f.maxParallelization, *f.ioBytePerSecond, *f.apply, nil, volumeIds)
}

type ecBalanceFlags struct {
	collection, dc, shardReplicaPlacement, diskType, volumeIds *string
	maxParallelization                                         *int
	ioBytePerSecond                                            *int64
	apply, force                                               *bool
}

func newEcBalanceFlagSet(name string) (*flag.FlagSet, *ecBalanceFlags) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return fs, &ecBalanceFlags{
		collection:            fs.String("collection", string(wildcard.CollectionFilterEach), "comma-separated collection names, wildcards, or regex patterns, \""+CollectionDefault+"\" for the collection with no name, or \""+string(wildcard.CollectionFilterEach)+"\" for each collection"),
		dc:                    fs.String("dataCenter", "", "only apply the balancing for this dataCenter"),
		shardReplicaPlacement: fs.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty"),
		diskType:              fs.String("diskType", "", "the disk type for EC shards (hdd, ssd, or empty for default hdd)"),
		volumeIds:             fs.String("volumeIds", "", "optional comma-separated list of ec volume ids to balance; defaults to all"),
		maxParallelization:    fs.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible"),
		ioBytePerSecond:       fs.Int64("ioBytePerSecond", 0, "limit the speed of each shard copy; 0 falls back to the volume server's maintenance rate"),
		apply:                 fs.Bool("apply", false, "apply the balancing plan"),
		force:                 fs.Bool("force", false, "apply the balancing plan (alias for -apply)"),
	}
}

// EcBalanceArgsApply reports whether ec.balance accepts these arguments and applies its plan.
func EcBalanceArgsApply(args []string) bool {
	fs, f := newEcBalanceFlagSet("ec.balance")
	fs.SetOutput(io.Discard)
	if fs.Parse(args) != nil {
		return false
	}
	handleDeprecatedForceFlag(io.Discard, fs, f.force, f.apply)
	return *f.apply
}
