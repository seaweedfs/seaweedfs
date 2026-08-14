package shell

import (
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/ec"
)

func init() {
	Commands = append(Commands, &commandEcShardUnmount{})
}

type commandEcShardUnmount struct {
}

func (c *commandEcShardUnmount) Name() string {
	return "ec.shard.unmount"
}

func (c *commandEcShardUnmount) Help() string {
	return `Unmounts, and optionally deletes, EC volume shards.

	ec.shard.unmount --volumeId=<volume id> --shardId=<comma-separated list of shard IDs> [--delete]

	Shard IDs can be specified either as a single numeric ID (f.ex. "2") or with a
	node address (<shard_id>@<address>, f.ex. "3@10.200.18.88:9007") to pick one
	copy when shards are repeated and/or co-located on the same node. The live
	topology is printed in the same form, so its entries can be copied verbatim.

	Unmounting only takes shards offline on their volume servers; pass --delete to
	also remove the shard files. This is useful to clean up over-replicated EC
	volumes without a full rebalance.

	Shards are processed one by one, best-effort: on error the command stops and
	leaves already-processed shards as they are.

	This command can, and will, irrevocably delete data if used incorrectly.
	Tread carefully.
`
}

func (c *commandEcShardUnmount) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcShardUnmount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	if handleHelpRequest(c, args, writer) {
		return nil
	}
	ecShardUnmountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeID := ecShardUnmountCommand.Uint("volumeId", 0, "volume ID for the shards to process")
	shardIDsStr := ecShardUnmountCommand.String("shardId", "", "comma-separated EC shard IDs for the volume")
	delete := ecShardUnmountCommand.Bool("delete", false, "also delete the shard files after unmounting them")
	ignoreInvalid := ecShardUnmountCommand.Bool("ignoreInvalid", false, "ignore user-provided shards that match nothing live, and try to proceed anyway")
	apply := ecShardUnmountCommand.Bool("apply", false, "Execute actions on the cluster. By default only lists what actions would be executed.")
	applyAlias := ecShardUnmountCommand.Bool("force", false, "Execute actions on the cluster (alias for -apply).")

	if err = ecShardUnmountCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if *volumeID == 0 {
		return fmt.Errorf("no volume ID")
	}
	if *volumeID > math.MaxUint32 {
		return fmt.Errorf("invalid volume ID %d", *volumeID)
	}

	if *shardIDsStr == "" {
		return fmt.Errorf("missing shardId")
	}
	shards, err := ec.ShardRefsFromString(*shardIDsStr)
	if err != nil {
		return err
	}

	// collect topology information
	topology, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	return ec.UnmountShards(commandEnv.ecEnv(), writer, ec.ShardUnmountRequest{
		Topology:      topology,
		VolumeID:      uint32(*volumeID),
		Shards:        shards,
		Delete:        *delete,
		IgnoreInvalid: *ignoreInvalid,
		Apply:         *apply || *applyAlias,
	})
}
