package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

const (
	gRPCTimeout = 1 * time.Minute
)

type Shard struct {
	ShardID     uint32
	NodeAddress string
}

func (s *Shard) String() string {
	if s.NodeAddress == "" {
		return fmt.Sprintf("%d", s.ShardID)
	}
	return fmt.Sprintf("%d (%s)", s.ShardID, s.NodeAddress)
}

func shardsFromString(shards string) ([]*Shard, error) {
	res := []*Shard{}

	for _, s := range strings.Split(shards, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("empty shard ID in %q", shards)
		}

		var sid uint32
		var addr string

		if i, err := strconv.Atoi(s); err == nil {
			// single shard ID
			sid = uint32(i)
		} else if ss := strings.SplitN(s, ":", 2); len(ss) == 2 {
			// <shard ID>:<node address>
			i, err := strconv.Atoi(ss[0])
			if err != nil {
				return nil, fmt.Errorf("invalid shard ID %q for %q", ss[0], s)
			}
			sid = uint32(i)
			addr = ss[1]
		} else {
			return nil, fmt.Errorf("invalid shard ID %q", s)
		}

		if sid >= erasure_coding.TotalShardsCount {
			return nil, fmt.Errorf("invalid shard ID for %v", s)
		}

		res = append(res, &Shard{ShardID: sid, NodeAddress: addr})
	}

	return res, nil
}

func init() {
	Commands = append(Commands, &commandEcShardDelete{})
}

type commandEcShardDelete struct {
	env           *CommandEnv
	writer        io.Writer
	topology      *master_pb.TopologyInfo
	volumeID      uint32
	delete        bool
	ignoreInvalid bool
	apply         bool
	shards        []*Shard
}

func (c *commandEcShardDelete) Name() string {
	return "ec.shard.delete"
}

func (c *commandEcShardDelete) Help() string {
	return `Unmounts/delete EC volume shards.

	ec.shard.delete --volumeId=<volume id> --shardId=<comma-separated list of shard IDs>

	Shard IDs can be specified either as a single numeric ID (f.ex. "2") or fully
	qualified (<shard_id>:<adddress>, f.ex. "03:10.200.18.88:9007") in case shards
	are repeated and/or co-located on the same node.

	This command can, and will, irrevocably delete data if used incorrectly.
	Tread carefully.
`
}

func (c *commandEcShardDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcShardDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	ecShardDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeID := ecShardDeleteCommand.Uint("volumeId", 0, "volume ID for the shards to process")
	shardIDsStr := ecShardDeleteCommand.String("shardId", "", "comma-separated EC shard IDs for the volume")
	delete := ecShardDeleteCommand.Bool("delete", false, "Whether to delete selected shards, or just unmount them")
	ignoreInvalid := ecShardDeleteCommand.Bool("ignoreInvalid", false, "Ignore invalid user-provide shards, and try to proceed anyway.")
	apply := ecShardDeleteCommand.Bool("apply", false, "Execute actions on the cluster. By default only lists what actions would be executed.")

	if err = ecShardDeleteCommand.Parse(args); err != nil {
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

	shards, err := shardsFromString(*shardIDsStr)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards, nothing to do")
	}

	// collect topology information
	topology, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	c.env = commandEnv
	c.writer = writer
	c.topology = topology
	c.volumeID = uint32(*volumeID)
	c.delete = *delete
	c.ignoreInvalid = *ignoreInvalid
	c.apply = *apply
	c.shards = shards

	return c.doShardsDelete()
}

func (c *commandEcShardDelete) write(format string, a ...any) {
	fmt.Fprintf(c.writer, format, a...)
}

func (c *commandEcShardDelete) liveShardsForVolume() []*Shard {
	shards := []*Shard{}

	for _, dci := range c.topology.GetDataCenterInfos() {
		for _, ri := range dci.GetRackInfos() {
			for _, dni := range ri.GetDataNodeInfos() {
				nodeAddress := dni.GetAddress()
				for _, di := range dni.GetDiskInfos() {
					for _, eci := range di.GetEcShardInfos() {
						if eci.GetId() == c.volumeID {
							sinfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
							for _, sid := range sinfo.Ids() {
								shards = append(shards, &Shard{
									ShardID:     uint32(sid),
									NodeAddress: nodeAddress,
								})
							}
						}
					}
				}
			}
		}
	}

	sort.SliceStable(shards, func(i, j int) bool { return shards[i].ShardID < shards[j].ShardID })
	return shards
}

func (c *commandEcShardDelete) printShards(ss []*Shard) {
	for _, s := range ss {
		c.write("\t%v\n", s)
	}
	c.write("\n")
}

func (c *commandEcShardDelete) doShardsDelete() error {
	liveShards := c.liveShardsForVolume()
	c.write("Live shard topology for volume ID %d (%d shards):\n", c.volumeID, len(liveShards))
	c.printShards(liveShards)

	// expand target shards for validation
	targetShards := []*Shard{}
	for _, ps := range c.shards {
		var result *Shard
		for _, ts := range liveShards {
			if ts.ShardID == ps.ShardID {
				if ps.NodeAddress == "" || ps.NodeAddress == ts.NodeAddress {
					if result != nil {
						return fmt.Errorf("shard %v is ambiguous", ps)
					}
					result = ts
				}
			}
		}
		if result == nil {
			if !c.ignoreInvalid {
				return fmt.Errorf("shard %v is invalid", ps)
			}
			c.write("!!! ignoring invalid shard %v\n", ps)
		} else {
			targetShards = append(targetShards, result)
		}
	}
	if len(targetShards) == 0 {
		return fmt.Errorf("got no shards to process")
	}

	mode := "unmount"
	if c.delete {
		mode = "unmount + delete"
	}
	c.write("Will %s %d shard(s):\n", mode, len(targetShards))
	c.printShards(targetShards)

	if !c.apply {
		c.write("Not proceeding in dry-run mode\n")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), gRPCTimeout)
	defer cancel()

	for _, s := range targetShards {
		if err := operation.WithVolumeServerClient(false, pb.ServerAddress(s.NodeAddress), c.env.option.GrpcDialOption, func(vsc volume_server_pb.VolumeServerClient) error {
			c.write("Unmounting shard %v for volume ID %d...\n", s, c.volumeID)
			if _, err := vsc.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: c.volumeID,
				ShardIds: []uint32{s.ShardID},
			}); err != nil {
				return err
			}

			if c.delete {
				c.write("Deleting shard %v for volume ID %d...\n", s, c.volumeID)
				if _, err := vsc.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
					VolumeId: c.volumeID,
					ShardIds: []uint32{s.ShardID},
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}

	c.write("\nAll done!\n")
	return nil
}
