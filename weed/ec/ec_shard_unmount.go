package ec

import (
	"context"
	"fmt"
	"io"
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
	ecShardActionTimeout = 1 * time.Minute
)

// ShardRef names one EC shard copy, optionally pinned to a node address.
type ShardRef struct {
	ShardID     uint32
	Collection  string
	NodeAddress string
}

func (s *ShardRef) String() string {
	if s.NodeAddress == "" {
		return fmt.Sprintf("%d", s.ShardID)
	}
	return fmt.Sprintf("%d@%s", s.ShardID, s.NodeAddress)
}

// ShardRefsFromString parses a comma-separated list of shard IDs, each either a
// bare numeric ID or <shard_id>@<node_address> to pick one copy.
func ShardRefsFromString(shards string) ([]*ShardRef, error) {
	res := []*ShardRef{}

	for _, s := range strings.Split(shards, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("empty shard ID in %q", shards)
		}

		// optional <shard ID>@<node address> to pick one copy
		idStr, addr, _ := strings.Cut(s, "@")

		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 || id >= erasure_coding.MaxShardCount {
			return nil, fmt.Errorf("invalid shard ID %q", s)
		}

		res = append(res, &ShardRef{ShardID: uint32(id), NodeAddress: addr})
	}

	return res, nil
}

// ShardUnmountRequest describes one ec.shard.unmount run.
type ShardUnmountRequest struct {
	Topology      *master_pb.TopologyInfo
	VolumeID      uint32
	Shards        []*ShardRef
	Delete        bool
	IgnoreInvalid bool
	Apply         bool
}

type ecShardUnmounter struct {
	env    *Env
	writer io.Writer
	req    ShardUnmountRequest
}

// UnmountShards unmounts, and optionally deletes, the requested EC shard
// copies, resolving them against the live topology first. Dry-run unless
// req.Apply is set.
func UnmountShards(env *Env, writer io.Writer, req ShardUnmountRequest) error {
	c := &ecShardUnmounter{env: env, writer: writer, req: req}
	return c.doShardsUnmount()
}

func (c *ecShardUnmounter) write(format string, a ...any) {
	fmt.Fprintf(c.writer, format, a...)
}

func (c *ecShardUnmounter) liveShardsForVolume() []*ShardRef {
	shards := []*ShardRef{}

	for _, dci := range c.req.Topology.GetDataCenterInfos() {
		for _, ri := range dci.GetRackInfos() {
			for _, dni := range ri.GetDataNodeInfos() {
				nodeAddress := dni.GetAddress()
				for _, di := range dni.GetDiskInfos() {
					for _, eci := range di.GetEcShardInfos() {
						if eci.GetId() == c.req.VolumeID {
							sinfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
							for _, sid := range sinfo.Ids() {
								shards = append(shards, &ShardRef{
									ShardID:     uint32(sid),
									Collection:  eci.GetCollection(),
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

func (c *ecShardUnmounter) printShards(ss []*ShardRef) {
	for _, s := range ss {
		c.write("\t%v\n", s)
	}
	c.write("\n")
}

func (c *ecShardUnmounter) doShardsUnmount() error {
	liveShards := c.liveShardsForVolume()
	c.write("Live shard topology for volume ID %d (%d shards):\n", c.req.VolumeID, len(liveShards))
	c.printShards(liveShards)

	// resolve target shards against the live topology
	targetShards := []*ShardRef{}
	for _, ps := range c.req.Shards {
		var result *ShardRef
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
			if !c.req.IgnoreInvalid {
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
	if c.req.Delete {
		mode = "unmount + delete"
	}
	c.write("Will %s %d shard(s):\n", mode, len(targetShards))
	c.printShards(targetShards)

	if !c.req.Apply {
		c.write("Not proceeding in dry-run mode\n")
		return nil
	}

	if !c.env.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	for _, s := range targetShards {
		if err := c.unmountShard(s); err != nil {
			return err
		}
	}

	c.write("\nAll done!\n")
	return nil
}

func (c *ecShardUnmounter) unmountShard(s *ShardRef) error {
	ctx, cancel := context.WithTimeout(context.Background(), ecShardActionTimeout)
	defer cancel()

	return operation.WithVolumeServerClient(false, pb.ServerAddress(s.NodeAddress), c.env.GrpcDialOption, func(vsc volume_server_pb.VolumeServerClient) error {
		c.write("Unmounting shard %v for volume ID %d...\n", s, c.req.VolumeID)
		if _, err := vsc.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: c.req.VolumeID,
			ShardIds: []uint32{s.ShardID},
		}); err != nil {
			return err
		}

		if c.req.Delete {
			c.write("Deleting shard %v for volume ID %d...\n", s, c.req.VolumeID)
			if _, err := vsc.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
				VolumeId:   c.req.VolumeID,
				Collection: s.Collection,
				ShardIds:   []uint32{s.ShardID},
			}); err != nil {
				return err
			}
		}

		return nil
	})
}
