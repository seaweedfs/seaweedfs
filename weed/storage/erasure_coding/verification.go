package erasure_coding

import (
	"context"
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

// ServerShardInventory is the per-shard view of one server's EC shards for a
// volume, as reported by VolumeEcShardsInfo. Size is the raw shard file size
// in bytes — callers compare sizes across servers to spot truncated shards.
type ServerShardInventory struct {
	Bits       ShardBits
	Sizes      map[ShardId]int64
	QueryError error
}

// VerifyShardsAcrossServers polls each server's VolumeEcShardsInfo and returns
// the union of shard ids observed plus a per-server breakdown. Query errors
// (including the volume server's "EC volume not found" reply) are recorded on
// the per-server entry and treated as zero shards — they do not abort the
// scan, so the caller can still see partial coverage from healthy peers.
//
// Callers gate destructive actions on RequireFullShardSet(...) against the
// returned union bitmap.
func VerifyShardsAcrossServers(ctx context.Context, volumeID uint32,
	servers []string, dialOption grpc.DialOption) (
	union ShardBits, perServer map[string]ServerShardInventory) {

	perServer = make(map[string]ServerShardInventory, len(servers))

	for _, server := range servers {
		if server == "" {
			continue
		}
		if _, seen := perServer[server]; seen {
			continue
		}

		inv := ServerShardInventory{Sizes: make(map[ShardId]int64)}

		callErr := operation.WithVolumeServerClient(false, pb.ServerAddress(server), dialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				resp, e := client.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
					VolumeId: volumeID,
				})
				if e != nil {
					return e
				}
				for _, s := range resp.EcShardInfos {
					if s.VolumeId != volumeID {
						continue
					}
					sid := ShardId(s.ShardId)
					inv.Bits = inv.Bits.Set(sid)
					inv.Sizes[sid] = s.Size
				}
				return nil
			})
		if callErr != nil {
			inv.QueryError = callErr
		}

		perServer[server] = inv
		union = ShardBits(uint32(union) | uint32(inv.Bits))
	}

	return union, perServer
}

// RequireFullShardSet returns nil if shardsPresent covers every shard id in
// [0, TotalShardsCount); otherwise it returns an error naming the missing
// ids. This is the gate erasure-coding callers use before destroying the
// source replica.
func RequireFullShardSet(volumeID uint32, shardsPresent ShardBits) error {
	var missing []int
	for id := 0; id < TotalShardsCount; id++ {
		if !shardsPresent.Has(ShardId(id)) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Ints(missing)
	return fmt.Errorf("EC shard set incomplete for volume %d: %d/%d shards present, missing shard ids %v",
		volumeID, shardsPresent.Count(), TotalShardsCount, missing)
}

// SummarizeShardInventory returns a string like
// "10.0.0.1:8081=[0 1 2 3] 10.0.0.2:8081=[4 5 6 ERR:not found]" suitable for
// logging the verification result.
func SummarizeShardInventory(perServer map[string]ServerShardInventory) string {
	servers := make([]string, 0, len(perServer))
	for s := range perServer {
		servers = append(servers, s)
	}
	sort.Strings(servers)

	var b []byte
	for i, s := range servers {
		if i > 0 {
			b = append(b, ' ')
		}
		inv := perServer[s]
		b = append(b, s...)
		b = append(b, '=')
		b = append(b, '[')
		ids := make([]int, 0)
		for id := 0; id < MaxShardCount; id++ {
			if inv.Bits.Has(ShardId(id)) {
				ids = append(ids, id)
			}
		}
		for j, id := range ids {
			if j > 0 {
				b = append(b, ' ')
			}
			b = append(b, []byte(fmt.Sprintf("%d", id))...)
		}
		if inv.QueryError != nil {
			if len(ids) > 0 {
				b = append(b, ' ')
			}
			b = append(b, []byte("ERR:"+inv.QueryError.Error())...)
		}
		b = append(b, ']')
	}
	return string(b)
}
