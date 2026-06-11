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

type ServerShardInventory struct {
	Bits       ShardBits
	QueryError error
}

// Query errors are recorded per-server and treated as zero shards rather
// than aborting the scan, so the caller still sees partial coverage from
// healthy peers when one server is down. The caller gates destructive
// actions on RequireRecoverableShardSet against the returned union.
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

		var inv ServerShardInventory

		callErr := operation.WithVolumeServerClient(false, pb.ServerAddress(server), dialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				resp, e := client.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
					VolumeId: volumeID,
				})
				if e != nil {
					return e
				}
				for _, s := range resp.EcShardInfos {
					if s.VolumeId != volumeID || s.ShardId >= MaxShardCount {
						continue
					}
					inv.Bits = inv.Bits.Set(ShardId(s.ShardId))
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

// RequireRecoverableShardSet gates source-volume deletion after EC encode:
// a non-empty .dat may only be deleted when enough distinct shards exist to
// reconstruct the volume (>= dataShards). A full set returns (false, nil); a
// degraded-but-recoverable set returns (true, nil) so the caller can warn and
// proceed -- the missing shards can be rebuilt from the survivors, while
// keeping the source next to live shards is the more dangerous mixed state.
// Below dataShards it returns an error and the source must be kept.
// dataShards/totalShards are passed as parameters (not derived from the
// package constants) so enterprise builds with custom EC ratios share this
// helper verbatim.
func RequireRecoverableShardSet(volumeID uint32, shardsPresent ShardBits, dataShards, totalShards int) (degraded bool, err error) {
	if totalShards <= 0 || totalShards > MaxShardCount {
		return false, fmt.Errorf("invalid totalShards %d for volume %d (must be in [1, %d])",
			totalShards, volumeID, MaxShardCount)
	}
	if dataShards <= 0 || dataShards > totalShards {
		return false, fmt.Errorf("invalid dataShards %d for volume %d (must be in [1, %d])",
			dataShards, volumeID, totalShards)
	}
	var missing []int
	for id := 0; id < totalShards; id++ {
		if !shardsPresent.Has(ShardId(id)) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	if totalShards-len(missing) >= dataShards {
		return true, nil
	}
	sort.Ints(missing)
	return false, fmt.Errorf("EC shard set unrecoverable for volume %d: %d/%d shards present, need %d to reconstruct, missing shard ids %v",
		volumeID, totalShards-len(missing), totalShards, dataShards, missing)
}

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
