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
// actions on RequireFullShardSet against the returned union.
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

// totalShards is the configured DataShards+ParityShards for this volume.
// Passed as a parameter (not derived from TotalShardsCount) so enterprise
// builds with custom EC ratios share this helper verbatim.
func RequireFullShardSet(volumeID uint32, shardsPresent ShardBits, totalShards int) error {
	if totalShards <= 0 || totalShards > MaxShardCount {
		return fmt.Errorf("invalid totalShards %d for volume %d (must be in [1, %d])",
			totalShards, volumeID, MaxShardCount)
	}
	var missing []int
	for id := 0; id < totalShards; id++ {
		if !shardsPresent.Has(ShardId(id)) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Ints(missing)
	return fmt.Errorf("EC shard set incomplete for volume %d: %d/%d shards present, missing shard ids %v",
		volumeID, shardsPresent.Count(), totalShards, missing)
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
