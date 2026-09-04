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
	// BlockSize is the shard block layout this holder reports for the volume —
	// the one it will serve reads through. A binary predating the uniform
	// layout drops the field off the wire and reports 0, which is how
	// RequireAgreedBlockLayout tells such a holder from one that agrees.
	BlockSize int64
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
				inv.BlockSize = resp.GetEcShardConfig().GetBlockSize()
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

// RequireAgreedBlockLayout gates source-volume deletion on every holder that
// answered agreeing with the layout the shards were encoded under.
//
// The uniform block layout lives in a `.vif` field that older volume servers
// have never heard of: such a server parses the file, silently discards the
// unknown field, and mounts the volume on the legacy 1GiB/1MiB striping. Its
// reads then land at the wrong shard offsets and return wrong bytes, and
// nothing else in the encode path notices — the shard files are the same
// length under either layout. Asking each holder which layout it is serving is
// the one question that separates the two, and asking it here means the answer
// arrives while the source .dat is still on disk.
//
// Holders that could not be reached, or that report no shard of this volume,
// are skipped: RequireRecoverableShardSet already covers a missing holder, and
// one that answered nothing has promised nothing.
func RequireAgreedBlockLayout(volumeID uint32, encodedBlockSize int64, perServer map[string]ServerShardInventory) error {
	disagreeing := make([]string, 0, len(perServer))
	for server, inv := range perServer {
		if inv.QueryError != nil || inv.Bits.Count() == 0 {
			continue
		}
		if inv.BlockSize != encodedBlockSize {
			disagreeing = append(disagreeing, fmt.Sprintf("%s serves block size %d", server, inv.BlockSize))
		}
	}
	if len(disagreeing) == 0 {
		return nil
	}
	sort.Strings(disagreeing)
	return fmt.Errorf("volume %d was encoded with shard block size %d but %v; upgrade every volume server to a build that understands the uniform shard block layout before encoding",
		volumeID, encodedBlockSize, disagreeing)
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

// VerifyShardsOnServer confirms one server really holds the named shards of a
// specific (collection, volume), for callers about to delete another copy.
//
// Collection is checked, unlike in VerifyShardsAcrossServers: the inventory RPC
// is keyed by volume id alone, so a server answering for volume N says nothing
// about which collection's volume N it means. Approving a delete on the
// strength of a different collection's shard would remove the last real copy —
// the exact outcome the caller is trying to prevent.
//
// A server that cannot be queried is unknown, not confirmed, and returns an
// error: treating an unreachable peer as proof of a surviving copy is how a
// network blip becomes data loss.
// CollectShardsOnServer asks one volume server which shards of the volume it
// actually serves right now — its live inventory, as opposed to what the
// master's possibly stale topology claims for it.
func CollectShardsOnServer(ctx context.Context, collection string, volumeID uint32,
	server string, dialOption grpc.DialOption) (present ShardBits, err error) {
	err = operation.WithVolumeServerClient(false, pb.ServerAddress(server), dialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			resp, e := client.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
				VolumeId: volumeID,
			})
			if e != nil {
				return e
			}
			for _, s := range resp.EcShardInfos {
				if s.VolumeId != volumeID || s.Collection != collection || s.ShardId >= MaxShardCount {
					continue
				}
				present = present.Set(ShardId(s.ShardId))
			}
			return nil
		})
	return present, err
}

func VerifyShardsOnServer(ctx context.Context, collection string, volumeID uint32,
	server string, shardIDs []uint32, dialOption grpc.DialOption) error {

	if server == "" {
		return fmt.Errorf("no server given to verify volume %d shard(s) %v", volumeID, shardIDs)
	}

	present, callErr := CollectShardsOnServer(ctx, collection, volumeID, server, dialOption)
	if callErr != nil {
		return fmt.Errorf("verify volume %d shard(s) %v on %s: %w", volumeID, shardIDs, server, callErr)
	}

	for _, sid := range shardIDs {
		if !present.Has(ShardId(sid)) {
			return fmt.Errorf("%s does not hold ec shard %d.%d of collection %q", server, volumeID, sid, collection)
		}
	}
	return nil
}
