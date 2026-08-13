package erasure_coding

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

// ErrFullTeardownNotAcked marks a reachable server that completed the delete
// RPC but did not report a full teardown (e.g. a pre-upgrade volume server), so
// a stale EC generation may remain on it. Callers distinguish this from an
// unreachable node (which may recover and be re-swept) with errors.Is.
var ErrFullTeardownNotAcked = errors.New("delete did not perform full teardown (pre-upgrade volume server?); a stale EC generation may remain")

// UnmountAndDeleteEcShards unmounts then tears down the named EC shards for a
// volume on one server. Unmount must precede delete (delete requires the shard
// be unmounted); both RPCs are idempotent against missing shards.
//
// encodeTsNs fences both RPCs:
//   - 0 selects the server's blanket, generation-independent teardown. This is
//     the correct choice for a pre-encode or rollback wipe: it clears same-
//     generation shards (a retried encode's prior attempt shares the job's
//     generation) and shards whose .vif generation is unreadable (an
//     interrupted distribute never landed the sidecar) — both of which a fenced
//     teardown preserves. The blanket path aborts rather than clobber a live
//     newer mount, and the caller must guarantee no concurrent newer encode of
//     this volume (e.g. the admin dedupe key, or an operator lock).
//   - a non-zero value fences the teardown to strictly-older generations,
//     preserving same-or-newer, generation 0, and an unreadable .vif — for a
//     stale-worker cleanup that must never wipe a newer run's live shards.
//
// Returns ErrFullTeardownNotAcked (wrapped, so errors.Is matches) when a
// reachable server does not ack the full teardown.
//
// This is the single teardown primitive shared by the plugin-worker EC task
// and the shell ec.encode pre-cleanup, so the fence semantics cannot drift
// between the two paths.
func UnmountAndDeleteEcShards(
	ctx context.Context,
	dialOption grpc.DialOption,
	server pb.ServerAddress,
	collection string,
	volumeID uint32,
	shardIds []uint32,
	encodeTsNs int64,
) error {
	return operation.WithVolumeServerClient(false, server, dialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if _, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId:   volumeID,
				ShardIds:   shardIds,
				EncodeTsNs: encodeTsNs,
			}); err != nil {
				return fmt.Errorf("unmount: %w", err)
			}
			resp, err := client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
				VolumeId:     volumeID,
				Collection:   collection,
				ShardIds:     shardIds,
				FullTeardown: true,
				EncodeTsNs:   encodeTsNs,
			})
			if err != nil {
				return fmt.Errorf("delete: %w", err)
			}
			if !resp.GetFullTeardownDone() {
				return fmt.Errorf("delete on %s: %w", server, ErrFullTeardownNotAcked)
			}
			return nil
		})
}
