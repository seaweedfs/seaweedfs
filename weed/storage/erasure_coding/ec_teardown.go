package erasure_coding

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

// EcFileGeneration parses the generation of a 2PC-staged <base>.v<N> file:
// -1 means the name is not a generation file of base.
func EcFileGeneration(name, base string) int64 {
	suffix, ok := strings.CutPrefix(name, base+".v")
	if !ok {
		return -1
	}
	generation, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || generation <= 0 {
		return -1
	}
	return generation
}

// RemoveEcGenerationFiles removes 2PC generation files staged under base:
// <base>.ecNN.v<N>, <base>.ecx.v<N>, <base>.ecj.v<N>, <base>.ecsum.v<N> and
// <base>.vif.v<N>. generationsOlderThan == 0 removes every generation;
// otherwise only generations strictly below it. Returns the first real
// removal failure.
func RemoveEcGenerationFiles(baseFileName string, generationsOlderThan uint32) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	dir, fileName := filepath.Dir(baseFileName), filepath.Base(baseFileName)
	ecPrefix, vifName := fileName+".ec", fileName+".vif"
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		// A generation file is <artifact>.v<N>; the last dot separates the
		// staged-generation suffix from the artifact name.
		artifact := name[:max(strings.LastIndexByte(name, '.'), 0)]
		if artifact != vifName && !strings.HasPrefix(artifact, ecPrefix) {
			continue
		}
		generation := EcFileGeneration(name, artifact)
		if generation < 0 || (generationsOlderThan > 0 && generation >= int64(generationsOlderThan)) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			record(err)
		}
	}
	return firstErr
}
