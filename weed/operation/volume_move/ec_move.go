package volume_move

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// EcShardMove is one planned relocation of mounted shards to another server.
type EcShardMove struct {
	VolumeId   needle.VolumeId
	Collection string
	ShardIds   []erasure_coding.ShardId
	Source     pb.ServerAddress
	Target     pb.ServerAddress
	// TargetDisk picks the destination disk (0 lets the server pick).
	TargetDisk uint32
}

// EcMoveOptions control MoveEcShards.
type EcMoveOptions struct {
	// IoBytePerSecond limits the shard copy rate; 0 falls back to the volume
	// server's maintenance rate.
	IoBytePerSecond int64
	// Writer receives human-readable progress lines (nil discards them).
	Writer io.Writer
	// Progress, when set, receives percent/stage callbacks as the move advances.
	Progress func(percent float64, stage string)
}

// MoveEcShards relocates mounted shards: copy+mount on the target, verify the
// target registered them, then unmount+delete on the source. The verification
// gates the destructive half — a copy/mount RPC can return OK while the shard
// is not loadable on the target, and deleting the source then would lose it;
// on a mismatch the source is kept so the caller can retry.
func (m *Mover) MoveEcShards(ctx context.Context, move EcShardMove, opts EcMoveOptions) error {
	writer := opts.Writer
	if writer == nil {
		writer = io.Discard
	}
	progress := opts.Progress
	if progress == nil {
		progress = func(float64, string) {}
	}

	// A same-server "move" cannot be expressed with these RPCs: the source
	// delete is server-wide, so it would erase the just-copied shard. Removing
	// a duplicate shard in place is RemoveEcShards. SameServer, not ==:
	// "node:8080" and "node:8080.18080" are one server.
	if SameServer(move.Source, move.Target) {
		return fmt.Errorf("refusing EC shard move of volume %d shard(s) %v onto its own server %s: the source delete is server-wide", move.VolumeId, move.ShardIds, move.Source)
	}

	progress(10, fmt.Sprintf("copying EC shard(s) %d.%v from %s to %s", move.VolumeId, move.ShardIds, move.Source, move.Target))
	if err := m.CopyAndMountEcShards(ctx, move.VolumeId, move.Collection, move.ShardIds, move.Source, move.Target, move.TargetDisk, opts.IoBytePerSecond, writer); err != nil {
		return err
	}

	progress(40, fmt.Sprintf("verifying EC shard(s) %d.%v on %s", move.VolumeId, move.ShardIds, move.Target))
	if err := m.VerifyEcShards(ctx, move.VolumeId, move.Target, move.ShardIds); err != nil {
		return err
	}

	progress(50, fmt.Sprintf("unmounting EC shard(s) %d.%v from %s", move.VolumeId, move.ShardIds, move.Source))
	fmt.Fprintf(writer, "unmount %d.%v from %s\n", move.VolumeId, move.ShardIds, move.Source)
	if err := m.UnmountEcShards(ctx, move.VolumeId, move.Source, move.ShardIds); err != nil {
		return fmt.Errorf("unmount %d.%v from %s: %v", move.VolumeId, move.ShardIds, move.Source, err)
	}

	progress(75, fmt.Sprintf("deleting EC shard(s) %d.%v from %s", move.VolumeId, move.ShardIds, move.Source))
	fmt.Fprintf(writer, "delete %d.%v from %s\n", move.VolumeId, move.ShardIds, move.Source)
	if err := m.DeleteEcShards(ctx, move.VolumeId, move.Collection, move.Source, move.ShardIds); err != nil {
		return fmt.Errorf("delete %d.%v from %s: %v", move.VolumeId, move.ShardIds, move.Source, err)
	}

	progress(100, fmt.Sprintf("moved EC shard(s) %d.%v from %s to %s", move.VolumeId, move.ShardIds, move.Source, move.Target))
	return nil
}

// CopyAndMountEcShards has the target copy the shards (with their .ecx/.ecj/
// .vif/.ecsum sidecars) from source and mount them. A same-address call skips
// the copy and just mounts — ec.encode uses that to bring freshly generated
// shards online in place.
func (m *Mover) CopyAndMountEcShards(ctx context.Context, volumeId needle.VolumeId, collection string, shardIds []erasure_coding.ShardId, source, target pb.ServerAddress, targetDisk uint32, ioBytePerSecond int64, writer io.Writer) error {
	if writer == nil {
		writer = io.Discard
	}
	// The target dials the embedded source itself; a malformed one would
	// abort the target server, not this client.
	if !SameServer(target, source) {
		if err := checkDialable(source); err != nil {
			return err
		}
	}
	return m.withClient(false, target, func(client volume_server_pb.VolumeServerClient) error {
		if !SameServer(target, source) {
			fmt.Fprintf(writer, "copy %d.%v %s => %s\n", volumeId, shardIds, source, target)
			_, copyErr := client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:        uint32(volumeId),
				Collection:      collection,
				ShardIds:        erasure_coding.ShardIdsToUint32(shardIds),
				CopyEcxFile:     true,
				CopyEcjFile:     true,
				CopyVifFile:     true,
				CopyEcsumFile:   true, // propagate the bitrot sidecar with the shards (no-op if the source has none)
				SourceDataNode:  string(source),
				DiskId:          targetDisk,
				IoBytePerSecond: ioBytePerSecond,
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s: %v", volumeId, shardIds, source, target, copyErr)
			}
		}
		fmt.Fprintf(writer, "mount %d.%v on %s\n", volumeId, shardIds, target)
		_, mountErr := client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(shardIds),
		})
		if mountErr != nil {
			return fmt.Errorf("mount %d.%v on %s: %v", volumeId, shardIds, target, mountErr)
		}
		return nil
	})
}

// VerifyEcShards confirms server has every shard in shardIds registered for
// the volume.
func (m *Mover) VerifyEcShards(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
			VolumeId: uint32(volumeId),
		})
		if err != nil {
			return fmt.Errorf("verify EC shard(s) on %s for volume %d: %v", server, volumeId, err)
		}
		var bits, zeroSized erasure_coding.ShardBits
		for _, s := range resp.EcShardInfos {
			if s.VolumeId != uint32(volumeId) || s.ShardId >= erasure_coding.MaxShardCount {
				continue
			}
			// A zero-sized shard is residue of a failed operation, not a
			// shard; counting it as present would let a broken copy pass
			// verification and the source be deleted behind it.
			if s.Size <= 0 {
				zeroSized = zeroSized.Set(erasure_coding.ShardId(s.ShardId))
				continue
			}
			bits = bits.Set(erasure_coding.ShardId(s.ShardId))
		}
		for _, sid := range shardIds {
			if bits.Has(sid) {
				continue
			}
			if zeroSized.Has(sid) {
				return fmt.Errorf("%s has a zero-sized EC shard %d.%d after copy/mount; keeping source", server, volumeId, sid)
			}
			return fmt.Errorf("%s missing EC shard %d.%d after copy/mount; keeping source", server, volumeId, sid)
		}
		return nil
	})
}

// MountEcShards mounts shards already on server.
func (m *Mover) MountEcShards(ctx context.Context, volumeId needle.VolumeId, collection string, server pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		_, mountErr := client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(shardIds),
		})
		return mountErr
	})
}

// UnmountEcShards unmounts shards on server.
func (m *Mover) UnmountEcShards(ctx context.Context, volumeId needle.VolumeId, server pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		_, unmountErr := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: erasure_coding.ShardIdsToUint32(shardIds),
		})
		return unmountErr
	})
}

// DeleteEcShards deletes shards on server. The delete is server-wide: it
// removes the shards from every disk of the server.
func (m *Mover) DeleteEcShards(ctx context.Context, volumeId needle.VolumeId, collection string, server pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	return m.withClient(false, server, func(client volume_server_pb.VolumeServerClient) error {
		_, deleteErr := client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   erasure_coding.ShardIdsToUint32(shardIds),
		})
		return deleteErr
	})
}

// RemoveEcShards unmounts then deletes shards in place — the dedup path for a
// shard that already has a copy elsewhere.
func (m *Mover) RemoveEcShards(ctx context.Context, volumeId needle.VolumeId, collection string, server pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	if err := m.UnmountEcShards(ctx, volumeId, server, shardIds); err != nil {
		return fmt.Errorf("unmount %d.%v from %s: %v", volumeId, shardIds, server, err)
	}
	if err := m.DeleteEcShards(ctx, volumeId, collection, server, shardIds); err != nil {
		return fmt.Errorf("delete %d.%v from %s: %v", volumeId, shardIds, server, err)
	}
	return nil
}
