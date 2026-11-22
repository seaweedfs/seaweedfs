package weed_server

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A volume mounted on two disks of one server keeps a deletion set per disk,
// and a read checks the set of the disk holding the shard, so a delete must
// reach every local copy.
func TestVolumeEcBlobDelete_RecordsOnEveryLocalCopy(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const (
		vid        = needle.VolumeId(902)
		collection = "split"
		fileKey    = types.NeedleId(5)
	)
	entry := needle_map.ToBytes(fileKey, types.ToOffset(8), 100)
	for i, dir := range []string{diskA, diskB} {
		seedEcShardOnDisk(t, store, vid, collection, dir, i, 0, 10, 4, 0)
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		require.NoError(t, os.WriteFile(base+".ecx", entry, 0o644))
		require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(i), make([]byte, 4096), 0o644))
	}
	copies := store.FindAllEcVolumes(vid)
	require.Len(t, copies, 2)

	vs := &VolumeServer{store: store}
	_, err := vs.VolumeEcBlobDelete(context.Background(), &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
		FileKey:    uint64(fileKey),
		Version:    uint32(needle.Version3),
	})
	require.NoError(t, err)
	for i, ev := range copies {
		require.True(t, ev.IsNeedleDeleted(fileKey), "copy %d still serves the deleted needle", i)
	}
}

// A retry on this server resolves the volume through its first copy, so a
// delete that fails on another copy must leave the first one live.
func TestVolumeEcBlobDelete_FailedCopyLeavesFirstLive(t *testing.T) {
	diskA, diskB := t.TempDir(), t.TempDir()
	store := newMultiDiskEcScrubStore(t, diskA, diskB)
	const (
		vid        = needle.VolumeId(903)
		collection = "split"
		fileKey    = types.NeedleId(5)
	)
	entry := needle_map.ToBytes(fileKey, types.ToOffset(8), 100)
	for i, dir := range []string{diskA, diskB} {
		seedEcShardOnDisk(t, store, vid, collection, dir, i, 0, 10, 4, 0)
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		require.NoError(t, os.WriteFile(base+".ecx", entry, 0o644))
		require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(i), make([]byte, 4096), 0o644))
	}
	copies := store.FindAllEcVolumes(vid)
	require.Len(t, copies, 2)

	// A directory in place of the journal makes the reopen fail and every
	// later append on that copy error out.
	broken := copies[1]
	unlock := broken.LockDeletionJournal()
	require.NoError(t, os.Remove(broken.FileName(".ecj")))
	require.NoError(t, os.Mkdir(broken.FileName(".ecj"), 0o755))
	require.Error(t, broken.ReopenDeletionJournal())
	unlock()

	vs := &VolumeServer{store: store}
	_, err := vs.VolumeEcBlobDelete(context.Background(), &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
		FileKey:    uint64(fileKey),
		Version:    uint32(needle.Version3),
	})
	require.Error(t, err)
	require.False(t, copies[0].IsNeedleDeleted(fileKey), "the first copy recorded a delete another copy failed")
}
