package weed_server

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// A corrupt shard that stays mounted while VolumeEcShardsRebuild regenerates it
// in place must stop serving the pre-repair bytes: the handler reopens the
// mounted shard so reads hit the regenerated file, not the detached inode.
// Regression test for ec.rebuild leaving a replaced shard file open until
// restart.
func TestVolumeEcShardsRebuild_ReopensMountedCorruptShard(t *testing.T) {
	if !erasure_coding.BitrotProtectionEnabled {
		t.Skip("bitrot protection is compiled out")
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dataDir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	st, err := storage.NewState(t.TempDir())
	require.NoError(t, err)
	store.State = st
	vs := &VolumeServer{store: store}

	collection := "col"
	vid := needle.VolumeId(42)
	base := erasure_coding.EcShardFileName(collection, dataDir, int(vid))

	dat := make([]byte, 7000)
	_, err = rand.Read(dat)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(base+".dat", dat, 0o644))
	prot, err := erasure_coding.WriteEcFiles(base, nil)
	require.NoError(t, err)
	require.NoError(t, erasure_coding.SaveBitrotSidecar(erasure_coding.BitrotSidecarPath(base, 0), prot))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, 16), 0o644))

	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))
	readShard := func() []byte {
		shard, found := store.Locations[0].FindEcShard(vid, 0)
		require.True(t, found)
		buf := make([]byte, shard.Size())
		_, err := shard.ReadAt(buf, 0)
		require.NoError(t, err)
		return buf
	}
	good := readShard()

	// Corrupt the file in place: the mounted descriptor now serves bad bytes.
	corrupt := bytes.Repeat([]byte{0xBB}, 1024)
	f, err := os.OpenFile(base+erasure_coding.ToExt(0), os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(corrupt, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(corrupt, readShard()[:len(corrupt)]))

	_, err = vs.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
	})
	require.NoError(t, err)

	require.True(t, bytes.Equal(good, readShard()),
		"mounted shard should serve the regenerated file after rebuild")
}

// A shard whose file is deleted under the mount is "missing" to the rebuild:
// it regenerates on the rebuild disk while the mounted handle still serves
// the detached inode on another disk. Reopening the mounted disk cannot fix
// that, so the handler moves the registration to the disk holding the rebuilt
// file — and still reports the rebuilt ids so the caller's copy cleanup keeps
// them.
func TestVolumeEcShardsRebuild_RelocatesMountedShardAcrossDisks(t *testing.T) {
	if !erasure_coding.BitrotProtectionEnabled {
		t.Skip("bitrot protection is compiled out")
	}
	tempDir := t.TempDir()
	dirA := filepath.Join(tempDir, "a")
	dirB := filepath.Join(tempDir, "b")
	require.NoError(t, os.MkdirAll(dirA, 0o755))
	require.NoError(t, os.MkdirAll(dirB, 0o755))
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dirA, dirB}, []int32{100, 100}, []util.MinFreeSpace{{}, {}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType, types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	st, err := storage.NewState(t.TempDir())
	require.NoError(t, err)
	store.State = st
	vs := &VolumeServer{store: store}

	collection := "col"
	vid := needle.VolumeId(42)
	baseA := erasure_coding.EcShardFileName(collection, dirA, int(vid))
	baseB := erasure_coding.EcShardFileName(collection, dirB, int(vid))
	shardName := erasure_coding.ToExt(0)

	dat := make([]byte, 7000)
	_, err = rand.Read(dat)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(baseA+".dat", dat, 0o644))
	prot, err := erasure_coding.WriteEcFiles(baseA, nil)
	require.NoError(t, err)
	require.NoError(t, erasure_coding.SaveBitrotSidecar(erasure_coding.BitrotSidecarPath(baseA, 0), prot))
	require.NoError(t, os.WriteFile(baseA+".ecx", make([]byte, 16), 0o644))

	original, err := os.ReadFile(baseA + shardName)
	require.NoError(t, err)

	// Mount shard 0 from disk B, then delete the file under the mount: the
	// handle keeps serving the detached inode while the disk reports the
	// shard missing.
	require.NoError(t, os.Rename(baseA+shardName, baseB+shardName))
	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))
	diskId, _, found := store.FindEcShard(vid, 0)
	require.True(t, found)
	require.Equal(t, uint32(1), diskId, "shard 0 should be mounted on disk B")
	require.NoError(t, os.Remove(baseB+shardName))

	resp, err := vs.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
	})
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, resp.RebuiltShardIds,
		"a mounted shard whose file was rebuilt on another disk must still be reported rebuilt")

	diskId, shard, found := store.FindEcShard(vid, 0)
	require.True(t, found)
	require.Equal(t, uint32(0), diskId, "rebuilt shard should be mounted on the disk holding its file")
	buf := make([]byte, shard.Size())
	_, err = shard.ReadAt(buf, 0)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, buf), "relocated shard should serve the regenerated file")
}
