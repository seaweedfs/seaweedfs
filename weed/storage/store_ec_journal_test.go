package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ecjIdSet(ids ...types.NeedleId) map[types.NeedleId]struct{} {
	s := make(map[types.NeedleId]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func ecjRecords(ids ...types.NeedleId) []byte {
	b := make([]byte, len(ids)*types.NeedleIdSize)
	for i, id := range ids {
		types.NeedleIdToBytes(b[i*types.NeedleIdSize:], id)
	}
	return b
}

func startEcJournalStore(t *testing.T, dataDir, idxDir string) *Store {
	t.Helper()
	for _, d := range []string{dataDir, idxDir} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dataDir},
		[]int32{100},
		[]util.MinFreeSpace{{}},
		idxDir,
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType},
		nil,
		3,
		stats.DefaultDiskIOProbeConfig(),
	)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewEcShardsChan:
			case <-store.NewVolumesChan:
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
	return store
}

// A mounted volume whose index was found in its data directory keeps its live
// journal there, while a shard copy targets the index directory. The merge
// must reach the live journal and the in-memory set, not a sibling file.
func TestMergeEcJournal_MountedVolumeJournalInDataDir(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	store := startEcJournalStore(t, dataDir, idxDir)

	const collection = "c"
	vid := needle.VolumeId(9)
	const datSize int64 = 1024 * 1024
	dataBase := erasure_coding.EcShardFileName(collection, dataDir, int(vid))
	f, err := os.Create(dataBase + erasure_coding.ToExt(0))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(calculateExpectedShardSize(datSize, 10)))
	require.NoError(t, f.Close())
	require.NoError(t, os.WriteFile(dataBase+".ecx", make([]byte, types.NeedleMapEntrySize), 0o644))
	require.NoError(t, os.WriteFile(dataBase+".ecj", ecjRecords(1), 0o644))
	require.NoError(t, volume_info.SaveVolumeInfo(dataBase+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		DatFileSize:   datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
	}))
	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))
	ev, found := store.Locations[0].FindEcVolume(vid)
	require.True(t, found)
	require.Equal(t, dataBase+".ecj", ev.FileName(".ecj"))

	idxJournal := erasure_coding.EcShardFileName(collection, idxDir, int(vid)) + ".ecj"
	added, err := store.MergeEcJournal(vid, idxJournal, ecjIdSet(1, 2))
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.True(t, ev.IsNeedleDeleted(2))
	assert.Equal(t, ecjRecords(1, 2), mustReadFile(t, dataBase+".ecj"))
	assert.NoFileExists(t, idxJournal)
}

// An unmounted journal is merged on disk, and merging the same peer journal
// again adds nothing.
func TestMergeEcJournal_UnmountedVolumeIsIdempotent(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	store := startEcJournalStore(t, dataDir, idxDir)

	vid := needle.VolumeId(9)
	journal := erasure_coding.EcShardFileName("", idxDir, int(vid)) + ".ecj"
	require.NoError(t, os.WriteFile(journal, ecjRecords(1, 2), 0o644))

	for i, want := range []int{2, 0, 0} {
		added, err := store.MergeEcJournal(vid, journal, ecjIdSet(2, 3, 4))
		require.NoError(t, err)
		assert.Equal(t, want, added, "merge %d", i)
	}
	assert.Equal(t, ecjRecords(1, 2, 3, 4), mustReadFile(t, journal))

	_, err := store.MergeEcJournal(vid, filepath.Join(tempDir, "elsewhere", "9.ecj"), ecjIdSet(1))
	assert.Error(t, err, "a journal outside every disk has no owner")
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
