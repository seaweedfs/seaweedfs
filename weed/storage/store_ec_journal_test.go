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
	return startEcJournalStoreDisks(t, idxDir, dataDir)
}

// startEcJournalStoreDisks starts a store with one disk per data dir. An
// empty idxDir gives each disk its data dir as index dir; otherwise all disks
// share idxDir.
func startEcJournalStoreDisks(t *testing.T, idxDir string, dataDirs ...string) *Store {
	t.Helper()
	dirs := append([]string{idxDir}, dataDirs...)
	if idxDir == "" {
		dirs = dataDirs
	}
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	n := len(dataDirs)
	maxCounts := make([]int32, n)
	minFree := make([]util.MinFreeSpace, n)
	diskTypes := make([]types.DiskType, n)
	for i := range dataDirs {
		maxCounts[i] = 100
		diskTypes[i] = types.HardDriveType
	}
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		dataDirs,
		maxCounts,
		minFree,
		idxDir,
		NeedleMapInMemory,
		diskTypes,
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
	writeEcShard0(t, dataDir, collection, vid)
	dataBase := writeEcIndex(t, dataDir, collection, vid, 1)
	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))
	ev, found := store.Locations[0].FindEcVolume(vid)
	require.True(t, found)
	require.Equal(t, dataBase+".ecj", ev.FileName(".ecj"))

	idxJournal := erasure_coding.EcShardFileName(collection, idxDir, int(vid)) + ".ecj"
	added, err := store.MergeEcJournal(vid, dataDir, idxJournal, ecjIdSet(1, 2))
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
		added, err := store.MergeEcJournal(vid, dataDir, journal, ecjIdSet(2, 3, 4))
		require.NoError(t, err)
		assert.Equal(t, want, added, "merge %d", i)
	}
	assert.Equal(t, ecjRecords(1, 2, 3, 4), mustReadFile(t, journal))

	_, err := store.MergeEcJournal(vid, filepath.Join(tempDir, "elsewhere"), journal, ecjIdSet(1))
	assert.Error(t, err, "a data dir that is no disk owns no journal")
}

// Disks sharing one index directory all hold the same journal path. Copying
// shards onto a disk that has not mounted vid must still reach the sibling
// runtime that holds that journal open, or the sibling keeps serving needles
// the peer deleted until it remounts.
func TestMergeEcJournal_SharedIndexDirReachesSiblingMount(t *testing.T) {
	tempDir := t.TempDir()
	disk0 := filepath.Join(tempDir, "d0")
	disk1 := filepath.Join(tempDir, "d1")
	idxDir := filepath.Join(tempDir, "idx")
	store := startEcJournalStoreDisks(t, idxDir, disk0, disk1)

	const collection = "c"
	vid := needle.VolumeId(9)
	writeEcShard0(t, disk0, collection, vid)
	idxBase := writeEcIndex(t, idxDir, collection, vid, 1)
	ev, err := store.Locations[0].LoadEcShard(collection, vid, 0)
	require.NoError(t, err)
	require.Equal(t, idxBase+".ecj", ev.FileName(".ecj"))

	// The copy lands on disk1, whose index dir is the shared one.
	added, err := store.MergeEcJournal(vid, disk1, idxBase+".ecj", ecjIdSet(1, 2))
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.True(t, ev.IsNeedleDeleted(2), "the mounted sibling must see the merged id")
	assert.Equal(t, ecjRecords(1, 2), mustReadFile(t, idxBase+".ecj"))
}

// A sibling disk can mount vid from the receiving disk's index (#9212) while
// the merge reads the journal unlocked. The append must notice that mount and
// go through it instead of writing behind its open handle.
func TestMergeEcJournal_SiblingMountDuringReadIsMergedThrough(t *testing.T) {
	tempDir := t.TempDir()
	disk0 := filepath.Join(tempDir, "d0")
	disk1 := filepath.Join(tempDir, "d1")
	store := startEcJournalStoreDisks(t, "", disk0, disk1)

	const collection = "c"
	vid := needle.VolumeId(9)
	ownerBase := writeEcIndex(t, disk0, collection, vid, 1)
	writeEcShard0(t, disk1, collection, vid)

	var sibling *erasure_coding.EcVolume
	read := func(path string) (map[types.NeedleId]struct{}, int64, error) {
		ids, size, err := erasure_coding.ReadEcjIds(path)
		if sibling == nil {
			var mountErr error
			sibling, mountErr = store.Locations[1].loadEcShardWithIdxDir(collection, vid, 0, disk0)
			require.NoError(t, mountErr)
			require.Equal(t, ownerBase+".ecj", sibling.FileName(".ecj"))
		}
		return ids, size, err
	}
	added, err := store.mergeEcJournal(vid, disk0, ownerBase+".ecj", ecjIdSet(1, 2), read)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	require.NotNil(t, sibling)
	assert.True(t, sibling.IsNeedleDeleted(2), "the sibling mounted mid-merge must see the merged id")
	assert.Equal(t, ecjRecords(1, 2), mustReadFile(t, ownerBase+".ecj"))
}

// writeEcShard0 writes shard 0 of vid and its .vif into dataDir.
func writeEcShard0(t *testing.T, dataDir, collection string, vid needle.VolumeId) {
	t.Helper()
	const datSize int64 = 1024 * 1024
	dataBase := erasure_coding.EcShardFileName(collection, dataDir, int(vid))
	f, err := os.Create(dataBase + erasure_coding.ToExt(0))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(calculateExpectedShardSize(datSize, 10)))
	require.NoError(t, f.Close())
	require.NoError(t, volume_info.SaveVolumeInfo(dataBase+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		DatFileSize:   datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
	}))
}

// writeEcIndex writes vid's .ecx and a .ecj holding deleted into dir and
// returns their base name.
func writeEcIndex(t *testing.T, dir, collection string, vid needle.VolumeId, deleted ...types.NeedleId) string {
	t.Helper()
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, types.NeedleMapEntrySize), 0o644))
	require.NoError(t, os.WriteFile(base+".ecj", ecjRecords(deleted...), 0o644))
	return base
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
