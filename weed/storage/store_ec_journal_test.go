package storage

import (
	"errors"
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
	mio := defaultEcjMergeIO
	mio.read = read
	added, err := store.mergeEcJournal(vid, disk0, ownerBase+".ecj", ecjIdSet(1, 2), mio)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	require.NotNil(t, sibling)
	assert.True(t, sibling.IsNeedleDeleted(2), "the sibling mounted mid-merge must see the merged id")
	assert.Equal(t, ecjRecords(1, 2), mustReadFile(t, ownerBase+".ecj"))
}

// The unmounted append's fsync runs with no disk's EC lock held, so a slow
// sync cannot hold off mounts, or the EC reads queued behind them, on every
// disk.
func TestMergeEcJournal_UnmountedSyncHoldsNoDiskLock(t *testing.T) {
	tempDir := t.TempDir()
	disk0 := filepath.Join(tempDir, "d0")
	disk1 := filepath.Join(tempDir, "d1")
	store := startEcJournalStoreDisks(t, "", disk0, disk1)

	vid := needle.VolumeId(9)
	journal := erasure_coding.EcShardFileName("", disk0, int(vid)) + ".ecj"
	require.NoError(t, os.WriteFile(journal, ecjRecords(1), 0o644))

	synced := false
	mio := defaultEcjMergeIO
	mio.sync = func(a *erasure_coding.EcjAppend) error {
		for i, loc := range store.Locations {
			if assert.True(t, loc.ecVolumesLock.TryLock(), "disk %d EC lock held across fsync", i) {
				loc.ecVolumesLock.Unlock()
			}
		}
		synced = true
		return a.Sync()
	}
	added, err := store.mergeEcJournal(vid, disk0, journal, ecjIdSet(1, 2), mio)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.True(t, synced)
	assert.Equal(t, ecjRecords(1, 2), mustReadFile(t, journal))
}

// A failed fsync removes the append when its records are still the journal's
// tail, also through every runtime that mounted the journal since the write:
// their deleted sets must not keep ids the journal may never have persisted,
// and a retried merge appends them again. Once any runtime has journaled
// after them, removing them would lose that delete, even if the runtime doing
// the rollback cached the older length; they stay and are rewritten and
// synced instead. If that sync fails too, the records stay but leave every
// deleted set, so a retried merge appends and syncs them again rather than
// trusting records never shown durable.
func TestMergeEcJournal_FailedSync(t *testing.T) {
	for _, tt := range []struct {
		name        string
		mounts      int  // runtimes on disks 1.. mounting the journal mid-sync
		journalVia  int  // 1-based runtime that journals id 3 mid-sync, 0 for none
		resyncFails bool // the fsync after the rewrite fails too
		wantJournal []types.NeedleId
	}{
		{name: "unmounted", wantJournal: []types.NeedleId{1}},
		{name: "mounted since the write", mounts: 1, wantJournal: []types.NeedleId{1}},
		{name: "two mounted since the write", mounts: 2, wantJournal: []types.NeedleId{1}},
		{name: "mounted and journaled since", mounts: 1, journalVia: 1, wantJournal: []types.NeedleId{1, 2, 3}},
		{name: "another runtime journaled since", mounts: 2, journalVia: 2, wantJournal: []types.NeedleId{1, 2, 3}},
		{name: "resync fails", mounts: 2, journalVia: 2, resyncFails: true, wantJournal: []types.NeedleId{1, 2, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			disks := []string{filepath.Join(tempDir, "d0"), filepath.Join(tempDir, "d1"), filepath.Join(tempDir, "d2")}
			store := startEcJournalStoreDisks(t, "", disks...)

			const collection = "c"
			vid := needle.VolumeId(9)
			journal := writeEcIndex(t, disks[0], collection, vid, 1) + ".ecj"
			for _, d := range disks[1:] {
				writeEcShard0(t, d, collection, vid)
			}

			var holders []*erasure_coding.EcVolume
			syncs := 0
			mio := defaultEcjMergeIO
			mio.sync = func(a *erasure_coding.EcjAppend) error {
				if syncs++; syncs > 1 {
					if tt.resyncFails {
						return errors.New("injected resync failure")
					}
					return a.Sync()
				}
				for i := 1; i <= tt.mounts; i++ {
					ev, err := store.Locations[i].loadEcShardWithIdxDir(collection, vid, 0, disks[0])
					require.NoError(t, err)
					require.True(t, ev.IsNeedleDeleted(2), "the mount loads the unsynced record")
					holders = append(holders, ev)
				}
				if tt.journalVia > 0 {
					_, err := holders[tt.journalVia-1].MergeJournal(ecjIdSet(3))
					require.NoError(t, err)
				}
				return errors.New("injected fsync failure")
			}
			added, err := store.mergeEcJournal(vid, disks[0], journal, ecjIdSet(1, 2), mio)
			assert.Equal(t, ecjRecords(tt.wantJournal...), mustReadFile(t, journal))
			if tt.journalVia > 0 {
				assert.True(t, holders[tt.journalVia-1].IsNeedleDeleted(3), "a later delete is never lost")
			}
			if tt.journalVia > 0 && !tt.resyncFails {
				require.NoError(t, err, "records followed by a delete are resynced")
				assert.Equal(t, 1, added)
				for _, ev := range holders {
					assert.True(t, ev.IsNeedleDeleted(2))
				}
				return
			}
			require.Error(t, err)
			for _, ev := range holders {
				assert.False(t, ev.IsNeedleDeleted(2), "no deleted set claims a record not shown durable")
			}

			// A retried merge appends and syncs the id again.
			added, err = store.MergeEcJournal(vid, disks[0], journal, ecjIdSet(1, 2))
			require.NoError(t, err)
			assert.Equal(t, 1, added)
			assert.Equal(t, ecjRecords(append(tt.wantJournal, 2)...), mustReadFile(t, journal))
			if len(holders) > 0 {
				assert.True(t, holders[0].IsNeedleDeleted(2))
			}
		})
	}
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
