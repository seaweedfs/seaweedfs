package storage

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/require"
)

// countingBackend counts Sync calls and can be made to fail them.
type countingBackend struct {
	backend.BackendStorageFile
	syncCount     int
	syncErr       error
	syncErrOnce   bool
	truncateErr   error
	truncateCount int
}

func (b *countingBackend) Sync() error {
	b.syncCount++
	if b.syncErr != nil {
		err := b.syncErr
		if b.syncErrOnce {
			b.syncErr = nil
			b.syncErrOnce = false
		}
		return err
	}
	return b.BackendStorageFile.Sync()
}

func (b *countingBackend) Truncate(off int64) error {
	b.truncateCount++
	if b.truncateErr != nil {
		return b.truncateErr
	}
	return b.BackendStorageFile.Truncate(off)
}

func newCountingVolume(t *testing.T) (*Volume, *countingBackend) {
	return newCountingVolumeWithKind(t, NeedleMapInMemory)
}

func newCountingVolumeWithKind(t *testing.T, needleMapKind NeedleMapKind) (*Volume, *countingBackend) {
	t.Helper()
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, needleMapKind, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	counting := &countingBackend{BackendStorageFile: v.DataBackend}
	v.DataBackend = counting
	return v, counting
}

func reopenCountingVolume(t *testing.T, v *Volume) *Volume {
	t.Helper()
	dir, dirIdx, id, needleMapKind := v.dir, v.dirIdx, v.Id, v.needleMapKind
	v.Close()
	reloaded, err := NewVolume(dir, dirIdx, "", id, needleMapKind,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	t.Cleanup(reloaded.Close)
	return reloaded
}

// A durable write reaching a stopping server used to silently drop its fsync,
// so the ack promised durability the .dat did not have. It now flushes inline
// instead of queueing on the batch worker that is winding down.
func TestWriteNeedle2FsyncsInlineWhileStopping(t *testing.T) {
	v, counting := newCountingVolume(t)

	_, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, true, true)
	require.NoError(t, err, "a durable write must still be accepted while stopping")
	require.Equal(t, 1, counting.syncCount, "the write should have been flushed inline")

	// A non-durable write keeps the drain cheap: still accepted, still no fsync.
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, counting.syncCount, "a write that did not ask for fsync must not pay for one")
}

func fixedNeedle(id uint64, data string) *needle.Needle {
	n := new(needle.Needle)
	n.Data = []byte(data)
	n.Checksum = needle.NewCRC(n.Data)
	n.Id = types.Uint64ToNeedleId(id)
	return n
}

// An append we could not flush is not data to vouch for: the inline path takes
// it back off the .dat and fails the write. The needle map has to come back
// with it, or it would resolve to an offset past the truncated end.
func TestWriteNeedle2TruncatesWhenInlineFsyncFails(t *testing.T) {
	v, counting := newCountingVolume(t)

	kept := fixedNeedle(1, "first-copy")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)
	keptEntry, found := v.nm.Get(kept.Id)
	require.True(t, found)
	keptOffset, keptSize := keptEntry.Offset, keptEntry.Size
	before, _, err := v.DataBackend.GetStat()
	require.NoError(t, err)

	counting.syncErr = errors.New("disk went away")
	_, _, _, err = v.writeNeedle2(fixedNeedle(1, "second-copy"), true, true, true)
	require.Error(t, err, "a write whose fsync failed must not be acked")

	after, _, err := v.DataBackend.GetStat()
	require.NoError(t, err)
	require.Equal(t, before, after, "the unflushed append should have been truncated away")

	now, found := v.nm.Get(kept.Id)
	require.True(t, found, "the mapping the failed write replaced should be back")
	require.Equal(t, keptOffset, now.Offset, "the index must not point past the truncated end")
	require.Equal(t, keptSize, now.Size)

	counting.syncErr = nil
	readBack := new(needle.Needle)
	readBack.Id = kept.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err, "the surviving needle must still be readable")
	require.Equal(t, []byte("first-copy"), readBack.Data)
}

// Same rollback for a needle the failed write introduced: with nothing to go
// back to, the mapping goes away rather than pointing past the end.
func TestWriteNeedle2DropsIndexOfUnflushedNewNeedle(t *testing.T) {
	v, counting := newCountingVolume(t)

	counting.syncErr = errors.New("disk went away")
	fresh := fixedNeedle(7, "never-landed")
	_, _, _, err := v.writeNeedle2(fresh, true, true, true)
	require.Error(t, err)

	if entry, found := v.nm.Get(fresh.Id); found {
		require.True(t, entry.Size.IsDeleted(), "a needle that never reached the disk must not resolve")
	}

	counting.syncErr = nil
	readBack := new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.Error(t, err, "reading the rolled-back needle should fail cleanly, not read past the end")
}

// The async durable path must roll back the mapper as well as the data tail
// when its batch fsync fails. The current worker only truncates the data file,
// so this test exposes the dangling mapping.
func TestWriteNeedle2RollsBackBatchFsyncFailure(t *testing.T) {
	v, counting := newCountingVolume(t)
	initialFileCount := v.nm.FileCount()
	initialDeletedCount := v.nm.DeletedCount()
	initialContentSize := v.nm.ContentSize()
	initialDeletedSize := v.nm.DeletedSize()
	initialMaxFileKey := v.nm.MaxFileKey()

	before, _, err := v.DataBackend.GetStat()
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	fresh := fixedNeedle(8, "batch-never-landed")
	_, _, _, err = v.writeNeedle2(fresh, true, true, false)
	require.Error(t, err, "a batch whose fsync failed must not be acknowledged")

	after, _, err := v.DataBackend.GetStat()
	require.NoError(t, err)
	require.Equal(t, before, after, "the failed batch append should be truncated")

	if entry, found := v.nm.Get(fresh.Id); found {
		require.True(t, entry.Size.IsDeleted(), "the failed batch must not leave a live mapping")
	}
	require.Equal(t, initialFileCount, v.nm.FileCount())
	require.Equal(t, initialDeletedCount, v.nm.DeletedCount())
	require.Equal(t, initialContentSize, v.nm.ContentSize())
	require.Equal(t, initialDeletedSize, v.nm.DeletedSize())
	require.Equal(t, initialMaxFileKey, v.nm.MaxFileKey())
}

func TestWriteNeedle2RollsBackBatchFsyncFailureAndReloadsNewNeedle(t *testing.T) {
	v, counting := newCountingVolume(t)
	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true

	fresh := fixedNeedle(9, "batch-never-landed")
	_, _, _, err := v.writeNeedle2(fresh, true, true, false)
	require.Error(t, err)

	reloaded := reopenCountingVolume(t, v)
	if entry, found := reloaded.nm.Get(fresh.Id); found {
		require.True(t, entry.Size.IsDeleted(), "the failed batch must not reload a live mapping")
	}

	readBack := new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.Error(t, err, "the failed batch must not be readable after reload")
	require.False(t, reloaded.IsReadOnly(), "a completed rollback should keep the volume healthy")
}

func TestWriteNeedle2RollsBackBatchFsyncFailureAndReloadsOverwrite(t *testing.T) {
	v, counting := newCountingVolume(t)

	kept := fixedNeedle(10, "first-copy")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)
	keptEntry, found := v.nm.Get(kept.Id)
	require.True(t, found)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	_, _, _, err = v.writeNeedle2(fixedNeedle(10, "second-copy"), true, true, false)
	require.Error(t, err)

	now, found := v.nm.Get(kept.Id)
	require.True(t, found)
	require.Equal(t, keptEntry.Offset, now.Offset)
	require.Equal(t, keptEntry.Size, now.Size)

	reloaded := reopenCountingVolume(t, v)
	readBack := new(needle.Needle)
	readBack.Id = kept.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("first-copy"), readBack.Data)
}

func TestBatchDeleteRollsBackOnFsyncFailure(t *testing.T) {
	v, counting := newCountingVolume(t)

	kept := fixedNeedle(11, "delete-me")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	deleteRequest := needle.NewAsyncRequest(&needle.Needle{Id: kept.Id}, false)
	deleteRequest.ActualSize = needle.GetActualSize(0, v.Version())
	require.True(t, v.asyncRequestAppend(deleteRequest))
	_, _, _, err = deleteRequest.WaitComplete()
	require.Error(t, err)

	readBack := new(needle.Needle)
	readBack.Id = kept.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err, "a failed batch delete must restore the live mapping")
	require.Equal(t, []byte("delete-me"), readBack.Data)

	reloaded := reopenCountingVolume(t, v)
	readBack = new(needle.Needle)
	readBack.Id = kept.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("delete-me"), readBack.Data)
}

func TestBatchSyncFailureEntersFailClosedWhenTruncateFails(t *testing.T) {
	v, counting := newCountingVolume(t)
	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	counting.truncateErr = errors.New("truncate failed")

	fresh := fixedNeedle(12, "cannot-be-recovered")
	_, _, _, err := v.writeNeedle2(fresh, true, true, false)
	require.Error(t, err)
	require.True(t, v.IsReadOnly(), "a failed recovery must make the volume unavailable")
	require.NotNil(t, v.unavailableError())

	readBack := new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.Error(t, err, "an unavailable volume must reject reads")

	_, _, _, err = v.writeNeedle2(fixedNeedle(13, "after-failure"), true, true, false)
	require.Error(t, err, "an unavailable volume must reject later writes")
	require.Equal(t, 1, counting.truncateCount)
}

func TestUnavailableVolumeCannotBeMarkedWritable(t *testing.T) {
	dir := t.TempDir()
	store := newIdxSplitStore(t, dir, dir)
	const vid = needle.VolumeId(19)
	require.NoError(t, store.AddVolume(vid, "", NeedleMapInMemory, "000", "", 0,
		needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	v := store.findVolume(vid)
	require.NotNil(t, v)
	v.markIoUnavailable(errors.New("batch recovery failed"))

	require.Error(t, store.MarkVolumeWritable(vid), "manual writable transition must not bypass quarantine")
	require.NotNil(t, v.unavailableError())
}

func TestMixedBatchSyncFailureRollsBackAsOneUnit(t *testing.T) {
	v, counting := newCountingVolume(t)

	overwritten := fixedNeedle(14, "keep-overwrite")
	deleted := fixedNeedle(15, "keep-delete")
	_, _, _, err := v.writeNeedle2(overwritten, true, true, true)
	require.NoError(t, err)
	_, _, _, err = v.writeNeedle2(deleted, true, true, true)
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	replacement := fixedNeedle(14, "failed-overwrite")
	fresh := fixedNeedle(16, "failed-new")
	deleteRequest := needle.NewAsyncRequest(&needle.Needle{Id: deleted.Id}, false)
	requests := []*needle.AsyncRequest{
		needle.NewAsyncRequest(replacement, true),
		deleteRequest,
		needle.NewAsyncRequest(fresh, true),
	}
	v.processBatch(requests)

	for _, request := range requests {
		_, _, _, err = request.WaitComplete()
		require.Error(t, err, "every request in a failed batch must fail")
	}

	readBack := new(needle.Needle)
	readBack.Id = overwritten.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("keep-overwrite"), readBack.Data)

	readBack = new(needle.Needle)
	readBack.Id = deleted.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("keep-delete"), readBack.Data)

	readBack = new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.Error(t, err)

	reloaded := reopenCountingVolume(t, v)
	readBack = new(needle.Needle)
	readBack.Id = overwritten.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("keep-overwrite"), readBack.Data)

	readBack = new(needle.Needle)
	readBack.Id = deleted.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("keep-delete"), readBack.Data)

	readBack = new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.Error(t, err)
}

func TestWriteNeedle2EntersFailClosedWhenInlineRollbackFails(t *testing.T) {
	v, counting := newCountingVolume(t)
	counting.syncErr = errors.New("inline fsync failed")
	counting.syncErrOnce = true
	counting.truncateErr = errors.New("truncate failed")

	_, _, _, err := v.writeNeedle2(fixedNeedle(20, "cannot-be-recovered"), true, true, true)
	require.Error(t, err)
	require.NotNil(t, v.unavailableError())

	readBack := new(needle.Needle)
	readBack.Id = 20
	_, err = v.readNeedle(readBack, nil, nil)
	require.Error(t, err)
}

func TestUnavailableVolumeStaysUnavailableAfterReload(t *testing.T) {
	v, _ := newCountingVolume(t)
	v.markIoUnavailable(errors.New("batch recovery failed"))

	reloaded := reopenCountingVolume(t, v)
	require.NotNil(t, reloaded.unavailableError(), "the unavailable state must survive a reload")
	require.True(t, reloaded.IsReadOnly())

	_, _, quarantined := reloaded.getIoErrorState()
	require.True(t, quarantined, "a reloaded unavailable volume must stay out of heartbeats")

	readBack := new(needle.Needle)
	readBack.Id = 1
	_, err := reloaded.readNeedle(readBack, nil, nil)
	require.Error(t, err)
	_, _, _, err = reloaded.writeNeedle2(fixedNeedle(1, "after-reload"), true, true, false)
	require.Error(t, err)
}

func TestBatchFsyncRollbackWithLevelDbMapper(t *testing.T) {
	v, counting := newCountingVolumeWithKind(t, NeedleMapLevelDb)

	kept := fixedNeedle(17, "leveldb-old")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	_, _, _, err = v.writeNeedle2(fixedNeedle(17, "leveldb-new"), true, true, false)
	require.Error(t, err)

	readBack := new(needle.Needle)
	readBack.Id = kept.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("leveldb-old"), readBack.Data)

	reloaded := reopenCountingVolume(t, v)
	readBack = new(needle.Needle)
	readBack.Id = kept.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("leveldb-old"), readBack.Data)
}

func TestBatchFsyncRollbackRestoresOriginalValueAfterRepeatedNeedleId(t *testing.T) {
	v, counting := newCountingVolume(t)

	kept := fixedNeedle(18, "original-value")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	firstReplacement := fixedNeedle(18, "first-failed-value")
	secondReplacement := fixedNeedle(18, "second-failed-value")
	requests := []*needle.AsyncRequest{
		needle.NewAsyncRequest(firstReplacement, true),
		needle.NewAsyncRequest(secondReplacement, true),
	}
	v.processBatch(requests)

	for _, request := range requests {
		_, _, _, err = request.WaitComplete()
		require.Error(t, err)
	}

	readBack := new(needle.Needle)
	readBack.Id = kept.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("original-value"), readBack.Data)

	reloaded := reopenCountingVolume(t, v)
	readBack = new(needle.Needle)
	readBack.Id = kept.Id
	_, err = reloaded.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("original-value"), readBack.Data)
}

// Rolling back a first-time write must not leave a tombstoned map entry: the
// stale offset would make the next write to that needle fail reading a header
// that no longer exists.
func TestBatchRollbackLeavesNoPhantomMapping(t *testing.T) {
	v, counting := newCountingVolume(t)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true
	fresh := fixedNeedle(24, "batch-never-landed")
	_, _, _, err := v.writeNeedle2(fresh, true, true, false)
	require.Error(t, err)

	_, _, _, err = v.writeNeedle2(fresh, true, true, false)
	require.NoError(t, err, "rewriting a rolled-back needle must succeed")

	readBack := new(needle.Needle)
	readBack.Id = fresh.Id
	_, err = v.readNeedle(readBack, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("batch-never-landed"), readBack.Data)
}

// A request that already failed on its own must still surface the batch
// failure: keeping its earlier error would hide that nothing was persisted,
// or that recovery itself failed.
func TestFailedBatchMarksEveryRequestFailed(t *testing.T) {
	v, counting := newCountingVolume(t)

	kept := fixedNeedle(22, "original")
	_, _, _, err := v.writeNeedle2(kept, true, true, true)
	require.NoError(t, err)

	counting.syncErr = errors.New("batch fsync failed")
	counting.syncErrOnce = true

	badCookie := fixedNeedle(22, "wrong-cookie")
	badCookie.Cookie = kept.Cookie + 1
	requests := []*needle.AsyncRequest{
		needle.NewAsyncRequest(badCookie, true),
		needle.NewAsyncRequest(fixedNeedle(23, "fresh"), true),
	}
	v.processBatch(requests)

	for _, request := range requests {
		_, _, _, err = request.WaitComplete()
		require.ErrorContains(t, err, "batch fsync failed")
	}
}

// The quarantine is what CollectHeartbeat keys on: an unavailable volume must
// not be announced to the master at all.
func TestUnavailableVolumeIsSkippedInHeartbeat(t *testing.T) {
	store := newTestStore(t, 1)
	v := mountTestVolume(t, store.Locations[0], 1, "pics")
	fillTestVolume(t, v)
	v.markIoUnavailable(errors.New("batch recovery failed"))

	heartbeat := store.CollectHeartbeat()
	for _, m := range heartbeat.Volumes {
		require.NotEqual(t, uint32(1), m.Id, "a failed-recovery volume must not be announced")
	}
}

// The pre-stop drain exists so writes already assigned to this server land.
// Refusing them once stopping would turn every rolling restart into client
// write failures for the length of the drain.
func TestStoreWriteVolumeNeedleStaysDurableWhileStopping(t *testing.T) {
	dir := t.TempDir()
	store := newIdxSplitStore(t, dir, dir)
	const vid = needle.VolumeId(1)
	require.NoError(t, store.AddVolume(vid, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	v := store.findVolume(vid)
	require.NotNil(t, v)
	counting := &countingBackend{BackendStorageFile: v.DataBackend}
	v.DataBackend = counting

	store.SetStopping()
	counting.syncCount = 0

	isUnchanged, err := store.WriteVolumeNeedle(vid, newRandomNeedle(1), true, true)
	require.NoError(t, err, "the drain must keep accepting durable writes")
	require.False(t, isUnchanged)
	require.Equal(t, 1, counting.syncCount, "the accepted write must actually be on disk")
}
