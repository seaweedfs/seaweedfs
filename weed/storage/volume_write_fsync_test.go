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
	syncCount int
	syncErr   error
}

func (b *countingBackend) Sync() error {
	b.syncCount++
	if b.syncErr != nil {
		return b.syncErr
	}
	return b.BackendStorageFile.Sync()
}

func newCountingVolume(t *testing.T) (*Volume, *countingBackend) {
	t.Helper()
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	counting := &countingBackend{BackendStorageFile: v.DataBackend}
	v.DataBackend = counting
	return v, counting
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
