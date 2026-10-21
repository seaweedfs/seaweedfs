package storage

import (
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/require"
)

// MarkTieredDeletable gives a freshly tiered single-copy volume the flags a
// restart gives it, and leaves an I/O quarantine alone.
func TestMarkTieredDeletable(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	v, _ := tierUpVolumeLive(t, t.TempDir(), 81, b)
	defer v.Close()
	v.noWriteOrDelete = true // as volume.tier.upload's mark leaves it
	require.True(t, v.MarkTieredDeletable())
	require.False(t, v.noWriteOrDelete)
	require.True(t, v.noWriteCanDelete)

	w, _ := tierUpVolumeLive(t, t.TempDir(), 82, b)
	defer w.Close()
	w.noWriteOrDelete = true
	w.markIoQuarantined()
	require.False(t, w.MarkTieredDeletable())
	require.True(t, w.noWriteOrDelete, "an I/O quarantine is not the tier upload's to lift")
}

// MarkTieredDeletable is only for a tiered volume: a stray call on a local
// volume must not pin it read-only-can-delete.
func TestMarkTieredDeletableLeavesANonTieredVolume(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 86, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	defer v.Close()

	require.False(t, v.MarkTieredDeletable())
	require.False(t, v.noWriteOrDelete)
	require.False(t, v.noWriteCanDelete)
}

// A heartbeat that quarantines the volume while MarkTieredDeletable waits for
// noWriteLock must keep its refusal: the quarantine check and the flag change
// happen under the same lock.
func TestMarkTieredDeletableKeepsAConcurrentQuarantine(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	v, _ := tierUpVolumeLive(t, t.TempDir(), 83, b)
	defer v.Close()
	v.noWriteOrDelete = true

	v.noWriteLock.Lock()
	done := make(chan bool)
	go func() { done <- v.MarkTieredDeletable() }()
	time.Sleep(100 * time.Millisecond) // let it reach noWriteLock
	// What CollectHeartbeat does: mark the quarantine, then refuse writes and deletes.
	v.markIoQuarantined()
	v.noWriteOrDelete = true
	v.noWriteLock.Unlock()

	require.False(t, <-done, "a volume quarantined before the flags changed must not be made deletable")
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	require.True(t, v.noWriteOrDelete, "the heartbeat's refusal was overwritten")
}

// A volume past the I/O error tolerance is about to be quarantined by the next
// heartbeat; it is not made deletable in the meantime.
func TestMarkTieredDeletableRefusesAVolumePastIoErrorTolerance(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	v, _ := tierUpVolumeLive(t, t.TempDir(), 84, b)
	defer v.Close()
	v.noWriteOrDelete = true
	for i := 0; i < IoErrorTolerance; i++ {
		v.noteIoError(syscall.EIO)
	}
	require.False(t, v.MarkTieredDeletable())
	require.True(t, v.noWriteOrDelete)

	v.clearIoError() // positive control: below the tolerance it is made deletable
	require.True(t, v.MarkTieredDeletable())
	require.False(t, v.noWriteOrDelete)
}

// DeleteVolumeNeedle reads the read-only flags under noWriteLock, so a delete
// that races the tier upload's flag change is not a data race (run with -race).
func TestDeleteVolumeNeedleRacesMarkTieredDeletable(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	v, _ := tierUpVolumeLive(t, t.TempDir(), 85, b)
	defer v.Close()
	v.noWriteOrDelete = true
	location := newTestLocation(10, false, 0)
	location.SetVolume(85, v)
	s := &Store{Locations: []*DiskLocation{location}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			v.MarkTieredDeletable()
		}
	}()
	for i := 0; i < 100; i++ {
		_, _ = s.DeleteVolumeNeedle(85, &needle.Needle{Id: types.Uint64ToNeedleId(1000)})
	}
	<-done

	// Positive control: once deletable, a delete of a live needle goes through.
	size, err := s.DeleteVolumeNeedle(85, &needle.Needle{Id: types.Uint64ToNeedleId(1)})
	require.NoError(t, err)
	require.NotZero(t, size)
}
