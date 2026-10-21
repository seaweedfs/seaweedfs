package storage

import (
	"syscall"
	"testing"
	"time"

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
