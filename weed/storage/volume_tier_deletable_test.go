package storage

import (
	"testing"

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
