package erasure_coding

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compactTestIds are the 100 distinct ids the bloated journals below repeat.
func compactTestIds() []types.NeedleId {
	ids := make([]types.NeedleId, 0, 100)
	for i := 0; i < 100; i++ {
		ids = append(ids, types.NeedleId(1000+i))
	}
	return ids
}

func compactEcjBytes(ids []types.NeedleId, repeats int) []byte {
	one := make([]byte, len(ids)*types.NeedleIdSize)
	for i, id := range ids {
		types.NeedleIdToBytes(one[i*types.NeedleIdSize:], id)
	}
	b := make([]byte, 0, len(one)*repeats)
	for i := 0; i < repeats; i++ {
		b = append(b, one...)
	}
	return b
}

// compactEcxEntry is one .ecx entry for a live needle, so it can be deleted.
func compactEcxEntry(id types.NeedleId) []byte {
	b := make([]byte, types.NeedleMapEntrySize)
	types.NeedleIdToBytes(b[0:types.NeedleIdSize], id)
	types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.ToOffset(8))
	types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:], types.Size(10))
	return b
}

// writeCompactTestVolume lays down .ecx (holding needle 7), .ecj and an empty
// .vif for volume vid in dir, returning the base file name.
func writeCompactTestVolume(t *testing.T, dir string, vid needle.VolumeId, ecj []byte) string {
	t.Helper()
	base := EcShardFileName("", dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", compactEcxEntry(7), 0644))
	require.NoError(t, os.WriteFile(base+".ecj", ecj, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	return base
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

var errInjectedRename = errors.New("injected rename failure")
var errInjectedReopen = errors.New("injected reopen failure")

func failingRename(string, string) error     { return errInjectedRename }
func failingReopen(string) (*os.File, error) { return nil, errInjectedReopen }
func failingFsync(string) error              { return errors.New("injected fsync failure") }
func withEcjOps(f func(*ecjFsOps)) ecjFsOps  { ops := realEcjFsOps; f(&ops); return ops }
func mountCompactTest(dir string, vid needle.VolumeId, ops ecjFsOps) (*EcVolume, error) {
	return newEcVolumeWith("hdd", dir, dir, "", vid, ops)
}

// A load error leaves only part of the journal in the set. Compacting from it
// would delete the unread records from disk for good.
func TestEcjNotCompactedAfterLoadError(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 60, compactEcjBytes(compactTestIds(), 4096))
	bloated := fileSize(t, base+".ecj")

	// Mount while a copy holds the path, so the mount itself does not compact.
	done := BeginEcjWrite(base + ".ecj")
	ev, err := mountCompactTest(dir, 60, realEcjFsOps)
	require.NoError(t, err)
	defer ev.Close()
	done()
	require.Equal(t, bloated, fileSize(t, base+".ecj"))

	require.NoError(t, ev.compactEcjAfterLoad(errors.New("injected load failure"), realEcjFsOps))
	assert.Equal(t, bloated, fileSize(t, base+".ecj"), "a partial set must never be written over the journal")

	require.NoError(t, ev.compactEcjAfterLoad(nil, realEcjFsOps))
	assert.Equal(t, int64(100*types.NeedleIdSize), fileSize(t, base+".ecj"))
}

// A shard mount can give disk B a volume whose .ecj is disk A's. When A's own
// volume mounts later it must not replace the file under B, whose deletes
// would go to an unlinked inode and vanish at the next mount.
func TestEcjSiblingHolderBlocksCompaction(t *testing.T) {
	root := t.TempDir()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(a, 0755))
	require.NoError(t, os.MkdirAll(b, 0755))
	ids := compactTestIds()
	base := writeCompactTestVolume(t, a, 61, compactEcjBytes(ids, 1))

	// B has no index of its own, so it resolves A's and holds A's journal.
	onB, err := NewEcVolume("hdd", b, a, "", 61)
	require.NoError(t, err)
	require.Equal(t, base+".ecj", onB.FileName(".ecj"))

	// The journal bloats (say, a copy appended to it) and A mounts.
	require.NoError(t, os.WriteFile(base+".ecj", compactEcjBytes(ids, 4096), 0644))
	bloated := fileSize(t, base+".ecj")
	onA, err := NewEcVolume("hdd", a, a, "", 61)
	require.NoError(t, err)
	assert.Equal(t, bloated, fileSize(t, base+".ecj"), "compaction must not replace a journal another volume holds open")

	// B's delete must land in the file at the journal's path.
	require.NoError(t, onB.DeleteNeedleFromEcx(7))
	assert.Equal(t, bloated+types.NeedleIdSize, fileSize(t, base+".ecj"))
	onB.Close()
	onA.Close()

	// Sole holder now: the next mount compacts and keeps B's delete.
	ev, err := NewEcVolume("hdd", a, a, "", 61)
	require.NoError(t, err)
	defer ev.Close()
	assert.Equal(t, int64((len(ids)+1)*types.NeedleIdSize), fileSize(t, base+".ecj"))
	assert.True(t, ev.IsNeedleDeleted(7))
}

// A copy appending to the journal by path must not have its bytes dropped by
// a compaction renaming over the file.
func TestEcjActiveCopyBlocksCompaction(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 62, compactEcjBytes(compactTestIds(), 4096))
	bloated := fileSize(t, base+".ecj")

	done := BeginEcjWrite(base + ".ecj")
	ev, err := NewEcVolume("hdd", dir, dir, "", 62)
	require.NoError(t, err)
	assert.Equal(t, bloated, fileSize(t, base+".ecj"))
	ev.Close()
	done()

	ev, err = NewEcVolume("hdd", dir, dir, "", 62)
	require.NoError(t, err)
	defer ev.Close()
	assert.Equal(t, int64(100*types.NeedleIdSize), fileSize(t, base+".ecj"))
}

// A copy that starts while a compaction holds the path waits for it to end,
// then appends to the compacted file.
func TestEcjWriterWaitsForCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "1.ecj")
	hold := acquireEcjHold(path)
	defer hold.release()
	end, ok := hold.tryBeginCompaction()
	require.True(t, ok)

	started := make(chan struct{})
	go func() {
		done := BeginEcjWrite(path)
		close(started)
		done()
	}()
	select {
	case <-started:
		t.Fatal("a writer must not start while a compaction holds the path")
	case <-time.After(100 * time.Millisecond):
	}
	end()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("writer must proceed once the compaction ends")
	}
}

// Two spellings of one directory must meet in the registry.
func TestEcjRegistryKeysResolveDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0755))
	a := acquireEcjHold(filepath.Join(dir, "1.ecj"))
	defer a.release()
	b := acquireEcjHold(filepath.Join(dir, "sub", "..", "1.ecj"))
	_, ok := a.tryBeginCompaction()
	assert.False(t, ok)
	b.release()
	end, ok := a.tryBeginCompaction()
	require.True(t, ok)
	end()
}

// A journal that grew, or was replaced, after it was loaded must not be
// compacted from the stale set.
func TestEcjChangedAfterLoadIsDetected(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 63, compactEcjBytes([]types.NeedleId{1}, 1))
	ev, err := NewEcVolume("hdd", dir, dir, "", 63)
	require.NoError(t, err)
	defer ev.Close()

	unchanged, err := ev.ecjUnchangedSinceLoad(base + ".ecj")
	require.NoError(t, err)
	assert.True(t, unchanged)

	f, err := os.OpenFile(base+".ecj", os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(make([]byte, types.NeedleIdSize))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	unchanged, err = ev.ecjUnchangedSinceLoad(base + ".ecj")
	require.NoError(t, err)
	assert.False(t, unchanged)

	// Same size as loaded, different file: a copy that replaced the journal.
	require.NoError(t, os.WriteFile(base+".replacement", make([]byte, types.NeedleIdSize), 0644))
	require.NoError(t, os.Rename(base+".replacement", base+".ecj"))
	unchanged, err = ev.ecjUnchangedSinceLoad(base + ".ecj")
	require.NoError(t, err)
	assert.False(t, unchanged)
}

// A failed rename publishes nothing: the mount succeeds on the original
// journal, byte for byte, with no tmp left, and deletes still reach the file
// at the journal's path.
func TestEcjRenameFailureKeepsOriginalJournal(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 64, compactEcjBytes(compactTestIds(), 4096))
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	ev, err := mountCompactTest(dir, 64, withEcjOps(func(o *ecjFsOps) { o.rename = failingRename }))
	require.NoError(t, err, "a failed rename must not fail the mount")
	defer ev.Close()

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.NoFileExists(t, base+EcjCompactTmpExt)

	require.NoError(t, ev.DeleteNeedleFromEcx(7))
	assert.Equal(t, int64(len(before)+types.NeedleIdSize), fileSize(t, base+".ecj"),
		"the restored handle must append to the original journal")
}

// A failed rename whose handle restore also fails leaves no usable handle:
// the mount fails, reports both errors, and the journal is untouched.
func TestEcjRenameAndRestoreFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 65, compactEcjBytes(compactTestIds(), 4096))
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	_, err = mountCompactTest(dir, 65, withEcjOps(func(o *ecjFsOps) {
		o.rename = failingRename
		o.reopen = failingReopen
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInjectedRename)
	assert.ErrorIs(t, err, errInjectedReopen)
	assert.Contains(t, err.Error(), "no usable journal handle")
	assert.Contains(t, err.Error(), "reopening the original journal")

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.NoFileExists(t, base+EcjCompactTmpExt)
}

// A reopen failure after the rename fails the mount: the journal was replaced
// and the volume has no handle to it.
func TestEcjPostRenameReopenFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 66, compactEcjBytes(compactTestIds(), 4096))

	_, err := mountCompactTest(dir, 66, withEcjOps(func(o *ecjFsOps) { o.reopen = failingReopen }))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInjectedReopen)
	assert.Contains(t, err.Error(), "could not reopen it")
	assert.Equal(t, int64(100*types.NeedleIdSize), fileSize(t, base+".ecj"), "the rename had published the compacted journal")
}

// A directory fsync failure after the rename fails the mount too.
func TestEcjPostRenameFsyncFailureIsMountError(t *testing.T) {
	dir := t.TempDir()
	writeCompactTestVolume(t, dir, 67, compactEcjBytes(compactTestIds(), 4096))

	_, err := mountCompactTest(dir, 67, withEcjOps(func(o *ecjFsOps) { o.fsyncDir = failingFsync }))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not fsync its directory")
}

// A compaction tmp left by a crash between its write and the rename is removed
// at the next mount, even when the journal needs no compaction, and by
// Destroy.
func TestEcjStaleCompactionTmpRemoved(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 68, compactEcjBytes([]types.NeedleId{1}, 1))
	require.NoError(t, os.WriteFile(base+EcjCompactTmpExt, make([]byte, 64), 0644))

	ev, err := NewEcVolume("hdd", dir, dir, "", 68)
	require.NoError(t, err)
	assert.NoFileExists(t, base+EcjCompactTmpExt)

	require.NoError(t, os.WriteFile(base+EcjCompactTmpExt, make([]byte, 64), 0644))
	ev.Destroy()
	assert.NoFileExists(t, base+EcjCompactTmpExt)
}

// A mount refused by a later check must leave the journal as it was:
// compaction runs only after every check that can fail the mount.
func TestEcjRefusedMountDoesNotCompact(t *testing.T) {
	dir := t.TempDir()
	base := writeCompactTestVolume(t, dir, 69, compactEcjBytes(compactTestIds(), 4096))
	require.NoError(t, os.WriteFile(base+".vif", []byte("{not a volume info"), 0644))
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	_, err = NewEcVolume("hdd", dir, dir, "", 69)
	require.Error(t, err, "a malformed .vif must refuse the mount")

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after)
}
