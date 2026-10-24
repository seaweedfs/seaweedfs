package erasure_coding_test

import (
	"os"
	"testing"
	"time"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ecjBytes(ids ...types.NeedleId) []byte {
	b := make([]byte, 0, len(ids)*types.NeedleIdSize)
	rec := make([]byte, types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(rec, id)
		b = append(b, rec...)
	}
	return b
}

func mountEcVolume(t *testing.T, dir string, ecx, ecj []byte) (*erasure_coding.EcVolume, string) {
	t.Helper()
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", ecx, 0644))
	if ecj != nil {
		require.NoError(t, os.WriteFile(base+".ecj", ecj, 0644))
	}
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	return ev, base
}

// A crash mid-append leaves a partial record at the end of .ecj. Mount must
// drop it: deletes append at the physical end, so keeping the fragment would
// misalign every later record and lose those deletes on the next mount.
func TestEcjTornTailTruncatedOnMount(t *testing.T) {
	dir := t.TempDir()

	ecx := append(makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(0), types.Size(100)),
		makeNeedleMapEntry(types.NeedleId(4), types.ToOffset(8), types.Size(100))...)
	ecj := append(ecjBytes(1, 2, 3), []byte{1, 2, 3, 4, 5}...)

	ev, base := mountEcVolume(t, dir, ecx, ecj)

	fi, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(3*types.NeedleIdSize), fi.Size())
	for _, id := range []types.NeedleId{1, 2, 3} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.False(t, ev.IsNeedleDeleted(4))

	require.NoError(t, ev.DeleteNeedleFromEcx(4))
	ev.Close()

	ev, _ = mountEcVolume(t, dir, ecx, nil)
	defer ev.Close()
	for _, id := range []types.NeedleId{1, 2, 3, 4} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
}

// A journal larger than one load chunk must seed every record, including the
// ones straddling and following the chunk boundary.
func TestEcjLoadsAcrossChunkBoundary(t *testing.T) {
	dir := t.TempDir()

	const count = types.NeedleId(200_000) // 1.6 MiB > 1 MiB chunk
	ecj := make([]byte, 0, int(count)*types.NeedleIdSize)
	for id := types.NeedleId(1); id <= count; id++ {
		rec := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(rec, id)
		ecj = append(ecj, rec...)
	}

	ev, _ := mountEcVolume(t, dir, nil, ecj)
	defer ev.Close()

	for _, id := range []types.NeedleId{1, 131072, 131073, count} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	assert.False(t, ev.IsNeedleDeleted(count+1))
}

// A journal of 1M records over 100 distinct ids must mount to those 100 ids
// and be folded down to 100*8 bytes. Mirrors the Rust bloated-compaction test
// and the production 1.51 TB failure.
func TestEcjBloatedJournalCompactedOnMount(t *testing.T) {
	dir := t.TempDir()

	const distinct = 100
	const repeats = 10_000 // 1M records = 8 MiB
	ids := make([]types.NeedleId, 0, distinct)
	for i := 0; i < distinct; i++ {
		ids = append(ids, types.NeedleId(1000+i))
	}
	ecj := make([]byte, 0, distinct*repeats*types.NeedleIdSize)
	one := ecjBytes(ids...)
	for i := 0; i < repeats; i++ {
		ecj = append(ecj, one...)
	}

	ev, base := mountEcVolume(t, dir, nil, ecj)
	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	ev.Close()

	fi, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(distinct*types.NeedleIdSize), fi.Size(), "journal should have been folded down to one entry per id")

	// No temp file left behind, and remount is stable.
	_, err = os.Stat(base + ".ecj.compact.tmp")
	assert.True(t, os.IsNotExist(err))
	ev2, _ := mountEcVolume(t, dir, nil, nil)
	defer ev2.Close()
	for _, id := range ids {
		assert.True(t, ev2.IsNeedleDeleted(id), "id %d", id)
	}
	fi2, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, int64(distinct*types.NeedleIdSize), fi2.Size())
}

// A healthy small journal must never be rewritten, however redundant.
func TestEcjHealthySmallJournalNotRewritten(t *testing.T) {
	dir := t.TempDir()

	ids := make([]types.NeedleId, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, types.NeedleId(i))
	}
	// Duplicated 3x, but only 2.4 KB — under ecjCompactMinBytes.
	ecj := append(append(ecjBytes(ids...), ecjBytes(ids...)...), ecjBytes(ids...)...)

	// Lay the files down and take the baseline BEFORE mounting, so a rewrite
	// during the mount shows up as a difference. The mtime is pushed into the
	// past so a rewrite within the filesystem's timestamp granularity still
	// changes it.
	base := erasure_coding.EcShardFileName("", dir, 7)
	require.NoError(t, os.WriteFile(base+".ecx", nil, 0644))
	require.NoError(t, os.WriteFile(base+".ecj", ecj, 0644))
	require.NoError(t, os.WriteFile(base+".vif", []byte{}, 0644))
	past := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(base+".ecj", past, past))
	before, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, "", 7)
	require.NoError(t, err)
	for _, id := range ids {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	ev.Close()

	after, err := os.ReadFile(base + ".ecj")
	require.NoError(t, err)
	assert.Equal(t, before, after, "small journal must not be rewritten")
	afterFi, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.True(t, afterFi.ModTime().Equal(past), "small journal mtime must be unchanged, got %v", afterFi.ModTime())
}
