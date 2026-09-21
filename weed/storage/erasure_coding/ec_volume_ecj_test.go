package erasure_coding_test

import (
	"os"
	"testing"

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
