package erasure_coding_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idSet(ids ...types.NeedleId) map[types.NeedleId]struct{} {
	s := make(map[types.NeedleId]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func readEcjRecords(t *testing.T, path string) []types.NeedleId {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Zero(t, len(data)%types.NeedleIdSize, "journal must stay aligned")
	var ids []types.NeedleId
	for i := 0; i+types.NeedleIdSize <= len(data); i += types.NeedleIdSize {
		ids = append(ids, types.BytesToNeedleId(data[i:i+types.NeedleIdSize]))
	}
	return ids
}

// mergeFile runs the unmounted merge the way the store does: read, then append.
func mergeFile(t *testing.T, path string, incoming map[types.NeedleId]struct{}) int {
	t.Helper()
	local, size, err := erasure_coding.ReadEcjIds(path)
	require.NoError(t, err)
	added, err := erasure_coding.AppendEcjIds(path, local, incoming, size)
	require.NoError(t, err)
	return added
}

// A CopyFile stream splits records at arbitrary byte boundaries.
func TestEcjIdDecoder_RecordsSplitAcrossChunks(t *testing.T) {
	stream := append(ecjBytes(1, 2, 3, 2, 0x0102030405060708), 9, 9, 9)
	for chunk := 1; chunk <= len(stream); chunk++ {
		d := erasure_coding.NewEcjIdDecoder()
		for off := 0; off < len(stream); off += chunk {
			d.Write(stream[off:min(off+chunk, len(stream))])
		}
		assert.Equal(t, idSet(1, 2, 3, 0x0102030405060708), d.Ids(), "chunk size %d", chunk)
	}
}

func TestReadEcjIds(t *testing.T) {
	dir := t.TempDir()

	ids, size, err := erasure_coding.ReadEcjIds(filepath.Join(dir, "missing.ecj"))
	require.NoError(t, err)
	assert.Empty(t, ids)
	assert.Zero(t, size)

	torn := filepath.Join(dir, "torn.ecj")
	require.NoError(t, os.WriteFile(torn, append(ecjBytes(1, 2, 1), 7, 7, 7), 0644))
	ids, size, err = erasure_coding.ReadEcjIds(torn)
	require.NoError(t, err)
	assert.Equal(t, idSet(1, 2), ids)
	assert.Equal(t, int64(3*types.NeedleIdSize), size, "size covers whole records only")

	// A bloated journal repeating a few ids across several read chunks keeps
	// only the distinct ids.
	bloated := filepath.Join(dir, "bloated.ecj")
	var data []byte
	for i := 0; i < 300_000; i++ {
		data = append(data, ecjBytes(types.NeedleId(i%3))...)
	}
	require.NoError(t, os.WriteFile(bloated, data, 0644))
	ids, size, err = erasure_coding.ReadEcjIds(bloated)
	require.NoError(t, err)
	assert.Equal(t, idSet(0, 1, 2), ids)
	assert.Equal(t, int64(len(data)), size)
}

// Local {1,2,3} + source {3,4} => {1,2,3,4}: only the missing id is appended.
func TestAppendEcjIds_AppendsOnlyMissingIds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.ecj")
	require.NoError(t, os.WriteFile(path, ecjBytes(1, 2, 3), 0644))

	assert.Equal(t, 1, mergeFile(t, path, idSet(3, 4)))
	assert.Equal(t, []types.NeedleId{1, 2, 3, 4}, readEcjRecords(t, path))
}

// Copying the same shard A->B->A->B keeps the journal size constant; the old
// append path doubled it on every trip.
func TestAppendEcjIds_RoundTripStaysConstant(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.ecj")
	b := filepath.Join(dir, "b.ecj")
	require.NoError(t, os.WriteFile(a, ecjBytes(1, 2), 0644))
	require.NoError(t, os.WriteFile(b, ecjBytes(2, 3), 0644))
	for i := 0; i < 20; i++ {
		src, dst := a, b
		if i%2 == 1 {
			src, dst = b, a
		}
		ids, _, err := erasure_coding.ReadEcjIds(src)
		require.NoError(t, err)
		mergeFile(t, dst, ids)
	}
	for _, path := range []string{a, b} {
		assert.ElementsMatch(t, []types.NeedleId{1, 2, 3}, readEcjRecords(t, path))
	}
}

func TestAppendEcjIds_NothingNewLeavesJournalAlone(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.ecj")
	assert.Zero(t, mergeFile(t, missing, idSet()))
	assert.NoFileExists(t, missing, "an empty merge must not create a journal")

	path := filepath.Join(dir, "vol.ecj")
	require.NoError(t, os.WriteFile(path, ecjBytes(1, 2), 0644))
	assert.Zero(t, mergeFile(t, path, idSet(2, 1)))
	assert.Equal(t, []types.NeedleId{1, 2}, readEcjRecords(t, path))
}

func TestAppendEcjIds_RepairsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.ecj")
	require.NoError(t, os.WriteFile(path, append(ecjBytes(1, 2), 9, 9, 9), 0644))

	assert.Equal(t, 1, mergeFile(t, path, idSet(3)))
	assert.Equal(t, []types.NeedleId{1, 2, 3}, readEcjRecords(t, path))
}

// A journal that grew after the read must not receive a delta computed
// against its old contents.
func TestAppendEcjIds_RejectsChangedJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.ecj")
	require.NoError(t, os.WriteFile(path, ecjBytes(1), 0644))
	local, size, err := erasure_coding.ReadEcjIds(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, ecjBytes(1, 5), 0644))

	_, err = erasure_coding.AppendEcjIds(path, local, idSet(2), size)
	assert.ErrorIs(t, err, erasure_coding.ErrEcjChanged)
	assert.Equal(t, []types.NeedleId{1, 5}, readEcjRecords(t, path))
}

// A mounted volume merges through its own handle: the journal keeps its inode,
// the in-memory set follows, and later deletes land in the same live file.
func TestMergeJournal_MountedVolumeAppendsInPlace(t *testing.T) {
	dir := t.TempDir()
	var ecx []byte
	for id := types.NeedleId(1); id <= 5; id++ {
		ecx = append(ecx, makeNeedleMapEntry(id, types.ToOffset(int64(id)*8), types.Size(100))...)
	}
	ev, base := mountEcVolume(t, dir, ecx, ecjBytes(1))
	before, err := os.Stat(base + ".ecj")
	require.NoError(t, err)

	added, err := ev.MergeJournal(idSet(1, 2, 3))
	require.NoError(t, err)
	assert.Equal(t, 2, added)
	for _, id := range []types.NeedleId{1, 2, 3} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d", id)
	}
	_, deleteCount := ev.FileAndDeleteCount()
	assert.Equal(t, uint64(3), deleteCount)

	added, err = ev.MergeJournal(idSet(2, 3))
	require.NoError(t, err)
	assert.Zero(t, added, "a repeated merge appends nothing")

	require.NoError(t, ev.DeleteNeedleFromEcx(4))
	after, err := os.Stat(base + ".ecj")
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "the journal must never be replaced under the open handle")
	assert.Equal(t, []types.NeedleId{1, 2, 3, 4}, readEcjRecords(t, base+".ecj"))
	ev.Close()

	ev, _ = mountEcVolume(t, dir, ecx, nil)
	defer ev.Close()
	for _, id := range []types.NeedleId{1, 2, 3, 4} {
		assert.True(t, ev.IsNeedleDeleted(id), "id %d survives remount", id)
	}
}

// Deletes racing a merge must all reach the journal, and each id only once.
func TestMergeJournal_ConcurrentDeletesPersist(t *testing.T) {
	dir := t.TempDir()
	const n = 200
	var ecx []byte
	for id := types.NeedleId(1); id <= 2*n; id++ {
		ecx = append(ecx, makeNeedleMapEntry(id, types.ToOffset(int64(id)*8), types.Size(100))...)
	}
	ev, base := mountEcVolume(t, dir, ecx, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := types.NeedleId(1); id <= n; id++ {
			assert.NoError(t, ev.DeleteNeedleFromEcx(id))
		}
	}()
	for id := types.NeedleId(n/2 + 1); id <= 2*n; id += 10 {
		batch := idSet()
		for k := id; k < id+10 && k <= 2*n; k++ {
			batch[k] = struct{}{}
		}
		_, err := ev.MergeJournal(batch)
		require.NoError(t, err)
	}
	wg.Wait()
	ev.Close()

	records := readEcjRecords(t, base+".ecj")
	assert.Len(t, records, 2*n, "every id journaled exactly once")
	ids, _, err := erasure_coding.ReadEcjIds(base + ".ecj")
	require.NoError(t, err)
	assert.Len(t, ids, 2*n)
}

// A merge that loses a race with unmount must fail cleanly and must not bring
// a destroyed journal back.
func TestMergeJournal_ClosedVolume(t *testing.T) {
	dir := t.TempDir()
	ev, base := mountEcVolume(t, dir, makeNeedleMapEntry(1, types.ToOffset(8), types.Size(100)), nil)
	ev.Destroy()

	_, err := ev.MergeJournal(idSet(1))
	assert.Error(t, err)
	assert.NoFileExists(t, base+".ecj")
}
