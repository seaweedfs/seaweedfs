package erasure_coding

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// TestRebuildEcFiles_BasicTwoMissing covers the simple repair path: 12 of 14
// shards present, 2 truly missing — must rebuild both and produce shards
// identical to the originals.
func TestRebuildEcFiles_BasicTwoMissing(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "_109")
	originalShards := encodeFixture(t, base, 50*1024*1024)

	// Drop shards 1 and 8.
	mustRemove(t, base+ToExt(1))
	mustRemove(t, base+ToExt(8))

	rebuilt, err := RebuildEcFiles(base)
	if err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	sort.Slice(rebuilt, func(i, j int) bool { return rebuilt[i] < rebuilt[j] })
	if want := []uint32{1, 8}; !equalUint32(rebuilt, want) {
		t.Fatalf("rebuilt = %v, want %v", rebuilt, want)
	}

	for _, idx := range []int{1, 8} {
		got := mustReadFile(t, base+ToExt(idx))
		if !bytes.Equal(got, originalShards[idx]) {
			t.Fatalf("shard %d content does not match the original encoding", idx)
		}
	}
}

// TestRebuildEcFiles_RejectsZeroByteResidue is the regression for issue #9340.
//
// When a previous ec.rebuild attempt aborts (e.g. crash, network error,
// reedsolomon "too few shards" mid-rebuild), it leaves 0-byte placeholder
// files for the shards it was about to write. On the next attempt, the old
// findShardFile saw those 0-byte ghosts as "present", short-circuited the
// reconstruction loop on the first n==0 read, and returned nil — making
// ec.rebuild claim success while leaving the volume with empty shards.
//
// The fix: skip 0-byte files when scanning, treat the shards as still-missing,
// and remove the residue after a successful rebuild so they don't shadow real
// shards on the next pass.
func TestRebuildEcFiles_RejectsZeroByteResidue(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "_109")
	originalShards := encodeFixture(t, base, 50*1024*1024)

	// Simulate the residue from a prior aborted rebuild: shard files for the
	// truly-missing shards exist on disk but are 0 bytes.
	for _, idx := range []int{1, 8} {
		mustRemove(t, base+ToExt(idx))
		mustWriteFile(t, base+ToExt(idx), nil)
	}

	rebuilt, err := RebuildEcFiles(base)
	if err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	sort.Slice(rebuilt, func(i, j int) bool { return rebuilt[i] < rebuilt[j] })
	if want := []uint32{1, 8}; !equalUint32(rebuilt, want) {
		t.Fatalf("rebuilt = %v, want %v (the old code reported [] and silently left 0-byte residue)", rebuilt, want)
	}

	for _, idx := range []int{1, 8} {
		fi, err := os.Stat(base + ToExt(idx))
		if err != nil {
			t.Fatalf("stat shard %d: %v", idx, err)
		}
		if fi.Size() == 0 {
			t.Fatalf("shard %d still 0 bytes after rebuild — residue was not cleared", idx)
		}
		got := mustReadFile(t, base+ToExt(idx))
		if !bytes.Equal(got, originalShards[idx]) {
			t.Fatalf("rebuilt shard %d does not match the original encoding", idx)
		}
	}
}

// TestRebuildEcFiles_ResidueOnAdditionalDisk reproduces the multi-disk variant
// of the residue bug. The .ecx and the real shards live on the rebuild disk;
// a sibling disk holds 0-byte placeholders for the shards that need to be
// regenerated (e.g. an earlier rebuild crashed while writing them there). The
// pre-fix findShardFile preferred the first existing path it saw — including
// the 0-byte one — so the rebuild silently succeeded with empty stripes.
func TestRebuildEcFiles_ResidueOnAdditionalDisk(t *testing.T) {
	root := t.TempDir()
	disk1 := filepath.Join(root, "d1")
	disk2 := filepath.Join(root, "d2")
	mustMkdir(t, disk1)
	mustMkdir(t, disk2)

	stagingBase := filepath.Join(root, "_109")
	originalShards := encodeFixture(t, stagingBase, 50*1024*1024)

	// Real shards (except 1 and 8) and .vif live on disk1.
	disk1Base := filepath.Join(disk1, "_109")
	for i := 0; i < TotalShardsCount; i++ {
		if i == 1 || i == 8 {
			continue
		}
		mustRename(t, stagingBase+ToExt(i), disk1Base+ToExt(i))
	}
	mustRename(t, stagingBase+".vif", disk1Base+".vif")

	// disk2 carries 0-byte residue for the missing shards.
	disk2Base := filepath.Join(disk2, "_109")
	for _, idx := range []int{1, 8} {
		mustWriteFile(t, disk2Base+ToExt(idx), nil)
	}

	rebuilt, err := RebuildEcFiles(disk1Base, disk2)
	if err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	sort.Slice(rebuilt, func(i, j int) bool { return rebuilt[i] < rebuilt[j] })
	if want := []uint32{1, 8}; !equalUint32(rebuilt, want) {
		t.Fatalf("rebuilt = %v, want %v", rebuilt, want)
	}

	for _, idx := range []int{1, 8} {
		got := mustReadFile(t, disk1Base+ToExt(idx))
		if !bytes.Equal(got, originalShards[idx]) {
			t.Fatalf("rebuilt shard %d on disk1 does not match the original", idx)
		}
		// Residue on the sibling disk must be cleaned up so it can't shadow
		// the real shard on a future scan.
		if _, err := os.Stat(disk2Base + ToExt(idx)); !os.IsNotExist(err) {
			t.Fatalf("shard %d residue on disk2 was not removed (err=%v)", idx, err)
		}
	}
}

// TestRebuildEcFiles_RejectsMismatchedShardSizes ensures we surface a clear
// error when surviving shards disagree on size, instead of feeding inconsistent
// data into reedsolomon and producing corrupted output.
func TestRebuildEcFiles_RejectsMismatchedShardSizes(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "_109")
	encodeFixture(t, base, 50*1024*1024)

	mustRemove(t, base+ToExt(1))
	// Truncate one surviving shard so its size disagrees with the others.
	if err := os.Truncate(base+ToExt(2), 1024); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	if _, err := RebuildEcFiles(base); err == nil {
		t.Fatalf("expected size-mismatch error, got nil")
	}
}

// encodeFixture writes a .dat file of the requested size, EC-encodes it, saves
// a matching .vif, and returns a snapshot of every original shard's bytes.
func encodeFixture(t *testing.T, baseFileName string, datSize int64) [TotalShardsCount][]byte {
	t.Helper()

	data := make([]byte, datSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if err := os.WriteFile(baseFileName+".dat", data, 0644); err != nil {
		t.Fatalf("write dat: %v", err)
	}
	if err := WriteEcFiles(baseFileName); err != nil {
		t.Fatalf("WriteEcFiles: %v", err)
	}

	// A minimal .vif so RebuildEcFiles picks up the right ec config rather
	// than warning and falling back to defaults.
	vif := &volume_server_pb.VolumeInfo{
		Version:     3,
		DatFileSize: datSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   DataShardsCount,
			ParityShards: ParityShardsCount,
		},
	}
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", vif); err != nil {
		t.Fatalf("save vif: %v", err)
	}

	var shards [TotalShardsCount][]byte
	for i := 0; i < TotalShardsCount; i++ {
		shards[i] = mustReadFile(t, baseFileName+ToExt(i))
	}
	return shards
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func mustRename(t *testing.T, from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Fatalf("rename %s -> %s: %v", from, to, err)
	}
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

func equalUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
