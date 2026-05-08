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

// TestRebuildEcFiles_EightDiskTopology reproduces the user's reported topology
// from #9340: a single volume server with 8 hdd locations
// (/mnt/d{1..8}/weed), .ecx on one disk, the surviving local shard on another,
// freshly-copied shards on a third, shards 1 and 8 truly missing. The volume
// server's VolumeEcShardsRebuild picks the .ecx-owning disk as rebuildLocation
// and passes every other disk as additionalDirs; we mirror that contract here.
//
// The test exercises:
//
//  1. Clean state — must rebuild shards 1 and 8 byte-identical to the
//     pre-removal originals.
//  2. End-to-end decode — concatenating the data shards back through
//     WriteDatFile reproduces the original .dat byte-for-byte, proving the
//     rebuild is semantically correct (not just non-zero).
//  3. Residue scenario — a previously aborted rebuild left 0-byte placeholders
//     for shards 1 and 8 on the rebuildLocation. Pre-fix this caused
//     findShardFile to return the ghost, the read loop to short-circuit on
//     n==0, and the rebuild to silently no-op. Post-fix the ghosts are
//     skipped, the rebuild succeeds, and the residue is removed.
func TestRebuildEcFiles_EightDiskTopology(t *testing.T) {
	for _, tc := range []struct {
		name      string
		residueOn func(disks []string) [][2]string // (disk, ext) pairs to seed as 0-byte ghosts
	}{
		{name: "clean", residueOn: nil},
		{
			name: "residue_on_rebuild_disk",
			residueOn: func(disks []string) [][2]string {
				return [][2]string{
					{disks[1], ToExt(1)}, // d2 == rebuildLocation in this fixture
					{disks[1], ToExt(8)},
				}
			},
		},
		{
			name: "residue_on_sibling_disk",
			residueOn: func(disks []string) [][2]string {
				return [][2]string{
					{disks[3], ToExt(1)}, // a totally unrelated sibling disk
					{disks[6], ToExt(8)},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runEightDiskRebuild(t, tc.residueOn)
		})
	}
}

func runEightDiskRebuild(t *testing.T, residueOn func(disks []string) [][2]string) {
	t.Helper()

	root := t.TempDir()
	disks := make([]string, 8)
	for i := range disks {
		disks[i] = filepath.Join(root, "d"+string(rune('1'+i)), "weed")
		mustMkdir(t, disks[i])
	}

	// Encode in a staging dir, then distribute shards across the 8 disks the
	// way the volume server would after a couple of rebuild rounds:
	// .ecx + .vif + a few shards on d2 (this is rebuildLocation), the
	// existing shard 2 on d1, copied shards spread across d3..d8.
	stagingBase := filepath.Join(root, "staging", "_109")
	mustMkdir(t, filepath.Dir(stagingBase))
	const datSize = int64(50 * 1024 * 1024)
	originalShards, originalDat := encodeFixtureWithDat(t, stagingBase, datSize)

	rebuildLocation := disks[1]                                                       // d2 — owns .ecx
	homeDisk := []int{0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}                      // d1 holds shard 2 + most copied shards
	homeDisk[3], homeDisk[4], homeDisk[5], homeDisk[6], homeDisk[7] = 2, 3, 4, 5, 6   // spread a few copied shards to d3..d7
	homeDisk[10], homeDisk[11], homeDisk[12], homeDisk[13] = 7, 0, 1, 2               // and the rest across d8/d1/d2/d3
	missing := map[int]struct{}{1: {}, 8: {}}

	rebuildBase := filepath.Join(rebuildLocation, "collect_109")
	mustRename(t, stagingBase+".vif", rebuildBase+".vif")
	if _, err := os.Stat(stagingBase + ".ecx"); err == nil {
		mustRename(t, stagingBase+".ecx", rebuildBase+".ecx")
	}
	for i := 0; i < TotalShardsCount; i++ {
		if _, gone := missing[i]; gone {
			mustRemove(t, stagingBase+ToExt(i))
			continue
		}
		dst := filepath.Join(disks[homeDisk[i]], "collect_109"+ToExt(i))
		mustRename(t, stagingBase+ToExt(i), dst)
	}

	if residueOn != nil {
		for _, ent := range residueOn(disks) {
			ghost := filepath.Join(ent[0], "collect_109"+ent[1])
			mustWriteFile(t, ghost, nil)
		}
	}

	// Mirror VolumeEcShardsRebuild's contract: rebuildLocation is the disk
	// with .ecx; every other disk goes into additionalDirs.
	var additionalDirs []string
	for i, d := range disks {
		if i == 1 {
			continue
		}
		additionalDirs = append(additionalDirs, d)
	}

	rebuilt, err := RebuildEcFiles(rebuildBase, additionalDirs...)
	if err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	sort.Slice(rebuilt, func(i, j int) bool { return rebuilt[i] < rebuilt[j] })
	if want := []uint32{1, 8}; !equalUint32(rebuilt, want) {
		t.Fatalf("rebuilt = %v, want %v", rebuilt, want)
	}

	// Each rebuilt shard must match the original encoding bit-for-bit.
	for idx := range missing {
		got := mustReadFile(t, rebuildBase+ToExt(idx))
		if !bytes.Equal(got, originalShards[idx]) {
			t.Fatalf("rebuilt shard %d does not match the original encoding", idx)
		}
	}

	// Any 0-byte residue we seeded on a sibling disk must be removed; residue
	// at the rebuild location's output path is fine — it gets overwritten.
	if residueOn != nil {
		for _, ent := range residueOn(disks) {
			ghost := filepath.Join(ent[0], "collect_109"+ent[1])
			if ent[0] == rebuildLocation {
				continue // overwritten by the output file
			}
			if _, statErr := os.Stat(ghost); !os.IsNotExist(statErr) {
				t.Fatalf("residue %s was not removed (statErr=%v)", ghost, statErr)
			}
		}
	}

	// Decode all 10 data shards back through WriteDatFile and verify the
	// reconstructed .dat matches the original byte-for-byte. This is the real
	// safety check — non-zero rebuilt files aren't enough; they must contain
	// the right data.
	dataShardPaths := make([]string, DataShardsCount)
	for i := 0; i < DataShardsCount; i++ {
		if _, gone := missing[i]; gone {
			dataShardPaths[i] = rebuildBase + ToExt(i)
			continue
		}
		dataShardPaths[i] = filepath.Join(disks[homeDisk[i]], "collect_109"+ToExt(i))
	}
	decodedBase := filepath.Join(root, "decoded", "out")
	mustMkdir(t, filepath.Dir(decodedBase))
	if err := WriteDatFile(decodedBase, datSize, dataShardPaths); err != nil {
		t.Fatalf("WriteDatFile: %v", err)
	}
	decoded := mustReadFile(t, decodedBase+".dat")
	if !bytes.Equal(decoded, originalDat) {
		t.Fatalf("decoded .dat does not match original (len got=%d want=%d)", len(decoded), len(originalDat))
	}
}

// encodeFixtureWithDat is encodeFixture but also returns the original .dat
// bytes so the caller can verify a full encode→rebuild→decode round trip.
func encodeFixtureWithDat(t *testing.T, baseFileName string, datSize int64) (shards [TotalShardsCount][]byte, dat []byte) {
	t.Helper()

	dat = make([]byte, datSize)
	if _, err := rand.Read(dat); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if err := os.WriteFile(baseFileName+".dat", dat, 0644); err != nil {
		t.Fatalf("write dat: %v", err)
	}
	if err := WriteEcFiles(baseFileName); err != nil {
		t.Fatalf("WriteEcFiles: %v", err)
	}
	if err := WriteSortedFileFromIdx(baseFileName, ".ecx"); err != nil {
		// No .idx in this fixture (we don't run any reads), so this is best-effort.
		t.Logf("WriteSortedFileFromIdx (best-effort): %v", err)
	}
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
	for i := 0; i < TotalShardsCount; i++ {
		shards[i] = mustReadFile(t, baseFileName+ToExt(i))
	}
	return
}
