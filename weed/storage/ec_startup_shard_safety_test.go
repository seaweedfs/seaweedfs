package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func writeSizedFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			f.Close()
			t.Fatalf("truncate %s: %v", path, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

// TestValidateEcVolume_PartialDatNextToFullShardsKeeps reproduces the headline
// data-loss geometry: an interrupted decode (or any stale/partial .dat) leaves
// a .dat SMALLER than the volume's real source next to the full-size EC shards
// that are the only copy. validateEcVolume must keep the shards, not delete
// them in favor of the partial .dat.
func TestValidateEcVolume_PartialDatNextToFullShardsKeeps(t *testing.T) {
	l := newTestDiskLocation(t.TempDir())
	base := erasure_coding.EcShardFileName("", l.Directory, 70)

	// Real shards sized for a 30 MB source volume.
	fullShardSize := calculateExpectedShardSize(30*1024*1024, erasure_coding.DataShardsCount)
	for i := 0; i < erasure_coding.DataShardsCount; i++ {
		writeSizedFile(t, base+erasure_coding.ToExt(i), fullShardSize)
	}
	// A partial .dat (e.g. a decode crashed mid-write) far smaller than the
	// shards' real source — bigger than a superblock so it is not swept as a
	// stub, but smaller than what these shards encode.
	writeSizedFile(t, base+".dat", 5*1024*1024)

	if !l.validateEcVolume("", 70) {
		t.Fatal("validateEcVolume deleted full-size shards beside a smaller (stale/partial) .dat; the shards may be the only copy")
	}
}

// TestValidateEcVolume_InterruptedEncodeReclaimsDat is the legitimate cleanup:
// a full source .dat next to shards SMALLER than it would encode (an encode
// interrupted mid-shard-write). The .dat is the complete source, so reclaiming
// it (deleting the partial shards) loses no data.
func TestValidateEcVolume_InterruptedEncodeReclaimsDat(t *testing.T) {
	l := newTestDiskLocation(t.TempDir())
	base := erasure_coding.EcShardFileName("", l.Directory, 71)

	datSize := int64(30 * 1024 * 1024)
	writeSizedFile(t, base+".dat", datSize)
	// Shards truncated well below the .dat's full encode (partial writes).
	partialShardSize := calculateExpectedShardSize(datSize, erasure_coding.DataShardsCount) / 3
	if partialShardSize <= 0 {
		t.Fatal("test setup: partial shard size must be positive")
	}
	for i := 0; i < erasure_coding.DataShardsCount; i++ {
		writeSizedFile(t, base+erasure_coding.ToExt(i), partialShardSize)
	}

	if l.validateEcVolume("", 71) {
		t.Fatal("validateEcVolume kept shards smaller than the full source .dat; an interrupted local encode should be reclaimable")
	}
}

// TestValidateEcVolume_TransientStatErrorKeeps confirms a non-ENOENT stat error
// never authorizes deletion: a shard directory that cannot be read (EACCES)
// must keep the data, not delete it.
func TestValidateEcVolume_TransientStatErrorKeeps(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	parent := t.TempDir()
	sub := filepath.Join(parent, "locked")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	base := erasure_coding.EcShardFileName("", sub, 73)
	writeSizedFile(t, base+".dat", int64(super_block.SuperBlockSize)+1024)
	writeSizedFile(t, base+erasure_coding.ToExt(0), 1024)
	if err := os.Chmod(sub, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(sub, 0o755) })

	l := newTestDiskLocation(sub)
	// stat under the 0000 dir fails with EACCES (not ENOENT); validateEcVolume
	// must keep (return true), never delete on a transient error.
	if !l.validateEcVolume("", 73) {
		t.Fatal("validateEcVolume returned delete on a transient stat error; must keep on ambiguity")
	}
}
