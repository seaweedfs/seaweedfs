package erasure_coding_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// setupScrubLocalVolume seeds one live .ecx entry and a shard 0 whose bytes form
// the needle's on-disk header, then returns a loaded EcVolume with shard 0 added.
func setupScrubLocalVolume(t *testing.T, shard0 []byte) *erasure_coding.EcVolume {
	t.Helper()
	dir := t.TempDir()
	collection := "test"
	vid := needle.VolumeId(1)
	base := filepath.Join(dir, collection+"_1")

	if err := os.WriteFile(base+".ecx", makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(0), types.Size(100)), 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}
	if err := os.WriteFile(base+".ecj", []byte{}, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}
	if err := os.WriteFile(base+".vif", []byte{}, 0644); err != nil {
		t.Fatalf("write vif: %v", err)
	}
	if err := os.WriteFile(base+".ec00", shard0, 0644); err != nil {
		t.Fatalf("write ec00: %v", err)
	}

	ecv, err := erasure_coding.NewEcVolume("hdd", dir, dir, collection, vid)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	shard, err := erasure_coding.NewEcVolumeShard("hdd", dir, collection, vid, 0)
	if err != nil {
		t.Fatalf("NewEcVolumeShard: %v", err)
	}
	ecv.AddEcVolumeShard(shard)
	return ecv
}

// A needle the .ecx still reports as live (size 100) but whose reassembled
// on-disk header carries size 0 is a delete-state disagreement, not corruption.
func TestScrubLocal_SuppressesDeleteStateDisagreement(t *testing.T) {
	ecv := setupScrubLocalVolume(t, make([]byte, 256)) // all-zero header => size 0
	if _, _, errs := ecv.ScrubLocal(); len(errs) != 0 {
		t.Fatalf("delete-state disagreement must be suppressed, got %v", errs)
	}
}

// A non-zero on-disk header size that disagrees with the index is genuine
// corruption and must still be reported.
func TestScrubLocal_ReportsGenuineSizeCorruption(t *testing.T) {
	shard0 := make([]byte, 256)
	shard0[15] = 50 // header size field (big-endian uint32) = 50, != index 100 and != 0
	ecv := setupScrubLocalVolume(t, shard0)
	if _, _, errs := ecv.ScrubLocal(); len(errs) == 0 {
		t.Fatal("a non-zero size mismatch is genuine corruption and must be reported")
	}
}
