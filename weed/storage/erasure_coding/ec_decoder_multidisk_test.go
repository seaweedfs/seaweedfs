package erasure_coding

import (
	"os"
	"path/filepath"
	"testing"
)

// TestFindDatFileSizeWithRelocatedShard0: on a multi-disk server a volume's
// shards can sit on several disks, so the .ec00 is not necessarily beside the
// EcVolume's base path. FindDatFileSize takes the resolved shard-0 path; it
// must work when that path points to a different directory than the index.
func TestFindDatFileSizeWithRelocatedShard0(t *testing.T) {
	diskA := t.TempDir()
	diskB := t.TempDir()

	for _, ext := range []string{".dat", ".idx"} {
		data, err := os.ReadFile("1" + ext)
		if err != nil {
			t.Fatalf("read fixture 1%s: %v", ext, err)
		}
		if err := os.WriteFile(filepath.Join(diskA, "1"+ext), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	base := filepath.Join(diskA, "1")
	ctx := NewDefaultECContext("", 0)
	if _, err := generateEcFiles(base, 50, largeBlockSize, smallBlockSize, ctx); err != nil {
		t.Fatalf("generateEcFiles: %v", err)
	}
	if err := WriteSortedFileFromIdx(base, ".ecx"); err != nil {
		t.Fatalf("WriteSortedFileFromIdx: %v", err)
	}

	wantSize, err := FindDatFileSize(base+".ec00", base)
	if err != nil {
		t.Fatalf("FindDatFileSize with co-located shard 0: %v", err)
	}

	// Relocate shard 0 to a sibling disk, as a balance or copy can leave it.
	moved := filepath.Join(diskB, "1.ec00")
	if err := os.Rename(base+".ec00", moved); err != nil {
		t.Fatal(err)
	}

	gotSize, err := FindDatFileSize(moved, base)
	if err != nil {
		t.Fatalf("FindDatFileSize with relocated shard 0: %v", err)
	}
	if gotSize != wantSize {
		t.Fatalf("dat size changed with relocated shard 0: got %d want %d", gotSize, wantSize)
	}
}
