package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestCompactionToEmpty(t *testing.T) {
	dir := t.TempDir()

	// 1. Create a new volume
	v, err := NewVolume(dir, dir, "", 678, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	// 2. Write a few needles
	numNeedles := 5
	for i := 1; i <= numNeedles; i++ {
		n := newRandomNeedle(uint64(i))
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	// 3. Delete all of them
	for i := 1; i <= numNeedles; i++ {
		n := newEmptyNeedle(uint64(i))
		_, err := v.deleteNeedle2(n)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	// 4. Run compaction
	err = v.Compact2(0, 0, nil)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}

	// 5. Commit compaction
	err = v.CommitCompact()
	if err != nil {
		t.Fatalf("commit compaction: %v", err)
	}

	// 6. Verify the resulting .dat file size
	datSize, _, _ := v.FileStat()
	if datSize != super_block.SuperBlockSize {
		t.Errorf("expected dat file size %d, got %d", super_block.SuperBlockSize, datSize)
	}

	// 7. Verify index file size (should be 0 or at least no entries)
	if v.nm.FileCount() != 0 {
		t.Errorf("expected 0 files in needle map, got %d", v.nm.FileCount())
	}

	// Check if the file itself exists and is empty (except for superblock)
	datFileContent, err := os.ReadFile(v.FileName(".dat"))
	if err != nil {
		t.Fatalf("read dat file: %v", err)
	}
	if len(datFileContent) != super_block.SuperBlockSize {
		t.Fatalf("dat file physical size mismatch: expected %d, got %d", super_block.SuperBlockSize, len(datFileContent))
	}
}
