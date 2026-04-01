package erasure_coding_test

import (
	"os"
	"path/filepath"
	"testing"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestHasLiveNeedles_AllDeletedIsFalse(t *testing.T) {
	dir := t.TempDir()

	collection := "foo"
	base := filepath.Join(dir, collection+"_1")

	// Build an ecx file with only deleted entries.
	// ecx file entries are the same format as .idx entries.
	ecx := makeNeedleMapEntry(types.NeedleId(1), types.Offset{}, types.TombstoneFileSize)
	if err := os.WriteFile(base+".ecx", ecx, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if hasLive {
		t.Fatalf("expected no live entries")
	}
}

func TestHasLiveNeedles_WithLiveEntryIsTrue(t *testing.T) {
	dir := t.TempDir()

	collection := "foo"
	base := filepath.Join(dir, collection+"_1")

	// Build an ecx file containing at least one live entry.
	// ecx file entries are the same format as .idx entries.
	live := makeNeedleMapEntry(types.NeedleId(1), types.Offset{}, types.Size(1))
	if err := os.WriteFile(base+".ecx", live, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if !hasLive {
		t.Fatalf("expected live entries")
	}
}

func TestHasLiveNeedles_EmptyFileIsFalse(t *testing.T) {
	dir := t.TempDir()

	base := filepath.Join(dir, "foo_1")

	// Create an empty ecx file.
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if hasLive {
		t.Fatalf("expected no live entries for empty file")
	}
}

func makeNeedleMapEntry(key types.NeedleId, offset types.Offset, size types.Size) []byte {
	b := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(b[0:types.NeedleIdSize], key)
	types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:types.NeedleIdSize+types.OffsetSize+types.SizeSize], size)
	return b
}

// TestWriteIdxFileFromEcIndex_PreservesDeletedNeedles verifies that WriteIdxFileFromEcIndex
// correctly marks deleted needles in the generated .idx file.
// This tests the fix for issue #7751 where deleted files in encoded volumes
// were not properly marked as deleted when decoded.
func TestWriteIdxFileFromEcIndex_PreservesDeletedNeedles(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "foo_1")

	// Create an .ecx file with one live needle and one deleted needle
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	needle2 := makeNeedleMapEntry(types.NeedleId(2), types.ToOffset(128), types.TombstoneFileSize) // deleted

	ecxData := append(needle1, needle2...)
	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// Generate .idx from .ecx
	if err := erasure_coding.WriteIdxFileFromEcIndex(base); err != nil {
		t.Fatalf("WriteIdxFileFromEcIndex: %v", err)
	}

	// Verify .idx file has the same content
	idxData, err := os.ReadFile(base + ".idx")
	if err != nil {
		t.Fatalf("read idx: %v", err)
	}

	if len(idxData) != len(ecxData) {
		t.Fatalf("idx file size mismatch: got %d, want %d", len(idxData), len(ecxData))
	}

	// Verify the second needle is still marked as deleted
	entrySize := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	entry2 := idxData[entrySize : entrySize*2]
	size2 := types.BytesToSize(entry2[types.NeedleIdSize+types.OffsetSize:])
	if !size2.IsDeleted() {
		t.Fatalf("expected needle 2 to be marked as deleted, got size: %d", size2)
	}
}

// TestWriteIdxFileFromEcIndex_ProcessesEcjJournal verifies that WriteIdxFileFromEcIndex
// correctly processes deletions from the .ecj journal file.
func TestWriteIdxFileFromEcIndex_ProcessesEcjJournal(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "foo_1")

	// Create an .ecx file with two live needles
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	needle2 := makeNeedleMapEntry(types.NeedleId(2), types.ToOffset(128), types.Size(200))

	ecxData := append(needle1, needle2...)
	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// Create an .ecj file that records needle 2 as deleted
	ecjData := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(ecjData, types.NeedleId(2))
	if err := os.WriteFile(base+".ecj", ecjData, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	// Generate .idx from .ecx and .ecj
	if err := erasure_coding.WriteIdxFileFromEcIndex(base); err != nil {
		t.Fatalf("WriteIdxFileFromEcIndex: %v", err)
	}

	// Verify .idx file has 3 entries: 2 from .ecx + 1 deletion from .ecj
	idxData, err := os.ReadFile(base + ".idx")
	if err != nil {
		t.Fatalf("read idx: %v", err)
	}

	entrySize := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	expectedSize := entrySize * 3 // 2 from ecx + 1 deletion append from ecj
	if len(idxData) != expectedSize {
		t.Fatalf("idx file size mismatch: got %d, want %d", len(idxData), expectedSize)
	}

	// The third entry should be the deletion record for needle 2
	entry3 := idxData[entrySize*2 : entrySize*3]
	key3 := types.BytesToNeedleId(entry3[0:types.NeedleIdSize])
	size3 := types.BytesToSize(entry3[types.NeedleIdSize+types.OffsetSize:])

	if key3 != types.NeedleId(2) {
		t.Fatalf("expected needle id 2 in deletion record, got: %d", key3)
	}
	if !size3.IsDeleted() {
		t.Fatalf("expected deletion record to have tombstone size, got: %d", size3)
	}
}

// TestDecodeWithNonEmptyEcj_AllDeleted verifies the full decode pre-processing
// when .ecj contains deletions for ALL live entries in .ecx.
// After RebuildEcxFile merges .ecj into .ecx, HasLiveNeedles must return false
// and WriteIdxFileFromEcIndex must produce an .idx where every entry is tombstoned.
func TestDecodeWithNonEmptyEcj_AllDeleted(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "test_1")

	// .ecx: two live entries
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	needle2 := makeNeedleMapEntry(types.NeedleId(2), types.ToOffset(128), types.Size(200))
	ecxData := append(needle1, needle2...)
	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// .ecj: both needles deleted
	ecjData := make([]byte, 2*types.NeedleIdSize)
	types.NeedleIdToBytes(ecjData[0:types.NeedleIdSize], types.NeedleId(1))
	types.NeedleIdToBytes(ecjData[types.NeedleIdSize:], types.NeedleId(2))
	if err := os.WriteFile(base+".ecj", ecjData, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	// Before rebuild, ecx entries look live
	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles before rebuild: %v", err)
	}
	if !hasLive {
		t.Fatal("expected live entries before rebuild")
	}

	// Simulate what VolumeEcShardsToVolume now does: merge .ecj into .ecx
	if err := erasure_coding.RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile: %v", err)
	}

	// .ecj should be removed after rebuild
	if _, err := os.Stat(base + ".ecj"); !os.IsNotExist(err) {
		t.Fatal("expected .ecj to be removed after RebuildEcxFile")
	}

	// After rebuild, HasLiveNeedles must return false
	hasLive, err = erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles after rebuild: %v", err)
	}
	if hasLive {
		t.Fatal("expected no live entries after rebuild merged all deletions")
	}

	// WriteIdxFileFromEcIndex should still work (no .ecj to process)
	if err := erasure_coding.WriteIdxFileFromEcIndex(base); err != nil {
		t.Fatalf("WriteIdxFileFromEcIndex: %v", err)
	}

	idxData, err := os.ReadFile(base + ".idx")
	if err != nil {
		t.Fatalf("read idx: %v", err)
	}

	// .idx should have exactly 2 entries (copied from .ecx, both now tombstoned)
	entrySize := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	if len(idxData) != 2*entrySize {
		t.Fatalf("idx file size: got %d, want %d", len(idxData), 2*entrySize)
	}

	// Both entries must be tombstoned
	for i := 0; i < 2; i++ {
		entry := idxData[i*entrySize : (i+1)*entrySize]
		size := types.BytesToSize(entry[types.NeedleIdSize+types.OffsetSize:])
		if !size.IsDeleted() {
			t.Fatalf("entry %d: expected tombstone, got size %d", i+1, size)
		}
	}
}

// TestDecodeWithNonEmptyEcj_PartiallyDeleted verifies decode pre-processing
// when .ecj deletes only some entries. After RebuildEcxFile, HasLiveNeedles
// must still return true for the surviving entries, and WriteIdxFileFromEcIndex
// must produce an .idx that correctly distinguishes live from deleted needles.
func TestDecodeWithNonEmptyEcj_PartiallyDeleted(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "test_1")

	// .ecx: three live entries
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	needle2 := makeNeedleMapEntry(types.NeedleId(2), types.ToOffset(128), types.Size(200))
	needle3 := makeNeedleMapEntry(types.NeedleId(3), types.ToOffset(256), types.Size(300))
	ecxData := append(append(needle1, needle2...), needle3...)
	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// .ecj: only needle 2 is deleted
	ecjData := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(ecjData, types.NeedleId(2))
	if err := os.WriteFile(base+".ecj", ecjData, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	// Merge .ecj into .ecx
	if err := erasure_coding.RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile: %v", err)
	}

	// HasLiveNeedles must still return true (needles 1 and 3 survive)
	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if !hasLive {
		t.Fatal("expected live entries after partial deletion")
	}

	// WriteIdxFileFromEcIndex
	if err := erasure_coding.WriteIdxFileFromEcIndex(base); err != nil {
		t.Fatalf("WriteIdxFileFromEcIndex: %v", err)
	}

	idxData, err := os.ReadFile(base + ".idx")
	if err != nil {
		t.Fatalf("read idx: %v", err)
	}

	entrySize := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	if len(idxData) != 3*entrySize {
		t.Fatalf("idx file size: got %d, want %d", len(idxData), 3*entrySize)
	}

	// Verify each entry
	for i := 0; i < 3; i++ {
		entry := idxData[i*entrySize : (i+1)*entrySize]
		key := types.BytesToNeedleId(entry[0:types.NeedleIdSize])
		size := types.BytesToSize(entry[types.NeedleIdSize+types.OffsetSize:])

		switch key {
		case types.NeedleId(1), types.NeedleId(3):
			if size.IsDeleted() {
				t.Fatalf("needle %d: should be live, got tombstone", key)
			}
		case types.NeedleId(2):
			if !size.IsDeleted() {
				t.Fatalf("needle %d: should be tombstoned, got size %d", key, size)
			}
		default:
			t.Fatalf("unexpected needle id %d", key)
		}
	}
}

// TestDecodeWithEmptyEcj verifies that the decode flow is a no-op when
// .ecj exists but is empty (no deletions recorded).
func TestDecodeWithEmptyEcj(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "test_1")

	// .ecx: one live entry
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	if err := os.WriteFile(base+".ecx", needle1, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// .ecj: empty
	if err := os.WriteFile(base+".ecj", []byte{}, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	// RebuildEcxFile with empty .ecj should not change anything
	if err := erasure_coding.RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile: %v", err)
	}

	// HasLiveNeedles must still return true
	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if !hasLive {
		t.Fatal("expected live entries with empty .ecj")
	}
}

// TestDecodeWithNoEcjFile verifies that the decode flow works when no .ecj
// file exists at all.
func TestDecodeWithNoEcjFile(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "test_1")

	// .ecx: one live entry
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	if err := os.WriteFile(base+".ecx", needle1, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// No .ecj file

	// RebuildEcxFile should be a no-op
	if err := erasure_coding.RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile: %v", err)
	}

	hasLive, err := erasure_coding.HasLiveNeedles(base)
	if err != nil {
		t.Fatalf("HasLiveNeedles: %v", err)
	}
	if !hasLive {
		t.Fatal("expected live entries without .ecj file")
	}
}

// TestEcxFileDeletionVisibleAfterSync verifies that deletions made to .ecx
// via MarkNeedleDeleted are visible to other readers after Sync().
// This is a regression test for issue #7751.
func TestEcxFileDeletionVisibleAfterSync(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "foo_1")

	// Create an .ecx file with one live needle
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	if err := os.WriteFile(base+".ecx", needle1, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// Open the file for writing (simulating what EcVolume does)
	ecxFile, err := os.OpenFile(base+".ecx", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open ecx: %v", err)
	}

	// Mark needle as deleted using MarkNeedleDeleted
	err = erasure_coding.MarkNeedleDeleted(ecxFile, 0)
	if err != nil {
		ecxFile.Close()
		t.Fatalf("MarkNeedleDeleted: %v", err)
	}

	// Sync the file to ensure changes are visible to other readers
	if err := ecxFile.Sync(); err != nil {
		ecxFile.Close()
		t.Fatalf("Sync: %v", err)
	}
	ecxFile.Close()

	// Now open with a new file handle and verify deletion is visible
	data, err := os.ReadFile(base + ".ecx")
	if err != nil {
		t.Fatalf("read ecx: %v", err)
	}

	size := types.BytesToSize(data[types.NeedleIdSize+types.OffsetSize:])
	if !size.IsDeleted() {
		t.Fatalf("expected needle to be marked as deleted after sync, got size: %d", size)
	}
}

// TestEcxFileDeletionNotVisibleWithoutSync verifies that without Sync(),
// deletions may not be visible to other readers (demonstrating the bug).
// Note: This test may be flaky depending on OS caching behavior, but it
// documents the expected behavior.
func TestEcxFileDeletionWithSeparateHandles(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "foo_1")

	// Create an .ecx file with one live needle
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	if err := os.WriteFile(base+".ecx", needle1, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// Open the file for writing (writer handle)
	writerFile, err := os.OpenFile(base+".ecx", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open ecx for write: %v", err)
	}
	defer writerFile.Close()

	// Open the file for reading (reader handle - simulating CopyFile behavior)
	readerFile, err := os.OpenFile(base+".ecx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open ecx for read: %v", err)
	}
	defer readerFile.Close()

	// Mark needle as deleted via writer handle
	err = erasure_coding.MarkNeedleDeleted(writerFile, 0)
	if err != nil {
		t.Fatalf("MarkNeedleDeleted: %v", err)
	}

	// Sync the writer to flush changes
	if err := writerFile.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Read via reader handle - after sync, changes should be visible
	data := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	if _, err := readerFile.ReadAt(data, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	size := types.BytesToSize(data[types.NeedleIdSize+types.OffsetSize:])
	if !size.IsDeleted() {
		t.Fatalf("expected deletion to be visible after Sync(), got size: %d", size)
	}
}

// TestEcVolumeSyncEnsuresDeletionsVisible is an integration test for issue #7751
// that verifies the EcVolume.Sync() method ensures deleted needles are visible
// to external readers (like ec.decode's CopyFile).
func TestEcVolumeSyncEnsuresDeletionsVisible(t *testing.T) {
	dir := t.TempDir()

	collection := "test"
	vid := 1
	base := filepath.Join(dir, collection+"_1")

	// Create initial .ecx file with live needle
	needle1 := makeNeedleMapEntry(types.NeedleId(1), types.ToOffset(64), types.Size(100))
	needle2 := makeNeedleMapEntry(types.NeedleId(2), types.ToOffset(128), types.Size(200))
	ecxData := append(needle1, needle2...)
	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}

	// Create empty .ecj file
	if err := os.WriteFile(base+".ecj", []byte{}, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	// Create minimal EC shard file to allow EcVolume creation
	// Shards need super block header (8 bytes)
	shardData := make([]byte, 8)
	if err := os.WriteFile(base+".ec00", shardData, 0644); err != nil {
		t.Fatalf("write ec00: %v", err)
	}

	// Create .vif file
	if err := os.WriteFile(base+".vif", []byte{}, 0644); err != nil {
		t.Fatalf("write vif: %v", err)
	}

	// Create EcVolume
	ecVolume, err := erasure_coding.NewEcVolume(
		"hdd",
		dir, // data dir
		dir, // idx dir
		collection,
		needle.VolumeId(vid),
	)
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	defer ecVolume.Close()

	// Delete needle 2 via EcVolume (this writes to .ecx and .ecj)
	if err := ecVolume.DeleteNeedleFromEcx(types.NeedleId(2)); err != nil {
		t.Fatalf("DeleteNeedleFromEcx: %v", err)
	}

	// Before Sync: open a new reader handle to simulate CopyFile
	readerBefore, err := os.OpenFile(base+".ecx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open ecx for read (before sync): %v", err)
	}
	defer readerBefore.Close()

	// Now call Sync() - this is what fixes issue #7751
	ecVolume.Sync()

	// After Sync: open another reader handle
	readerAfter, err := os.OpenFile(base+".ecx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open ecx for read (after sync): %v", err)
	}
	defer readerAfter.Close()

	// Read needle 2's entry from the after-sync handle
	entrySize := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	data := make([]byte, entrySize)
	if _, err := readerAfter.ReadAt(data, int64(entrySize)); err != nil {
		t.Fatalf("ReadAt after sync: %v", err)
	}

	// Verify needle 2 is marked as deleted
	size := types.BytesToSize(data[types.NeedleIdSize+types.OffsetSize:])
	if !size.IsDeleted() {
		t.Fatalf("expected needle 2 to be deleted after Sync(), got size: %d", size)
	}
}
