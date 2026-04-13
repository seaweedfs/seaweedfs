package erasure_coding_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// makeEntry builds one .ecx index entry (needle id, offset, size).
func makeEntry(key types.NeedleId, offset types.Offset, size types.Size) []byte {
	b := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(b[0:types.NeedleIdSize], key)
	types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:], size)
	return b
}

func writeFixture(t *testing.T, dir, collection string, vid int, ecxData []byte) *erasure_coding.EcVolume {
	t.Helper()
	return writeFixtureWithEcj(t, dir, collection, vid, ecxData, nil)
}

func writeFixtureWithEcj(t *testing.T, dir, collection string, vid int, ecxData []byte, ecjIds []types.NeedleId) *erasure_coding.EcVolume {
	t.Helper()
	base := filepath.Join(dir, collection+"_1")

	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}
	ecjData := make([]byte, 0, len(ecjIds)*types.NeedleIdSize)
	for _, id := range ecjIds {
		b := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(b, id)
		ecjData = append(ecjData, b...)
	}
	if err := os.WriteFile(base+".ecj", ecjData, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}
	if err := os.WriteFile(base+".ec00", make([]byte, 8), 0644); err != nil {
		t.Fatalf("write ec00: %v", err)
	}
	if err := os.WriteFile(base+".vif", []byte{}, 0644); err != nil {
		t.Fatalf("write vif: %v", err)
	}

	ev, err := erasure_coding.NewEcVolume("hdd", dir, dir, collection, needle.VolumeId(vid))
	if err != nil {
		t.Fatalf("NewEcVolume: %v", err)
	}
	return ev
}

// TestEcVolumeFileAndDeleteCountInitial seeds a .ecx containing a mix of live
// and tombstoned entries and verifies the lazy walk populates counts correctly.
func TestEcVolumeFileAndDeleteCountInitial(t *testing.T) {
	dir := t.TempDir()

	entries := []byte{}
	// 3 live needles
	entries = append(entries, makeEntry(1, types.ToOffset(64), 100)...)
	entries = append(entries, makeEntry(2, types.ToOffset(128), 200)...)
	entries = append(entries, makeEntry(3, types.ToOffset(256), 300)...)
	// 2 tombstoned needles
	entries = append(entries, makeEntry(4, types.ToOffset(512), types.TombstoneFileSize)...)
	entries = append(entries, makeEntry(5, types.ToOffset(1024), types.TombstoneFileSize)...)

	ev := writeFixture(t, dir, "test", 1, entries)
	defer ev.Close()

	fileCount, deleteCount := ev.FileAndDeleteCount()
	if fileCount != 3 {
		t.Errorf("fileCount: got %d, want 3", fileCount)
	}
	if deleteCount != 2 {
		t.Errorf("deleteCount: got %d, want 2", deleteCount)
	}
}

// TestEcVolumeFileAndDeleteCountAfterDelete verifies that DeleteNeedleFromEcx
// keeps the cached counts in sync, including idempotent deletes of already
// tombstoned needles.
func TestEcVolumeFileAndDeleteCountAfterDelete(t *testing.T) {
	dir := t.TempDir()

	entries := []byte{}
	entries = append(entries, makeEntry(1, types.ToOffset(64), 100)...)
	entries = append(entries, makeEntry(2, types.ToOffset(128), 200)...)

	ev := writeFixture(t, dir, "test", 1, entries)
	defer ev.Close()

	// Initialize the cache.
	if fc, dc := ev.FileAndDeleteCount(); fc != 2 || dc != 0 {
		t.Fatalf("initial: got (%d, %d), want (2, 0)", fc, dc)
	}

	if err := ev.DeleteNeedleFromEcx(2); err != nil {
		t.Fatalf("DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 1 || dc != 1 {
		t.Errorf("after first delete: got (%d, %d), want (1, 1)", fc, dc)
	}

	// Re-deleting an already tombstoned needle must not drift the counts.
	if err := ev.DeleteNeedleFromEcx(2); err != nil {
		t.Fatalf("idempotent DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 1 || dc != 1 {
		t.Errorf("after idempotent delete: got (%d, %d), want (1, 1)", fc, dc)
	}

	// Deleting a non-existent needle is a no-op on counts.
	if err := ev.DeleteNeedleFromEcx(99); err != nil {
		t.Fatalf("missing DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 1 || dc != 1 {
		t.Errorf("after missing delete: got (%d, %d), want (1, 1)", fc, dc)
	}
}

// TestEcVolumeFileAndDeleteCountEcjUnion verifies that the delete count is the
// union of .ecx tombstones and .ecj journal entries, deduped by needle id.
func TestEcVolumeFileAndDeleteCountEcjUnion(t *testing.T) {
	dir := t.TempDir()

	// .ecx: ids 1..4. id 3 is already tombstoned.
	entries := []byte{}
	entries = append(entries, makeEntry(1, types.ToOffset(64), 100)...)
	entries = append(entries, makeEntry(2, types.ToOffset(128), 200)...)
	entries = append(entries, makeEntry(3, types.ToOffset(256), types.TombstoneFileSize)...)
	entries = append(entries, makeEntry(4, types.ToOffset(512), 400)...)

	// .ecj records:
	//   - id 2 (live in .ecx, pending-delete in journal)
	//   - id 3 (already tombstoned in .ecx; must dedup with the tombstone)
	//   - id 3 again (duplicate journal entry; must also dedup)
	//   - id 99 (not present in .ecx; still a journaled delete)
	ecjIds := []types.NeedleId{2, 3, 3, 99}

	ev := writeFixtureWithEcj(t, dir, "test", 1, entries, ecjIds)
	defer ev.Close()

	fileCount, deleteCount := ev.FileAndDeleteCount()

	// Live: ids 1 and 4 (2 is pending-delete via .ecj, 3 is tombstoned).
	if fileCount != 2 {
		t.Errorf("fileCount: got %d, want 2", fileCount)
	}
	// Deleted (deduped union):
	//   - id 2 (from .ecj, live in .ecx)
	//   - id 3 (tombstone in .ecx, also in .ecj twice — count once)
	//   - id 99 (only in .ecj, not in .ecx)
	if deleteCount != 3 {
		t.Errorf("deleteCount: got %d, want 3", deleteCount)
	}
}
