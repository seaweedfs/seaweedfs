package erasure_coding_test

import (
	"fmt"
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

// encodeEcjIds packs a slice of needle ids into the binary .ecj on-disk format.
func encodeEcjIds(ids []types.NeedleId) []byte {
	buf := make([]byte, 0, len(ids)*types.NeedleIdSize)
	for _, id := range ids {
		b := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(b, id)
		buf = append(buf, b...)
	}
	return buf
}

func writeFixture(t *testing.T, dir, collection string, vid int, ecxData []byte, ecjIds []types.NeedleId) *erasure_coding.EcVolume {
	t.Helper()
	base := filepath.Join(dir, fmt.Sprintf("%s_%d", collection, vid))

	if err := os.WriteFile(base+".ecx", ecxData, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}
	if err := os.WriteFile(base+".ecj", encodeEcjIds(ecjIds), 0644); err != nil {
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

// TestEcVolumeFileAndDeleteCountInitial verifies that FileAndDeleteCount is
// derived from file sizes at load: fileCount = .ecx size / NeedleMapEntrySize
// and deleteCount = .ecj size / NeedleIdSize. No index walk is performed.
func TestEcVolumeFileAndDeleteCountInitial(t *testing.T) {
	dir := t.TempDir()

	// 3 needles in .ecx, 2 deletions already recorded in .ecj.
	ecx := []byte{}
	ecx = append(ecx, makeEntry(1, types.ToOffset(64), 100)...)
	ecx = append(ecx, makeEntry(2, types.ToOffset(128), 200)...)
	ecx = append(ecx, makeEntry(3, types.ToOffset(256), 300)...)
	ecj := []types.NeedleId{2, 3}

	ev := writeFixture(t, dir, "test", 1, ecx, ecj)
	defer ev.Close()

	fileCount, deleteCount := ev.FileAndDeleteCount()
	if fileCount != 3 {
		t.Errorf("fileCount: got %d, want 3 (.ecx entries)", fileCount)
	}
	if deleteCount != 2 {
		t.Errorf("deleteCount: got %d, want 2 (.ecj entries)", deleteCount)
	}
}

// TestEcVolumeFileAndDeleteCountAfterDelete verifies that DeleteNeedleFromEcx
// increments the derived delete count by appending to .ecj, while leaving
// fileCount (derived from the sealed .ecx) unchanged. Idempotent re-deletes
// and deletes of missing needles must not drift the count.
func TestEcVolumeFileAndDeleteCountAfterDelete(t *testing.T) {
	dir := t.TempDir()

	ecx := []byte{}
	ecx = append(ecx, makeEntry(1, types.ToOffset(64), 100)...)
	ecx = append(ecx, makeEntry(2, types.ToOffset(128), 200)...)

	ev := writeFixture(t, dir, "test", 1, ecx, nil)
	defer ev.Close()

	if fc, dc := ev.FileAndDeleteCount(); fc != 2 || dc != 0 {
		t.Fatalf("initial: got (%d, %d), want (2, 0)", fc, dc)
	}

	if err := ev.DeleteNeedleFromEcx(2); err != nil {
		t.Fatalf("DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 2 || dc != 1 {
		t.Errorf("after first delete: got (%d, %d), want (2, 1)", fc, dc)
	}

	// Re-deleting an already tombstoned needle is a no-op on .ecj, so
	// deleteCount must stay at 1.
	if err := ev.DeleteNeedleFromEcx(2); err != nil {
		t.Fatalf("idempotent DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 2 || dc != 1 {
		t.Errorf("after idempotent delete: got (%d, %d), want (2, 1)", fc, dc)
	}

	// Deleting a non-existent needle is a no-op: search returns NotFound,
	// no .ecj append.
	if err := ev.DeleteNeedleFromEcx(99); err != nil {
		t.Fatalf("missing DeleteNeedleFromEcx: %v", err)
	}
	if fc, dc := ev.FileAndDeleteCount(); fc != 2 || dc != 1 {
		t.Errorf("after missing delete: got (%d, %d), want (2, 1)", fc, dc)
	}
}
