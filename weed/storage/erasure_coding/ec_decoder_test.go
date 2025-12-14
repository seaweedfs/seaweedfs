package erasure_coding_test

import (
	"os"
	"path/filepath"
	"testing"

	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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

func makeNeedleMapEntry(key types.NeedleId, offset types.Offset, size types.Size) []byte {
	b := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(b[0:types.NeedleIdSize], key)
	types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:types.NeedleIdSize+types.OffsetSize+types.SizeSize], size)
	return b
}
