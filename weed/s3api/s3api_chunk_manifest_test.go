package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestPartRange(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,a", Offset: 0, Size: 8},
		{FileId: "1,b", Offset: 8, Size: 8},
		{FileId: "1,c", Offset: 16, Size: 24},
	}

	// Byte offsets win even when the chunk indexes no longer match the list.
	start, end, ok := partRange(&PartBoundaryInfo{StartChunk: 40, EndChunk: 80, StartOffset: 16, EndOffset: 40}, chunks)
	if !ok || start != 16 || end != 39 {
		t.Errorf("offset boundary: got [%d,%d] ok=%v, want [16,39]", start, end, ok)
	}

	// Legacy record: range from chunk indexes.
	start, end, ok = partRange(&PartBoundaryInfo{StartChunk: 1, EndChunk: 3}, chunks)
	if !ok || start != 8 || end != 39 {
		t.Errorf("legacy boundary: got [%d,%d] ok=%v, want [8,39]", start, end, ok)
	}

	// Legacy record with indexes off the list must report, not panic.
	for _, b := range []*PartBoundaryInfo{
		{StartChunk: 2, EndChunk: 9},
		{StartChunk: -1, EndChunk: 2},
		{StartChunk: 2, EndChunk: 2},
	} {
		if _, _, ok := partRange(b, chunks); ok {
			t.Errorf("boundary %+v should not resolve", b)
		}
	}
}
