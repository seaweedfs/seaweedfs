package s3api

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// The writer (multipartPartBoundary) and reader (PartBoundaryInfo) structs are
// kept separate; their JSON keys must stay in sync, including the offset
// fields added for manifest-chunked entries.
func TestPartBoundaryJsonCompatibility(t *testing.T) {
	written, err := json.Marshal([]multipartPartBoundary{
		{PartNumber: 1, StartChunk: 0, EndChunk: 2, ETag: "abc", StartOffset: 0, EndOffset: 16},
		{PartNumber: 2, StartChunk: 2, EndChunk: 3, ETag: "def", StartOffset: 16, EndOffset: 24},
	})
	if err != nil {
		t.Fatal(err)
	}

	var read []PartBoundaryInfo
	if err := json.Unmarshal(written, &read); err != nil {
		t.Fatal(err)
	}
	if len(read) != 2 {
		t.Fatalf("expected 2 boundaries, got %d", len(read))
	}
	if read[0].EndChunk != 2 || read[0].ETag != "abc" || read[0].StartOffset != 0 || read[0].EndOffset != 16 {
		t.Errorf("boundary 1 mismatch: %+v", read[0])
	}
	if read[1].StartChunk != 2 || read[1].StartOffset != 16 || read[1].EndOffset != 24 {
		t.Errorf("boundary 2 mismatch: %+v", read[1])
	}

	// Legacy records carry no offset fields; they must unmarshal to zero so
	// readers fall back to the chunk-index path.
	var legacy []PartBoundaryInfo
	if err := json.Unmarshal([]byte(`[{"part":1,"start":0,"end":2,"etag":"abc"}]`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy[0].StartOffset != 0 || legacy[0].EndOffset != 0 {
		t.Errorf("legacy boundary should have zero offsets: %+v", legacy[0])
	}
}

func partsEntry(t *testing.T, boundaries []multipartPartBoundary, chunks []*filer_pb.FileChunk) *filer_pb.Entry {
	t.Helper()
	boundariesJSON, err := json.Marshal(boundaries)
	if err != nil {
		t.Fatal(err)
	}
	return &filer_pb.Entry{
		Chunks: chunks,
		Extended: map[string][]byte{
			s3_constants.SeaweedFSMultipartPartBoundaries: boundariesJSON,
		},
	}
}

func TestBuildObjectAttributesPartsPrefersOffsets(t *testing.T) {
	s3a := &S3ApiServer{}

	// Entry whose chunk list was folded into a single manifest chunk: the
	// stored chunk indexes no longer address the flat list, but the byte
	// offsets still describe each part.
	entry := partsEntry(t, []multipartPartBoundary{
		{PartNumber: 1, StartChunk: 0, EndChunk: 2, StartOffset: 0, EndOffset: 16},
		{PartNumber: 2, StartChunk: 2, EndChunk: 4, StartOffset: 16, EndOffset: 40},
	}, []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 40, IsChunkManifest: true},
	})

	parts := s3a.buildObjectAttributesParts(entry, 1000, 0)
	if parts == nil || len(parts.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %+v", parts)
	}
	if parts.Parts[0].Size != 16 {
		t.Errorf("part 1 size = %d, want 16", parts.Parts[0].Size)
	}
	if parts.Parts[1].Size != 24 {
		t.Errorf("part 2 size = %d, want 24", parts.Parts[1].Size)
	}
}

func TestBuildObjectAttributesPartsLegacyChunkIndexes(t *testing.T) {
	s3a := &S3ApiServer{}

	chunks := []*filer_pb.FileChunk{
		{FileId: "1,ab", Offset: 0, Size: 8},
		{FileId: "1,ac", Offset: 8, Size: 8},
		{FileId: "1,ad", Offset: 16, Size: 24},
	}
	entry := partsEntry(t, []multipartPartBoundary{
		{PartNumber: 1, StartChunk: 0, EndChunk: 2},
		{PartNumber: 2, StartChunk: 2, EndChunk: 3},
	}, chunks)

	parts := s3a.buildObjectAttributesParts(entry, 1000, 0)
	if parts == nil || len(parts.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %+v", parts)
	}
	if parts.Parts[0].Size != 16 {
		t.Errorf("part 1 size = %d, want 16", parts.Parts[0].Size)
	}
	if parts.Parts[1].Size != 24 {
		t.Errorf("part 2 size = %d, want 24", parts.Parts[1].Size)
	}

	// Out-of-range legacy indexes (e.g. metadata from a differently shaped
	// entry) must not panic; the part is reported with size 0.
	badEntry := partsEntry(t, []multipartPartBoundary{
		{PartNumber: 1, StartChunk: 5, EndChunk: 9},
	}, chunks)
	parts = s3a.buildObjectAttributesParts(badEntry, 1000, 0)
	if parts == nil || len(parts.Parts) != 1 || parts.Parts[0].Size != 0 {
		t.Fatalf("expected 1 part with size 0, got %+v", parts)
	}
}
