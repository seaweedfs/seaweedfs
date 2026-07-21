package s3api

import (
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func flatChunks(n int) []*filer_pb.FileChunk {
	chunks := make([]*filer_pb.FileChunk, n)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{
			FileId: fmt.Sprintf("1,%x", i+1),
			Offset: int64(i) * 8,
			Size:   8,
		}
	}
	return chunks
}

type fakeManifestStore struct {
	saves   int
	failOn  int // 1-based save call to fail on; 0 = never
	deleted []*filer_pb.FileChunk
}

func (s *fakeManifestStore) save(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
	s.saves++
	if s.saves == s.failOn {
		return nil, fmt.Errorf("save %d failed", s.saves)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return nil, err
	}
	return &filer_pb.FileChunk{FileId: fmt.Sprintf("2,%x", s.saves), Offset: offset}, nil
}

func (s *fakeManifestStore) delete(chunks []*filer_pb.FileChunk) {
	s.deleted = append(s.deleted, chunks...)
}

func TestManifestizeOrKeepFlatFolds(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatChunks(filer.ManifestBatch + 50)

	result := manifestizeOrKeepFlat(store.save, store.delete, "/buckets/b/o", chunks)

	manifests, data := filer.SeparateManifestChunks(result)
	if len(manifests) != 1 || len(data) != 50 {
		t.Fatalf("expected 1 manifest + 50 flat chunks, got %d + %d", len(manifests), len(data))
	}
	if store.saves != 1 || len(store.deleted) != 0 {
		t.Errorf("expected 1 save and no deletes, got %d saves, %d deleted", store.saves, len(store.deleted))
	}
	if manifests[0].Offset != 0 || manifests[0].Size != uint64(filer.ManifestBatch*8) {
		t.Errorf("manifest span [%d,%d) wrong", manifests[0].Offset, manifests[0].Size)
	}
}

func TestManifestizeOrKeepFlatBelowThreshold(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatChunks(filer.ManifestBatch - 1)

	result := manifestizeOrKeepFlat(store.save, store.delete, "/buckets/b/o", chunks)

	if len(result) != len(chunks) || store.saves != 0 {
		t.Fatalf("expected untouched flat list, got %d chunks, %d saves", len(result), store.saves)
	}
}

func TestManifestizeOrKeepFlatSkipsSse(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatChunks(filer.ManifestBatch + 50)
	chunks[0].SseType = filer_pb.SSEType_SSE_S3

	result := manifestizeOrKeepFlat(store.save, store.delete, "/buckets/b/o", chunks)

	if len(result) != len(chunks) || store.saves != 0 {
		t.Fatalf("SSE chunks must not be folded, got %d chunks, %d saves", len(result), store.saves)
	}
}

// A fold that fails midway must fall back to the flat list and delete the
// manifest blobs its earlier batches already uploaded.
func TestManifestizeOrKeepFlatRollsBackPartialFold(t *testing.T) {
	store := &fakeManifestStore{failOn: 2}
	chunks := flatChunks(2*filer.ManifestBatch + 50)

	result := manifestizeOrKeepFlat(store.save, store.delete, "/buckets/b/o", chunks)

	if len(result) != len(chunks) {
		t.Fatalf("expected fallback to %d flat chunks, got %d", len(chunks), len(result))
	}
	if filer.HasChunkManifest(result) {
		t.Error("fallback list must not contain manifest chunks")
	}
	if len(store.deleted) != 1 || store.deleted[0].FileId != "2,1" {
		t.Fatalf("expected the first saved blob deleted, got %+v", store.deleted)
	}
}

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
