package filer

import (
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func flatTestChunks(n int) []*filer_pb.FileChunk {
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

func TestMaybeManifestizeFolds(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatTestChunks(ManifestBatch + 50)

	result, err := MaybeManifestize(store.save, store.delete, chunks)
	if err != nil {
		t.Fatalf("MaybeManifestize: %v", err)
	}

	manifests, data := SeparateManifestChunks(result)
	if len(manifests) != 1 || len(data) != 50 {
		t.Fatalf("expected 1 manifest + 50 flat chunks, got %d + %d", len(manifests), len(data))
	}
	if store.saves != 1 || len(store.deleted) != 0 {
		t.Errorf("expected 1 save and no deletes, got %d saves, %d deleted", store.saves, len(store.deleted))
	}
	if manifests[0].Offset != 0 || manifests[0].Size != uint64(ManifestBatch*8) {
		t.Errorf("manifest span [%d,%d) wrong", manifests[0].Offset, manifests[0].Size)
	}
}

func TestMaybeManifestizeBelowThreshold(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatTestChunks(ManifestBatch - 1)

	result, err := MaybeManifestize(store.save, store.delete, chunks)
	if err != nil {
		t.Fatalf("MaybeManifestize: %v", err)
	}
	if len(result) != len(chunks) || store.saves != 0 {
		t.Fatalf("expected untouched flat list, got %d chunks, %d saves", len(result), store.saves)
	}
}

func TestMaybeManifestizeSkipsSse(t *testing.T) {
	store := &fakeManifestStore{}
	chunks := flatTestChunks(ManifestBatch + 50)
	chunks[0].SseType = filer_pb.SSEType_SSE_S3

	result, err := MaybeManifestize(store.save, store.delete, chunks)
	if err != nil {
		t.Fatalf("MaybeManifestize: %v", err)
	}
	if len(result) != len(chunks) || store.saves != 0 {
		t.Fatalf("SSE chunks must not be folded, got %d chunks, %d saves", len(result), store.saves)
	}
}

// A fold that fails midway must hand back the caller's own list and delete the
// manifest blobs its earlier batches already uploaded.
func TestMaybeManifestizeRollsBackPartialFold(t *testing.T) {
	store := &fakeManifestStore{failOn: 2}
	chunks := flatTestChunks(2*ManifestBatch + 50)

	result, err := MaybeManifestize(store.save, store.delete, chunks)
	if err == nil {
		t.Fatal("expected the failed save to be reported")
	}
	if len(result) != len(chunks) {
		t.Fatalf("expected fallback to %d flat chunks, got %d", len(chunks), len(result))
	}
	if HasChunkManifest(result) {
		t.Error("fallback list must not contain manifest chunks")
	}
	if len(store.deleted) != 1 || store.deleted[0].FileId != "2,1" {
		t.Fatalf("expected the first saved blob deleted, got %+v", store.deleted)
	}
}

// The manifests a caller came in with are not re-folded, and must survive a
// fold that fails on the flat remainder: returning only the data chunks would
// lose everything they cover.
func TestMaybeManifestizeKeepsExistingManifestsOnFailure(t *testing.T) {
	store := &fakeManifestStore{failOn: 2}
	existing := &filer_pb.FileChunk{FileId: "m,1", IsChunkManifest: true, Offset: 0, Size: 24}
	chunks := append([]*filer_pb.FileChunk{existing}, flatTestChunks(2*ManifestBatch+50)...)

	result, _ := MaybeManifestize(store.save, store.delete, chunks)

	if len(result) != len(chunks) {
		t.Fatalf("expected original %d chunks, got %d", len(chunks), len(result))
	}
	if result[0].FileId != "m,1" || !result[0].IsChunkManifest {
		t.Fatalf("existing manifest dropped: %+v", result[0])
	}
	if len(store.deleted) != 1 || store.deleted[0].FileId != "2,1" {
		t.Fatalf("expected the first saved blob deleted, got %+v", store.deleted)
	}
}

// Without a deleter the fold still falls back to the caller's list; the blobs
// it saved are only reported.
func TestMaybeManifestizeRollbackWithoutDeleter(t *testing.T) {
	store := &fakeManifestStore{failOn: 2}
	chunks := flatTestChunks(2*ManifestBatch + 50)

	result, err := MaybeManifestize(store.save, nil, chunks)
	if err == nil {
		t.Fatal("expected the failed save to be reported")
	}
	if len(result) != len(chunks) || HasChunkManifest(result) {
		t.Fatalf("expected fallback to %d flat chunks, got %d", len(chunks), len(result))
	}
}
