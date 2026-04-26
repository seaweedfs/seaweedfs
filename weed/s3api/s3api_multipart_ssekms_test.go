package s3api

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestBuildMultipartSSEKMSReader_RejectsBadIVBeforeAnyFetch pins the contract
// that a per-chunk SSE-KMS metadata blob with a missing or wrong-length IV is
// rejected during preparation, before any volume-server fetch fires.
//
// DeserializeSSEKMSMetadata only proves the JSON parses; it leaves the
// kmsKey.IV field at whatever the metadata actually carried. CreateSSEKMSDecryptedReader
// does call ValidateIV, but only when the wrap closure runs -- after the
// chunk's HTTP body has already been opened. The lazy reader's whole point
// is to never start an HTTP fetch for a chunk we know we cannot decrypt, so
// IV validation must happen in the prep loop. This test is the regression
// guard for that, addressing CodeRabbit review feedback on PR #9228.
func TestBuildMultipartSSEKMSReader_RejectsBadIVBeforeAnyFetch(t *testing.T) {
	makeMetadata := func(iv []byte) []byte {
		t.Helper()
		key := &SSEKMSKey{
			KeyID:            "test-kms-key",
			EncryptedDataKey: bytes.Repeat([]byte{0x42}, 32),
			IV:               iv,
		}
		md, err := SerializeSSEKMSMetadata(key)
		if err != nil {
			t.Fatalf("SerializeSSEKMSMetadata: %v", err)
		}
		return md
	}

	cases := []struct {
		name      string
		iv        []byte
		expectErr string
	}{
		{"missing IV", nil, "invalid"},
		{"empty IV", []byte{}, "invalid"},
		{"short IV", []byte("too-short"), "invalid"}, // 9 bytes, not 16
		{"long IV", bytes.Repeat([]byte{1}, 32), "invalid"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fetchCalled := false
			fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
				fetchCalled = true
				return io.NopCloser(bytes.NewReader([]byte("ignored"))), nil
			}

			chunks := []*filer_pb.FileChunk{
				{
					FileId:      "1,bad-iv",
					Offset:      0,
					Size:        8,
					SseType:     filer_pb.SSEType_SSE_KMS,
					SseMetadata: makeMetadata(tc.iv),
				},
			}

			_, err := buildMultipartSSEKMSReader(chunks, fetch)
			if err == nil {
				t.Fatal("expected error for invalid SSE-KMS IV, got nil")
			}
			if !strings.Contains(err.Error(), tc.expectErr) {
				t.Errorf("expected %q in error, got: %v", tc.expectErr, err)
			}
			// The whole point of upfront validation: no HTTP fetch must fire
			// for a chunk that fails the metadata gate.
			if fetchCalled {
				t.Error("fetchChunk was called for a chunk with invalid IV; metadata validation must run before any fetch")
			}
		})
	}
}

// TestBuildMultipartSSEKMSReader_RejectsMissingMetadataBeforeAnyFetch verifies
// that a chunk tagged SSE-KMS but with no SseMetadata bytes is rejected during
// preparation, also without firing a fetch. Mirrors the SSE-S3 contract pinned
// by TestBuildMultipartSSES3Reader_RejectsBadChunkBeforeAnyFetch.
func TestBuildMultipartSSEKMSReader_RejectsMissingMetadataBeforeAnyFetch(t *testing.T) {
	fetched := map[string]int{}
	fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
		fetched[c.GetFileIdString()]++
		return io.NopCloser(bytes.NewReader([]byte("ignored"))), nil
	}

	// First chunk has valid SSE-KMS metadata; second chunk is tagged SSE-KMS
	// but has no metadata blob. The eager pre-#9228 implementation would have
	// opened chunk 0's HTTP body before discovering chunk 1's problem; the
	// lazy implementation must reject up front and leave both alone.
	validKey := &SSEKMSKey{
		KeyID:            "test-kms-key",
		EncryptedDataKey: bytes.Repeat([]byte{0x42}, 32),
		IV:               make([]byte, s3_constants.AESBlockSize),
	}
	if _, err := rand.Read(validKey.IV); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	validMeta, err := SerializeSSEKMSMetadata(validKey)
	if err != nil {
		t.Fatalf("SerializeSSEKMSMetadata: %v", err)
	}

	chunks := []*filer_pb.FileChunk{
		{
			FileId:      "1,good",
			Offset:      0,
			Size:        16,
			SseType:     filer_pb.SSEType_SSE_KMS,
			SseMetadata: validMeta,
		},
		{
			FileId:      "2,no-metadata",
			Offset:      16,
			Size:        16,
			SseType:     filer_pb.SSEType_SSE_KMS,
			SseMetadata: nil, // triggers "missing per-chunk metadata"
		},
	}

	_, err = buildMultipartSSEKMSReader(chunks, fetch)
	if err == nil {
		t.Fatal("expected error from missing chunk metadata, got nil")
	}
	if !strings.Contains(err.Error(), "missing per-chunk metadata") {
		t.Errorf("expected 'missing per-chunk metadata' in error, got: %v", err)
	}
	if len(fetched) != 0 {
		t.Errorf("expected no chunks fetched on validation failure, got %v", fetched)
	}
}

// TestBuildMultipartSSEKMSReader_RejectsUnparseableMetadataBeforeAnyFetch
// covers the prep-loop branch where SseMetadata is non-empty but JSON-malformed
// so DeserializeSSEKMSMetadata itself returns an error. Same contract: no
// fetch fires.
func TestBuildMultipartSSEKMSReader_RejectsUnparseableMetadataBeforeAnyFetch(t *testing.T) {
	fetchCalled := false
	fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
		fetchCalled = true
		return io.NopCloser(bytes.NewReader([]byte("ignored"))), nil
	}

	chunks := []*filer_pb.FileChunk{
		{
			FileId:      "1,garbage",
			Offset:      0,
			Size:        8,
			SseType:     filer_pb.SSEType_SSE_KMS,
			SseMetadata: []byte("{not-json"),
		},
	}

	_, err := buildMultipartSSEKMSReader(chunks, fetch)
	if err == nil {
		t.Fatal("expected error from unparseable SSE-KMS metadata, got nil")
	}
	if !strings.Contains(err.Error(), "deserialize SSE-KMS metadata") {
		t.Errorf("expected 'deserialize SSE-KMS metadata' in error, got: %v", err)
	}
	if fetchCalled {
		t.Error("fetchChunk was called for a chunk with garbage metadata; deserialize must fail before any fetch")
	}
}

// TestBuildMultipartSSEKMSReader_SortsByOffset verifies that the prep loop
// reorders chunks by Offset before constructing the lazy reader, matching
// the documented contract and the SSE-S3 helper.
//
// Driving the reader's Read() to observe fetch order does not work as a full
// ordering check: CreateSSEKMSDecryptedReader requires a live KMS provider to
// unwrap the encrypted DEK, which is unavailable in this unit test, so the
// wrap closure fails on the first chunk and the lazy reader marks itself
// finished -- only one fetch is ever observed. Instead, since the lazy
// reader and its prepared chunks live in the same package, we type-assert
// the returned reader to *lazyMultipartChunkReader and inspect the prepared
// chunks slice directly. This is a stronger check (the entire ordering, not
// just the first element) and does not depend on KMS availability.
func TestBuildMultipartSSEKMSReader_SortsByOffset(t *testing.T) {
	makeChunk := func(fid string, offset int64) *filer_pb.FileChunk {
		key := &SSEKMSKey{
			KeyID:            "test-kms-key",
			EncryptedDataKey: bytes.Repeat([]byte{0x42}, 32),
			IV:               bytes.Repeat([]byte{0x10}, s3_constants.AESBlockSize),
		}
		meta, err := SerializeSSEKMSMetadata(key)
		if err != nil {
			t.Fatalf("SerializeSSEKMSMetadata: %v", err)
		}
		return &filer_pb.FileChunk{
			FileId:      fid,
			Offset:      offset,
			Size:        1,
			SseType:     filer_pb.SSEType_SSE_KMS,
			SseMetadata: meta,
		}
	}
	chunks := []*filer_pb.FileChunk{
		makeChunk("c2", 200),
		makeChunk("c0", 0),
		makeChunk("c1", 100),
	}

	fetchCalled := false
	fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
		fetchCalled = true
		return nil, fmt.Errorf("fetch must not be called: ordering is checked via prepared chunks")
	}

	reader, err := buildMultipartSSEKMSReader(chunks, fetch)
	if err != nil {
		t.Fatalf("buildMultipartSSEKMSReader: %v", err)
	}
	if fetchCalled {
		t.Fatal("fetch must not be invoked during prep; ordering is verified statically")
	}

	lazy, ok := reader.(*lazyMultipartChunkReader)
	if !ok {
		t.Fatalf("expected *lazyMultipartChunkReader, got %T", reader)
	}
	if len(lazy.chunks) != 3 {
		t.Fatalf("expected 3 prepared chunks, got %d", len(lazy.chunks))
	}
	gotOrder := []string{
		lazy.chunks[0].chunk.GetFileIdString(),
		lazy.chunks[1].chunk.GetFileIdString(),
		lazy.chunks[2].chunk.GetFileIdString(),
	}
	wantOrder := []string{"c0", "c1", "c2"}
	for i, want := range wantOrder {
		if gotOrder[i] != want {
			t.Errorf("prepared chunks not in offset order: got %v, want %v", gotOrder, wantOrder)
			break
		}
	}
}
