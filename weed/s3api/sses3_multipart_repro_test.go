package s3api

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestMultipartSSES3RealisticEndToEnd reproduces the production multipart SSE-S3
// flow where ALL parts share the same DEK and baseIV (the upload-init key/IV),
// and each part is encrypted with partOffset=0. Each part is then chunked at 8MB
// boundaries the way UploadReaderInChunks does. After completion, the chunks
// have global offsets but per-chunk stored IVs derived from part-local offsets.
//
// buildMultipartSSES3Reader is then run on the assembled chunks; the
// concatenated decrypted output must equal the concatenation of the part
// plaintexts. This is the round trip that fails in #8908 if anything in the
// encrypt or decrypt path is inconsistent.
func TestMultipartSSES3RealisticEndToEnd(t *testing.T) {
	keyManager := initSSES3KeyManagerForTest(t)

	// One DEK and one baseIV, shared by all parts (the upload-init values).
	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	baseIV := make([]byte, s3_constants.AESBlockSize)
	if _, err := rand.Read(baseIV); err != nil {
		t.Fatalf("rand.Read baseIV: %v", err)
	}

	const chunkSize = int64(8 * 1024 * 1024)

	// Realistic mix of part sizes: small (one chunk), exact 8MB, >8MB (two
	// chunks), much larger (multiple chunks).
	partSizes := []int{
		5 * 1024 * 1024,         // 5MB (single chunk)
		8 * 1024 * 1024,         // 8MB exactly (single chunk, full)
		8*1024*1024 + 123,       // crosses chunk boundary (two chunks)
		17 * 1024 * 1024,        // three chunks
		1234,                    // tiny
	}

	parts := make([][]byte, len(partSizes))
	for i, n := range partSizes {
		parts[i] = makeRandomPlaintext(t, n)
	}

	// Build the chunks list the way completion would produce it: encrypt each
	// part with partOffset=0, slice the ciphertext at chunkSize boundaries,
	// store per-chunk metadata IV = calculateIVWithOffset(baseIV, partLocalOff),
	// then assign GLOBAL offsets to the FileChunk.
	type chunkBlob struct {
		fid       string
		ciphertext []byte
	}
	var chunks []*filer_pb.FileChunk
	chunkData := map[string][]byte{}
	var globalOffset int64
	for partIdx, partPlaintext := range parts {
		encReader, _, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(partPlaintext), key, baseIV, 0)
		if err != nil {
			t.Fatalf("CreateSSES3EncryptedReaderWithBaseIV(part %d): %v", partIdx, err)
		}
		ciphertext, err := io.ReadAll(encReader)
		if err != nil {
			t.Fatalf("read encrypted part %d: %v", partIdx, err)
		}

		for partLocalOff := int64(0); partLocalOff < int64(len(ciphertext)); partLocalOff += chunkSize {
			end := partLocalOff + chunkSize
			if end > int64(len(ciphertext)) {
				end = int64(len(ciphertext))
			}
			cipherSlice := ciphertext[partLocalOff:end]

			chunkIV, _ := calculateIVWithOffset(baseIV, partLocalOff)
			chunkKey := &SSES3Key{
				Key:       key.Key,
				KeyID:     key.KeyID,
				Algorithm: key.Algorithm,
				IV:        chunkIV,
			}
			meta, err := SerializeSSES3Metadata(chunkKey)
			if err != nil {
				t.Fatalf("SerializeSSES3Metadata(part %d off %d): %v", partIdx, partLocalOff, err)
			}

			fid := fmt.Sprintf("%d,%d", partIdx+1, partLocalOff)
			chunks = append(chunks, &filer_pb.FileChunk{
				FileId:      fid,
				Offset:      globalOffset, // global offset assigned at completion
				Size:        uint64(end - partLocalOff),
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: meta,
			})
			chunkData[fid] = cipherSlice
			globalOffset += end - partLocalOff
		}
	}

	fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
		data, ok := chunkData[c.GetFileIdString()]
		if !ok {
			return nil, fmt.Errorf("unexpected chunk %s", c.GetFileIdString())
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	reader, err := buildMultipartSSES3Reader(chunks, keyManager, fetch)
	if err != nil {
		t.Fatalf("buildMultipartSSES3Reader: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll decrypted output: %v", err)
	}

	want := bytes.Join(parts, nil)
	if !bytes.Equal(got, want) {
		idx := firstMismatch(got, want)
		end := idx + 32
		if end > len(got) {
			end = len(got)
		}
		if end > len(want) {
			end = len(want)
		}
		t.Fatalf("decrypted output mismatch at byte %d (total len got=%d want=%d)\n got: %x\nwant: %x",
			idx, len(got), len(want), got[idx:end], want[idx:end])
	}
}

// TestBuildMultipartSSES3Reader_LazyChunkFetch pins the lazy behavior of
// buildMultipartSSES3Reader: chunk N's HTTP fetch only happens after chunk
// N-1 has been fully consumed. The original eager loop opened every chunk's
// HTTP response upfront and held them open while io.MultiReader walked
// through readers[0]; for objects with many chunks (e.g. a 200MB Docker image
// blob), this could trip volume-server idle/keepalive limits and produce
// truncated reads at the client (issue #8908).
//
// The test installs a fetch hook that tracks how many chunks have been
// opened and when each one is closed, and verifies:
//   - At any point during streaming, at most one chunk's reader is open.
//   - The number of opened chunks grows as bytes are read out, not upfront.
//   - All chunks are closed when the outer reader is fully drained.
func TestBuildMultipartSSES3Reader_LazyChunkFetch(t *testing.T) {
	keyManager := initSSES3KeyManagerForTest(t)

	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	baseIV := make([]byte, s3_constants.AESBlockSize)
	if _, err := rand.Read(baseIV); err != nil {
		t.Fatalf("rand.Read baseIV: %v", err)
	}

	// Many small chunks (mirrors many-part Docker Registry uploads).
	const numChunks = 8
	const chunkPayload = 1024
	plaintexts := make([][]byte, numChunks)
	chunkData := map[string][]byte{}
	chunks := make([]*filer_pb.FileChunk, 0, numChunks)
	for i := 0; i < numChunks; i++ {
		plaintexts[i] = makeRandomPlaintext(t, chunkPayload)

		// Encrypt as a fresh "part" with partOffset=0 (matching putToFiler).
		encReader, _, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(plaintexts[i]), key, baseIV, 0)
		if err != nil {
			t.Fatalf("encrypt chunk %d: %v", i, err)
		}
		ciphertext, err := io.ReadAll(encReader)
		if err != nil {
			t.Fatalf("read ciphertext %d: %v", i, err)
		}

		chunkIV, _ := calculateIVWithOffset(baseIV, 0)
		chunkKey := &SSES3Key{
			Key:       key.Key,
			KeyID:     key.KeyID,
			Algorithm: key.Algorithm,
			IV:        chunkIV,
		}
		meta, err := SerializeSSES3Metadata(chunkKey)
		if err != nil {
			t.Fatalf("serialize meta %d: %v", i, err)
		}

		fid := fmt.Sprintf("vol,c%d", i)
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId:      fid,
			Offset:      int64(i) * chunkPayload,
			Size:        uint64(chunkPayload),
			SseType:     filer_pb.SSEType_SSE_S3,
			SseMetadata: meta,
		})
		chunkData[fid] = ciphertext
	}

	var openCount int64 // total opens
	var liveCount int64 // currently open
	var maxLive int64

	fetch := func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
		atomic.AddInt64(&openCount, 1)
		if live := atomic.AddInt64(&liveCount, 1); live > atomic.LoadInt64(&maxLive) {
			atomic.StoreInt64(&maxLive, live)
		}
		data, ok := chunkData[c.GetFileIdString()]
		if !ok {
			return nil, fmt.Errorf("unexpected chunk %s", c.GetFileIdString())
		}
		return &liveTrackingReadCloser{Reader: bytes.NewReader(data), live: &liveCount}, nil
	}

	reader, err := buildMultipartSSES3Reader(chunks, keyManager, fetch)
	if err != nil {
		t.Fatalf("buildMultipartSSES3Reader: %v", err)
	}

	// Construction alone must not have opened any chunk reader.
	if got := atomic.LoadInt64(&openCount); got != 0 {
		t.Fatalf("expected no chunks opened before any Read, got %d", got)
	}

	// Read first byte: should open chunk 0 only.
	one := make([]byte, 1)
	if n, err := reader.Read(one); n != 1 || err != nil {
		t.Fatalf("first Read: n=%d err=%v", n, err)
	}
	if got := atomic.LoadInt64(&openCount); got != 1 {
		t.Errorf("after first byte, expected 1 chunk opened, got %d", got)
	}
	if got := atomic.LoadInt64(&liveCount); got != 1 {
		t.Errorf("after first byte, expected 1 chunk live, got %d", got)
	}

	// Drain the rest.
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	got := append(one, rest...)
	want := bytes.Join(plaintexts, nil)
	if !bytes.Equal(got, want) {
		idx := firstMismatch(got, want)
		t.Fatalf("decrypted output mismatch at byte %d (got len %d, want len %d)", idx, len(got), len(want))
	}
	if got := atomic.LoadInt64(&openCount); got != int64(numChunks) {
		t.Errorf("expected exactly %d chunk opens after drain, got %d", numChunks, got)
	}
	if got := atomic.LoadInt64(&maxLive); got > 1 {
		t.Errorf("expected at most 1 chunk reader live at a time (lazy), saw peak of %d", got)
	}
	if got := atomic.LoadInt64(&liveCount); got != 0 {
		t.Errorf("expected all chunks closed after drain, %d still live", got)
	}
}

type liveTrackingReadCloser struct {
	io.Reader
	live *int64
	once bool
}

func (r *liveTrackingReadCloser) Close() error {
	if r.once {
		return nil
	}
	r.once = true
	atomic.AddInt64(r.live, -1)
	return nil
}
