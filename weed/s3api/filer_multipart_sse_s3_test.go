package s3api

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestCompletedMultipartChunkBackfillsSSES3MetadataFromUploadEntry(t *testing.T) {
	keyManager := initSSES3KeyManagerForTest(t)

	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	keyData, err := SerializeSSES3Metadata(key)
	if err != nil {
		t.Fatalf("SerializeSSES3Metadata: %v", err)
	}

	baseIV := []byte("1234567890abcdef")
	uploadEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Encryption: []byte(s3_constants.SSEAlgorithmAES256),
			s3_constants.SeaweedFSSSES3BaseIV:     []byte(base64.StdEncoding.EncodeToString(baseIV)),
			s3_constants.SeaweedFSSSES3KeyData:    []byte(base64.StdEncoding.EncodeToString(keyData)),
		},
	}

	sses3Info, err := extractMultipartSSES3Info(uploadEntry)
	if err != nil {
		t.Fatalf("extractMultipartSSES3Info: %v", err)
	}

	partLocalOffset := int64(8 * 1024 * 1024)
	finalObjectOffset := int64(40 * 1024 * 1024)
	chunk := &filer_pb.FileChunk{
		FileId: "1,abc",
		Offset: partLocalOffset,
		Size:   1024,
	}

	finalChunk, err := completedMultipartChunk(chunk, finalObjectOffset, sses3Info)
	if err != nil {
		t.Fatalf("completedMultipartChunk: %v", err)
	}
	if finalChunk.GetOffset() != finalObjectOffset {
		t.Fatalf("final chunk offset = %d, want %d", finalChunk.GetOffset(), finalObjectOffset)
	}
	if finalChunk.GetSseType() != filer_pb.SSEType_SSE_S3 {
		t.Fatalf("final chunk SSE type = %s, want SSE_S3", finalChunk.GetSseType())
	}
	if len(finalChunk.GetSseMetadata()) == 0 {
		t.Fatal("expected final chunk SSE metadata")
	}

	chunkKey, err := DeserializeSSES3Metadata(finalChunk.GetSseMetadata(), keyManager)
	if err != nil {
		t.Fatalf("DeserializeSSES3Metadata: %v", err)
	}
	if !bytes.Equal(chunkKey.Key, key.Key) {
		t.Fatal("chunk metadata key does not match upload key")
	}
	expectedIV, _ := calculateIVWithOffset(baseIV, partLocalOffset)
	if !bytes.Equal(chunkKey.IV, expectedIV) {
		t.Fatalf("chunk metadata IV = %x, want %x", chunkKey.IV, expectedIV)
	}
}

func TestCompletedMultipartChunkPreservesExistingSSES3Metadata(t *testing.T) {
	existingMetadata := []byte("already-present")
	chunk := &filer_pb.FileChunk{
		FileId:      "2,abc",
		Offset:      1024,
		Size:        2048,
		SseType:     filer_pb.SSEType_SSE_S3,
		SseMetadata: existingMetadata,
	}

	finalChunk, err := completedMultipartChunk(chunk, 4096, &multipartSSES3Info{})
	if err != nil {
		t.Fatalf("completedMultipartChunk: %v", err)
	}
	if !bytes.Equal(finalChunk.GetSseMetadata(), existingMetadata) {
		t.Fatal("existing SSE-S3 metadata should be preserved")
	}
}

// TestApplyMultipartSSES3HeadersFromUploadEntry pins the canonical object-level
// SSE-S3 attributes onto the completed entry: SeaweedFSSSES3Key (used by
// IsSSES3EncryptedInternal and the read-side decryption-key lookup) and
// X-Amz-Server-Side-Encryption (used by detectPrimarySSEType for non-chunked
// reads). Pre-existing values must not be clobbered.
func TestApplyMultipartSSES3HeadersFromUploadEntry(t *testing.T) {
	initSSES3KeyManagerForTest(t)

	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	keyData, err := SerializeSSES3Metadata(key)
	if err != nil {
		t.Fatalf("SerializeSSES3Metadata: %v", err)
	}

	baseIV := []byte("1234567890abcdef")
	uploadEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Encryption: []byte(s3_constants.SSEAlgorithmAES256),
			s3_constants.SeaweedFSSSES3BaseIV:     []byte(base64.StdEncoding.EncodeToString(baseIV)),
			s3_constants.SeaweedFSSSES3KeyData:    []byte(base64.StdEncoding.EncodeToString(keyData)),
		},
	}
	sses3Info, err := extractMultipartSSES3Info(uploadEntry)
	if err != nil {
		t.Fatalf("extractMultipartSSES3Info: %v", err)
	}

	t.Run("backfills missing canonical attributes", func(t *testing.T) {
		finalEntry := &filer_pb.Entry{Extended: map[string][]byte{}}
		applyMultipartSSES3HeadersFromUploadEntry(finalEntry, sses3Info)
		if !bytes.Equal(finalEntry.Extended[s3_constants.SeaweedFSSSES3Key], keyData) {
			t.Fatal("final entry did not receive object-level SSE-S3 key metadata")
		}
		if got := string(finalEntry.Extended[s3_constants.AmzServerSideEncryption]); got != s3_constants.SSEAlgorithmAES256 {
			t.Fatalf("final entry SSE header = %q, want %q", got, s3_constants.SSEAlgorithmAES256)
		}
	})

	t.Run("does not clobber existing canonical attributes", func(t *testing.T) {
		existingKey := []byte("existing-key-bytes")
		existingHeader := []byte("aws:kms")
		finalEntry := &filer_pb.Entry{Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Key:      existingKey,
			s3_constants.AmzServerSideEncryption: existingHeader,
		}}
		applyMultipartSSES3HeadersFromUploadEntry(finalEntry, sses3Info)
		if !bytes.Equal(finalEntry.Extended[s3_constants.SeaweedFSSSES3Key], existingKey) {
			t.Fatal("existing SSE-S3 key was overwritten")
		}
		if !bytes.Equal(finalEntry.Extended[s3_constants.AmzServerSideEncryption], existingHeader) {
			t.Fatal("existing X-Amz-Server-Side-Encryption was overwritten")
		}
	})

	t.Run("nil info is a no-op", func(t *testing.T) {
		finalEntry := &filer_pb.Entry{Extended: map[string][]byte{}}
		applyMultipartSSES3HeadersFromUploadEntry(finalEntry, nil)
		if len(finalEntry.Extended) != 0 {
			t.Fatalf("nil sses3Info should not write any keys, got %d", len(finalEntry.Extended))
		}
	})
}

// TestCompletedMultipartChunkBackfilledIVDecryptsActualCiphertext is a round
// trip across the encryption boundary: it encrypts simulated multipart parts
// using the same call path putToFiler uses (CreateSSES3EncryptedReaderWithBaseIV
// with partOffset=0, then chunked at fixed boundaries), throws away the chunk
// level SSE metadata to simulate the bug from #8908, runs completedMultipartChunk
// to backfill it, and decrypts each chunk's slice of ciphertext with the
// backfilled IV. The plaintext must come back intact.
//
// Regression coverage for the gemini review on #9224 which suggested using
// (partNumber-1)*PartOffsetMultiplier + chunk.Offset for the backfill IV. That
// formula does not match what the encryption side actually does (PartOffsetMultiplier
// is defined but not used in the write path), so adopting it would silently
// produce backfilled IVs that fail to decrypt the bytes on disk.
func TestCompletedMultipartChunkBackfilledIVDecryptsActualCiphertext(t *testing.T) {
	keyManager := initSSES3KeyManagerForTest(t)

	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	keyData, err := SerializeSSES3Metadata(key)
	if err != nil {
		t.Fatalf("SerializeSSES3Metadata: %v", err)
	}

	baseIV := make([]byte, s3_constants.AESBlockSize)
	if _, err := rand.Read(baseIV); err != nil {
		t.Fatalf("rand.Read baseIV: %v", err)
	}

	uploadEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Encryption: []byte(s3_constants.SSEAlgorithmAES256),
			s3_constants.SeaweedFSSSES3BaseIV:     []byte(base64.StdEncoding.EncodeToString(baseIV)),
			s3_constants.SeaweedFSSSES3KeyData:    []byte(base64.StdEncoding.EncodeToString(keyData)),
		},
	}
	sses3Info, err := extractMultipartSSES3Info(uploadEntry)
	if err != nil {
		t.Fatalf("extractMultipartSSES3Info: %v", err)
	}

	const chunkSize = int64(8 * 1024 * 1024)

	// Two parts: part 1 spans three internal chunks (offsets 0, 8MB, 16MB),
	// part 2 spans two (offsets 0, 8MB). Both parts are encrypted independently
	// with partOffset=0, the way putToFiler invokes the encryption path today.
	parts := []struct {
		name      string
		plaintext []byte
	}{
		{"part1", makeRandomPlaintext(t, int(chunkSize)*3-1234)},
		{"part2", makeRandomPlaintext(t, int(chunkSize)*2+5678)},
	}

	for _, p := range parts {
		t.Run(p.name, func(t *testing.T) {
			// Encrypt the part stream the same way handleSSES3MultipartEncryption does.
			encReader, _, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(p.plaintext), key, baseIV, 0)
			if err != nil {
				t.Fatalf("CreateSSES3EncryptedReaderWithBaseIV: %v", err)
			}
			ciphertext, err := io.ReadAll(encReader)
			if err != nil {
				t.Fatalf("read encrypted stream: %v", err)
			}
			if len(ciphertext) != len(p.plaintext) {
				t.Fatalf("ciphertext length %d != plaintext length %d", len(ciphertext), len(p.plaintext))
			}

			// Slice the encrypted stream into chunks the same way UploadReaderInChunks
			// would, and build FileChunks tagged SseType=NONE to simulate the bug.
			var bugChunks []*filer_pb.FileChunk
			for off := int64(0); off < int64(len(ciphertext)); off += chunkSize {
				end := off + chunkSize
				if end > int64(len(ciphertext)) {
					end = int64(len(ciphertext))
				}
				bugChunks = append(bugChunks, &filer_pb.FileChunk{
					FileId: "1,deadbeef",
					Offset: off,
					Size:   uint64(end - off),
					// SseType deliberately left at default NONE.
				})
			}

			// finalOffset doesn't influence the IV calculation, only the chunk's
			// position in the assembled object. Use a fake non-zero value to
			// confirm the function does not accidentally use it for the IV.
			const finalOffset int64 = 1 << 40

			for _, bugChunk := range bugChunks {
				finalChunk, err := completedMultipartChunk(bugChunk, finalOffset+bugChunk.Offset, sses3Info)
				if err != nil {
					t.Fatalf("completedMultipartChunk(offset=%d): %v", bugChunk.Offset, err)
				}
				if finalChunk.GetSseType() != filer_pb.SSEType_SSE_S3 {
					t.Fatalf("chunk@%d not retagged SSE_S3", bugChunk.Offset)
				}
				meta, err := DeserializeSSES3Metadata(finalChunk.GetSseMetadata(), keyManager)
				if err != nil {
					t.Fatalf("DeserializeSSES3Metadata(chunk@%d): %v", bugChunk.Offset, err)
				}

				cipherSlice := ciphertext[bugChunk.Offset : bugChunk.Offset+int64(bugChunk.Size)]
				wantPlain := p.plaintext[bugChunk.Offset : bugChunk.Offset+int64(bugChunk.Size)]

				decReader, err := CreateSSES3DecryptedReader(bytes.NewReader(cipherSlice), key, meta.IV)
				if err != nil {
					t.Fatalf("CreateSSES3DecryptedReader(chunk@%d): %v", bugChunk.Offset, err)
				}
				gotPlain, err := io.ReadAll(decReader)
				if err != nil {
					t.Fatalf("read decrypted chunk@%d: %v", bugChunk.Offset, err)
				}
				if !bytes.Equal(gotPlain, wantPlain) {
					t.Fatalf("chunk@%d: backfilled IV produced wrong plaintext (first mismatch byte at %d)",
						bugChunk.Offset, firstMismatch(gotPlain, wantPlain))
				}
			}
		})
	}
}

// TestCompletedMultipartChunkRejectsPartNumberMultiplierFormula is a guard
// against re-introducing the gemini review's suggested formula, which would
// produce IVs incompatible with the encryption side. The test simulates what
// would happen if the backfill used (partNumber-1)*PartOffsetMultiplier as an
// extra offset term: the resulting IV does not decrypt the actual ciphertext.
func TestCompletedMultipartChunkRejectsPartNumberMultiplierFormula(t *testing.T) {
	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("GenerateSSES3Key: %v", err)
	}
	baseIV := make([]byte, s3_constants.AESBlockSize)
	if _, err := rand.Read(baseIV); err != nil {
		t.Fatalf("rand.Read baseIV: %v", err)
	}

	plaintext := makeRandomPlaintext(t, 4096)

	encReader, _, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(plaintext), key, baseIV, 0)
	if err != nil {
		t.Fatalf("CreateSSES3EncryptedReaderWithBaseIV: %v", err)
	}
	ciphertext, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("read encrypted stream: %v", err)
	}

	// "partNumber=2" with PartOffsetMultiplier added would shift the IV by 8GB.
	wrongIV, _ := calculateIVWithOffset(baseIV, s3_constants.PartOffsetMultiplier+0)

	decReader, err := CreateSSES3DecryptedReader(bytes.NewReader(ciphertext), key, wrongIV)
	if err != nil {
		t.Fatalf("CreateSSES3DecryptedReader: %v", err)
	}
	got, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("read decrypted: %v", err)
	}
	if bytes.Equal(got, plaintext) {
		t.Fatal("partNumber-multiplier IV decrypted plaintext correctly; the backfill formula must NOT include this term")
	}
}

func makeRandomPlaintext(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func firstMismatch(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
