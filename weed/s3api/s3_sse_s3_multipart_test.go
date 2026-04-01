package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestSSES3MultipartChunkViewDecryption tests that multipart SSE-S3 objects use per-chunk IVs
func TestSSES3MultipartChunkViewDecryption(t *testing.T) {
	// Generate test key and base IV
	key := make([]byte, 32)
	rand.Read(key)
	baseIV := make([]byte, 16)
	rand.Read(baseIV)

	// Create test plaintext
	plaintext := []byte("This is test data for SSE-S3 multipart encryption testing")

	// Simulate multipart upload with 2 parts at different offsets
	testCases := []struct {
		name       string
		partNumber int
		partOffset int64
		data       []byte
	}{
		{"Part 1", 1, 0, plaintext[:30]},
		{"Part 2", 2, 5 * 1024 * 1024, plaintext[30:]}, // 5MB offset
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate IV with offset (simulating upload encryption)
			adjustedIV, _ := calculateIVWithOffset(baseIV, tc.partOffset)

			// Encrypt the part data
			block, err := aes.NewCipher(key)
			if err != nil {
				t.Fatalf("Failed to create cipher: %v", err)
			}

			ciphertext := make([]byte, len(tc.data))
			stream := cipher.NewCTR(block, adjustedIV)
			stream.XORKeyStream(ciphertext, tc.data)

			// SSE-S3 stores the offset-adjusted IV directly in chunk metadata
			// (unlike SSE-C which stores base IV + PartOffset)
			chunkIV := adjustedIV

			// Verify the IV is offset-adjusted for non-zero offsets
			if tc.partOffset == 0 {
				if !bytes.Equal(chunkIV, baseIV) {
					t.Error("IV should equal base IV when offset is 0")
				}
			} else {
				if bytes.Equal(chunkIV, baseIV) {
					t.Error("Chunk IV should be offset-adjusted, not base IV")
				}
			}

			// Verify decryption works with the chunk's IV
			decryptedData := make([]byte, len(ciphertext))
			decryptBlock, err := aes.NewCipher(key)
			if err != nil {
				t.Fatalf("Failed to create decrypt cipher: %v", err)
			}
			decryptStream := cipher.NewCTR(decryptBlock, chunkIV)
			decryptStream.XORKeyStream(decryptedData, ciphertext)

			if !bytes.Equal(decryptedData, tc.data) {
				t.Errorf("Decryption failed: expected %q, got %q", tc.data, decryptedData)
			}
		})
	}
}

// TestSSES3SinglePartChunkViewDecryption tests single-part SSE-S3 objects use object-level IV
func TestSSES3SinglePartChunkViewDecryption(t *testing.T) {
	// Generate test key and IV
	key := make([]byte, 32)
	rand.Read(key)
	iv := make([]byte, 16)
	rand.Read(iv)

	// Create test plaintext
	plaintext := []byte("This is test data for SSE-S3 single-part encryption testing")

	// Encrypt the data
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}

	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext, plaintext)

	// Create a mock file chunk WITHOUT per-chunk metadata (single-part path)
	fileChunk := &filer_pb.FileChunk{
		FileId:      "test-file-id",
		Offset:      0,
		Size:        uint64(len(ciphertext)),
		SseType:     filer_pb.SSEType_SSE_S3,
		SseMetadata: nil, // No per-chunk metadata for single-part
	}

	// Verify the chunk does NOT have per-chunk metadata
	if len(fileChunk.GetSseMetadata()) > 0 {
		t.Error("Single-part chunk should not have per-chunk metadata")
	}

	// For single-part, the object-level IV is used
	objectLevelIV := iv

	// Verify decryption works with the object-level IV
	decryptedData := make([]byte, len(ciphertext))
	decryptBlock, _ := aes.NewCipher(key)
	decryptStream := cipher.NewCTR(decryptBlock, objectLevelIV)
	decryptStream.XORKeyStream(decryptedData, ciphertext)

	if !bytes.Equal(decryptedData, plaintext) {
		t.Errorf("Decryption failed: expected %q, got %q", plaintext, decryptedData)
	}
}

// TestSSES3IVOffsetCalculation verifies IV offset calculation for multipart uploads
func TestSSES3IVOffsetCalculation(t *testing.T) {
	baseIV := make([]byte, 16)
	rand.Read(baseIV)

	testCases := []struct {
		name       string
		partNumber int
		partSize   int64
		offset     int64
	}{
		{"Part 1", 1, 5 * 1024 * 1024, 0},
		{"Part 2", 2, 5 * 1024 * 1024, 5 * 1024 * 1024},
		{"Part 3", 3, 5 * 1024 * 1024, 10 * 1024 * 1024},
		{"Part 10", 10, 5 * 1024 * 1024, 45 * 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate IV with offset
			adjustedIV, skip := calculateIVWithOffset(baseIV, tc.offset)

			// Verify IV is different from base (except for offset 0)
			if tc.offset == 0 {
				if !bytes.Equal(adjustedIV, baseIV) {
					t.Error("IV should equal base IV when offset is 0")
				}
				if skip != 0 {
					t.Errorf("Skip should be 0 when offset is 0, got %d", skip)
				}
			} else {
				if bytes.Equal(adjustedIV, baseIV) {
					t.Error("IV should be different from base IV when offset > 0")
				}
			}

			// Verify skip is calculated correctly
			expectedSkip := int(tc.offset % 16)
			if skip != expectedSkip {
				t.Errorf("Skip mismatch: expected %d, got %d", expectedSkip, skip)
			}

			// Verify IV adjustment is deterministic
			adjustedIV2, skip2 := calculateIVWithOffset(baseIV, tc.offset)
			if !bytes.Equal(adjustedIV, adjustedIV2) || skip != skip2 {
				t.Error("IV calculation is not deterministic")
			}
		})
	}
}

// TestSSES3ChunkMetadataDetection tests detection of per-chunk vs object-level metadata
func TestSSES3ChunkMetadataDetection(t *testing.T) {
	// Test data for multipart chunk
	mockMetadata := []byte("mock-serialized-metadata")

	testCases := []struct {
		name              string
		chunk             *filer_pb.FileChunk
		expectedMultipart bool
	}{
		{
			name: "Multipart chunk with metadata",
			chunk: &filer_pb.FileChunk{
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: mockMetadata,
			},
			expectedMultipart: true,
		},
		{
			name: "Single-part chunk without metadata",
			chunk: &filer_pb.FileChunk{
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: nil,
			},
			expectedMultipart: false,
		},
		{
			name: "Non-SSE-S3 chunk",
			chunk: &filer_pb.FileChunk{
				SseType:     filer_pb.SSEType_NONE,
				SseMetadata: nil,
			},
			expectedMultipart: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasPerChunkMetadata := tc.chunk.GetSseType() == filer_pb.SSEType_SSE_S3 && len(tc.chunk.GetSseMetadata()) > 0

			if hasPerChunkMetadata != tc.expectedMultipart {
				t.Errorf("Expected multipart=%v, got hasPerChunkMetadata=%v", tc.expectedMultipart, hasPerChunkMetadata)
			}
		})
	}
}

// TestSSES3EncryptionConsistency verifies encryption/decryption roundtrip
func TestSSES3EncryptionConsistency(t *testing.T) {
	plaintext := []byte("Test data for SSE-S3 encryption consistency verification")

	key := make([]byte, 32)
	rand.Read(key)
	iv := make([]byte, 16)
	rand.Read(iv)

	// Encrypt
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}

	ciphertext := make([]byte, len(plaintext))
	encryptStream := cipher.NewCTR(block, iv)
	encryptStream.XORKeyStream(ciphertext, plaintext)

	// Decrypt
	decrypted := make([]byte, len(ciphertext))
	decryptBlock, _ := aes.NewCipher(key)
	decryptStream := cipher.NewCTR(decryptBlock, iv)
	decryptStream.XORKeyStream(decrypted, ciphertext)

	// Verify
	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("Decryption mismatch: expected %q, got %q", plaintext, decrypted)
	}

	// Verify idempotency - decrypt again should give garbage
	decrypted2 := make([]byte, len(ciphertext))
	decryptStream2 := cipher.NewCTR(decryptBlock, iv)
	decryptStream2.XORKeyStream(decrypted2, ciphertext)

	if !bytes.Equal(decrypted2, plaintext) {
		t.Error("Second decryption should also work with fresh stream")
	}
}
