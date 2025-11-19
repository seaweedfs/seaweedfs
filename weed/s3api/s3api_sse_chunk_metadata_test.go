package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestSSEKMSChunkMetadataAssignment tests that SSE-KMS creates per-chunk metadata
// with correct ChunkOffset values for each chunk (matching the fix in putToFiler)
func TestSSEKMSChunkMetadataAssignment(t *testing.T) {
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	// Generate SSE-KMS key by encrypting test data (this gives us a real SSEKMSKey)
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)
	testData := "Test data for SSE-KMS chunk metadata validation"
	encryptedReader, sseKMSKey, err := CreateSSEKMSEncryptedReader(bytes.NewReader([]byte(testData)), kmsKey.KeyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}
	// Read to complete encryption setup
	io.ReadAll(encryptedReader)

	// Serialize the base metadata (what putToFiler receives before chunking)
	baseMetadata, err := SerializeSSEKMSMetadata(sseKMSKey)
	if err != nil {
		t.Fatalf("Failed to serialize base SSE-KMS metadata: %v", err)
	}

	// Simulate multi-chunk upload scenario (what putToFiler does after UploadReaderInChunks)
	simulatedChunks := []*filer_pb.FileChunk{
		{FileId: "chunk1", Offset: 0, Size: 8 * 1024 * 1024},                // 8MB chunk at offset 0
		{FileId: "chunk2", Offset: 8 * 1024 * 1024, Size: 8 * 1024 * 1024},  // 8MB chunk at offset 8MB
		{FileId: "chunk3", Offset: 16 * 1024 * 1024, Size: 4 * 1024 * 1024}, // 4MB chunk at offset 16MB
	}

	// THIS IS THE CRITICAL FIX: Create per-chunk metadata (lines 421-443 in putToFiler)
	for _, chunk := range simulatedChunks {
		chunk.SseType = filer_pb.SSEType_SSE_KMS

		// Create a copy of the SSE-KMS key with chunk-specific offset
		chunkSSEKey := &SSEKMSKey{
			KeyID:             sseKMSKey.KeyID,
			EncryptedDataKey:  sseKMSKey.EncryptedDataKey,
			EncryptionContext: sseKMSKey.EncryptionContext,
			BucketKeyEnabled:  sseKMSKey.BucketKeyEnabled,
			IV:                sseKMSKey.IV,
			ChunkOffset:       chunk.Offset, // Set chunk-specific offset
		}

		// Serialize per-chunk metadata
		chunkMetadata, serErr := SerializeSSEKMSMetadata(chunkSSEKey)
		if serErr != nil {
			t.Fatalf("Failed to serialize SSE-KMS metadata for chunk at offset %d: %v", chunk.Offset, serErr)
		}
		chunk.SseMetadata = chunkMetadata
	}

	// VERIFICATION 1: Each chunk should have different metadata (due to different ChunkOffset)
	metadataSet := make(map[string]bool)
	for i, chunk := range simulatedChunks {
		metadataStr := string(chunk.SseMetadata)
		if metadataSet[metadataStr] {
			t.Errorf("Chunk %d has duplicate metadata (should be unique per chunk)", i)
		}
		metadataSet[metadataStr] = true

		// Deserialize and verify ChunkOffset
		var metadata SSEKMSMetadata
		if err := json.Unmarshal(chunk.SseMetadata, &metadata); err != nil {
			t.Fatalf("Failed to deserialize chunk %d metadata: %v", i, err)
		}

		expectedOffset := chunk.Offset
		if metadata.PartOffset != expectedOffset {
			t.Errorf("Chunk %d: expected PartOffset=%d, got %d", i, expectedOffset, metadata.PartOffset)
		}

		t.Logf("✓ Chunk %d: PartOffset=%d (correct)", i, metadata.PartOffset)
	}

	// VERIFICATION 2: Verify metadata can be deserialized and has correct ChunkOffset
	for i, chunk := range simulatedChunks {
		// Deserialize chunk metadata
		deserializedKey, err := DeserializeSSEKMSMetadata(chunk.SseMetadata)
		if err != nil {
			t.Fatalf("Failed to deserialize chunk %d metadata: %v", i, err)
		}

		// Verify the deserialized key has correct ChunkOffset
		if deserializedKey.ChunkOffset != chunk.Offset {
			t.Errorf("Chunk %d: deserialized ChunkOffset=%d, expected %d",
				i, deserializedKey.ChunkOffset, chunk.Offset)
		}

		// Verify IV is set (should be inherited from base)
		if len(deserializedKey.IV) != aes.BlockSize {
			t.Errorf("Chunk %d: invalid IV length: %d", i, len(deserializedKey.IV))
		}

		// Verify KeyID matches
		if deserializedKey.KeyID != sseKMSKey.KeyID {
			t.Errorf("Chunk %d: KeyID mismatch", i)
		}

		t.Logf("✓ Chunk %d: metadata deserialized successfully (ChunkOffset=%d, KeyID=%s)",
			i, deserializedKey.ChunkOffset, deserializedKey.KeyID)
	}

	// VERIFICATION 3: Ensure base metadata is NOT reused (the bug we're preventing)
	var baseMetadataStruct SSEKMSMetadata
	if err := json.Unmarshal(baseMetadata, &baseMetadataStruct); err != nil {
		t.Fatalf("Failed to deserialize base metadata: %v", err)
	}

	// Base metadata should have ChunkOffset=0
	if baseMetadataStruct.PartOffset != 0 {
		t.Errorf("Base metadata should have PartOffset=0, got %d", baseMetadataStruct.PartOffset)
	}

	// Chunks 2 and 3 should NOT have the same metadata as base (proving we're not reusing)
	for i := 1; i < len(simulatedChunks); i++ {
		if bytes.Equal(simulatedChunks[i].SseMetadata, baseMetadata) {
			t.Errorf("CRITICAL BUG: Chunk %d reuses base metadata (should have per-chunk metadata)", i)
		}
	}

	t.Log("✓ All chunks have unique per-chunk metadata (bug prevented)")
}

// TestSSES3ChunkMetadataAssignment tests that SSE-S3 creates per-chunk metadata
// with offset-adjusted IVs for each chunk (matching the fix in putToFiler)
func TestSSES3ChunkMetadataAssignment(t *testing.T) {
	// Initialize global SSE-S3 key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	rand.Read(keyManager.superKey)

	// Generate SSE-S3 key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	// Generate base IV
	baseIV := make([]byte, aes.BlockSize)
	rand.Read(baseIV)
	sseS3Key.IV = baseIV

	// Serialize base metadata (what putToFiler receives)
	baseMetadata, err := SerializeSSES3Metadata(sseS3Key)
	if err != nil {
		t.Fatalf("Failed to serialize base SSE-S3 metadata: %v", err)
	}

	// Simulate multi-chunk upload scenario (what putToFiler does after UploadReaderInChunks)
	simulatedChunks := []*filer_pb.FileChunk{
		{FileId: "chunk1", Offset: 0, Size: 8 * 1024 * 1024},                // 8MB chunk at offset 0
		{FileId: "chunk2", Offset: 8 * 1024 * 1024, Size: 8 * 1024 * 1024},  // 8MB chunk at offset 8MB
		{FileId: "chunk3", Offset: 16 * 1024 * 1024, Size: 4 * 1024 * 1024}, // 4MB chunk at offset 16MB
	}

	// THIS IS THE CRITICAL FIX: Create per-chunk metadata (lines 444-468 in putToFiler)
	for _, chunk := range simulatedChunks {
		chunk.SseType = filer_pb.SSEType_SSE_S3

		// Calculate chunk-specific IV using base IV and chunk offset
		chunkIV, _ := calculateIVWithOffset(sseS3Key.IV, chunk.Offset)

		// Create a copy of the SSE-S3 key with chunk-specific IV
		chunkSSEKey := &SSES3Key{
			Key:       sseS3Key.Key,
			KeyID:     sseS3Key.KeyID,
			Algorithm: sseS3Key.Algorithm,
			IV:        chunkIV, // Use chunk-specific IV
		}

		// Serialize per-chunk metadata
		chunkMetadata, serErr := SerializeSSES3Metadata(chunkSSEKey)
		if serErr != nil {
			t.Fatalf("Failed to serialize SSE-S3 metadata for chunk at offset %d: %v", chunk.Offset, serErr)
		}
		chunk.SseMetadata = chunkMetadata
	}

	// VERIFICATION 1: Each chunk should have different metadata (due to different IVs)
	metadataSet := make(map[string]bool)
	for i, chunk := range simulatedChunks {
		metadataStr := string(chunk.SseMetadata)
		if metadataSet[metadataStr] {
			t.Errorf("Chunk %d has duplicate metadata (should be unique per chunk)", i)
		}
		metadataSet[metadataStr] = true

		// Deserialize and verify IV
		deserializedKey, err := DeserializeSSES3Metadata(chunk.SseMetadata, keyManager)
		if err != nil {
			t.Fatalf("Failed to deserialize chunk %d metadata: %v", i, err)
		}

		// Calculate expected IV for this chunk
		expectedIV, _ := calculateIVWithOffset(baseIV, chunk.Offset)
		if !bytes.Equal(deserializedKey.IV, expectedIV) {
			t.Errorf("Chunk %d: IV mismatch\nExpected: %x\nGot: %x",
				i, expectedIV[:8], deserializedKey.IV[:8])
		}

		t.Logf("✓ Chunk %d: IV correctly adjusted for offset=%d", i, chunk.Offset)
	}

	// VERIFICATION 2: Verify decryption works with per-chunk IVs
	for i, chunk := range simulatedChunks {
		// Deserialize chunk metadata
		deserializedKey, err := DeserializeSSES3Metadata(chunk.SseMetadata, keyManager)
		if err != nil {
			t.Fatalf("Failed to deserialize chunk %d metadata: %v", i, err)
		}

		// Simulate encryption/decryption with the chunk's IV
		testData := []byte("Test data for SSE-S3 chunk decryption verification")
		block, err := aes.NewCipher(deserializedKey.Key)
		if err != nil {
			t.Fatalf("Failed to create cipher: %v", err)
		}

		// Encrypt with chunk's IV
		ciphertext := make([]byte, len(testData))
		stream := cipher.NewCTR(block, deserializedKey.IV)
		stream.XORKeyStream(ciphertext, testData)

		// Decrypt with chunk's IV
		plaintext := make([]byte, len(ciphertext))
		block2, _ := aes.NewCipher(deserializedKey.Key)
		stream2 := cipher.NewCTR(block2, deserializedKey.IV)
		stream2.XORKeyStream(plaintext, ciphertext)

		if !bytes.Equal(plaintext, testData) {
			t.Errorf("Chunk %d: decryption failed", i)
		}

		t.Logf("✓ Chunk %d: encryption/decryption successful with chunk-specific IV", i)
	}

	// VERIFICATION 3: Ensure base IV is NOT reused for non-zero offset chunks (the bug we're preventing)
	for i := 1; i < len(simulatedChunks); i++ {
		if bytes.Equal(simulatedChunks[i].SseMetadata, baseMetadata) {
			t.Errorf("CRITICAL BUG: Chunk %d reuses base metadata (should have per-chunk metadata)", i)
		}

		// Verify chunk metadata has different IV than base IV
		deserializedKey, _ := DeserializeSSES3Metadata(simulatedChunks[i].SseMetadata, keyManager)
		if bytes.Equal(deserializedKey.IV, baseIV) {
			t.Errorf("CRITICAL BUG: Chunk %d uses base IV (should use offset-adjusted IV)", i)
		}
	}

	t.Log("✓ All chunks have unique per-chunk IVs (bug prevented)")
}

// TestSSEChunkMetadataComparison tests that the bug (reusing same metadata for all chunks)
// would cause decryption failures, while the fix (per-chunk metadata) works correctly
func TestSSEChunkMetadataComparison(t *testing.T) {
	// Generate test key and IV
	key := make([]byte, 32)
	rand.Read(key)
	baseIV := make([]byte, aes.BlockSize)
	rand.Read(baseIV)

	// Create test data for 3 chunks
	chunk0Data := []byte("Chunk 0 data at offset 0")
	chunk1Data := []byte("Chunk 1 data at offset 8MB")
	chunk2Data := []byte("Chunk 2 data at offset 16MB")

	chunkOffsets := []int64{0, 8 * 1024 * 1024, 16 * 1024 * 1024}
	chunkDataList := [][]byte{chunk0Data, chunk1Data, chunk2Data}

	// Scenario 1: BUG - Using same IV for all chunks (what the old code did)
	t.Run("Bug: Reusing base IV causes decryption failures", func(t *testing.T) {
		var encryptedChunks [][]byte

		// Encrypt each chunk with offset-adjusted IV (what encryption does)
		for i, offset := range chunkOffsets {
			adjustedIV, _ := calculateIVWithOffset(baseIV, offset)
			block, _ := aes.NewCipher(key)
			stream := cipher.NewCTR(block, adjustedIV)

			ciphertext := make([]byte, len(chunkDataList[i]))
			stream.XORKeyStream(ciphertext, chunkDataList[i])
			encryptedChunks = append(encryptedChunks, ciphertext)
		}

		// Try to decrypt with base IV (THE BUG)
		for i := range encryptedChunks {
			block, _ := aes.NewCipher(key)
			stream := cipher.NewCTR(block, baseIV) // BUG: Always using base IV

			plaintext := make([]byte, len(encryptedChunks[i]))
			stream.XORKeyStream(plaintext, encryptedChunks[i])

			if i == 0 {
				// Chunk 0 should work (offset 0 means base IV = adjusted IV)
				if !bytes.Equal(plaintext, chunkDataList[i]) {
					t.Errorf("Chunk 0 decryption failed (unexpected)")
				}
			} else {
				// Chunks 1 and 2 should FAIL (wrong IV)
				if bytes.Equal(plaintext, chunkDataList[i]) {
					t.Errorf("BUG NOT REPRODUCED: Chunk %d decrypted correctly with base IV (should fail)", i)
				} else {
					t.Logf("✓ Chunk %d: Correctly failed to decrypt with base IV (bug reproduced)", i)
				}
			}
		}
	})

	// Scenario 2: FIX - Using per-chunk offset-adjusted IVs (what the new code does)
	t.Run("Fix: Per-chunk IVs enable correct decryption", func(t *testing.T) {
		var encryptedChunks [][]byte
		var chunkIVs [][]byte

		// Encrypt each chunk with offset-adjusted IV
		for i, offset := range chunkOffsets {
			adjustedIV, _ := calculateIVWithOffset(baseIV, offset)
			chunkIVs = append(chunkIVs, adjustedIV)

			block, _ := aes.NewCipher(key)
			stream := cipher.NewCTR(block, adjustedIV)

			ciphertext := make([]byte, len(chunkDataList[i]))
			stream.XORKeyStream(ciphertext, chunkDataList[i])
			encryptedChunks = append(encryptedChunks, ciphertext)
		}

		// Decrypt with per-chunk IVs (THE FIX)
		for i := range encryptedChunks {
			block, _ := aes.NewCipher(key)
			stream := cipher.NewCTR(block, chunkIVs[i]) // FIX: Using per-chunk IV

			plaintext := make([]byte, len(encryptedChunks[i]))
			stream.XORKeyStream(plaintext, encryptedChunks[i])

			if !bytes.Equal(plaintext, chunkDataList[i]) {
				t.Errorf("Chunk %d decryption failed with per-chunk IV (unexpected)", i)
			} else {
				t.Logf("✓ Chunk %d: Successfully decrypted with per-chunk IV", i)
			}
		}
	})
}
