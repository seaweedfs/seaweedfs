package s3api

import (
	"bytes"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// NOTE: These are integration tests that test the end-to-end encryption/decryption flow.
// Full HTTP handler tests (PUT -> GET) would require a complete mock server with filer,
// which is complex to set up. These tests focus on the critical decrypt path.

// TestSSES3EndToEndSmallFile tests the complete encryption->storage->decryption cycle for small inline files
// This test would have caught the IV retrieval bug for inline files
func TestSSES3EndToEndSmallFile(t *testing.T) {
	// Initialize global SSE-S3 key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	// Set up the key manager with a super key for testing
	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i)
	}

	testCases := []struct {
		name string
		data []byte
	}{
		{"tiny file (10 bytes)", []byte("test12345")},
		{"small file (50 bytes)", []byte("This is a small test file for SSE-S3 encryption")},
		{"medium file (256 bytes)", bytes.Repeat([]byte("a"), 256)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: Encrypt (simulates what happens during PUT)
			sseS3Key, err := GenerateSSES3Key()
			if err != nil {
				t.Fatalf("Failed to generate SSE-S3 key: %v", err)
			}

			encryptedReader, iv, err := CreateSSES3EncryptedReader(bytes.NewReader(tc.data), sseS3Key)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader: %v", err)
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted data: %v", err)
			}

			// Store IV in the key (this is critical for inline files!)
			sseS3Key.IV = iv

			// Serialize the metadata (this is stored in entry.Extended)
			serializedMetadata, err := SerializeSSES3Metadata(sseS3Key)
			if err != nil {
				t.Fatalf("Failed to serialize SSE-S3 metadata: %v", err)
			}

			// Step 2: Simulate storage (inline file - no chunks)
			// For inline files, data is in Content, metadata in Extended
			mockEntry := &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSES3Key:       serializedMetadata,
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Content: encryptedData,
				Chunks:  []*filer_pb.FileChunk{}, // Critical: inline files have NO chunks
			}

			// Step 3: Decrypt (simulates what happens during GET)
			// This tests the IV retrieval path for inline files

			// First, deserialize metadata from storage
			retrievedKeyData := mockEntry.Extended[s3_constants.SeaweedFSSSES3Key]
			retrievedKey, err := DeserializeSSES3Metadata(retrievedKeyData, keyManager)
			if err != nil {
				t.Fatalf("Failed to deserialize SSE-S3 metadata: %v", err)
			}

			// CRITICAL TEST: For inline files, IV must be in object-level metadata
			var retrievedIV []byte
			if len(retrievedKey.IV) > 0 {
				// Success path: IV found in object-level key
				retrievedIV = retrievedKey.IV
			} else if len(mockEntry.GetChunks()) > 0 {
				// Fallback path: would check chunks (but inline files have no chunks)
				t.Fatal("Inline file should have IV in object-level metadata, not chunks")
			}

			if len(retrievedIV) == 0 {
				// THIS IS THE BUG WE FIXED: inline files had no way to get IV!
				t.Fatal("Failed to retrieve IV for inline file - this is the bug we fixed!")
			}

			// Now decrypt with the retrieved IV
			decryptedReader, err := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData), retrievedKey, retrievedIV)
			if err != nil {
				t.Fatalf("Failed to create decrypted reader: %v", err)
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data: %v", err)
			}

			// Verify decrypted data matches original
			if !bytes.Equal(decryptedData, tc.data) {
				t.Errorf("Decrypted data doesn't match original.\nExpected: %q\nGot: %q", tc.data, decryptedData)
			}
		})
	}
}

// TestSSES3EndToEndChunkedFile tests the complete flow for chunked files
func TestSSES3EndToEndChunkedFile(t *testing.T) {
	// Initialize global SSE-S3 key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i)
	}

	// Generate SSE-S3 key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	// Create test data for two chunks
	chunk1Data := []byte("This is chunk 1 data for SSE-S3 encryption test")
	chunk2Data := []byte("This is chunk 2 data for SSE-S3 encryption test")

	// Encrypt chunk 1
	encryptedReader1, iv1, err := CreateSSES3EncryptedReader(bytes.NewReader(chunk1Data), sseS3Key)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader for chunk 1: %v", err)
	}
	encryptedChunk1, _ := io.ReadAll(encryptedReader1)

	// Encrypt chunk 2
	encryptedReader2, iv2, err := CreateSSES3EncryptedReader(bytes.NewReader(chunk2Data), sseS3Key)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader for chunk 2: %v", err)
	}
	encryptedChunk2, _ := io.ReadAll(encryptedReader2)

	// Create metadata for each chunk
	chunk1Key := &SSES3Key{
		Key:       sseS3Key.Key,
		IV:        iv1,
		Algorithm: sseS3Key.Algorithm,
		KeyID:     sseS3Key.KeyID,
	}
	chunk2Key := &SSES3Key{
		Key:       sseS3Key.Key,
		IV:        iv2,
		Algorithm: sseS3Key.Algorithm,
		KeyID:     sseS3Key.KeyID,
	}

	serializedChunk1Meta, _ := SerializeSSES3Metadata(chunk1Key)
	serializedChunk2Meta, _ := SerializeSSES3Metadata(chunk2Key)
	serializedObjMeta, _ := SerializeSSES3Metadata(sseS3Key)

	// Create mock entry with chunks
	mockEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Key:       serializedObjMeta,
			s3_constants.AmzServerSideEncryption: []byte("AES256"),
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId:      "chunk1,123",
				Offset:      0,
				Size:        uint64(len(encryptedChunk1)),
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: serializedChunk1Meta,
			},
			{
				FileId:      "chunk2,456",
				Offset:      int64(len(chunk1Data)),
				Size:        uint64(len(encryptedChunk2)),
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: serializedChunk2Meta,
			},
		},
	}

	// Verify multipart detection
	sses3Chunks := 0
	for _, chunk := range mockEntry.GetChunks() {
		if chunk.GetSseType() == filer_pb.SSEType_SSE_S3 && len(chunk.GetSseMetadata()) > 0 {
			sses3Chunks++
		}
	}

	isMultipart := sses3Chunks > 1
	if !isMultipart {
		t.Error("Expected multipart SSE-S3 object detection")
	}

	if sses3Chunks != 2 {
		t.Errorf("Expected 2 SSE-S3 chunks, got %d", sses3Chunks)
	}

	// Verify each chunk has valid metadata with IV
	for i, chunk := range mockEntry.GetChunks() {
		deserializedKey, err := DeserializeSSES3Metadata(chunk.GetSseMetadata(), keyManager)
		if err != nil {
			t.Errorf("Failed to deserialize chunk %d metadata: %v", i, err)
		}
		if len(deserializedKey.IV) == 0 {
			t.Errorf("Chunk %d has no IV", i)
		}

		// Decrypt this chunk to verify it works
		var chunkData []byte
		if i == 0 {
			chunkData = encryptedChunk1
		} else {
			chunkData = encryptedChunk2
		}

		decryptedReader, err := CreateSSES3DecryptedReader(bytes.NewReader(chunkData), deserializedKey, deserializedKey.IV)
		if err != nil {
			t.Errorf("Failed to decrypt chunk %d: %v", i, err)
			continue
		}

		decrypted, _ := io.ReadAll(decryptedReader)
		var expectedData []byte
		if i == 0 {
			expectedData = chunk1Data
		} else {
			expectedData = chunk2Data
		}

		if !bytes.Equal(decrypted, expectedData) {
			t.Errorf("Chunk %d decryption failed", i)
		}
	}
}

// TestSSES3EndToEndWithDetectPrimaryType tests that type detection works correctly for different scenarios
func TestSSES3EndToEndWithDetectPrimaryType(t *testing.T) {
	s3a := &S3ApiServer{}

	testCases := []struct {
		name          string
		entry         *filer_pb.Entry
		expectedType  string
		shouldBeSSES3 bool
	}{
		{
			name: "Inline SSE-S3 file (no chunks)",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Content:    []byte("encrypted data"),
				Chunks:     []*filer_pb.FileChunk{},
			},
			expectedType:  s3_constants.SSETypeS3,
			shouldBeSSES3: true,
		},
		{
			name: "Single chunk SSE-S3 file",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId:      "1,123",
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata"),
					},
				},
			},
			expectedType:  s3_constants.SSETypeS3,
			shouldBeSSES3: true,
		},
		{
			name: "SSE-KMS file (has KMS key ID)",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption:            []byte("AES256"),
					s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("kms-key-123"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks:     []*filer_pb.FileChunk{},
			},
			expectedType:  s3_constants.SSETypeKMS,
			shouldBeSSES3: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			detectedType := s3a.detectPrimarySSEType(tc.entry)
			if detectedType != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, detectedType)
			}
			if (detectedType == s3_constants.SSETypeS3) != tc.shouldBeSSES3 {
				t.Errorf("SSE-S3 detection mismatch: expected %v, got %v", tc.shouldBeSSES3, detectedType == s3_constants.SSETypeS3)
			}
		})
	}
}
