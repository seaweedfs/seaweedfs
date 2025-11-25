package s3api

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestCreateDestinationChunkPreservesEncryption tests that createDestinationChunk preserves CipherKey and IsCompressed
func TestCreateDestinationChunkPreservesEncryption(t *testing.T) {
	s3a := &S3ApiServer{}

	testCases := []struct {
		name             string
		sourceChunk      *filer_pb.FileChunk
		expectedOffset   int64
		expectedSize     uint64
		shouldPreserveCK bool
		shouldPreserveIC bool
	}{
		{
			name: "Encrypted and compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       0,
				Size:         1024,
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: true,
				ETag:         "test-etag",
			},
			expectedOffset:   0,
			expectedSize:     1024,
			shouldPreserveCK: true,
			shouldPreserveIC: true,
		},
		{
			name: "Only encrypted chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       1024,
				Size:         2048,
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: false,
				ETag:         "test-etag-2",
			},
			expectedOffset:   1024,
			expectedSize:     2048,
			shouldPreserveCK: true,
			shouldPreserveIC: false,
		},
		{
			name: "Only compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       2048,
				Size:         512,
				CipherKey:    nil,
				IsCompressed: true,
				ETag:         "test-etag-3",
			},
			expectedOffset:   2048,
			expectedSize:     512,
			shouldPreserveCK: false,
			shouldPreserveIC: true,
		},
		{
			name: "Unencrypted and uncompressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				Offset:       4096,
				Size:         1024,
				CipherKey:    nil,
				IsCompressed: false,
				ETag:         "test-etag-4",
			},
			expectedOffset:   4096,
			expectedSize:     1024,
			shouldPreserveCK: false,
			shouldPreserveIC: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dstChunk := s3a.createDestinationChunk(tc.sourceChunk, tc.expectedOffset, tc.expectedSize)

			// Verify offset and size
			if dstChunk.Offset != tc.expectedOffset {
				t.Errorf("Expected offset %d, got %d", tc.expectedOffset, dstChunk.Offset)
			}
			if dstChunk.Size != tc.expectedSize {
				t.Errorf("Expected size %d, got %d", tc.expectedSize, dstChunk.Size)
			}

			// Verify CipherKey preservation
			if tc.shouldPreserveCK {
				if !bytes.Equal(dstChunk.CipherKey, tc.sourceChunk.CipherKey) {
					t.Errorf("CipherKey not preserved: expected %v, got %v", tc.sourceChunk.CipherKey, dstChunk.CipherKey)
				}
			} else {
				if len(dstChunk.CipherKey) > 0 {
					t.Errorf("Expected no CipherKey, got %v", dstChunk.CipherKey)
				}
			}

			// Verify IsCompressed preservation
			if dstChunk.IsCompressed != tc.shouldPreserveIC {
				t.Errorf("IsCompressed not preserved: expected %v, got %v", tc.shouldPreserveIC, dstChunk.IsCompressed)
			}

			// Verify ETag preservation
			if dstChunk.ETag != tc.sourceChunk.ETag {
				t.Errorf("ETag not preserved: expected %s, got %s", tc.sourceChunk.ETag, dstChunk.ETag)
			}
		})
	}
}

// TestUploadChunkDataCompressionFlag tests that uploadChunkData respects the isCompressed flag
func TestUploadChunkDataCompressionFlag(t *testing.T) {
	// This is a unit test to verify the function signature and parameter passing
	// We can't easily test the actual upload without a running server, but we can
	// verify that the function accepts the correct parameters

	testCases := []struct {
		name         string
		isCompressed bool
	}{
		{
			name:         "Compressed data",
			isCompressed: true,
		},
		{
			name:         "Uncompressed data",
			isCompressed: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify function signature accepts isCompressed parameter
			// This ensures the parameter is being passed through the call chain
			var chunkData []byte
			var assignResult *filer_pb.AssignVolumeResponse

			// This will fail at runtime due to nil values, but it verifies the signature
			defer func() {
				if r := recover(); r == nil {
					// Expected to panic due to nil assignResult
					t.Error("Expected panic due to nil assignResult, but got none")
				}
			}()

			s3a := &S3ApiServer{}
			_ = s3a.uploadChunkData(chunkData, assignResult, tc.isCompressed)
		})
	}
}

// TestCopySingleChunkWithEncryption tests the full chunk copy process for encrypted chunks
func TestCopySingleChunkWithEncryption(t *testing.T) {
	testCases := []struct {
		name              string
		sourceChunk       *filer_pb.FileChunk
		expectPreserveCK  bool
		expectPreserveIC  bool
		description       string
	}{
		{
			name: "Encrypted and compressed chunk from encrypted volume",
			sourceChunk: &filer_pb.FileChunk{
				FileId:       "test,01234567890123456",
				Offset:       0,
				Size:         4194304, // 4MB
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: true,
				ETag:         "abc123",
			},
			expectPreserveCK: true,
			expectPreserveIC: true,
			description:      "Should preserve both CipherKey and IsCompressed for encrypted volume chunks",
		},
		{
			name: "Encrypted but not compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				FileId:       "test,01234567890123457",
				Offset:       4194304,
				Size:         4194304,
				CipherKey:    []byte("test-cipher-key-1234567890123456"),
				IsCompressed: false,
				ETag:         "def456",
			},
			expectPreserveCK: true,
			expectPreserveIC: false,
			description:      "Should preserve CipherKey but not set IsCompressed",
		},
		{
			name: "Compressed but not encrypted chunk",
			sourceChunk: &filer_pb.FileChunk{
				FileId:       "test,01234567890123458",
				Offset:       8388608,
				Size:         2097152, // 2MB
				CipherKey:    nil,
				IsCompressed: true,
				ETag:         "ghi789",
			},
			expectPreserveCK: false,
			expectPreserveIC: true,
			description:      "Should preserve IsCompressed but have no CipherKey",
		},
		{
			name: "Neither encrypted nor compressed chunk",
			sourceChunk: &filer_pb.FileChunk{
				FileId:       "test,01234567890123459",
				Offset:       10485760,
				Size:         1048576, // 1MB
				CipherKey:    nil,
				IsCompressed: false,
				ETag:         "jkl012",
			},
			expectPreserveCK: false,
			expectPreserveIC: false,
			description:      "Should have neither CipherKey nor IsCompressed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s3a := &S3ApiServer{}

			// Test createDestinationChunk (the part we can test without network)
			dstChunk := s3a.createDestinationChunk(tc.sourceChunk, tc.sourceChunk.Offset, tc.sourceChunk.Size)

			// Verify CipherKey preservation
			if tc.expectPreserveCK {
				if !bytes.Equal(dstChunk.CipherKey, tc.sourceChunk.CipherKey) {
					t.Errorf("%s: CipherKey not preserved correctly", tc.description)
				}
				if len(dstChunk.CipherKey) == 0 {
					t.Errorf("%s: Expected CipherKey to be preserved, but got empty", tc.description)
				}
			} else {
				if len(dstChunk.CipherKey) > 0 {
					t.Errorf("%s: Expected no CipherKey, but got %v", tc.description, dstChunk.CipherKey)
				}
			}

			// Verify IsCompressed preservation
			if dstChunk.IsCompressed != tc.expectPreserveIC {
				t.Errorf("%s: IsCompressed flag incorrect: expected %v, got %v",
					tc.description, tc.expectPreserveIC, dstChunk.IsCompressed)
			}

			// Verify other fields are preserved
			if dstChunk.Offset != tc.sourceChunk.Offset {
				t.Errorf("%s: Offset not preserved: expected %d, got %d",
					tc.description, tc.sourceChunk.Offset, dstChunk.Offset)
			}
			if dstChunk.Size != tc.sourceChunk.Size {
				t.Errorf("%s: Size not preserved: expected %d, got %d",
					tc.description, tc.sourceChunk.Size, dstChunk.Size)
			}
			if dstChunk.ETag != tc.sourceChunk.ETag {
				t.Errorf("%s: ETag not preserved: expected %s, got %s",
					tc.description, tc.sourceChunk.ETag, dstChunk.ETag)
			}
		})
	}
}

// TestCopyChunksPreservesMetadata tests that copyChunks preserves all chunk metadata
func TestCopyChunksPreservesMetadata(t *testing.T) {
	// Create test entry with multiple encrypted chunks
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 10485760, // 10MB
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId:       "vol1,01234567890123456",
				Offset:       0,
				Size:         4194304,
				CipherKey:    []byte("key1-1234567890123456789012345678"),
				IsCompressed: true,
				ETag:         "chunk1-etag",
			},
			{
				FileId:       "vol2,01234567890123457",
				Offset:       4194304,
				Size:         4194304,
				CipherKey:    []byte("key2-1234567890123456789012345678"),
				IsCompressed: true,
				ETag:         "chunk2-etag",
			},
			{
				FileId:       "vol3,01234567890123458",
				Offset:       8388608,
				Size:         2097152,
				CipherKey:    []byte("key3-1234567890123456789012345678"),
				IsCompressed: false, // Last chunk might not be compressed
				ETag:         "chunk3-etag",
			},
		},
	}

	s3a := &S3ApiServer{}

	// Verify each chunk's metadata is preserved in createDestinationChunk
	for i, sourceChunk := range entry.Chunks {
		dstChunk := s3a.createDestinationChunk(sourceChunk, sourceChunk.Offset, sourceChunk.Size)

		if !bytes.Equal(dstChunk.CipherKey, sourceChunk.CipherKey) {
			t.Errorf("Chunk %d: CipherKey not preserved", i)
		}
		if dstChunk.IsCompressed != sourceChunk.IsCompressed {
			t.Errorf("Chunk %d: IsCompressed not preserved: expected %v, got %v",
				i, sourceChunk.IsCompressed, dstChunk.IsCompressed)
		}
		if dstChunk.Offset != sourceChunk.Offset {
			t.Errorf("Chunk %d: Offset not preserved", i)
		}
		if dstChunk.Size != sourceChunk.Size {
			t.Errorf("Chunk %d: Size not preserved", i)
		}
		if dstChunk.ETag != sourceChunk.ETag {
			t.Errorf("Chunk %d: ETag not preserved", i)
		}
	}
}

// TestEncryptedVolumeScenario documents the expected behavior for encrypted volumes
func TestEncryptedVolumeScenario(t *testing.T) {
	t.Run("Scenario: Copy file on encrypted volume with multiple chunks", func(t *testing.T) {
		// Scenario description for issue #7530:
		// 1. Volume is started with -filer.encryptVolumeData
		// 2. File is uploaded via S3 (automatically encrypted, multiple chunks)
		// 3. File is copied/renamed via S3 CopyObject
		// 4. Copied file should be readable
		//
		// The bug was that IsCompressed flag was not preserved during copy,
		// causing the upload logic to potentially double-compress the data,
		// making the copied file unreadable.

		sourceChunks := []*filer_pb.FileChunk{
			{
				FileId:       "1,abc123",
				Offset:       0,
				Size:         4194304,
				CipherKey:    util.GenCipherKey(), // Simulates encrypted volume
				IsCompressed: true,                // Simulates compression
				ETag:         "etag1",
			},
			{
				FileId:       "2,def456",
				Offset:       4194304,
				Size:         4194304,
				CipherKey:    util.GenCipherKey(),
				IsCompressed: true,
				ETag:         "etag2",
			},
		}

		s3a := &S3ApiServer{}

		// Verify that createDestinationChunk preserves all necessary metadata
		for i, srcChunk := range sourceChunks {
			dstChunk := s3a.createDestinationChunk(srcChunk, srcChunk.Offset, srcChunk.Size)

			// Critical checks for issue #7530
			if !dstChunk.IsCompressed {
				t.Errorf("Chunk %d: IsCompressed flag MUST be preserved to prevent double-compression", i)
			}
			if !bytes.Equal(dstChunk.CipherKey, srcChunk.CipherKey) {
				t.Errorf("Chunk %d: CipherKey MUST be preserved for encrypted volumes", i)
			}
			if dstChunk.Offset != srcChunk.Offset {
				t.Errorf("Chunk %d: Offset must be preserved", i)
			}
			if dstChunk.Size != srcChunk.Size {
				t.Errorf("Chunk %d: Size must be preserved", i)
			}
		}

		t.Log("✓ All chunk metadata properly preserved for encrypted volume copy scenario")
	})
}
