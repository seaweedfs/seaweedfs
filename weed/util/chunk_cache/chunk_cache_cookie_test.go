package chunk_cache

import (
	"bytes"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestChunkCacheCookieValidation(t *testing.T) {
	// Create temporary directory for cache
	tmpDir, err := os.MkdirTemp("", "chunk_cache_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache
	cache := NewTieredChunkCache(100, tmpDir, 1024, 1024*1024)
	defer cache.Shutdown()

	// Test data
	testData := []byte("test chunk data for cookie validation")

	// Test case 1: Valid file ID - should hit cache
	fileId1 := "100,abcd0000abcd"

	// Store chunk
	cache.SetChunk(fileId1, testData)

	// Should be in cache
	if !cache.IsInCache(fileId1, true) {
		t.Errorf("Expected fileId %s to be in cache", fileId1)
	}

	// Should read successfully
	readBuffer := make([]byte, len(testData))
	n, err := cache.ReadChunkAt(readBuffer, fileId1, 0)
	if err != nil {
		t.Errorf("Failed to read from cache: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
	}
	if string(readBuffer) != string(testData) {
		t.Errorf("Data mismatch: expected %s, got %s", string(testData), string(readBuffer))
	}

	// Test case 2: Same needle ID (abcd) but different cookie - should miss cache
	fileId2 := "200,abcd00001234" // Different volume and cookie, same needle ID

	// Should not be in cache (cookie mismatch)
	if cache.IsInCache(fileId2, true) {
		t.Errorf("Expected fileId %s to NOT be in cache (cookie mismatch)", fileId2)
	}

	// Should not read from cache (cookie mismatch)
	n, err = cache.ReadChunkAt(readBuffer, fileId2, 0)
	if n != 0 {
		t.Errorf("Expected cache miss for fileId %s with different cookie, but got %d bytes", fileId2, n)
	}

	// Test case 3: Read from offset to verify efficiency (no memory waste)
	offsetReadBuffer := make([]byte, 10)
	n, err = cache.ReadChunkAt(offsetReadBuffer, fileId1, 10) // Read 10 bytes starting from offset 10
	if err != nil {
		t.Errorf("Failed to read from cache at offset: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected to read 10 bytes at offset, got %d", n)
	}
	// Verify the data matches the expected slice
	expectedData := testData[10:20]
	if !bytes.Equal(offsetReadBuffer[:n], expectedData) {
		t.Errorf("Data mismatch at offset 10: expected %q, got %q",
			string(expectedData), string(offsetReadBuffer[:n]))
	}

	// Test case 4: Test different volume with same needle ID and cookie (very unlikely but possible)
	// This tests the volume ID validation specifically
	fileId3 := "200,abcd0000abcd" // Same needle ID and cookie as fileId1, but different volume

	// Should not be in cache (volume ID mismatch)
	if cache.IsInCache(fileId3, true) {
		t.Errorf("Expected fileId %s to NOT be in cache (volume ID mismatch)", fileId3)
	}
}

func TestFileIdParsing(t *testing.T) {
	testCases := []struct {
		fileId   string
		volumeId needle.VolumeId
		needleId types.NeedleId
		cookie   types.Cookie
	}{
		{"100,123456789012345678901234", 100, 0x1234567890123456, types.Uint32ToCookie(uint32(0x78901234))},
		{"100,abcd0000abcd", 100, 0xabcd, types.Uint32ToCookie(uint32(0xabcd))},
	}

	for _, tc := range testCases {
		fid, err := needle.ParseFileIdFromString(tc.fileId)
		if err != nil {
			t.Errorf("Failed to parse fileId %s: %v", tc.fileId, err)
			continue
		}

		if fid.VolumeId != tc.volumeId {
			t.Errorf("VolumeId mismatch for %s: expected %d, got %d",
				tc.fileId, tc.volumeId, fid.VolumeId)
		}

		if fid.Key != tc.needleId {
			t.Errorf("NeedleId mismatch for %s: expected %x, got %x",
				tc.fileId, tc.needleId, fid.Key)
		}

		if fid.Cookie != tc.cookie {
			t.Errorf("Cookie mismatch for %s: expected %x, got %x",
				tc.fileId, tc.cookie, fid.Cookie)
		}
	}
}
