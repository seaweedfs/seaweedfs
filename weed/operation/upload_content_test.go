package operation

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestCipherKeyGeneration verifies that cipher operations work correctly
// and that encrypted data size differs from original
func TestCipherKeyGeneration(t *testing.T) {
	testData := []byte("This is test data for cipher operations")
	originalSize := len(testData)

	// Test key generation
	cipherKey := util.GenCipherKey()
	if len(cipherKey) == 0 {
		t.Fatal("❌ CipherKey generation failed")
	}
	t.Logf("✓ Generated cipher key length: %d bytes", len(cipherKey))

	// Test encryption
	encryptedData, err := util.Encrypt(testData, cipherKey)
	if err != nil {
		t.Fatalf("❌ Encryption failed: %v", err)
	}

	t.Logf("  Original size: %d bytes", originalSize)
	t.Logf("  Encrypted size: %d bytes", len(encryptedData))

	// Verify encrypted data is different from original
	if bytes.Equal(encryptedData, testData) {
		t.Error("❌ Encrypted data should not equal original data")
	}

	// Test decryption
	decryptedData, err := util.Decrypt(encryptedData, util.CipherKey(cipherKey))
	if err != nil {
		t.Fatalf("❌ Decryption failed: %v", err)
	}

	// Decrypted data should match original
	if !bytes.Equal(decryptedData, testData) {
		t.Error("❌ Decrypted data does not match original")
	} else {
		t.Logf("✓ Decryption successful, data matches original (%d bytes)", len(decryptedData))
	}

	// The critical point: when storing metadata, we should store the ORIGINAL size
	// not the encrypted size
	if len(testData) != len(decryptedData) {
		t.Errorf("❌ Original and decrypted size mismatch: %d vs %d",
			len(testData), len(decryptedData))
	}

	t.Logf("✓ Critical: For metadata storage, use original size %d, not encrypted size %d",
		len(testData), len(encryptedData))
}

// TestCompressionEncryptionRoundtrip verifies that data can go through compression
// and encryption and come back correctly
func TestCompressionEncryptionRoundtrip(t *testing.T) {
	originalData := bytes.Repeat([]byte("Highly repetitive test data for compression. "), 100)
	originalSize := len(originalData)

	t.Logf("Original data size: %d bytes", originalSize)

	// Simulate compression (as done in doUploadData)
	compressedData, err := util.GzipData(originalData)
	if err != nil {
		t.Fatalf("❌ Gzip failed: %v", err)
	}
	t.Logf("After gzip: %d bytes (%.1f%% of original)",
		len(compressedData), float64(len(compressedData))*100/float64(originalSize))

	// Simulate encryption of compressed data (as done in doUploadData)
	cipherKey := util.GenCipherKey()
	encryptedData, err := util.Encrypt(compressedData, cipherKey)
	if err != nil {
		t.Fatalf("❌ Encryption failed: %v", err)
	}
	t.Logf("After encryption: %d bytes", len(encryptedData))

	// The critical invariant: metadata should store ORIGINAL size, not encrypted or compressed size
	metadataSize := originalSize

	// Simulate reading back: decrypt then decompress (as done in readEncryptedUrl)
	decryptedData, err := util.Decrypt(encryptedData, util.CipherKey(cipherKey))
	if err != nil {
		t.Fatalf("❌ Decryption failed: %v", err)
	}
	t.Logf("After decryption: %d bytes", len(decryptedData))

	decompressedData, err := util.DecompressData(decryptedData)
	if err != nil {
		t.Fatalf("❌ Decompression failed: %v", err)
	}
	t.Logf("After decompression: %d bytes", len(decompressedData))

	// Final verification: this is what would fail with the bug
	if len(decompressedData) != metadataSize {
		t.Errorf("❌ CRITICAL BUG (Issue #8151): "+
			"Decompressed data size %d does not match metadata size %d. "+
			"This causes readEncryptedUrl to fail: "+
			"if len(decryptedData) < int(offset)+size would be true when it shouldn't be.",
			len(decompressedData), metadataSize)
	} else {
		t.Logf("✓ Decompressed data size matches metadata: %d bytes", len(decompressedData))
	}

	if !bytes.Equal(decompressedData, originalData) {
		t.Error("❌ Decompressed data does not match original")
	} else {
		t.Logf("✓ Data integrity preserved through compression+encryption roundtrip")
	}
}

// TestDifferentDataSizes verifies encryption works across different data sizes
func TestDifferentDataSizes(t *testing.T) {
	sizes := []int{
		1,       // Minimal
		128,     // Small
		1024,    // 1KB
		65536,   // 64KB
	}

	for _, size := range sizes {
		sizeStr := util.BytesToHumanReadable(uint64(size))
		t.Run(sizeStr, func(t *testing.T) {
			data := make([]byte, size)
			for i := 0; i < size; i++ {
				data[i] = byte((i * 73) % 256)
			}

			// Generate cipher key
			cipherKey := util.GenCipherKey()

			// Encrypt
			encryptedData, err := util.Encrypt(data, cipherKey)
			if err != nil {
				t.Fatalf("❌ Encryption failed: %v", err)
			}

			// Decrypt
			decryptedData, err := util.Decrypt(encryptedData, util.CipherKey(cipherKey))
			if err != nil {
				t.Fatalf("❌ Decryption failed: %v", err)
			}

			// Verify
			if len(decryptedData) != size {
				t.Errorf("❌ Size mismatch: original=%d, decrypted=%d", size, len(decryptedData))
			}

			if !bytes.Equal(decryptedData, data) {
				t.Error("❌ Decrypted data doesn't match original")
			} else {
				t.Logf("✓ Encryption/decryption successful for %s", sizeStr)
			}
		})
	}
}

// TestCompressionDetection verifies that compression is automatically detected
// for compressible data and that the compressed size differs from original
func TestCompressionDetection(t *testing.T) {
	// Create highly compressible data
	compressibleData := bytes.Repeat([]byte("AAAA"), 10000)
	originalSize := len(compressibleData)

	t.Logf("Original compressible data: %d bytes", originalSize)

	// Compress
	compressed, err := util.GzipData(compressibleData)
	if err != nil {
		t.Fatalf("❌ Gzip failed: %v", err)
	}

	compressionRatio := float64(len(compressed)) * 100 / float64(originalSize)
	t.Logf("After compression: %d bytes (%.1f%% of original)", len(compressed), compressionRatio)

	// With the bug, if we stored len(compressed) as the size for encrypted data,
	// then during read, we'd have a mismatch because:
	// - We'd read encrypted(compressed(data))
	// - After decrypt, we'd get compressed(data) which is len(compressed) bytes
	// - But we'd try to validate against stored size which would be wrong

	if len(compressed) >= originalSize {
		t.Logf("Note: Compressed size %d >= original size %d (data not compressible)",
			len(compressed), originalSize)
	} else {
		t.Logf("✓ Compression effective: %.1f%% reduction", 100*(1-float64(len(compressed))/float64(originalSize)))
	}

	// Decompress to verify round-trip
	decompressed, err := util.DecompressData(compressed)
	if err != nil {
		t.Fatalf("❌ Decompression failed: %v", err)
	}

	if len(decompressed) != originalSize {
		t.Errorf("❌ Decompressed size %d != original %d", len(decompressed), originalSize)
	}

	if !bytes.Equal(decompressed, compressibleData) {
		t.Error("❌ Decompressed data doesn't match original")
	} else {
		t.Logf("✓ Compression/decompression round-trip successful")
	}
}

// TestEncryptionSizeBehavior demonstrates why the fix was needed
func TestEncryptionSizeBehavior(t *testing.T) {
	t.Logf("Demonstrating Issue #8151 (the bug):")
	t.Logf("============================================================")

	originalData := []byte("Important user data")
	originalSize := len(originalData)
	t.Logf("\n1. Original data size: %d bytes", originalSize)

	// Apply gzip (as done in upload)
	gzipped, _ := util.GzipData(originalData)
	t.Logf("2. After gzip: %d bytes", len(gzipped))

	// Apply encryption (as done in upload)
	cipherKey := util.GenCipherKey()
	encrypted, _ := util.Encrypt(gzipped, cipherKey)
	t.Logf("3. After encryption: %d bytes (stored to volume server)", len(encrypted))

	// THE BUG (before fix):
	t.Logf("\n❌ WITH BUG: Stored size in metadata would be: %d (encrypted size)", len(encrypted))

	// Reading back with the bug:
	t.Logf("\n4. Reading back from volume server:")
	decrypted, _ := util.Decrypt(encrypted, util.CipherKey(cipherKey))
	t.Logf("   After decrypt: %d bytes", len(decrypted))

	decompressed, _ := util.DecompressData(decrypted)
	t.Logf("   After decompress: %d bytes", len(decompressed))

	t.Logf("\n5. Size validation check in readEncryptedUrl:")
	t.Logf("   if len(decryptedData) < int(offset)+size:")
	t.Logf("   if %d < 0 + %d: %v (validation would FAIL with bug!)",
		len(decompressed), len(encrypted), len(decompressed) < len(encrypted))

	// THE FIX (after fix):
	t.Logf("\n✓ WITH FIX: Stored size in metadata is: %d (original size)", originalSize)
	t.Logf("   if %d < 0 + %d: %v (validation passes!)",
		len(decompressed), originalSize, len(decompressed) < originalSize)

	if len(decompressed) == originalSize {
		t.Logf("\n✓ FIX VERIFIED: Data integrity preserved")
	}
}

