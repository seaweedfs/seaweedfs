package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"testing"
)

// TestCalculateIVWithOffset tests the calculateIVWithOffset function
func TestCalculateIVWithOffset(t *testing.T) {
	baseIV := make([]byte, 16)
	rand.Read(baseIV)

	tests := []struct {
		name           string
		offset         int64
		expectedSkip   int
		expectedBlock  int64
	}{
		{"BlockAligned_0", 0, 0, 0},
		{"BlockAligned_16", 16, 0, 1},
		{"BlockAligned_32", 32, 0, 2},
		{"BlockAligned_48", 48, 0, 3},
		{"NonAligned_1", 1, 1, 0},
		{"NonAligned_5", 5, 5, 0},
		{"NonAligned_10", 10, 10, 0},
		{"NonAligned_15", 15, 15, 0},
		{"NonAligned_17", 17, 1, 1},
		{"NonAligned_21", 21, 5, 1},
		{"NonAligned_33", 33, 1, 2},
		{"NonAligned_47", 47, 15, 2},
		{"LargeOffset", 1000, 1000 % 16, 1000 / 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adjustedIV, skip := calculateIVWithOffset(baseIV, tt.offset)

			// Verify skip is correct
			if skip != tt.expectedSkip {
				t.Errorf("calculateIVWithOffset(%d) skip = %d, want %d", tt.offset, skip, tt.expectedSkip)
			}

			// Verify IV length is preserved
			if len(adjustedIV) != 16 {
				t.Errorf("calculateIVWithOffset(%d) IV length = %d, want 16", tt.offset, len(adjustedIV))
			}

			// Verify IV was adjusted correctly (last 8 bytes incremented by blockOffset)
			if tt.expectedBlock == 0 {
				if !bytes.Equal(adjustedIV, baseIV) {
					t.Errorf("calculateIVWithOffset(%d) IV changed when blockOffset=0", tt.offset)
				}
			} else {
				// IV should be different for non-zero block offsets
				if bytes.Equal(adjustedIV, baseIV) {
					t.Errorf("calculateIVWithOffset(%d) IV not changed when blockOffset=%d", tt.offset, tt.expectedBlock)
				}
			}
		})
	}
}

// TestCTRDecryptionWithNonBlockAlignedOffset tests that CTR decryption works correctly
// for non-block-aligned offsets (the critical bug fix)
func TestCTRDecryptionWithNonBlockAlignedOffset(t *testing.T) {
	// Generate test data
	plaintext := make([]byte, 1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	// Generate random key and IV
	key := make([]byte, 32) // AES-256
	iv := make([]byte, 16)
	rand.Read(key)
	rand.Read(iv)

	// Encrypt the entire plaintext
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}

	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext, plaintext)

	// Test various offsets (both block-aligned and non-block-aligned)
	testOffsets := []int64{0, 1, 5, 10, 15, 16, 17, 21, 32, 33, 47, 48, 100, 500}

	for _, offset := range testOffsets {
		t.Run(string(rune('A'+offset)), func(t *testing.T) {
			// Calculate adjusted IV and skip
			adjustedIV, skip := calculateIVWithOffset(iv, offset)

			// CRITICAL: Start from the block-aligned offset, not the user offset
			// CTR mode works on 16-byte blocks, so we need to decrypt from the block start
			blockAlignedOffset := offset - int64(skip)

			// Decrypt from the block-aligned offset
			decryptBlock, err := aes.NewCipher(key)
			if err != nil {
				t.Fatalf("Failed to create decrypt cipher: %v", err)
			}

			decryptStream := cipher.NewCTR(decryptBlock, adjustedIV)
			
			// Create a reader for the ciphertext starting at block-aligned offset
			ciphertextFromBlockStart := ciphertext[blockAlignedOffset:]
			decryptedFromBlockStart := make([]byte, len(ciphertextFromBlockStart))
			decryptStream.XORKeyStream(decryptedFromBlockStart, ciphertextFromBlockStart)

			// CRITICAL: Skip the intra-block bytes to get to the user-requested offset
			if skip > 0 {
				if skip > len(decryptedFromBlockStart) {
					t.Fatalf("Skip %d exceeds decrypted data length %d", skip, len(decryptedFromBlockStart))
				}
				decryptedFromBlockStart = decryptedFromBlockStart[skip:]
			}

			// Rename for consistency
			decryptedFromOffset := decryptedFromBlockStart

			// Verify decrypted data matches original plaintext
			expectedPlaintext := plaintext[offset:]
			if !bytes.Equal(decryptedFromOffset, expectedPlaintext) {
				t.Errorf("Decryption mismatch at offset %d (skip=%d)", offset, skip)
				previewLen := 32
				if len(expectedPlaintext) < previewLen {
					previewLen = len(expectedPlaintext)
				}
				t.Errorf("  Expected first 32 bytes: %x", expectedPlaintext[:previewLen])
				previewLen2 := 32
				if len(decryptedFromOffset) < previewLen2 {
					previewLen2 = len(decryptedFromOffset)
				}
				t.Errorf("  Got first 32 bytes:      %x", decryptedFromOffset[:previewLen2])
				
				// Find first mismatch
				for i := 0; i < len(expectedPlaintext) && i < len(decryptedFromOffset); i++ {
					if expectedPlaintext[i] != decryptedFromOffset[i] {
						t.Errorf("  First mismatch at byte %d: expected %02x, got %02x", i, expectedPlaintext[i], decryptedFromOffset[i])
						break
					}
				}
			}
		})
	}
}

// TestCTRRangeRequestSimulation simulates a real-world S3 range request scenario
func TestCTRRangeRequestSimulation(t *testing.T) {
	// Simulate uploading a 5MB object
	objectSize := 5 * 1024 * 1024
	plaintext := make([]byte, objectSize)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	// Encrypt the object
	key := make([]byte, 32)
	iv := make([]byte, 16)
	rand.Read(key)
	rand.Read(iv)

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}

	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext, plaintext)

	// Simulate various S3 range requests
	rangeTests := []struct {
		name  string
		start int64
		end   int64
	}{
		{"First byte", 0, 0},
		{"First 100 bytes", 0, 99},
		{"Mid-block range", 5, 100},          // Critical: starts at non-aligned offset
		{"Single mid-block byte", 17, 17},    // Critical: single byte at offset 17
		{"Cross-block range", 10, 50},        // Spans multiple blocks
		{"Large range", 1000, 10000},
		{"Tail range", int64(objectSize - 1000), int64(objectSize - 1)},
	}

	for _, rt := range rangeTests {
		t.Run(rt.name, func(t *testing.T) {
			rangeSize := rt.end - rt.start + 1
			
			// Calculate adjusted IV and skip for the range start
			adjustedIV, skip := calculateIVWithOffset(iv, rt.start)

			// CRITICAL: Start decryption from block-aligned offset
			blockAlignedStart := rt.start - int64(skip)

			// Create decryption stream
			decryptBlock, err := aes.NewCipher(key)
			if err != nil {
				t.Fatalf("Failed to create decrypt cipher: %v", err)
			}

			decryptStream := cipher.NewCTR(decryptBlock, adjustedIV)
			
			// Decrypt from block-aligned start through the end of range
			ciphertextFromBlock := ciphertext[blockAlignedStart : rt.end+1]
			decryptedFromBlock := make([]byte, len(ciphertextFromBlock))
			decryptStream.XORKeyStream(decryptedFromBlock, ciphertextFromBlock)

			// CRITICAL: Skip intra-block bytes to get to user-requested start
			if skip > 0 {
				decryptedFromBlock = decryptedFromBlock[skip:]
			}

			decryptedRange := decryptedFromBlock

			// Verify decrypted range matches original plaintext
			expectedPlaintext := plaintext[rt.start : rt.end+1]
			if !bytes.Equal(decryptedRange, expectedPlaintext) {
				t.Errorf("Range decryption mismatch for %s (offset=%d, size=%d, skip=%d)", 
					rt.name, rt.start, rangeSize, skip)
				previewLen := 64
				if len(expectedPlaintext) < previewLen {
					previewLen = len(expectedPlaintext)
				}
				t.Errorf("  Expected: %x", expectedPlaintext[:previewLen])
				previewLen2 := previewLen
				if len(decryptedRange) < previewLen2 {
					previewLen2 = len(decryptedRange)
				}
				t.Errorf("  Got:      %x", decryptedRange[:previewLen2])
			}
		})
	}
}

// TestCTRDecryptionWithIOReader tests the integration with io.Reader
func TestCTRDecryptionWithIOReader(t *testing.T) {
	plaintext := []byte("Hello, World! This is a test of CTR mode decryption with non-aligned offsets.")
	
	key := make([]byte, 32)
	iv := make([]byte, 16)
	rand.Read(key)
	rand.Read(iv)

	// Encrypt
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}

	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext, plaintext)

	// Test reading from various offsets using io.Reader
	testOffsets := []int64{0, 5, 10, 16, 17, 30}

	for _, offset := range testOffsets {
		t.Run(string(rune('A'+offset)), func(t *testing.T) {
			// Calculate adjusted IV and skip
			adjustedIV, skip := calculateIVWithOffset(iv, offset)

			// CRITICAL: Start reading from block-aligned offset in ciphertext
			blockAlignedOffset := offset - int64(skip)

			// Create decrypted reader
			decryptBlock, err := aes.NewCipher(key)
			if err != nil {
				t.Fatalf("Failed to create decrypt cipher: %v", err)
			}

			decryptStream := cipher.NewCTR(decryptBlock, adjustedIV)
			ciphertextReader := bytes.NewReader(ciphertext[blockAlignedOffset:])
			decryptedReader := &cipher.StreamReader{S: decryptStream, R: ciphertextReader}

			// Skip intra-block bytes to get to user-requested offset
			if skip > 0 {
				_, err := io.CopyN(io.Discard, decryptedReader, int64(skip))
				if err != nil {
					t.Fatalf("Failed to skip %d bytes: %v", skip, err)
				}
			}

			// Read decrypted data
			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data: %v", err)
			}

			// Verify
			expectedPlaintext := plaintext[offset:]
			if !bytes.Equal(decryptedData, expectedPlaintext) {
				t.Errorf("Decryption mismatch at offset %d (skip=%d)", offset, skip)
				t.Errorf("  Expected: %q", expectedPlaintext)
				t.Errorf("  Got:      %q", decryptedData)
			}
		})
	}
}

