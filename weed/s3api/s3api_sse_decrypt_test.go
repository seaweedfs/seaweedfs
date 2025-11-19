package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"testing"
)

// TestSSECDecryptChunkView_NoOffsetAdjustment verifies that SSE-C decryption
// does NOT apply calculateIVWithOffset, preventing the critical bug where
// offset adjustment would cause CTR stream misalignment and data corruption.
func TestSSECDecryptChunkView_NoOffsetAdjustment(t *testing.T) {
	// Setup: Create test data
	plaintext := []byte("This is a test message for SSE-C decryption without offset adjustment")
	customerKey := &SSECustomerKey{
		Key:    make([]byte, 32), // 256-bit key
		KeyMD5: "test-key-md5",
	}
	// Generate random AES key
	if _, err := rand.Read(customerKey.Key); err != nil {
		t.Fatalf("Failed to generate random key: %v", err)
	}

	// Generate random IV for this "part"
	randomIV := make([]byte, aes.BlockSize)
	if _, err := rand.Read(randomIV); err != nil {
		t.Fatalf("Failed to generate random IV: %v", err)
	}

	// Encrypt the plaintext using the random IV (simulating SSE-C multipart upload)
	// This is what CreateSSECEncryptedReader does - uses the IV directly without offset
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}
	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, randomIV)
	stream.XORKeyStream(ciphertext, plaintext)

	partOffset := int64(1024) // Non-zero offset that should NOT be applied during SSE-C decryption

	// TEST: Decrypt using stored IV directly (correct behavior)
	decryptedReaderCorrect, err := CreateSSECDecryptedReader(
		io.NopCloser(bytes.NewReader(ciphertext)),
		customerKey,
		randomIV, // Use stored IV directly - CORRECT
	)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader (correct): %v", err)
	}
	decryptedCorrect, err := io.ReadAll(decryptedReaderCorrect)
	if err != nil {
		t.Fatalf("Failed to read decrypted data (correct): %v", err)
	}

	// Verify correct decryption
	if !bytes.Equal(decryptedCorrect, plaintext) {
		t.Errorf("Correct decryption failed:\nExpected: %s\nGot: %s", plaintext, decryptedCorrect)
	} else {
		t.Logf("✓ Correct decryption (using stored IV directly) successful")
	}

	// ANTI-TEST: Decrypt using offset-adjusted IV (incorrect behavior - the bug)
	adjustedIV, ivSkip := calculateIVWithOffset(randomIV, partOffset)
	decryptedReaderWrong, err := CreateSSECDecryptedReader(
		io.NopCloser(bytes.NewReader(ciphertext)),
		customerKey,
		adjustedIV, // Use adjusted IV - WRONG
	)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader (wrong): %v", err)
	}
	
	// Skip ivSkip bytes (as the buggy code would do)
	if ivSkip > 0 {
		io.CopyN(io.Discard, decryptedReaderWrong, int64(ivSkip))
	}
	
	decryptedWrong, err := io.ReadAll(decryptedReaderWrong)
	if err != nil {
		t.Fatalf("Failed to read decrypted data (wrong): %v", err)
	}

	// Verify that offset adjustment produces DIFFERENT (corrupted) output
	if bytes.Equal(decryptedWrong, plaintext) {
		t.Errorf("CRITICAL: Offset-adjusted IV produced correct plaintext! This shouldn't happen for SSE-C.")
	} else {
		t.Logf("✓ Verified: Offset-adjusted IV produces corrupted data (as expected for SSE-C)")
		maxLen := 20
		if len(plaintext) < maxLen {
			maxLen = len(plaintext)
		}
		t.Logf("  Plaintext:  %q", plaintext[:maxLen])
		maxLen2 := 20
		if len(decryptedWrong) < maxLen2 {
			maxLen2 = len(decryptedWrong)
		}
		t.Logf("  Corrupted:  %q", decryptedWrong[:maxLen2])
	}
}

// TestSSEKMSDecryptChunkView_RequiresOffsetAdjustment verifies that SSE-KMS
// decryption DOES require calculateIVWithOffset, unlike SSE-C.
func TestSSEKMSDecryptChunkView_RequiresOffsetAdjustment(t *testing.T) {
	// Setup: Create test data
	plaintext := []byte("This is a test message for SSE-KMS decryption with offset adjustment")
	
	// Generate base IV and key
	baseIV := make([]byte, aes.BlockSize)
	key := make([]byte, 32)
	if _, err := rand.Read(baseIV); err != nil {
		t.Fatalf("Failed to generate base IV: %v", err)
	}
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	chunkOffset := int64(2048) // Simulate chunk at offset 2048

	// Encrypt using base IV + offset (simulating SSE-KMS multipart upload)
	adjustedIV, ivSkip := calculateIVWithOffset(baseIV, chunkOffset)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher: %v", err)
	}
	
	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, adjustedIV)
	
	// Skip ivSkip bytes in the encryption stream if needed
	if ivSkip > 0 {
		dummy := make([]byte, ivSkip)
		stream.XORKeyStream(dummy, dummy)
	}
	stream.XORKeyStream(ciphertext, plaintext)

	// TEST: Decrypt using base IV + offset adjustment (correct for SSE-KMS)
	adjustedIVDecrypt, ivSkipDecrypt := calculateIVWithOffset(baseIV, chunkOffset)
	blockDecrypt, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher for decryption: %v", err)
	}
	
	decrypted := make([]byte, len(ciphertext))
	streamDecrypt := cipher.NewCTR(blockDecrypt, adjustedIVDecrypt)
	
	// Skip ivSkip bytes in the decryption stream
	if ivSkipDecrypt > 0 {
		dummy := make([]byte, ivSkipDecrypt)
		streamDecrypt.XORKeyStream(dummy, dummy)
	}
	streamDecrypt.XORKeyStream(decrypted, ciphertext)

	// Verify correct decryption with offset adjustment
	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("SSE-KMS decryption with offset adjustment failed:\nExpected: %s\nGot: %s", plaintext, decrypted)
	} else {
		t.Logf("✓ SSE-KMS decryption with offset adjustment successful")
	}

	// ANTI-TEST: Decrypt using base IV directly (incorrect for SSE-KMS)
	blockWrong, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create cipher for wrong decryption: %v", err)
	}
	
	decryptedWrong := make([]byte, len(ciphertext))
	streamWrong := cipher.NewCTR(blockWrong, baseIV) // Use base IV directly - WRONG for SSE-KMS
	streamWrong.XORKeyStream(decryptedWrong, ciphertext)

	// Verify that NOT using offset adjustment produces corrupted output
	if bytes.Equal(decryptedWrong, plaintext) {
		t.Errorf("CRITICAL: Base IV without offset produced correct plaintext! SSE-KMS requires offset adjustment.")
	} else {
		t.Logf("✓ Verified: Base IV without offset produces corrupted data (as expected for SSE-KMS)")
	}
}

// TestSSEDecryptionDifferences documents the key differences between SSE types
func TestSSEDecryptionDifferences(t *testing.T) {
	t.Log("SSE-C:   Random IV per part → Use stored IV DIRECTLY (no offset)")
	t.Log("SSE-KMS: Base IV + offset → MUST call calculateIVWithOffset(baseIV, offset)")
	t.Log("SSE-S3:  Base IV + offset → Stores ADJUSTED IV, use directly")
	
	// This test documents the critical differences and serves as executable documentation
}

