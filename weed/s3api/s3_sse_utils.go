package s3api

import "github.com/seaweedfs/seaweedfs/weed/glog"

// calculateIVWithOffset calculates a unique IV by combining a base IV with an offset.
// This ensures each chunk/part uses a unique IV, preventing CTR mode IV reuse vulnerabilities.
// Returns the adjusted IV and the number of bytes to skip from the decrypted stream.
// The skip is needed because CTR mode operates on 16-byte blocks, but the offset may not be block-aligned.
// This function is shared between SSE-KMS and SSE-S3 implementations for consistency.
func calculateIVWithOffset(baseIV []byte, offset int64) ([]byte, int) {
	if len(baseIV) != 16 {
		glog.Errorf("Invalid base IV length: expected 16, got %d", len(baseIV))
		return baseIV, 0 // Return original IV as fallback
	}

	// Create a copy of the base IV to avoid modifying the original
	iv := make([]byte, 16)
	copy(iv, baseIV)

	// Calculate the block offset (AES block size is 16 bytes) and intra-block skip
	blockOffset := offset / 16
	skip := int(offset % 16)
	originalBlockOffset := blockOffset

	// Add the block offset to the IV counter (last 8 bytes, big-endian)
	// This matches how AES-CTR mode increments the counter
	// Process from least significant byte (index 15) to most significant byte (index 8)
	carry := uint64(0)
	for i := 15; i >= 8; i-- {
		sum := uint64(iv[i]) + uint64(blockOffset&0xFF) + carry
		iv[i] = byte(sum & 0xFF)
		carry = sum >> 8
		blockOffset = blockOffset >> 8

		// If no more blockOffset bits and no carry, we can stop early
		if blockOffset == 0 && carry == 0 {
			break
		}
	}

	// Single consolidated debug log to avoid performance impact in high-throughput scenarios
	glog.V(4).Infof("calculateIVWithOffset: baseIV=%x, offset=%d, blockOffset=%d, skip=%d, derivedIV=%x",
		baseIV, offset, originalBlockOffset, skip, iv)
	return iv, skip
}
