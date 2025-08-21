package s3api

import "github.com/seaweedfs/seaweedfs/weed/glog"

// calculateIVWithOffset calculates a unique IV by combining a base IV with an offset.
// This ensures each chunk/part uses a unique IV, preventing CTR mode IV reuse vulnerabilities.
// This function is shared between SSE-KMS and SSE-S3 implementations for consistency.
func calculateIVWithOffset(baseIV []byte, offset int64) []byte {
	if len(baseIV) != 16 {
		glog.Errorf("Invalid base IV length: expected 16, got %d", len(baseIV))
		return baseIV // Return original IV as fallback
	}

	// Create a copy of the base IV to avoid modifying the original
	iv := make([]byte, 16)
	copy(iv, baseIV)

	// Calculate the block offset (AES block size is 16 bytes)
	blockOffset := offset / 16
	glog.V(4).Infof("calculateIVWithOffset DEBUG: offset=%d, blockOffset=%d (0x%x)",
		offset, blockOffset, blockOffset)

	// Add the block offset to the IV counter (last 8 bytes, big-endian)
	// This matches how AES-CTR mode increments the counter
	// Process from least significant byte (index 15) to most significant byte (index 8)
	originalBlockOffset := blockOffset
	carry := uint64(0)
	for i := 15; i >= 8; i-- {
		sum := uint64(iv[i]) + uint64(blockOffset&0xFF) + carry
		oldByte := iv[i]
		iv[i] = byte(sum & 0xFF)
		carry = sum >> 8
		blockOffset = blockOffset >> 8
		glog.V(4).Infof("calculateIVWithOffset DEBUG: i=%d, oldByte=0x%02x, newByte=0x%02x, carry=%d, blockOffset=0x%x",
			i, oldByte, iv[i], carry, blockOffset)

		// If no more blockOffset bits and no carry, we can stop early
		if blockOffset == 0 && carry == 0 {
			break
		}
	}

	glog.V(4).Infof("calculateIVWithOffset: baseIV=%x, offset=%d, blockOffset=%d, calculatedIV=%x",
		baseIV, offset, originalBlockOffset, iv)
	return iv
}
