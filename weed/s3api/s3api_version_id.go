package s3api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Version ID format constants
// New format uses inverted timestamps so newer versions sort first lexicographically
// Old format used raw timestamps where older versions sorted first
const (
	// Threshold to distinguish old vs new format version IDs
	// Around year 2024-2025:
	//   - Old format (raw ns): ~1.7×10¹⁸ ≈ 0x17... (BELOW threshold)
	//   - New format (MaxInt64 - ns): ~7.5×10¹⁸ ≈ 0x68... (ABOVE threshold)
	// We use 0x4000000000000000 (~4.6×10¹⁸) as threshold
	versionIdFormatThreshold = 0x4000000000000000
)

// generateVersionId creates a unique version ID
// If useInvertedFormat is true, uses inverted timestamps so newer versions sort first
// If false, uses raw timestamps (old format) for backward compatibility
func generateVersionId(useInvertedFormat bool) string {
	now := time.Now().UnixNano()
	var timestampHex string

	if useInvertedFormat {
		// INVERTED timestamp: newer versions have SMALLER values
		// This makes lexicographic sorting return newest versions first
		invertedTimestamp := math.MaxInt64 - now
		timestampHex = fmt.Sprintf("%016x", invertedTimestamp)
	} else {
		// Raw timestamp: older versions have SMALLER values (old format)
		timestampHex = fmt.Sprintf("%016x", now)
	}

	// Generate random 8 bytes for uniqueness (last 16 chars of version ID)
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		glog.Errorf("Failed to generate random bytes for version ID: %v", err)
		// Fallback to timestamp-only if random generation fails
		return timestampHex + "0000000000000000"
	}

	// Combine timestamp (16 chars) + random (16 chars) = 32 chars total
	randomHex := hex.EncodeToString(randBytes)
	return timestampHex + randomHex
}

// isNewFormatVersionId returns true if the version ID uses the new inverted timestamp format
func isNewFormatVersionId(versionId string) bool {
	if len(versionId) < 16 || versionId == "null" {
		return false
	}
	// Parse the first 16 hex chars as the timestamp portion
	timestampPart, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return false
	}
	// New format has inverted timestamps (MaxInt64 - ns), which are ABOVE the threshold (~0x68...)
	// Old format has raw timestamps, which are BELOW the threshold (~0x17...)
	return timestampPart > versionIdFormatThreshold
}

// getVersionTimestamp extracts the actual timestamp from a version ID,
// handling both old (raw) and new (inverted) formats
func getVersionTimestamp(versionId string) int64 {
	if len(versionId) < 16 || versionId == "null" {
		return 0
	}
	timestampPart, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return 0
	}
	if timestampPart > versionIdFormatThreshold {
		// New format: inverted timestamp (above threshold), convert back
		return int64(math.MaxInt64 - timestampPart)
	}
	// Validate old format timestamp is within int64 range
	if timestampPart > math.MaxInt64 {
		return 0
	}
	// Old format: raw timestamp (below threshold)
	return int64(timestampPart)
}

// compareVersionIds compares two version IDs for sorting (newest first)
// Returns: negative if a is newer, positive if b is newer, 0 if equal
// Handles both old and new format version IDs
func compareVersionIds(a, b string) int {
	if a == b {
		return 0
	}
	if a == "null" {
		return 1 // null versions sort last
	}
	if b == "null" {
		return -1
	}

	aIsNew := isNewFormatVersionId(a)
	bIsNew := isNewFormatVersionId(b)

	if aIsNew == bIsNew {
		// Same format - compare lexicographically
		// For new format: smaller value = newer (correct)
		// For old format: smaller value = older (need to invert)
		if aIsNew {
			// New format: lexicographic order is correct (smaller = newer)
			if a < b {
				return -1
			}
			return 1
		} else {
			// Old format: lexicographic order is inverted (smaller = older)
			if a < b {
				return 1
			}
			return -1
		}
	}

	// Mixed formats - compare by actual timestamp
	aTime := getVersionTimestamp(a)
	bTime := getVersionTimestamp(b)
	if aTime > bTime {
		return -1 // a is newer
	}
	if aTime < bTime {
		return 1 // b is newer
	}
	return 0
}

// getVersionedObjectDir returns the directory path for storing object versions
func (s3a *S3ApiServer) getVersionedObjectDir(bucket, object string) string {
	return s3a.option.BucketsPath + "/" + bucket + "/" + object + s3_constants.VersionsFolder
}

// getVersionFileName returns the filename for a specific version
func (s3a *S3ApiServer) getVersionFileName(versionId string) string {
	return fmt.Sprintf("v_%s", versionId)
}

// getVersionIdFormat checks the .versions directory to determine which version ID format to use.
// Returns true if inverted format (new format) should be used.
// For new .versions directories, returns true (use new format).
// For existing directories, infers format from the latest version ID.
func (s3a *S3ApiServer) getVersionIdFormat(bucket, object string) bool {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsPath := object + s3_constants.VersionsFolder

	// Try to get the .versions directory entry
	versionsEntry, err := s3a.getEntry(bucketDir, versionsPath)
	if err != nil {
		// .versions directory doesn't exist yet - use new format
		return true
	}

	// Infer format from the latest version ID stored in metadata
	if versionsEntry.Extended != nil {
		if latestVersionId, exists := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]; exists {
			return isNewFormatVersionId(string(latestVersionId))
		}
	}

	// No latest version metadata - this is likely a new or empty directory
	// Use new format
	return true
}

// generateVersionIdForObject generates a version ID using the appropriate format for the object.
// For new objects, uses inverted format. For existing versioned objects, uses their existing format.
func (s3a *S3ApiServer) generateVersionIdForObject(bucket, object string) string {
	useInvertedFormat := s3a.getVersionIdFormat(bucket, object)
	return generateVersionId(useInvertedFormat)
}
