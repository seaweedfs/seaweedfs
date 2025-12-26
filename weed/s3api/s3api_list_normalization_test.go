package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestPrefixNormalizationInList verifies that prefixes are normalized consistently in list operations
func TestPrefixNormalizationInList(t *testing.T) {
	tests := []struct {
		name           string
		inputPrefix    string
		expectedPrefix string
		description    string
	}{
		{
			name:           "simple prefix",
			inputPrefix:    "parquet-tests/abc123/",
			expectedPrefix: "parquet-tests/abc123/",
			description:    "Normal prefix with trailing slash",
		},
		{
			name:           "leading slash",
			inputPrefix:    "/parquet-tests/abc123/",
			expectedPrefix: "parquet-tests/abc123/",
			description:    "Prefix with leading slash should be stripped",
		},
		{
			name:           "duplicate slashes",
			inputPrefix:    "parquet-tests//abc123/",
			expectedPrefix: "parquet-tests/abc123/",
			description:    "Prefix with duplicate slashes should be cleaned",
		},
		{
			name:           "backslashes",
			inputPrefix:    "parquet-tests\\abc123\\",
			expectedPrefix: "parquet-tests/abc123/",
			description:    "Backslashes should be converted to forward slashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize using NormalizeObjectKey (same as object keys)
			normalizedPrefix := s3_constants.NormalizeObjectKey(tt.inputPrefix)
			
			if normalizedPrefix != tt.expectedPrefix {
				t.Errorf("Prefix normalization mismatch:\n  Input:    %q\n  Expected: %q\n  Got:      %q\n  Desc:     %s",
					tt.inputPrefix, tt.expectedPrefix, normalizedPrefix, tt.description)
			}
		})
	}
}

// TestListPrefixConsistency verifies that objects written and listed use consistent key formats
func TestListPrefixConsistency(t *testing.T) {
	// When an object is written to "parquet-tests/123/data.parquet",
	// and we list with prefix "parquet-tests/123/",
	// we should find that object

	objectKey := "parquet-tests/123/data.parquet"
	listPrefix := "parquet-tests/123/"

	// Normalize as would happen in PUT
	normalizedObjectKey := s3_constants.NormalizeObjectKey(objectKey)

	// Check that the list prefix would match the object path
	if !startsWithPrefix(normalizedObjectKey, listPrefix) {
		t.Errorf("List prefix mismatch:\n  Object:   %q\n  Prefix:   %q\n  Object doesn't start with prefix",
			normalizedObjectKey, listPrefix)
	}
}

func startsWithPrefix(objectKey, prefix string) bool {
	// Normalize the prefix using the same logic as NormalizeObjectKey
	normalizedPrefix := s3_constants.NormalizeObjectKey(prefix)
	
	// Check if the object starts with the normalized prefix
	if normalizedPrefix == "" {
		return true
	}
	
	return objectKey == normalizedPrefix || objectKey[:len(normalizedPrefix)] == normalizedPrefix
}
