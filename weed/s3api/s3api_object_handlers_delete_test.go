package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestObjectLockVersionToCheckForDelete(t *testing.T) {
	tests := []struct {
		name              string
		versioningState   string
		requestedVersionID string
		expectedVersionID string
	}{
		{
			name:              "enabled versioning without version id checks latest version",
			versioningState:   s3_constants.VersioningEnabled,
			requestedVersionID: "",
			expectedVersionID: "",
		},
		{
			name:              "suspended versioning without version id checks null version",
			versioningState:   s3_constants.VersioningSuspended,
			requestedVersionID: "",
			expectedVersionID: "null",
		},
		{
			name:              "specific version id is always checked",
			versioningState:   s3_constants.VersioningEnabled,
			requestedVersionID: "3LgYQ7f7VxQ3",
			expectedVersionID: "3LgYQ7f7VxQ3",
		},
		{
			name:              "non-versioned buckets still check current object",
			versioningState:   "",
			requestedVersionID: "",
			expectedVersionID: "",
		},
		{
			name:              "unknown versioning state defaults to empty version",
			versioningState:   "UnexpectedState",
			requestedVersionID: "",
			expectedVersionID: "",
		},
		{
			name:              "suspended versioning with specific version id checks that version",
			versioningState:   s3_constants.VersioningSuspended,
			requestedVersionID: "abc123",
			expectedVersionID: "abc123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			versionID := objectLockVersionToCheckForDelete(tc.versioningState, tc.requestedVersionID)
			assert.Equal(t, tc.expectedVersionID, versionID)
		})
	}
}
