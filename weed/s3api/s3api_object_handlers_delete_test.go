package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestObjectLockVersionToCheckForDelete(t *testing.T) {
	tests := []struct {
		name                string
		versioningState     string
		requestedVersionID  string
		expectedVersionID   string
		expectedShouldCheck bool
	}{
		{
			name:                "enabled versioning without version id checks latest version",
			versioningState:     s3_constants.VersioningEnabled,
			requestedVersionID:  "",
			expectedVersionID:   "",
			expectedShouldCheck: true,
		},
		{
			name:                "suspended versioning without version id checks null version",
			versioningState:     s3_constants.VersioningSuspended,
			requestedVersionID:  "",
			expectedVersionID:   "null",
			expectedShouldCheck: true,
		},
		{
			name:                "specific version id is always checked",
			versioningState:     s3_constants.VersioningEnabled,
			requestedVersionID:  "3LgYQ7f7VxQ3",
			expectedVersionID:   "3LgYQ7f7VxQ3",
			expectedShouldCheck: true,
		},
		{
			name:                "non-versioned buckets still check current object",
			versioningState:     "",
			requestedVersionID:  "",
			expectedVersionID:   "",
			expectedShouldCheck: true,
		},
		{
			name:                "unknown versioning state without version id is skipped",
			versioningState:     "UnexpectedState",
			requestedVersionID:  "",
			expectedVersionID:   "",
			expectedShouldCheck: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			versionID, shouldCheck := objectLockVersionToCheckForDelete(tc.versioningState, tc.requestedVersionID)
			assert.Equal(t, tc.expectedVersionID, versionID)
			assert.Equal(t, tc.expectedShouldCheck, shouldCheck)
		})
	}
}
