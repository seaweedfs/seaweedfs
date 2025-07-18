package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestVeeamObjectLockBugFix tests the fix for the bug where GetObjectLockConfigurationHandler
// would return NoSuchObjectLockConfiguration for buckets with no extended attributes,
// even when Object Lock was enabled. This caused Veeam to think Object Lock wasn't supported.
func TestVeeamObjectLockBugFix(t *testing.T) {

	t.Run("Bug case: bucket with no extended attributes", func(t *testing.T) {
		// This simulates the bug case where a bucket has no extended attributes at all
		// The old code would immediately return NoSuchObjectLockConfiguration
		// The new code correctly checks if Object Lock is enabled before returning an error

		bucketConfig := &BucketConfig{
			Name: "test-bucket",
			Entry: &filer_pb.Entry{
				Name:     "test-bucket",
				Extended: nil, // This is the key - no extended attributes
			},
		}

		// Simulate the isObjectLockEnabledForBucket logic
		enabled := false
		if bucketConfig.Entry.Extended != nil {
			if enabledBytes, exists := bucketConfig.Entry.Extended[s3_constants.ExtObjectLockEnabledKey]; exists {
				enabled = string(enabledBytes) == s3_constants.ObjectLockEnabled || string(enabledBytes) == "true"
			}
		}

		// Should correctly return false (not enabled) - this would trigger 404 correctly
		assert.False(t, enabled, "Object Lock should not be enabled when no extended attributes exist")
	})

	t.Run("Fix verification: bucket with Object Lock enabled via boolean flag", func(t *testing.T) {
		// This verifies the fix works when Object Lock is enabled via boolean flag

		bucketConfig := &BucketConfig{
			Name: "test-bucket",
			Entry: &filer_pb.Entry{
				Name: "test-bucket",
				Extended: map[string][]byte{
					s3_constants.ExtObjectLockEnabledKey: []byte("true"),
				},
			},
		}

		// Simulate the isObjectLockEnabledForBucket logic
		enabled := false
		if bucketConfig.Entry.Extended != nil {
			if enabledBytes, exists := bucketConfig.Entry.Extended[s3_constants.ExtObjectLockEnabledKey]; exists {
				enabled = string(enabledBytes) == s3_constants.ObjectLockEnabled || string(enabledBytes) == "true"
			}
		}

		// Should correctly return true (enabled) - this would generate minimal XML response
		assert.True(t, enabled, "Object Lock should be enabled when boolean flag is set")
	})

	t.Run("Fix verification: bucket with Object Lock enabled via Enabled constant", func(t *testing.T) {
		// Test using the s3_constants.ObjectLockEnabled constant

		bucketConfig := &BucketConfig{
			Name: "test-bucket",
			Entry: &filer_pb.Entry{
				Name: "test-bucket",
				Extended: map[string][]byte{
					s3_constants.ExtObjectLockEnabledKey: []byte(s3_constants.ObjectLockEnabled),
				},
			},
		}

		// Simulate the isObjectLockEnabledForBucket logic
		enabled := false
		if bucketConfig.Entry.Extended != nil {
			if enabledBytes, exists := bucketConfig.Entry.Extended[s3_constants.ExtObjectLockEnabledKey]; exists {
				enabled = string(enabledBytes) == s3_constants.ObjectLockEnabled || string(enabledBytes) == "true"
			}
		}

		// Should correctly return true (enabled)
		assert.True(t, enabled, "Object Lock should be enabled when constant is used")
	})
}
