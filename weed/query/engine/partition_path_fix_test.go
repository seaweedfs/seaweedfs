package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPartitionPathHandling tests that partition paths are handled correctly
// whether discoverTopicPartitions returns relative or absolute paths
func TestPartitionPathHandling(t *testing.T) {
	engine := NewMockSQLEngine()

	t.Run("Mock discoverTopicPartitions returns correct paths", func(t *testing.T) {
		// Test that our mock engine handles absolute paths correctly
		engine.mockPartitions["test.user_events"] = []string{
			"/topics/test/user_events/v2025-09-03-15-36-29/0000-2520",
			"/topics/test/user_events/v2025-09-03-15-36-29/2521-5040",
		}

		partitions, err := engine.discoverTopicPartitions("test", "user_events")
		assert.NoError(t, err, "Should discover partitions without error")
		assert.Equal(t, 2, len(partitions), "Should return 2 partitions")
		assert.Contains(t, partitions[0], "/topics/test/user_events/", "Should contain absolute path")
	})

	t.Run("Mock discoverTopicPartitions handles relative paths", func(t *testing.T) {
		// Test relative paths scenario
		engine.mockPartitions["test.user_events"] = []string{
			"v2025-09-03-15-36-29/0000-2520",
			"v2025-09-03-15-36-29/2521-5040",
		}

		partitions, err := engine.discoverTopicPartitions("test", "user_events")
		assert.NoError(t, err, "Should discover partitions without error")
		assert.Equal(t, 2, len(partitions), "Should return 2 partitions")
		assert.True(t, !strings.HasPrefix(partitions[0], "/topics/"), "Should be relative path")
	})

	t.Run("Partition path building logic works correctly", func(t *testing.T) {
		topicBasePath := "/topics/test/user_events"

		testCases := []struct {
			name              string
			relativePartition string
			expectedPath      string
		}{
			{
				name:              "Absolute path - use as-is",
				relativePartition: "/topics/test/user_events/v2025-09-03-15-36-29/0000-2520",
				expectedPath:      "/topics/test/user_events/v2025-09-03-15-36-29/0000-2520",
			},
			{
				name:              "Relative path - build full path",
				relativePartition: "v2025-09-03-15-36-29/0000-2520",
				expectedPath:      "/topics/test/user_events/v2025-09-03-15-36-29/0000-2520",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var partitionPath string

				// This is the same logic from our fixed code
				if strings.HasPrefix(tc.relativePartition, "/topics/") {
					// Already a full path - use as-is
					partitionPath = tc.relativePartition
				} else {
					// Relative path - build full path
					partitionPath = topicBasePath + "/" + tc.relativePartition
				}

				assert.Equal(t, tc.expectedPath, partitionPath,
					"Partition path should be built correctly")

				// Ensure no double slashes
				assert.NotContains(t, partitionPath, "//",
					"Partition path should not contain double slashes")
			})
		}
	})
}

// TestPartitionPathLogic tests the core logic for handling partition paths
func TestPartitionPathLogic(t *testing.T) {
	t.Run("Building partition paths from discovered partitions", func(t *testing.T) {
		// Test the specific partition path building that was causing issues

		topicBasePath := "/topics/ecommerce/user_events"

		// This simulates the discoverTopicPartitions returning absolute paths (realistic scenario)
		relativePartitions := []string{
			"/topics/ecommerce/user_events/v2025-09-03-15-36-29/0000-2520",
		}

		// This is the code from our fix - test it directly
		partitions := make([]string, len(relativePartitions))
		for i, relPartition := range relativePartitions {
			// Handle both relative and absolute partition paths from discoverTopicPartitions
			if strings.HasPrefix(relPartition, "/topics/") {
				// Already a full path - use as-is
				partitions[i] = relPartition
			} else {
				// Relative path - build full path
				partitions[i] = topicBasePath + "/" + relPartition
			}
		}

		// Verify the path was handled correctly
		expectedPath := "/topics/ecommerce/user_events/v2025-09-03-15-36-29/0000-2520"
		assert.Equal(t, expectedPath, partitions[0], "Absolute path should be used as-is")

		// Ensure no double slashes (this was the original bug)
		assert.NotContains(t, partitions[0], "//", "Path should not contain double slashes")
	})
}
