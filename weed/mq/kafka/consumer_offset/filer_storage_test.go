package consumer_offset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Note: These tests require a running filer instance
// They are marked as integration tests and should be run with:
// go test -tags=integration

func TestFilerStorageCommitAndFetch(t *testing.T) {
	t.Skip("Requires running filer - integration test")

	// This will be implemented once we have test infrastructure
	// Test will:
	// 1. Create filer storage
	// 2. Commit offset
	// 3. Fetch offset
	// 4. Verify values match
}

func TestFilerStoragePersistence(t *testing.T) {
	t.Skip("Requires running filer - integration test")

	// Test will:
	// 1. Commit offset with first storage instance
	// 2. Close first instance
	// 3. Create new storage instance
	// 4. Fetch offset and verify it persisted
}

func TestFilerStorageMultipleGroups(t *testing.T) {
	t.Skip("Requires running filer - integration test")

	// Test will:
	// 1. Commit offsets for multiple groups
	// 2. Fetch all offsets per group
	// 3. Verify isolation between groups
}

func TestFilerStoragePath(t *testing.T) {
	// Test path generation (doesn't require filer)
	storage := &FilerStorage{}

	group := "test-group"
	topic := "test-topic"
	partition := int32(5)

	groupPath := storage.getGroupPath(group)
	assert.Equal(t, ConsumerOffsetsBasePath+"/test-group", groupPath)

	topicPath := storage.getTopicPath(group, topic)
	assert.Equal(t, ConsumerOffsetsBasePath+"/test-group/test-topic", topicPath)

	partitionPath := storage.getPartitionPath(group, topic, partition)
	assert.Equal(t, ConsumerOffsetsBasePath+"/test-group/test-topic/5", partitionPath)

	offsetPath := storage.getOffsetPath(group, topic, partition)
	assert.Equal(t, ConsumerOffsetsBasePath+"/test-group/test-topic/5/offset", offsetPath)

	metadataPath := storage.getMetadataPath(group, topic, partition)
	assert.Equal(t, ConsumerOffsetsBasePath+"/test-group/test-topic/5/metadata", metadataPath)
}
