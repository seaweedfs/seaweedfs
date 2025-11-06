package consumer_offset

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorageCommitAndFetch(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	group := "test-group"
	topic := "test-topic"
	partition := int32(0)
	offset := int64(42)
	metadata := "test-metadata"

	// Commit offset
	err := storage.CommitOffset(group, topic, partition, offset, metadata)
	require.NoError(t, err)

	// Fetch offset
	fetchedOffset, fetchedMetadata, err := storage.FetchOffset(group, topic, partition)
	require.NoError(t, err)
	assert.Equal(t, offset, fetchedOffset)
	assert.Equal(t, metadata, fetchedMetadata)
}

func TestMemoryStorageFetchNonExistent(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	// Fetch offset for non-existent group
	offset, metadata, err := storage.FetchOffset("non-existent", "topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset)
	assert.Equal(t, "", metadata)
}

func TestMemoryStorageFetchAllOffsets(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	group := "test-group"

	// Commit offsets for multiple partitions
	err := storage.CommitOffset(group, "topic1", 0, 10, "meta1")
	require.NoError(t, err)
	err = storage.CommitOffset(group, "topic1", 1, 20, "meta2")
	require.NoError(t, err)
	err = storage.CommitOffset(group, "topic2", 0, 30, "meta3")
	require.NoError(t, err)

	// Fetch all offsets
	offsets, err := storage.FetchAllOffsets(group)
	require.NoError(t, err)
	assert.Equal(t, 3, len(offsets))

	// Verify each offset
	tp1 := TopicPartition{Topic: "topic1", Partition: 0}
	assert.Equal(t, int64(10), offsets[tp1].Offset)
	assert.Equal(t, "meta1", offsets[tp1].Metadata)

	tp2 := TopicPartition{Topic: "topic1", Partition: 1}
	assert.Equal(t, int64(20), offsets[tp2].Offset)

	tp3 := TopicPartition{Topic: "topic2", Partition: 0}
	assert.Equal(t, int64(30), offsets[tp3].Offset)
}

func TestMemoryStorageDeleteGroup(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	group := "test-group"

	// Commit offset
	err := storage.CommitOffset(group, "topic", 0, 100, "")
	require.NoError(t, err)

	// Verify offset exists
	offset, _, err := storage.FetchOffset(group, "topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offset)

	// Delete group
	err = storage.DeleteGroup(group)
	require.NoError(t, err)

	// Verify offset is gone
	offset, _, err = storage.FetchOffset(group, "topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset)
}

func TestMemoryStorageListGroups(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	// Initially empty
	groups, err := storage.ListGroups()
	require.NoError(t, err)
	assert.Equal(t, 0, len(groups))

	// Commit offsets for multiple groups
	err = storage.CommitOffset("group1", "topic", 0, 10, "")
	require.NoError(t, err)
	err = storage.CommitOffset("group2", "topic", 0, 20, "")
	require.NoError(t, err)
	err = storage.CommitOffset("group3", "topic", 0, 30, "")
	require.NoError(t, err)

	// List groups
	groups, err = storage.ListGroups()
	require.NoError(t, err)
	assert.Equal(t, 3, len(groups))
	assert.Contains(t, groups, "group1")
	assert.Contains(t, groups, "group2")
	assert.Contains(t, groups, "group3")
}

func TestMemoryStorageConcurrency(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	group := "concurrent-group"
	topic := "topic"
	numGoroutines := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines to commit offsets concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(partition int32, offset int64) {
			defer wg.Done()
			err := storage.CommitOffset(group, topic, partition, offset, "")
			assert.NoError(t, err)
		}(int32(i%10), int64(i))
	}

	wg.Wait()

	// Verify we can fetch offsets without errors
	offsets, err := storage.FetchAllOffsets(group)
	require.NoError(t, err)
	assert.Greater(t, len(offsets), 0)
}

func TestMemoryStorageInvalidInputs(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	// Invalid offset (less than -1)
	err := storage.CommitOffset("group", "topic", 0, -2, "")
	assert.ErrorIs(t, err, ErrInvalidOffset)

	// Invalid partition (negative)
	err = storage.CommitOffset("group", "topic", -1, 10, "")
	assert.ErrorIs(t, err, ErrInvalidPartition)
}

func TestMemoryStorageClosedOperations(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Close()

	// Operations on closed storage should return error
	err := storage.CommitOffset("group", "topic", 0, 10, "")
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, _, err = storage.FetchOffset("group", "topic", 0)
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = storage.FetchAllOffsets("group")
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = storage.DeleteGroup("group")
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = storage.ListGroups()
	assert.ErrorIs(t, err, ErrStorageClosed)
}

func TestMemoryStorageOverwrite(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	group := "test-group"
	topic := "topic"
	partition := int32(0)

	// Commit initial offset
	err := storage.CommitOffset(group, topic, partition, 10, "meta1")
	require.NoError(t, err)

	// Overwrite with new offset
	err = storage.CommitOffset(group, topic, partition, 20, "meta2")
	require.NoError(t, err)

	// Fetch should return latest offset
	offset, metadata, err := storage.FetchOffset(group, topic, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(20), offset)
	assert.Equal(t, "meta2", metadata)
}
