package consumer_offset

import (
	"fmt"
)

// TopicPartition uniquely identifies a topic partition
type TopicPartition struct {
	Topic     string
	Partition int32
}

// OffsetMetadata contains offset and associated metadata
type OffsetMetadata struct {
	Offset   int64
	Metadata string
}

// String returns a string representation of TopicPartition
func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// OffsetStorage defines the interface for storing and retrieving consumer offsets
type OffsetStorage interface {
	// CommitOffset commits an offset for a consumer group, topic, and partition
	// offset is the next offset to read (Kafka convention)
	// metadata is optional application-specific data
	CommitOffset(group, topic string, partition int32, offset int64, metadata string) error

	// FetchOffset fetches the committed offset for a consumer group, topic, and partition
	// Returns -1 if no offset has been committed
	// Returns error if the group or topic doesn't exist (depending on implementation)
	FetchOffset(group, topic string, partition int32) (int64, string, error)

	// FetchAllOffsets fetches all committed offsets for a consumer group
	// Returns map of TopicPartition to OffsetMetadata
	// Returns empty map if group doesn't exist
	FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error)

	// DeleteGroup deletes all offset data for a consumer group
	DeleteGroup(group string) error

	// ListGroups returns all consumer group IDs
	ListGroups() ([]string, error)

	// Close releases any resources held by the storage
	Close() error
}

// Common errors
var (
	ErrGroupNotFound    = fmt.Errorf("consumer group not found")
	ErrOffsetNotFound   = fmt.Errorf("offset not found")
	ErrInvalidOffset    = fmt.Errorf("invalid offset value")
	ErrInvalidPartition = fmt.Errorf("invalid partition")
	ErrStorageClosed    = fmt.Errorf("storage is closed")
)
