package protocol

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer_offset"
)

// offsetStorageAdapter adapts consumer_offset.OffsetStorage to ConsumerOffsetStorage interface
type offsetStorageAdapter struct {
	storage consumer_offset.OffsetStorage
}

// newOffsetStorageAdapter creates a new adapter
func newOffsetStorageAdapter(storage consumer_offset.OffsetStorage) ConsumerOffsetStorage {
	return &offsetStorageAdapter{storage: storage}
}

func (a *offsetStorageAdapter) CommitOffset(group, topic string, partition int32, offset int64, metadata string) error {
	return a.storage.CommitOffset(group, topic, partition, offset, metadata)
}

func (a *offsetStorageAdapter) FetchOffset(group, topic string, partition int32) (int64, string, error) {
	return a.storage.FetchOffset(group, topic, partition)
}

func (a *offsetStorageAdapter) FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error) {
	offsets, err := a.storage.FetchAllOffsets(group)
	if err != nil {
		return nil, err
	}

	// Convert from consumer_offset types to protocol types
	result := make(map[TopicPartition]OffsetMetadata, len(offsets))
	for tp, om := range offsets {
		result[TopicPartition{Topic: tp.Topic, Partition: tp.Partition}] = OffsetMetadata{
			Offset:   om.Offset,
			Metadata: om.Metadata,
		}
	}

	return result, nil
}

func (a *offsetStorageAdapter) DeleteGroup(group string) error {
	return a.storage.DeleteGroup(group)
}

func (a *offsetStorageAdapter) Close() error {
	return a.storage.Close()
}
