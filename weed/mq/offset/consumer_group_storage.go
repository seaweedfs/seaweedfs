package offset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ConsumerGroupPosition represents a consumer's position in a partition
// This can be either a timestamp or an offset
type ConsumerGroupPosition struct {
	Type        string `json:"type"`         // "offset" or "timestamp"
	Value       int64  `json:"value"`        // The actual offset or timestamp value
	OffsetType  string `json:"offset_type"`  // Optional: OffsetType enum name (e.g., "EXACT_OFFSET")
	CommittedAt int64  `json:"committed_at"` // Unix timestamp in milliseconds when committed
	Metadata    string `json:"metadata"`     // Optional: application-specific metadata
}

// ConsumerGroupOffsetStorage handles consumer group offset persistence
// Each consumer group gets its own offset file in a dedicated consumers/ subfolder:
// Path: /topics/{namespace}/{topic}/{version}/{partition}/consumers/{consumer_group}.offset
type ConsumerGroupOffsetStorage interface {
	// SaveConsumerGroupOffset saves the committed offset for a consumer group
	SaveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error

	// SaveConsumerGroupPosition saves the committed position (offset or timestamp) for a consumer group
	SaveConsumerGroupPosition(t topic.Topic, p topic.Partition, consumerGroup string, position *ConsumerGroupPosition) error

	// LoadConsumerGroupOffset loads the committed offset for a consumer group (backward compatible)
	LoadConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) (int64, error)

	// LoadConsumerGroupPosition loads the committed position for a consumer group
	LoadConsumerGroupPosition(t topic.Topic, p topic.Partition, consumerGroup string) (*ConsumerGroupPosition, error)

	// ListConsumerGroups returns all consumer groups for a topic partition
	ListConsumerGroups(t topic.Topic, p topic.Partition) ([]string, error)

	// DeleteConsumerGroupOffset removes the offset file for a consumer group
	DeleteConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) error
}

// FilerConsumerGroupOffsetStorage implements ConsumerGroupOffsetStorage using SeaweedFS filer
type FilerConsumerGroupOffsetStorage struct {
	filerClientAccessor *filer_client.FilerClientAccessor
}

// NewFilerConsumerGroupOffsetStorageWithAccessor creates storage using a shared filer client accessor
func NewFilerConsumerGroupOffsetStorageWithAccessor(filerClientAccessor *filer_client.FilerClientAccessor) *FilerConsumerGroupOffsetStorage {
	return &FilerConsumerGroupOffsetStorage{
		filerClientAccessor: filerClientAccessor,
	}
}

// SaveConsumerGroupOffset saves the committed offset for a consumer group
// Stores as: /topics/{namespace}/{topic}/{version}/{partition}/consumers/{consumer_group}.offset
// This is a convenience method that wraps SaveConsumerGroupPosition
func (f *FilerConsumerGroupOffsetStorage) SaveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error {
	position := &ConsumerGroupPosition{
		Type:        "offset",
		Value:       offset,
		OffsetType:  schema_pb.OffsetType_EXACT_OFFSET.String(),
		CommittedAt: time.Now().UnixMilli(),
	}
	return f.SaveConsumerGroupPosition(t, p, consumerGroup, position)
}

// SaveConsumerGroupPosition saves the committed position (offset or timestamp) for a consumer group
// Stores as JSON: /topics/{namespace}/{topic}/{version}/{partition}/consumers/{consumer_group}.offset
func (f *FilerConsumerGroupOffsetStorage) SaveConsumerGroupPosition(t topic.Topic, p topic.Partition, consumerGroup string, position *ConsumerGroupPosition) error {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	// Marshal position to JSON
	jsonBytes, err := json.Marshal(position)
	if err != nil {
		return fmt.Errorf("failed to marshal position to JSON: %w", err)
	}

	return f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, consumersDir, offsetFileName, jsonBytes)
	})
}

// LoadConsumerGroupOffset loads the committed offset for a consumer group
// This method provides backward compatibility and returns just the offset value
func (f *FilerConsumerGroupOffsetStorage) LoadConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) (int64, error) {
	position, err := f.LoadConsumerGroupPosition(t, p, consumerGroup)
	if err != nil {
		return -1, err
	}
	return position.Value, nil
}

// LoadConsumerGroupPosition loads the committed position for a consumer group
func (f *FilerConsumerGroupOffsetStorage) LoadConsumerGroupPosition(t topic.Topic, p topic.Partition, consumerGroup string) (*ConsumerGroupPosition, error) {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	var position *ConsumerGroupPosition
	err := f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, consumersDir, offsetFileName)
		if err != nil {
			return err
		}

		// Parse JSON format
		position = &ConsumerGroupPosition{}
		if err := json.Unmarshal(data, position); err != nil {
			return fmt.Errorf("invalid consumer group offset file format: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return position, nil
}

// ListConsumerGroups returns all consumer groups for a topic partition
func (f *FilerConsumerGroupOffsetStorage) ListConsumerGroups(t topic.Topic, p topic.Partition) ([]string, error) {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	var consumerGroups []string

	err := f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Use ListEntries to get directory contents
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: consumersDir,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			entry := resp.Entry
			if entry != nil && !entry.IsDirectory && entry.Name != "" {
				// Check if this is a consumer group offset file (ends with .offset)
				if len(entry.Name) > 7 && entry.Name[len(entry.Name)-7:] == ".offset" {
					// Extract consumer group name (remove .offset suffix)
					consumerGroup := entry.Name[:len(entry.Name)-7]
					consumerGroups = append(consumerGroups, consumerGroup)
				}
			}
		}
		return nil
	})

	return consumerGroups, err
}

// DeleteConsumerGroupOffset removes the offset file for a consumer group
func (f *FilerConsumerGroupOffsetStorage) DeleteConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) error {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	return f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(context.Background(), client, consumersDir, offsetFileName, false, false, false, false, nil, false, "")
	})
}
