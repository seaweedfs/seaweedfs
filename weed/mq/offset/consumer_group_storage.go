package offset

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ConsumerGroupOffsetStorage handles consumer group offset persistence
// Each consumer group gets its own offset file in a dedicated consumers/ subfolder:
// Path: /topics/{namespace}/{topic}/{version}/{partition}/consumers/{consumer_group}.offset
type ConsumerGroupOffsetStorage interface {
	// SaveConsumerGroupOffset saves the committed offset for a consumer group
	SaveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error

	// LoadConsumerGroupOffset loads the committed offset for a consumer group
	LoadConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) (int64, error)

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
func (f *FilerConsumerGroupOffsetStorage) SaveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	// Use SMQ's 8-byte offset format
	offsetBytes := make([]byte, 8)
	util.Uint64toBytes(offsetBytes, uint64(offset))

	return f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, consumersDir, offsetFileName, offsetBytes)
	})
}

// LoadConsumerGroupOffset loads the committed offset for a consumer group
func (f *FilerConsumerGroupOffsetStorage) LoadConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) (int64, error) {
	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	var offset int64 = -1
	err := f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, consumersDir, offsetFileName)
		if err != nil {
			return err
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid consumer group offset file format: expected 8 bytes, got %d", len(data))
		}
		offset = int64(util.BytesToUint64(data))
		return nil
	})

	if err != nil {
		return -1, err
	}

	return offset, nil
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
		return filer_pb.DoRemove(context.Background(), client, consumersDir, offsetFileName, false, false, false, false, nil)
	})
}
