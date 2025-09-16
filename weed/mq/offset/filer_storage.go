package offset

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// FilerOffsetStorage implements OffsetStorage using SeaweedFS filer
// Stores offset data as files in the same directory structure as SMQ
// Path: /topics/{namespace}/{topic}/{version}/{partition}/checkpoint.offset
type FilerOffsetStorage struct {
	filerClientAccessor *filer_client.FilerClientAccessor
	namespace           string
	topicName           string
}

// NewFilerOffsetStorage creates a new filer-based offset storage
func NewFilerOffsetStorage(filerAddress string, namespace string, topicName string, grpcDialOption grpc.DialOption) *FilerOffsetStorage {
	if grpcDialOption == nil {
		grpcDialOption = grpc.WithInsecure()
	}

	filerClientAccessor := &filer_client.FilerClientAccessor{
		GetFiler: func() pb.ServerAddress {
			return pb.ServerAddress(filerAddress)
		},
		GetGrpcDialOption: func() grpc.DialOption {
			return grpcDialOption
		},
	}

	return &FilerOffsetStorage{
		filerClientAccessor: filerClientAccessor,
		namespace:           namespace,
		topicName:           topicName,
	}
}

// SaveCheckpoint saves the checkpoint for a partition
// Stores as: /topics/{namespace}/{topic}/{version}/{partition}/checkpoint.offset
func (f *FilerOffsetStorage) SaveCheckpoint(partition *schema_pb.Partition, offset int64) error {
	partitionDir := f.getPartitionDir(partition)
	fileName := "checkpoint.offset"
	
	// Use SMQ's 8-byte offset format
	offsetBytes := make([]byte, 8)
	util.Uint64toBytes(offsetBytes, uint64(offset))

	return f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, partitionDir, fileName, offsetBytes)
	})
}

// LoadCheckpoint loads the checkpoint for a partition
func (f *FilerOffsetStorage) LoadCheckpoint(partition *schema_pb.Partition) (int64, error) {
	partitionDir := f.getPartitionDir(partition)
	fileName := "checkpoint.offset"

	var offset int64 = -1
	err := f.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, partitionDir, fileName)
		if err != nil {
			return err
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid checkpoint file format: expected 8 bytes, got %d", len(data))
		}
		offset = int64(util.BytesToUint64(data))
		return nil
	})

	if err != nil {
		return -1, err
	}

	return offset, nil
}

// GetHighestOffset returns the highest offset stored for a partition
// For filer storage, this is the same as the checkpoint since we don't store individual records
func (f *FilerOffsetStorage) GetHighestOffset(partition *schema_pb.Partition) (int64, error) {
	return f.LoadCheckpoint(partition)
}

// Reset clears all data for testing
func (f *FilerOffsetStorage) Reset() error {
	// For testing, we could delete all offset files, but this is dangerous
	// Instead, just return success - individual tests should clean up their own data
	return nil
}

// Helper methods

// getPartitionDir returns the directory path for a partition following SMQ convention
// Format: /topics/{namespace}/{topic}/{version}/{partition}
func (f *FilerOffsetStorage) getPartitionDir(partition *schema_pb.Partition) string {
	// Generate version from UnixTimeNs
	version := time.Unix(0, partition.UnixTimeNs).UTC().Format("v2006-01-02-15-04-05")
	
	// Generate partition range string
	partitionRange := fmt.Sprintf("%04d-%04d", partition.RangeStart, partition.RangeStop)
	
	return fmt.Sprintf("%s/%s/%s/%s/%s", filer.TopicsDir, f.namespace, f.topicName, version, partitionRange)
}

// getPartitionKey generates a unique key for a partition
func (f *FilerOffsetStorage) getPartitionKey(partition *schema_pb.Partition) string {
	return fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		partition.RingSize, partition.RangeStart, partition.RangeStop, partition.UnixTimeNs)
}
