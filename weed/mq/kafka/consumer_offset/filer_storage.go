package consumer_offset

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// FilerStorage implements OffsetStorage using SeaweedFS filer
// Offsets are stored in: /kafka/consumer_offsets/{group}/{topic}/{partition}/offset
// Metadata is stored in: /kafka/consumer_offsets/{group}/{topic}/{partition}/metadata
type FilerStorage struct {
	fca    *filer_client.FilerClientAccessor
	closed bool
}

// NewFilerStorage creates a new filer-based offset storage
func NewFilerStorage(fca *filer_client.FilerClientAccessor) *FilerStorage {
	return &FilerStorage{
		fca:    fca,
		closed: false,
	}
}

// CommitOffset commits an offset for a consumer group
func (f *FilerStorage) CommitOffset(group, topic string, partition int32, offset int64, metadata string) error {
	if f.closed {
		return ErrStorageClosed
	}

	// Validate inputs
	if offset < -1 {
		return ErrInvalidOffset
	}
	if partition < 0 {
		return ErrInvalidPartition
	}

	offsetPath := f.getOffsetPath(group, topic, partition)
	metadataPath := f.getMetadataPath(group, topic, partition)

	// Store offset
	offsetData := fmt.Sprintf("%d", offset)
	if err := f.writeFile(offsetPath, []byte(offsetData)); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}

	// Store metadata
	if err := f.writeFile(metadataPath, []byte(metadata)); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// FetchOffset fetches the committed offset for a consumer group
func (f *FilerStorage) FetchOffset(group, topic string, partition int32) (int64, string, error) {
	if f.closed {
		return -1, "", ErrStorageClosed
	}

	offsetPath := f.getOffsetPath(group, topic, partition)
	metadataPath := f.getMetadataPath(group, topic, partition)

	// Read offset
	offsetData, err := f.readFile(offsetPath)
	if err != nil {
		// File doesn't exist, no offset committed
		return -1, "", nil
	}

	var offset int64
	_, err = fmt.Sscanf(string(offsetData), "%d", &offset)
	if err != nil {
		return -1, "", fmt.Errorf("failed to parse offset: %w", err)
	}

	// Read metadata
	metadataData, err := f.readFile(metadataPath)
	metadata := ""
	if err == nil {
		metadata = string(metadataData)
	}

	return offset, metadata, nil
}

// FetchAllOffsets fetches all committed offsets for a consumer group
func (f *FilerStorage) FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error) {
	if f.closed {
		return nil, ErrStorageClosed
	}

	result := make(map[TopicPartition]OffsetMetadata)
	groupPath := f.getGroupPath(group)

	// List all topics for this group
	topics, err := f.listDirectory(groupPath)
	if err != nil {
		// Group doesn't exist, return empty map
		return result, nil
	}

	// For each topic, list all partitions
	for _, topicName := range topics {
		topicPath := fmt.Sprintf("%s/%s", groupPath, topicName)
		partitions, err := f.listDirectory(topicPath)
		if err != nil {
			continue
		}

		// For each partition, read the offset
		for _, partitionName := range partitions {
			var partition int32
			_, err := fmt.Sscanf(partitionName, "%d", &partition)
			if err != nil {
				continue
			}

			offset, metadata, err := f.FetchOffset(group, topicName, partition)
			if err == nil && offset >= 0 {
				tp := TopicPartition{Topic: topicName, Partition: partition}
				result[tp] = OffsetMetadata{Offset: offset, Metadata: metadata}
			}
		}
	}

	return result, nil
}

// DeleteGroup deletes all offset data for a consumer group
func (f *FilerStorage) DeleteGroup(group string) error {
	if f.closed {
		return ErrStorageClosed
	}

	groupPath := f.getGroupPath(group)
	return f.deleteDirectory(groupPath)
}

// ListGroups returns all consumer group IDs
func (f *FilerStorage) ListGroups() ([]string, error) {
	if f.closed {
		return nil, ErrStorageClosed
	}

	basePath := "/kafka/consumer_offsets"
	return f.listDirectory(basePath)
}

// Close releases resources
func (f *FilerStorage) Close() error {
	f.closed = true
	return nil
}

// Helper methods

func (f *FilerStorage) getGroupPath(group string) string {
	return fmt.Sprintf("/kafka/consumer_offsets/%s", group)
}

func (f *FilerStorage) getTopicPath(group, topic string) string {
	return fmt.Sprintf("%s/%s", f.getGroupPath(group), topic)
}

func (f *FilerStorage) getPartitionPath(group, topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", f.getTopicPath(group, topic), partition)
}

func (f *FilerStorage) getOffsetPath(group, topic string, partition int32) string {
	return fmt.Sprintf("%s/offset", f.getPartitionPath(group, topic, partition))
}

func (f *FilerStorage) getMetadataPath(group, topic string, partition int32) string {
	return fmt.Sprintf("%s/metadata", f.getPartitionPath(group, topic, partition))
}

func (f *FilerStorage) writeFile(path string, data []byte) error {
	fullPath := util.FullPath(path)
	dir, name := fullPath.DirAndName()

	return f.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Create entry
		entry := &filer_pb.Entry{
			Name:        name,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Crtime:   time.Now().Unix(),
				Mtime:    time.Now().Unix(),
				FileMode: 0644,
				FileSize: uint64(len(data)),
			},
			Chunks: []*filer_pb.FileChunk{},
		}

		// For small files, store inline
		if len(data) > 0 {
			entry.Content = data
		}

		// Create or update the entry
		return filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
	})
}

func (f *FilerStorage) readFile(path string) ([]byte, error) {
	fullPath := util.FullPath(path)
	dir, name := fullPath.DirAndName()

	var data []byte
	err := f.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get the entry
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}

		entry := resp.Entry
		if entry.IsDirectory {
			return fmt.Errorf("path is a directory")
		}

		// Read inline content if available
		if len(entry.Content) > 0 {
			data = entry.Content
			return nil
		}

		// If no chunks, file is empty
		if len(entry.Chunks) == 0 {
			data = []byte{}
			return nil
		}

		return fmt.Errorf("chunked files not supported for offset storage")
	})

	return data, err
}

func (f *FilerStorage) listDirectory(path string) ([]string, error) {
	var entries []string

	err := f.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: path,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			if resp.Entry.IsDirectory {
				entries = append(entries, resp.Entry.Name)
			}
		}

		return nil
	})

	return entries, err
}

func (f *FilerStorage) deleteDirectory(path string) error {
	fullPath := util.FullPath(path)
	dir, name := fullPath.DirAndName()

	return f.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:            dir,
			Name:                 name,
			IsDeleteData:         true,
			IsRecursive:          true,
			IgnoreRecursiveError: true,
		})
		return err
	})
}

// normalizePath removes leading/trailing slashes and collapses multiple slashes
func normalizePath(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	normalized := []string{}
	for _, part := range parts {
		if part != "" {
			normalized = append(normalized, part)
		}
	}
	return "/" + strings.Join(normalized, "/")
}
