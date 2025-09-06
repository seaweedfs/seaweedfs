package topic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

type Topic struct {
	Namespace string
	Name      string
}

func NewTopic(namespace string, name string) Topic {
	return Topic{
		Namespace: namespace,
		Name:      name,
	}
}
func FromPbTopic(topic *schema_pb.Topic) Topic {
	return Topic{
		Namespace: topic.Namespace,
		Name:      topic.Name,
	}
}

func (t Topic) ToPbTopic() *schema_pb.Topic {
	return &schema_pb.Topic{
		Namespace: t.Namespace,
		Name:      t.Name,
	}
}

func (t Topic) String() string {
	return fmt.Sprintf("%s.%s", t.Namespace, t.Name)
}

func (t Topic) Dir() string {
	return fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
}

func (t Topic) ReadConfFile(client filer_pb.SeaweedFilerClient) (*mq_pb.ConfigureTopicResponse, error) {
	data, err := filer.ReadInsideFiler(client, t.Dir(), filer.TopicConfFile)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read topic.conf of %v: %w", t, err)
	}
	// parse into filer conf object
	conf := &mq_pb.ConfigureTopicResponse{}
	if err = jsonpb.Unmarshal(data, conf); err != nil {
		return nil, fmt.Errorf("unmarshal topic %v conf: %w", t, err)
	}
	return conf, nil
}

// ReadConfFileWithMetadata reads the topic configuration and returns it along with file metadata
func (t Topic) ReadConfFileWithMetadata(client filer_pb.SeaweedFilerClient) (*mq_pb.ConfigureTopicResponse, int64, int64, error) {
	// Use LookupDirectoryEntry to get both content and metadata
	request := &filer_pb.LookupDirectoryEntryRequest{
		Directory: t.Dir(),
		Name:      filer.TopicConfFile,
	}

	resp, err := filer_pb.LookupEntry(context.Background(), client, request)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, 0, 0, err
		}
		return nil, 0, 0, fmt.Errorf("lookup topic.conf of %v: %w", t, err)
	}

	// Get file metadata
	var createdAtNs, modifiedAtNs int64
	if resp.Entry.Attributes != nil {
		createdAtNs = resp.Entry.Attributes.Crtime * 1e9 // convert seconds to nanoseconds
		modifiedAtNs = resp.Entry.Attributes.Mtime * 1e9 // convert seconds to nanoseconds
	}

	// Parse the configuration
	conf := &mq_pb.ConfigureTopicResponse{}
	if err = jsonpb.Unmarshal(resp.Entry.Content, conf); err != nil {
		return nil, 0, 0, fmt.Errorf("unmarshal topic %v conf: %w", t, err)
	}

	return conf, createdAtNs, modifiedAtNs, nil
}

func (t Topic) WriteConfFile(client filer_pb.SeaweedFilerClient, conf *mq_pb.ConfigureTopicResponse) error {
	var buf bytes.Buffer
	filer.ProtoToText(&buf, conf)
	if err := filer.SaveInsideFiler(client, t.Dir(), filer.TopicConfFile, buf.Bytes()); err != nil {
		return fmt.Errorf("save topic %v conf: %w", t, err)
	}
	return nil
}

// DiscoverPartitions discovers all partition directories for a topic by scanning the filesystem
// This centralizes partition discovery logic used across query engine, shell commands, etc.
func (t Topic) DiscoverPartitions(ctx context.Context, filerClient filer_pb.FilerClient) ([]string, error) {
	var partitionPaths []string

	// Scan the topic directory for version directories (e.g., v2025-09-01-07-16-34)
	err := filer_pb.ReadDirAllEntries(ctx, filerClient, util.FullPath(t.Dir()), "", func(versionEntry *filer_pb.Entry, isLast bool) error {
		if !versionEntry.IsDirectory {
			return nil // Skip non-directories
		}

		// Parse version timestamp from directory name (e.g., "v2025-09-01-07-16-34")
		if !IsValidVersionDirectory(versionEntry.Name) {
			// Skip directories that don't match the version format
			return nil
		}

		// Scan partition directories within this version (e.g., 0000-0630)
		versionDir := fmt.Sprintf("%s/%s", t.Dir(), versionEntry.Name)
		return filer_pb.ReadDirAllEntries(ctx, filerClient, util.FullPath(versionDir), "", func(partitionEntry *filer_pb.Entry, isLast bool) error {
			if !partitionEntry.IsDirectory {
				return nil // Skip non-directories
			}

			// Parse partition boundary from directory name (e.g., "0000-0630")
			if !IsValidPartitionDirectory(partitionEntry.Name) {
				return nil // Skip invalid partition names
			}

			// Add this partition path to the list
			partitionPath := fmt.Sprintf("%s/%s", versionDir, partitionEntry.Name)
			partitionPaths = append(partitionPaths, partitionPath)
			return nil
		})
	})

	return partitionPaths, err
}

// IsValidVersionDirectory checks if a directory name matches the topic version format
// Format: v2025-09-01-07-16-34
func IsValidVersionDirectory(name string) bool {
	if !strings.HasPrefix(name, "v") || len(name) != 20 {
		return false
	}

	// Try to parse the timestamp part
	timestampStr := name[1:] // Remove 'v' prefix
	_, err := time.Parse("2006-01-02-15-04-05", timestampStr)
	return err == nil
}

// IsValidPartitionDirectory checks if a directory name matches the partition boundary format
// Format: 0000-0630 (rangeStart-rangeStop)
func IsValidPartitionDirectory(name string) bool {
	// Use existing ParsePartitionBoundary function to validate
	start, stop := ParsePartitionBoundary(name)

	// Valid partition ranges should have start < stop (and not both be 0, which indicates parse error)
	return start < stop && start >= 0
}
