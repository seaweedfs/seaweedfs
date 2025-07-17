package dash

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// TopicRetentionPurger handles topic data purging based on retention policies
type TopicRetentionPurger struct {
	adminServer *AdminServer
}

// NewTopicRetentionPurger creates a new topic retention purger
func NewTopicRetentionPurger(adminServer *AdminServer) *TopicRetentionPurger {
	return &TopicRetentionPurger{
		adminServer: adminServer,
	}
}

// PurgeExpiredTopicData purges expired topic data based on retention policies
func (p *TopicRetentionPurger) PurgeExpiredTopicData() error {
	glog.V(1).Infof("Starting topic data purge based on retention policies")

	// Get all topics with retention enabled
	topics, err := p.getTopicsWithRetention()
	if err != nil {
		return fmt.Errorf("failed to get topics with retention: %w", err)
	}

	glog.V(1).Infof("Found %d topics with retention enabled", len(topics))

	// Process each topic
	for _, topicRetention := range topics {
		err := p.purgeTopicData(topicRetention)
		if err != nil {
			glog.Errorf("Failed to purge data for topic %s: %v", topicRetention.TopicName, err)
			continue
		}
	}

	glog.V(1).Infof("Completed topic data purge")
	return nil
}

// TopicRetentionConfig represents a topic with its retention configuration
type TopicRetentionConfig struct {
	TopicName        string
	Namespace        string
	Name             string
	RetentionSeconds int64
}

// getTopicsWithRetention retrieves all topics that have retention enabled
func (p *TopicRetentionPurger) getTopicsWithRetention() ([]TopicRetentionConfig, error) {
	var topicsWithRetention []TopicRetentionConfig

	// Find broker leader to get topics
	brokerLeader, err := p.adminServer.findBrokerLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to find broker leader: %w", err)
	}

	// Get all topics from the broker
	err = p.adminServer.withBrokerClient(brokerLeader, func(client mq_pb.SeaweedMessagingClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.ListTopics(ctx, &mq_pb.ListTopicsRequest{})
		if err != nil {
			return err
		}

		// Check each topic for retention configuration
		for _, pbTopic := range resp.Topics {
			configResp, err := client.GetTopicConfiguration(ctx, &mq_pb.GetTopicConfigurationRequest{
				Topic: pbTopic,
			})
			if err != nil {
				glog.Warningf("Failed to get configuration for topic %s.%s: %v", pbTopic.Namespace, pbTopic.Name, err)
				continue
			}

			// Check if retention is enabled
			if configResp.Retention != nil && configResp.Retention.Enabled && configResp.Retention.RetentionSeconds > 0 {
				topicRetention := TopicRetentionConfig{
					TopicName:        fmt.Sprintf("%s.%s", pbTopic.Namespace, pbTopic.Name),
					Namespace:        pbTopic.Namespace,
					Name:             pbTopic.Name,
					RetentionSeconds: configResp.Retention.RetentionSeconds,
				}
				topicsWithRetention = append(topicsWithRetention, topicRetention)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return topicsWithRetention, nil
}

// purgeTopicData purges expired data for a specific topic
func (p *TopicRetentionPurger) purgeTopicData(topicRetention TopicRetentionConfig) error {
	glog.V(1).Infof("Purging expired data for topic %s with retention %d seconds", topicRetention.TopicName, topicRetention.RetentionSeconds)

	// Calculate cutoff time
	cutoffTime := time.Now().Add(-time.Duration(topicRetention.RetentionSeconds) * time.Second)

	// Get topic directory
	topicObj := topic.NewTopic(topicRetention.Namespace, topicRetention.Name)
	topicDir := topicObj.Dir()

	var purgedDirs []string

	err := p.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List all version directories under the topic directory
		versionStream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          topicDir,
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000,
		})
		if err != nil {
			return fmt.Errorf("failed to list topic directory %s: %v", topicDir, err)
		}

		var versionDirs []VersionDirInfo

		// Collect all version directories
		for {
			versionResp, err := versionStream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to receive version entries: %w", err)
			}

			// Only process directories that are versions (start with "v")
			if versionResp.Entry.IsDirectory && strings.HasPrefix(versionResp.Entry.Name, "v") {
				versionTime, err := p.parseVersionTime(versionResp.Entry.Name)
				if err != nil {
					glog.Warningf("Failed to parse version time from %s: %v", versionResp.Entry.Name, err)
					continue
				}

				versionDirs = append(versionDirs, VersionDirInfo{
					Name:        versionResp.Entry.Name,
					VersionTime: versionTime,
					ModTime:     time.Unix(versionResp.Entry.Attributes.Mtime, 0),
				})
			}
		}

		// Sort version directories by time (oldest first)
		sort.Slice(versionDirs, func(i, j int) bool {
			return versionDirs[i].VersionTime.Before(versionDirs[j].VersionTime)
		})

		// Keep at least the most recent version directory, even if it's expired
		if len(versionDirs) <= 1 {
			glog.V(1).Infof("Topic %s has %d version directories, keeping all", topicRetention.TopicName, len(versionDirs))
			return nil
		}

		// Purge expired directories (keep the most recent one)
		for i := 0; i < len(versionDirs)-1; i++ {
			versionDir := versionDirs[i]

			// Check if this version directory is expired
			if versionDir.VersionTime.Before(cutoffTime) {
				dirPath := filepath.Join(topicDir, versionDir.Name)

				// Delete the entire version directory
				err := p.deleteDirectoryRecursively(client, dirPath)
				if err != nil {
					glog.Errorf("Failed to delete expired directory %s: %v", dirPath, err)
				} else {
					purgedDirs = append(purgedDirs, dirPath)
					glog.V(1).Infof("Purged expired directory: %s (created: %s)", dirPath, versionDir.VersionTime.Format("2006-01-02 15:04:05"))
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	if len(purgedDirs) > 0 {
		glog.V(0).Infof("Purged %d expired directories for topic %s", len(purgedDirs), topicRetention.TopicName)
	}

	return nil
}

// VersionDirInfo represents a version directory with its timestamp
type VersionDirInfo struct {
	Name        string
	VersionTime time.Time
	ModTime     time.Time
}

// parseVersionTime parses the version directory name to extract the timestamp
// Version format: v2025-01-10-05-44-34
func (p *TopicRetentionPurger) parseVersionTime(versionName string) (time.Time, error) {
	// Remove the 'v' prefix
	if !strings.HasPrefix(versionName, "v") {
		return time.Time{}, fmt.Errorf("invalid version format: %s", versionName)
	}

	timeStr := versionName[1:] // Remove 'v'

	// Parse the time format: 2025-01-10-05-44-34
	versionTime, err := time.Parse("2006-01-02-15-04-05", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse version time %s: %v", timeStr, err)
	}

	return versionTime, nil
}

// deleteDirectoryRecursively deletes a directory and all its contents
func (p *TopicRetentionPurger) deleteDirectoryRecursively(client filer_pb.SeaweedFilerClient, dirPath string) error {
	// List all entries in the directory
	stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
		Directory:          dirPath,
		Prefix:             "",
		StartFromFileName:  "",
		InclusiveStartFrom: false,
		Limit:              1000,
	})
	if err != nil {
		return fmt.Errorf("failed to list directory %s: %v", dirPath, err)
	}

	// Delete all entries
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to receive entries: %w", err)
		}

		entryPath := filepath.Join(dirPath, resp.Entry.Name)

		if resp.Entry.IsDirectory {
			// Recursively delete subdirectory
			err = p.deleteDirectoryRecursively(client, entryPath)
			if err != nil {
				return fmt.Errorf("failed to delete subdirectory %s: %v", entryPath, err)
			}
		} else {
			// Delete file
			_, err = client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
				Directory: dirPath,
				Name:      resp.Entry.Name,
			})
			if err != nil {
				return fmt.Errorf("failed to delete file %s: %v", entryPath, err)
			}
		}
	}

	// Delete the directory itself
	parentDir := filepath.Dir(dirPath)
	dirName := filepath.Base(dirPath)

	_, err = client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
		Directory: parentDir,
		Name:      dirName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete directory %s: %v", dirPath, err)
	}

	return nil
}
