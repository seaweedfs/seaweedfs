package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) GetOrGenerateLocalPartition(t topic.Topic, partition topic.Partition) (localTopicPartition *topic.LocalPartition, getOrGenError error) {
	// get or generate a local partition
	topicKey := t.String()

	// Check cache first to avoid expensive filer reads (60% CPU overhead!)
	b.topicConfCacheMu.RLock()
	if entry, found := b.topicConfCache[topicKey]; found {
		if time.Now().Before(entry.expiresAt) {
			conf := entry.conf
			b.topicConfCacheMu.RUnlock()
			glog.V(4).Infof("TopicConf cache HIT for %s", topicKey)
			localTopicPartition, _, getOrGenError = b.doGetOrGenLocalPartition(t, partition, conf)
			if getOrGenError != nil {
				glog.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
				return nil, fmt.Errorf("topic %v partition %v not setup: %w", t, partition, getOrGenError)
			}
			return localTopicPartition, nil
		}
	}
	b.topicConfCacheMu.RUnlock()

	// Cache miss or expired - read from filer
	glog.V(4).Infof("TopicConf cache MISS for %s, reading from filer", topicKey)
	conf, readConfErr := b.fca.ReadTopicConfFromFiler(t)
	if readConfErr != nil {
		glog.Errorf("topic %v not found: %v", t, readConfErr)
		return nil, fmt.Errorf("topic %v not found: %w", t, readConfErr)
	}

	// Cache the result
	b.topicConfCacheMu.Lock()
	b.topicConfCache[topicKey] = &topicConfCacheEntry{
		conf:      conf,
		expiresAt: time.Now().Add(b.topicConfCacheTTL),
	}
	b.topicConfCacheMu.Unlock()
	glog.V(4).Infof("TopicConf cached for %s", topicKey)

	localTopicPartition, _, getOrGenError = b.doGetOrGenLocalPartition(t, partition, conf)
	if getOrGenError != nil {
		glog.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
		return nil, fmt.Errorf("topic %v partition %v not setup: %w", t, partition, getOrGenError)
	}
	return localTopicPartition, nil
}

// invalidateTopicConfCache removes a topic config from the cache
// Should be called when a topic configuration is updated
func (b *MessageQueueBroker) invalidateTopicConfCache(t topic.Topic) {
	topicKey := t.String()
	b.topicConfCacheMu.Lock()
	delete(b.topicConfCache, topicKey)
	b.topicConfCacheMu.Unlock()
	glog.V(4).Infof("Invalidated TopicConf cache for %s", topicKey)
}

func (b *MessageQueueBroker) doGetOrGenLocalPartition(t topic.Topic, partition topic.Partition, conf *mq_pb.ConfigureTopicResponse) (localPartition *topic.LocalPartition, isGenerated bool, err error) {
	b.accessLock.Lock()
	defer b.accessLock.Unlock()

	if localPartition = b.localTopicManager.GetLocalPartition(t, partition); localPartition == nil {
		localPartition, isGenerated, err = b.genLocalPartitionFromFiler(t, partition, conf)
		if err != nil {
			return nil, false, err
		}
	}
	return localPartition, isGenerated, nil
}

func (b *MessageQueueBroker) genLocalPartitionFromFiler(t topic.Topic, partition topic.Partition, conf *mq_pb.ConfigureTopicResponse) (localPartition *topic.LocalPartition, isGenerated bool, err error) {
	self := b.option.BrokerAddress()
	glog.V(4).Infof("genLocalPartitionFromFiler for %s %s, self=%s", t, partition, self)
	glog.V(4).Infof("conf.BrokerPartitionAssignments: %v", conf.BrokerPartitionAssignments)
	for _, assignment := range conf.BrokerPartitionAssignments {
		assignmentPartition := topic.FromPbPartition(assignment.Partition)
		glog.V(4).Infof("checking assignment: LeaderBroker=%s, Partition=%s", assignment.LeaderBroker, assignmentPartition)
		glog.V(4).Infof("comparing self=%s with LeaderBroker=%s: %v", self, assignment.LeaderBroker, assignment.LeaderBroker == string(self))
		glog.V(4).Infof("comparing partition=%s with assignmentPartition=%s: %v", partition.String(), assignmentPartition.String(), partition.Equals(assignmentPartition))
		glog.V(4).Infof("logical comparison (RangeStart, RangeStop only): %v", partition.LogicalEquals(assignmentPartition))
		glog.V(4).Infof("partition details: RangeStart=%d, RangeStop=%d, RingSize=%d, UnixTimeNs=%d", partition.RangeStart, partition.RangeStop, partition.RingSize, partition.UnixTimeNs)
		glog.V(4).Infof("assignmentPartition details: RangeStart=%d, RangeStop=%d, RingSize=%d, UnixTimeNs=%d", assignmentPartition.RangeStart, assignmentPartition.RangeStop, assignmentPartition.RingSize, assignmentPartition.UnixTimeNs)
		if assignment.LeaderBroker == string(self) && partition.LogicalEquals(assignmentPartition) {
			glog.V(4).Infof("Creating local partition for %s %s", t, partition)
			localPartition = topic.NewLocalPartition(partition, b.option.LogFlushInterval, b.genLogFlushFunc(t, partition), logstore.GenMergedReadFunc(b, t, partition))

			// Initialize offset from existing data to ensure continuity on restart
			b.initializePartitionOffsetFromExistingData(localPartition, t, partition)

			b.localTopicManager.AddLocalPartition(t, localPartition)
			isGenerated = true
			glog.V(4).Infof("Successfully added local partition %s %s to localTopicManager", t, partition)
			break
		}
	}

	if !isGenerated {
		glog.V(4).Infof("No matching assignment found for %s %s", t, partition)
	}

	return localPartition, isGenerated, nil
}

func (b *MessageQueueBroker) ensureTopicActiveAssignments(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (err error) {
	// also fix assignee broker if invalid
	hasChanges := pub_balancer.EnsureAssignmentsToActiveBrokers(b.PubBalancer.Brokers, 1, conf.BrokerPartitionAssignments)
	if hasChanges {
		glog.V(0).Infof("topic %v partition updated assignments: %v", t, conf.BrokerPartitionAssignments)
		if err = b.fca.SaveTopicConfToFiler(t, conf); err != nil {
			return err
		}
	}

	return err
}

// initializePartitionOffsetFromExistingData initializes the LogBuffer offset from existing data on filer
// This ensures offset continuity when SMQ restarts
func (b *MessageQueueBroker) initializePartitionOffsetFromExistingData(localPartition *topic.LocalPartition, t topic.Topic, partition topic.Partition) {
	// Create a function to get the highest existing offset from chunk metadata
	getHighestOffsetFn := func() (int64, error) {
		// Use the existing chunk metadata approach to find the highest offset
		if b.fca == nil {
			return -1, fmt.Errorf("no filer client accessor available")
		}

		// Use the same logic as getOffsetRangeFromChunkMetadata but only get the highest offset
		_, highWaterMark, err := b.getOffsetRangeFromChunkMetadata(t, partition)
		if err != nil {
			return -1, err
		}

		// The high water mark is the next offset to be assigned, so the highest existing offset is hwm - 1
		if highWaterMark > 0 {
			return highWaterMark - 1, nil
		}

		return -1, nil // No existing data
	}

	// Initialize the LogBuffer offset from existing data
	if err := localPartition.LogBuffer.InitializeOffsetFromExistingData(getHighestOffsetFn); err != nil {
		glog.V(0).Infof("Failed to initialize offset for partition %s %s: %v", t, partition, err)
	}
}

// getOffsetRangeFromChunkMetadata reads chunk metadata to find both earliest and latest offsets
func (b *MessageQueueBroker) getOffsetRangeFromChunkMetadata(t topic.Topic, partition topic.Partition) (earliestOffset int64, highWaterMark int64, err error) {
	if b.fca == nil {
		return 0, 0, fmt.Errorf("filer client accessor not available")
	}

	// Get the topic path and find the latest version
	topicPath := fmt.Sprintf("/topics/%s/%s", t.Namespace, t.Name)

	// First, list the topic versions to find the latest
	var latestVersion string
	err = b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: topicPath,
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
			if resp.Entry.IsDirectory && strings.HasPrefix(resp.Entry.Name, "v") {
				if latestVersion == "" || resp.Entry.Name > latestVersion {
					latestVersion = resp.Entry.Name
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list topic versions: %v", err)
	}

	if latestVersion == "" {
		glog.V(0).Infof("No version directory found for topic %s", t)
		return 0, 0, nil
	}

	// Find the partition directory
	versionPath := fmt.Sprintf("%s/%s", topicPath, latestVersion)
	var partitionDir string
	err = b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: versionPath,
		})
		if err != nil {
			return err
		}

		// Look for the partition directory that matches our partition range
		targetPartitionName := fmt.Sprintf("%04d-%04d", partition.RangeStart, partition.RangeStop)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if resp.Entry.IsDirectory && resp.Entry.Name == targetPartitionName {
				partitionDir = resp.Entry.Name
				break
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list partition directories: %v", err)
	}

	if partitionDir == "" {
		glog.V(0).Infof("No partition directory found for topic %s partition %s", t, partition)
		return 0, 0, nil
	}

	// Scan all message files to find the highest offset_max and lowest offset_min
	partitionPath := fmt.Sprintf("%s/%s", versionPath, partitionDir)
	highWaterMark = 0
	earliestOffset = -1 // -1 indicates no data found yet

	err = b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: partitionPath,
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
			if !resp.Entry.IsDirectory && resp.Entry.Name != "checkpoint.offset" {
				// Check for offset ranges in Extended attributes (both log files and parquet files)
				if resp.Entry.Extended != nil {
					fileType := "log"
					if strings.HasSuffix(resp.Entry.Name, ".parquet") {
						fileType = "parquet"
					}

					// Track maximum offset for high water mark
					if maxOffsetBytes, exists := resp.Entry.Extended[mq.ExtendedAttrOffsetMax]; exists && len(maxOffsetBytes) == 8 {
						maxOffset := int64(binary.BigEndian.Uint64(maxOffsetBytes))
						if maxOffset > highWaterMark {
							highWaterMark = maxOffset
						}
						glog.V(2).Infof("%s file %s has offset_max=%d", fileType, resp.Entry.Name, maxOffset)
					}

					// Track minimum offset for earliest offset
					if minOffsetBytes, exists := resp.Entry.Extended[mq.ExtendedAttrOffsetMin]; exists && len(minOffsetBytes) == 8 {
						minOffset := int64(binary.BigEndian.Uint64(minOffsetBytes))
						if earliestOffset == -1 || minOffset < earliestOffset {
							earliestOffset = minOffset
						}
						glog.V(2).Infof("%s file %s has offset_min=%d", fileType, resp.Entry.Name, minOffset)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan message files: %v", err)
	}

	// High water mark is the next offset after the highest written offset
	if highWaterMark > 0 {
		highWaterMark++
	}

	// If no data found, set earliest offset to 0
	if earliestOffset == -1 {
		earliestOffset = 0
	}

	glog.V(0).Infof("Offset range for topic %s partition %s: earliest=%d, highWaterMark=%d", t, partition, earliestOffset, highWaterMark)
	return earliestOffset, highWaterMark, nil
}
