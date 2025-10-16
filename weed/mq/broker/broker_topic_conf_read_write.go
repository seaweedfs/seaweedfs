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
	// get or generate a local partition using cached topic config
	conf, err := b.getTopicConfFromCache(t)
	if err != nil {
		glog.Errorf("topic %v not found: %v", t, err)
		return nil, fmt.Errorf("topic %v not found: %w", t, err)
	}

	localTopicPartition, _, getOrGenError = b.doGetOrGenLocalPartition(t, partition, conf)
	if getOrGenError != nil {
		glog.Errorf("topic %v partition %v not setup: %v", t, partition, getOrGenError)
		return nil, fmt.Errorf("topic %v partition %v not setup: %w", t, partition, getOrGenError)
	}
	return localTopicPartition, nil
}

// invalidateTopicCache removes a topic from the unified cache
// Should be called when a topic is created, deleted, or config is updated
func (b *MessageQueueBroker) invalidateTopicCache(t topic.Topic) {
	topicKey := t.String()
	b.topicCacheMu.Lock()
	delete(b.topicCache, topicKey)
	b.topicCacheMu.Unlock()
	glog.V(4).Infof("Invalidated topic cache for %s", topicKey)
}

// getTopicConfFromCache reads topic configuration with caching
// Returns the config or error if not found. Uses unified cache to avoid expensive filer reads.
// On cache miss, validates broker assignments to ensure they're still active (14% CPU overhead).
// This is the public API for reading topic config - always use this instead of direct filer reads.
func (b *MessageQueueBroker) getTopicConfFromCache(t topic.Topic) (*mq_pb.ConfigureTopicResponse, error) {
	topicKey := t.String()

	// Check unified cache first
	b.topicCacheMu.RLock()
	if entry, found := b.topicCache[topicKey]; found {
		if time.Now().Before(entry.expiresAt) {
			conf := entry.conf
			b.topicCacheMu.RUnlock()

			// If conf is nil, topic was cached as non-existent
			if conf == nil {
				glog.V(4).Infof("Topic cache HIT for %s: topic doesn't exist", topicKey)
				return nil, fmt.Errorf("topic %v not found (cached)", t)
			}

			glog.V(4).Infof("Topic cache HIT for %s (skipping assignment validation)", topicKey)
			// Cache hit - return immediately without validating assignments
			// Assignments were validated when we first cached this config
			return conf, nil
		}
	}
	b.topicCacheMu.RUnlock()

	// Cache miss or expired - read from filer
	glog.V(4).Infof("Topic cache MISS for %s, reading from filer", topicKey)
	conf, readConfErr := b.fca.ReadTopicConfFromFiler(t)

	if readConfErr != nil {
		// Negative cache: topic doesn't exist
		b.topicCacheMu.Lock()
		b.topicCache[topicKey] = &topicCacheEntry{
			conf:      nil,
			expiresAt: time.Now().Add(b.topicCacheTTL),
		}
		b.topicCacheMu.Unlock()
		glog.V(4).Infof("Topic cached as non-existent: %s", topicKey)
		return nil, fmt.Errorf("topic %v not found: %w", t, readConfErr)
	}

	// Validate broker assignments before caching (NOT holding cache lock)
	// This ensures cached configs always have valid broker assignments
	// Only done on cache miss (not on every lookup), saving 14% CPU
	glog.V(4).Infof("Validating broker assignments for %s", topicKey)
	hasChanges := b.ensureTopicActiveAssignmentsUnsafe(t, conf)
	if hasChanges {
		glog.V(0).Infof("topic %v partition assignments updated due to broker changes", t)
		// Save updated assignments to filer immediately to ensure persistence
		if err := b.fca.SaveTopicConfToFiler(t, conf); err != nil {
			glog.Errorf("failed to save updated topic config for %s: %v", topicKey, err)
			// Don't cache on error - let next request retry
			return conf, err
		}
		// CRITICAL FIX: Invalidate cache while holding lock to prevent race condition
		// Before the fix, between checking the cache and invalidating it, another goroutine
		// could read stale data. Now we hold the lock throughout.
		b.topicCacheMu.Lock()
		delete(b.topicCache, topicKey)
		// Cache the updated config with validated assignments
		b.topicCache[topicKey] = &topicCacheEntry{
			conf:      conf,
			expiresAt: time.Now().Add(b.topicCacheTTL),
		}
		b.topicCacheMu.Unlock()
		glog.V(4).Infof("Updated cache for %s after assignment update", topicKey)
		return conf, nil
	}

	// Positive cache: topic exists with validated assignments
	b.topicCacheMu.Lock()
	b.topicCache[topicKey] = &topicCacheEntry{
		conf:      conf,
		expiresAt: time.Now().Add(b.topicCacheTTL),
	}
	b.topicCacheMu.Unlock()
	glog.V(4).Infof("Topic config cached for %s", topicKey)

	return conf, nil
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

// ensureTopicActiveAssignmentsUnsafe validates that partition assignments reference active brokers
// Returns true if assignments were changed. Caller must save config to filer if hasChanges=true.
// Note: Assumes caller holds topicCacheMu lock or is OK with concurrent access to conf
func (b *MessageQueueBroker) ensureTopicActiveAssignmentsUnsafe(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (hasChanges bool) {
	// also fix assignee broker if invalid
	hasChanges = pub_balancer.EnsureAssignmentsToActiveBrokers(b.PubBalancer.Brokers, 1, conf.BrokerPartitionAssignments)
	return hasChanges
}

func (b *MessageQueueBroker) ensureTopicActiveAssignments(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) (err error) {
	// Validate and save if needed
	hasChanges := b.ensureTopicActiveAssignmentsUnsafe(t, conf)
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
