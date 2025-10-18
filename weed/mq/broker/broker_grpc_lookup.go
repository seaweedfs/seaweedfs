package broker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// LookupTopicBrokers returns the brokers that are serving the topic
func (b *MessageQueueBroker) LookupTopicBrokers(ctx context.Context, request *mq_pb.LookupTopicBrokersRequest) (resp *mq_pb.LookupTopicBrokersResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.LookupTopicBrokers(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	t := topic.FromPbTopic(request.Topic)
	ret := &mq_pb.LookupTopicBrokersResponse{}
	ret.Topic = request.Topic

	// Use cached topic config to avoid expensive filer reads (26% CPU overhead!)
	// getTopicConfFromCache also validates broker assignments on cache miss (saves 14% CPU)
	conf, err := b.getTopicConfFromCache(t)
	if err != nil {
		glog.V(0).Infof("lookup topic %s conf: %v", request.Topic, err)
		return ret, err
	}

	// Note: Assignment validation is now done inside getTopicConfFromCache on cache misses
	// This avoids 14% CPU overhead from validating on EVERY lookup
	ret.BrokerPartitionAssignments = conf.BrokerPartitionAssignments

	return ret, nil
}

func (b *MessageQueueBroker) ListTopics(ctx context.Context, request *mq_pb.ListTopicsRequest) (resp *mq_pb.ListTopicsResponse, err error) {
	glog.V(4).Infof("📋 ListTopics called, isLockOwner=%v", b.isLockOwner())

	if !b.isLockOwner() {
		glog.V(4).Infof("📋 ListTopics proxying to lock owner: %s", b.lockAsBalancer.LockOwner())
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ListTopics(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	glog.V(4).Infof("📋 ListTopics starting - getting in-memory topics")
	ret := &mq_pb.ListTopicsResponse{}

	// First, get topics from in-memory state (includes unflushed topics)
	inMemoryTopics := b.localTopicManager.ListTopicsInMemory()
	glog.V(4).Infof("📋 ListTopics found %d in-memory topics", len(inMemoryTopics))
	topicMap := make(map[string]*schema_pb.Topic)

	// Add in-memory topics to the result
	for _, topic := range inMemoryTopics {
		topicMap[topic.String()] = &schema_pb.Topic{
			Namespace: topic.Namespace,
			Name:      topic.Name,
		}
	}

	// Then, scan the filer directory structure to find persisted topics (fallback for topics not in memory)
	// Use a shorter timeout for filer scanning to ensure Metadata requests remain fast
	filerCtx, filerCancel := context.WithTimeout(ctx, 2*time.Second)
	defer filerCancel()

	glog.V(4).Infof("📋 ListTopics scanning filer for persisted topics (2s timeout)")
	err = b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// List all namespaces under /topics
		glog.V(4).Infof("📋 ListTopics calling ListEntries for %s", filer.TopicsDir)
		stream, err := client.ListEntries(filerCtx, &filer_pb.ListEntriesRequest{
			Directory: filer.TopicsDir,
			Limit:     1000,
		})
		if err != nil {
			glog.V(0).Infof("list namespaces in %s: %v", filer.TopicsDir, err)
			return err
		}
		glog.V(4).Infof("📋 ListTopics got ListEntries stream, processing namespaces...")

		// Process each namespace
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				return err
			}

			if !resp.Entry.IsDirectory {
				continue
			}

			namespaceName := resp.Entry.Name
			namespacePath := fmt.Sprintf("%s/%s", filer.TopicsDir, namespaceName)

			// List all topics in this namespace
			topicStream, err := client.ListEntries(filerCtx, &filer_pb.ListEntriesRequest{
				Directory: namespacePath,
				Limit:     1000,
			})
			if err != nil {
				glog.V(0).Infof("list topics in namespace %s: %v", namespacePath, err)
				continue
			}

			// Process each topic in the namespace
			for {
				topicResp, err := topicStream.Recv()
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					glog.V(0).Infof("error reading topic stream in namespace %s: %v", namespaceName, err)
					break
				}

				if !topicResp.Entry.IsDirectory {
					continue
				}

				topicName := topicResp.Entry.Name

				// Check if topic.conf exists
				topicPath := fmt.Sprintf("%s/%s", namespacePath, topicName)
				confResp, err := client.LookupDirectoryEntry(filerCtx, &filer_pb.LookupDirectoryEntryRequest{
					Directory: topicPath,
					Name:      filer.TopicConfFile,
				})
				if err != nil {
					glog.V(0).Infof("lookup topic.conf in %s: %v", topicPath, err)
					continue
				}

				if confResp.Entry != nil {
					// This is a valid persisted topic - add to map if not already present
					topicKey := fmt.Sprintf("%s.%s", namespaceName, topicName)
					if _, exists := topicMap[topicKey]; !exists {
						topicMap[topicKey] = &schema_pb.Topic{
							Namespace: namespaceName,
							Name:      topicName,
						}
					}
				}
			}
		}

		return nil
	})

	// Convert map to slice for response (combines in-memory and persisted topics)
	for _, topic := range topicMap {
		ret.Topics = append(ret.Topics, topic)
	}

	if err != nil {
		glog.V(0).Infof("ListTopics: filer scan failed: %v (returning %d in-memory topics)", err, len(inMemoryTopics))
		// Still return in-memory topics even if filer fails
	} else {
		glog.V(4).Infof("📋 ListTopics completed successfully: %d total topics (in-memory + persisted)", len(ret.Topics))
	}

	return ret, nil
}

// TopicExists checks if a topic exists in memory or filer
// Uses unified cache (checks if config is non-nil) to reduce filer load
func (b *MessageQueueBroker) TopicExists(ctx context.Context, request *mq_pb.TopicExistsRequest) (*mq_pb.TopicExistsResponse, error) {
	if !b.isLockOwner() {
		var resp *mq_pb.TopicExistsResponse
		var err error
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.TopicExists(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	if request.Topic == nil {
		return &mq_pb.TopicExistsResponse{Exists: false}, nil
	}

	// Convert schema_pb.Topic to topic.Topic
	topicObj := topic.Topic{
		Namespace: request.Topic.Namespace,
		Name:      request.Topic.Name,
	}
	topicKey := topicObj.String()

	// First check in-memory state (includes unflushed topics)
	if b.localTopicManager.TopicExistsInMemory(topicObj) {
		return &mq_pb.TopicExistsResponse{Exists: true}, nil
	}

	// Check unified cache (if conf != nil, topic exists; if conf == nil, doesn't exist)
	b.topicCacheMu.RLock()
	if entry, found := b.topicCache[topicKey]; found {
		if time.Now().Before(entry.expiresAt) {
			exists := entry.conf != nil
			b.topicCacheMu.RUnlock()
			glog.V(4).Infof("Topic cache HIT for %s: exists=%v", topicKey, exists)
			return &mq_pb.TopicExistsResponse{Exists: exists}, nil
		}
	}
	b.topicCacheMu.RUnlock()

	// Cache miss or expired - query filer for persisted topics (lightweight check)
	glog.V(4).Infof("Topic cache MISS for %s, querying filer for existence", topicKey)
	exists := false
	err := b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		topicPath := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, request.Topic.Namespace, request.Topic.Name)
		confResp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: topicPath,
			Name:      filer.TopicConfFile,
		})
		if err == nil && confResp.Entry != nil {
			exists = true
		}
		return nil // Don't propagate error, just check existence
	})

	if err != nil {
		glog.V(0).Infof("check topic existence in filer: %v", err)
		// Don't cache errors - return false and let next check retry
		return &mq_pb.TopicExistsResponse{Exists: false}, nil
	}

	// Update unified cache with lightweight result (don't read full config yet)
	// Cache existence info: conf=nil for non-existent (we don't have full config yet for existent)
	b.topicCacheMu.Lock()
	if !exists {
		// Negative cache: topic definitely doesn't exist
		b.topicCache[topicKey] = &topicCacheEntry{
			conf:      nil,
			expiresAt: time.Now().Add(b.topicCacheTTL),
		}
		glog.V(4).Infof("Topic cached as non-existent: %s", topicKey)
	}
	// Note: For positive existence, we don't cache here to avoid partial state
	// The config will be cached when GetOrGenerateLocalPartition reads it
	b.topicCacheMu.Unlock()

	return &mq_pb.TopicExistsResponse{Exists: exists}, nil
}

// GetTopicConfiguration returns the complete configuration of a topic including schema and partition assignments
func (b *MessageQueueBroker) GetTopicConfiguration(ctx context.Context, request *mq_pb.GetTopicConfigurationRequest) (resp *mq_pb.GetTopicConfigurationResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.GetTopicConfiguration(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	t := topic.FromPbTopic(request.Topic)
	var conf *mq_pb.ConfigureTopicResponse
	var createdAtNs, modifiedAtNs int64

	if conf, createdAtNs, modifiedAtNs, err = b.fca.ReadTopicConfFromFilerWithMetadata(t); err != nil {
		glog.V(0).Infof("get topic configuration %s: %v", request.Topic, err)
		return nil, fmt.Errorf("failed to read topic configuration: %w", err)
	}

	// Ensure topic assignments are active
	err = b.ensureTopicActiveAssignments(t, conf)
	if err != nil {
		glog.V(0).Infof("ensure topic active assignments %s: %v", request.Topic, err)
		return nil, fmt.Errorf("failed to ensure topic assignments: %w", err)
	}

	// Build the response with complete configuration including metadata
	ret := &mq_pb.GetTopicConfigurationResponse{
		Topic:                      request.Topic,
		PartitionCount:             int32(len(conf.BrokerPartitionAssignments)),
		MessageRecordType:          conf.MessageRecordType,
		KeyColumns:                 conf.KeyColumns,
		BrokerPartitionAssignments: conf.BrokerPartitionAssignments,
		CreatedAtNs:                createdAtNs,
		LastUpdatedNs:              modifiedAtNs,
		Retention:                  conf.Retention,
	}

	return ret, nil
}

// GetTopicPublishers returns the active publishers for a topic
func (b *MessageQueueBroker) GetTopicPublishers(ctx context.Context, request *mq_pb.GetTopicPublishersRequest) (resp *mq_pb.GetTopicPublishersResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.GetTopicPublishers(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	t := topic.FromPbTopic(request.Topic)
	var publishers []*mq_pb.TopicPublisher

	// Get topic configuration to find partition assignments
	var conf *mq_pb.ConfigureTopicResponse
	if conf, _, _, err = b.fca.ReadTopicConfFromFilerWithMetadata(t); err != nil {
		glog.V(0).Infof("get topic configuration for publishers %s: %v", request.Topic, err)
		return nil, fmt.Errorf("failed to read topic configuration: %w", err)
	}

	// Collect publishers from each partition that is hosted on this broker
	for _, assignment := range conf.BrokerPartitionAssignments {
		// Only collect from partitions where this broker is the leader
		if assignment.LeaderBroker == b.option.BrokerAddress().String() {
			partition := topic.FromPbPartition(assignment.Partition)
			if localPartition := b.localTopicManager.GetLocalPartition(t, partition); localPartition != nil {
				// Get publisher information from local partition
				localPartition.Publishers.ForEachPublisher(func(clientName string, publisher *topic.LocalPublisher) {
					connectTimeNs, lastSeenTimeNs := publisher.GetTimestamps()
					lastPublishedOffset, lastAckedOffset := publisher.GetOffsets()
					publishers = append(publishers, &mq_pb.TopicPublisher{
						PublisherName:       clientName,
						ClientId:            clientName, // For now, client name is used as client ID
						Partition:           assignment.Partition,
						ConnectTimeNs:       connectTimeNs,
						LastSeenTimeNs:      lastSeenTimeNs,
						Broker:              assignment.LeaderBroker,
						IsActive:            true,
						LastPublishedOffset: lastPublishedOffset,
						LastAckedOffset:     lastAckedOffset,
					})
				})
			}
		}
	}

	return &mq_pb.GetTopicPublishersResponse{
		Publishers: publishers,
	}, nil
}

// GetTopicSubscribers returns the active subscribers for a topic
func (b *MessageQueueBroker) GetTopicSubscribers(ctx context.Context, request *mq_pb.GetTopicSubscribersRequest) (resp *mq_pb.GetTopicSubscribersResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.GetTopicSubscribers(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	t := topic.FromPbTopic(request.Topic)
	var subscribers []*mq_pb.TopicSubscriber

	// Get topic configuration to find partition assignments
	var conf *mq_pb.ConfigureTopicResponse
	if conf, _, _, err = b.fca.ReadTopicConfFromFilerWithMetadata(t); err != nil {
		glog.V(0).Infof("get topic configuration for subscribers %s: %v", request.Topic, err)
		return nil, fmt.Errorf("failed to read topic configuration: %w", err)
	}

	// Collect subscribers from each partition that is hosted on this broker
	for _, assignment := range conf.BrokerPartitionAssignments {
		// Only collect from partitions where this broker is the leader
		if assignment.LeaderBroker == b.option.BrokerAddress().String() {
			partition := topic.FromPbPartition(assignment.Partition)
			if localPartition := b.localTopicManager.GetLocalPartition(t, partition); localPartition != nil {
				// Get subscriber information from local partition
				localPartition.Subscribers.ForEachSubscriber(func(clientName string, subscriber *topic.LocalSubscriber) {
					// Parse client name to extract consumer group and consumer ID
					// Format is typically: "consumerGroup/consumerID"
					consumerGroup := "default"
					consumerID := clientName
					if idx := strings.Index(clientName, "/"); idx != -1 {
						consumerGroup = clientName[:idx]
						consumerID = clientName[idx+1:]
					}

					connectTimeNs, lastSeenTimeNs := subscriber.GetTimestamps()
					lastReceivedOffset, lastAckedOffset := subscriber.GetOffsets()

					subscribers = append(subscribers, &mq_pb.TopicSubscriber{
						ConsumerGroup:      consumerGroup,
						ConsumerId:         consumerID,
						ClientId:           clientName, // Full client name as client ID
						Partition:          assignment.Partition,
						ConnectTimeNs:      connectTimeNs,
						LastSeenTimeNs:     lastSeenTimeNs,
						Broker:             assignment.LeaderBroker,
						IsActive:           true,
						CurrentOffset:      lastAckedOffset, // for compatibility
						LastReceivedOffset: lastReceivedOffset,
					})
				})
			}
		}
	}

	return &mq_pb.GetTopicSubscribersResponse{
		Subscribers: subscribers,
	}, nil
}

func (b *MessageQueueBroker) isLockOwner() bool {
	return b.lockAsBalancer.LockOwner() == b.option.BrokerAddress().String()
}
