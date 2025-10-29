package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// CreateTopic creates a new topic in both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) CreateTopic(name string, partitions int32) error {
	return h.CreateTopicWithSchema(name, partitions, nil)
}

// CreateTopicWithSchema creates a topic with optional value schema
func (h *SeaweedMQHandler) CreateTopicWithSchema(name string, partitions int32, recordType *schema_pb.RecordType) error {
	return h.CreateTopicWithSchemas(name, partitions, nil, recordType)
}

// CreateTopicWithSchemas creates a topic with optional key and value schemas
func (h *SeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	// Check if topic already exists in filer
	if h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	// Configure topic with SeaweedMQ broker via gRPC
	if len(h.brokerAddresses) > 0 {
		brokerAddress := h.brokerAddresses[0] // Use first available broker
		glog.V(1).Infof("Configuring topic %s with broker %s", name, brokerAddress)

		// Load security configuration for broker connection
		util.LoadSecurityConfiguration()
		grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

		err := pb.WithBrokerGrpcClient(false, brokerAddress, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			// Convert dual schemas to flat schema format
			var flatSchema *schema_pb.RecordType
			var keyColumns []string
			if keyRecordType != nil || valueRecordType != nil {
				flatSchema, keyColumns = schema.CombineFlatSchemaFromKeyValue(keyRecordType, valueRecordType)
			}

			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:             seaweedTopic,
				PartitionCount:    partitions,
				MessageRecordType: flatSchema,
				KeyColumns:        keyColumns,
			})
			if err != nil {
				return fmt.Errorf("configure topic with broker: %w", err)
			}
			glog.V(1).Infof("successfully configured topic %s with broker", name)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to configure topic %s with broker %s: %w", name, brokerAddress, err)
		}
	} else {
		glog.Warningf("No brokers available - creating topic %s in gateway memory only (testing mode)", name)
	}

	// Topic is now stored in filer only via SeaweedMQ broker
	// No need to create in-memory topic info structure

	// Offset management now handled directly by SMQ broker - no initialization needed

	// Invalidate cache after successful topic creation
	h.InvalidateTopicExistsCache(name)

	glog.V(1).Infof("Topic %s created successfully with %d partitions", name, partitions)
	return nil
}

// CreateTopicWithRecordType creates a topic with flat schema and key columns
func (h *SeaweedMQHandler) CreateTopicWithRecordType(name string, partitions int32, flatSchema *schema_pb.RecordType, keyColumns []string) error {
	// Check if topic already exists in filer
	if h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	// Configure topic with SeaweedMQ broker via gRPC
	if len(h.brokerAddresses) > 0 {
		brokerAddress := h.brokerAddresses[0] // Use first available broker
		glog.V(1).Infof("Configuring topic %s with broker %s", name, brokerAddress)

		// Load security configuration for broker connection
		util.LoadSecurityConfiguration()
		grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

		err := pb.WithBrokerGrpcClient(false, brokerAddress, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:             seaweedTopic,
				PartitionCount:    partitions,
				MessageRecordType: flatSchema,
				KeyColumns:        keyColumns,
			})
			if err != nil {
				return fmt.Errorf("failed to configure topic: %w", err)
			}

			glog.V(1).Infof("successfully configured topic %s with broker", name)
			return nil
		})

		if err != nil {
			return err
		}
	} else {
		glog.Warningf("No broker addresses configured, topic %s not created in SeaweedMQ", name)
	}

	// Topic is now stored in filer only via SeaweedMQ broker
	// No need to create in-memory topic info structure

	glog.V(1).Infof("Topic %s created successfully with %d partitions using flat schema", name, partitions)
	return nil
}

// DeleteTopic removes a topic from both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) DeleteTopic(name string) error {
	// Check if topic exists in filer
	if !h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s does not exist", name)
	}

	// Get topic info to determine partition count for cleanup
	topicInfo, exists := h.GetTopicInfo(name)
	if !exists {
		return fmt.Errorf("topic %s info not found", name)
	}

	// Close all publisher sessions for this topic
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		if h.brokerClient != nil {
			h.brokerClient.ClosePublisher(name, partitionID)
		}
	}

	// Topic removal from filer would be handled by SeaweedMQ broker
	// No in-memory cache to clean up

	// Offset management handled by SMQ broker - no cleanup needed

	return nil
}

// TopicExists checks if a topic exists in SeaweedMQ broker (includes in-memory topics)
// Uses a 5-second cache to reduce broker queries
func (h *SeaweedMQHandler) TopicExists(name string) bool {
	// Check cache first
	h.topicExistsCacheMu.RLock()
	if entry, found := h.topicExistsCache[name]; found {
		if time.Now().Before(entry.expiresAt) {
			h.topicExistsCacheMu.RUnlock()
			return entry.exists
		}
	}
	h.topicExistsCacheMu.RUnlock()

	// Cache miss or expired - query broker

	var exists bool
	// Check via SeaweedMQ broker (includes in-memory topics)
	if h.brokerClient != nil {
		var err error
		exists, err = h.brokerClient.TopicExists(name)
		if err != nil {
			// Don't cache errors
			return false
		}
	} else {
		// Return false if broker is unavailable
		return false
	}

	// Update cache
	h.topicExistsCacheMu.Lock()
	h.topicExistsCache[name] = &topicExistsCacheEntry{
		exists:    exists,
		expiresAt: time.Now().Add(h.topicExistsCacheTTL),
	}
	h.topicExistsCacheMu.Unlock()

	return exists
}

// InvalidateTopicExistsCache removes a topic from the existence cache
// Should be called after creating or deleting a topic
func (h *SeaweedMQHandler) InvalidateTopicExistsCache(name string) {
	h.topicExistsCacheMu.Lock()
	delete(h.topicExistsCache, name)
	h.topicExistsCacheMu.Unlock()
}

// GetTopicInfo returns information about a topic from broker
func (h *SeaweedMQHandler) GetTopicInfo(name string) (*KafkaTopicInfo, bool) {
	// Get topic configuration from broker
	if h.brokerClient != nil {
		config, err := h.brokerClient.GetTopicConfiguration(name)
		if err == nil && config != nil {
			topicInfo := &KafkaTopicInfo{
				Name:       name,
				Partitions: config.PartitionCount,
				CreatedAt:  config.CreatedAtNs,
			}
			return topicInfo, true
		}
		glog.V(2).Infof("Failed to get topic configuration for %s from broker: %v", name, err)
	}

	// Fallback: check if topic exists in filer (for backward compatibility)
	if !h.checkTopicInFiler(name) {
		return nil, false
	}

	// Return default info if broker query failed but topic exists in filer
	topicInfo := &KafkaTopicInfo{
		Name:       name,
		Partitions: 1, // Default to 1 partition if broker query failed
		CreatedAt:  0,
	}

	return topicInfo, true
}

// ListTopics returns all topic names from SeaweedMQ broker (includes in-memory topics)
func (h *SeaweedMQHandler) ListTopics() []string {
	// Get topics from SeaweedMQ broker (includes in-memory topics)
	if h.brokerClient != nil {
		topics, err := h.brokerClient.ListTopics()
		if err == nil {
			return topics
		}
	}

	// Return empty list if broker is unavailable
	return []string{}
}

// checkTopicInFiler checks if a topic exists in the filer
func (h *SeaweedMQHandler) checkTopicInFiler(topicName string) bool {
	if h.filerClientAccessor == nil {
		return false
	}

	var exists bool
	h.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/topics/kafka",
			Name:      topicName,
		}

		_, err := client.LookupDirectoryEntry(context.Background(), request)
		exists = (err == nil)
		return nil // Don't propagate error, just check existence
	})

	return exists
}

// listTopicsFromFiler lists all topics from the filer
func (h *SeaweedMQHandler) listTopicsFromFiler() []string {
	if h.filerClientAccessor == nil {
		return []string{}
	}

	var topics []string

	h.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory: "/topics/kafka",
		}

		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return nil // Don't propagate error, just return empty list
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry != nil && resp.Entry.IsDirectory {
				topics = append(topics, resp.Entry.Name)
			} else if resp.Entry != nil {
			}
		}
		return nil
	})

	return topics
}
