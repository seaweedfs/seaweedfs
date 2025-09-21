package dash

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// GetTopics retrieves message queue topics data
func (s *AdminServer) GetTopics() (*TopicsData, error) {
	var topics []TopicInfo

	// Find broker leader and get topics
	brokerLeader, err := s.findBrokerLeader()
	if err != nil {
		// If no broker leader found, return empty data
		return &TopicsData{
			Topics:      topics,
			TotalTopics: len(topics),
			LastUpdated: time.Now(),
		}, nil
	}

	// Connect to broker leader and list topics
	err = s.withBrokerClient(brokerLeader, func(client mq_pb.SeaweedMessagingClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.ListTopics(ctx, &mq_pb.ListTopicsRequest{})
		if err != nil {
			return err
		}

		// Convert protobuf topics to TopicInfo - only include available data
		for _, pbTopic := range resp.Topics {
			topicInfo := TopicInfo{
				Name:       fmt.Sprintf("%s.%s", pbTopic.Namespace, pbTopic.Name),
				Partitions: 0, // Will be populated by LookupTopicBrokers call
				Retention: TopicRetentionInfo{
					Enabled:      false,
					DisplayValue: 0,
					DisplayUnit:  "days",
				},
			}

			// Get topic configuration to get partition count and retention info
			lookupResp, err := client.LookupTopicBrokers(ctx, &mq_pb.LookupTopicBrokersRequest{
				Topic: pbTopic,
			})
			if err == nil {
				topicInfo.Partitions = len(lookupResp.BrokerPartitionAssignments)
			}

			// Get topic configuration for retention information
			configResp, err := client.GetTopicConfiguration(ctx, &mq_pb.GetTopicConfigurationRequest{
				Topic: pbTopic,
			})
			if err == nil && configResp.Retention != nil {
				topicInfo.Retention = convertTopicRetention(configResp.Retention)
			}

			topics = append(topics, topicInfo)
		}

		return nil
	})

	if err != nil {
		// If connection fails, return empty data
		return &TopicsData{
			Topics:      topics,
			TotalTopics: len(topics),
			LastUpdated: time.Now(),
		}, nil
	}

	return &TopicsData{
		Topics:      topics,
		TotalTopics: len(topics),
		LastUpdated: time.Now(),
		// Don't include TotalMessages and TotalSize as they're not available
	}, nil
}

// GetSubscribers retrieves message queue subscribers data
func (s *AdminServer) GetSubscribers() (*SubscribersData, error) {
	var subscribers []SubscriberInfo

	// Find broker leader and get subscriber info from broker stats
	brokerLeader, err := s.findBrokerLeader()
	if err != nil {
		// If no broker leader found, return empty data
		return &SubscribersData{
			Subscribers:       subscribers,
			TotalSubscribers:  len(subscribers),
			ActiveSubscribers: 0,
			LastUpdated:       time.Now(),
		}, nil
	}

	// Connect to broker leader and get subscriber information
	// Note: SeaweedMQ doesn't have a direct API to list all subscribers
	// We would need to collect this information from broker statistics
	// For now, return empty data structure as subscriber info is not
	// directly available through the current MQ API
	err = s.withBrokerClient(brokerLeader, func(client mq_pb.SeaweedMessagingClient) error {
		// TODO: Implement subscriber data collection from broker statistics
		// This would require access to broker internal statistics about
		// active subscribers, consumer groups, etc.
		return nil
	})

	if err != nil {
		// If connection fails, return empty data
		return &SubscribersData{
			Subscribers:       subscribers,
			TotalSubscribers:  len(subscribers),
			ActiveSubscribers: 0,
			LastUpdated:       time.Now(),
		}, nil
	}

	activeCount := 0
	for _, sub := range subscribers {
		if sub.Status == "active" {
			activeCount++
		}
	}

	return &SubscribersData{
		Subscribers:       subscribers,
		TotalSubscribers:  len(subscribers),
		ActiveSubscribers: activeCount,
		LastUpdated:       time.Now(),
	}, nil
}

// GetTopicDetails retrieves detailed information about a specific topic
func (s *AdminServer) GetTopicDetails(namespace, topicName string) (*TopicDetailsData, error) {
	// Find broker leader
	brokerLeader, err := s.findBrokerLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to find broker leader: %w", err)
	}

	var topicDetails *TopicDetailsData

	// Connect to broker leader and get topic configuration
	err = s.withBrokerClient(brokerLeader, func(client mq_pb.SeaweedMessagingClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Get topic configuration using the new API
		configResp, err := client.GetTopicConfiguration(ctx, &mq_pb.GetTopicConfigurationRequest{
			Topic: &schema_pb.Topic{
				Namespace: namespace,
				Name:      topicName,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to get topic configuration: %w", err)
		}

		// Initialize topic details
		topicDetails = &TopicDetailsData{
			TopicName:            fmt.Sprintf("%s.%s", namespace, topicName),
			Namespace:            namespace,
			Name:                 topicName,
			Partitions:           []PartitionInfo{},
			Publishers:           []PublisherInfo{},
			Subscribers:          []TopicSubscriberInfo{},
			ConsumerGroupOffsets: []ConsumerGroupOffsetInfo{},
			Retention:            convertTopicRetention(configResp.Retention),
			CreatedAt:            time.Unix(0, configResp.CreatedAtNs),
			LastUpdated:          time.Unix(0, configResp.LastUpdatedNs),
		}

		// Set current time if timestamps are not available
		if configResp.CreatedAtNs == 0 {
			topicDetails.CreatedAt = time.Now()
		}
		if configResp.LastUpdatedNs == 0 {
			topicDetails.LastUpdated = time.Now()
		}

		// Process partitions
		for _, assignment := range configResp.BrokerPartitionAssignments {
			if assignment.Partition != nil {
				partitionInfo := PartitionInfo{
					ID:             assignment.Partition.RangeStart,
					LeaderBroker:   assignment.LeaderBroker,
					FollowerBroker: assignment.FollowerBroker,
					MessageCount:   0,           // Will be enhanced later with actual stats
					TotalSize:      0,           // Will be enhanced later with actual stats
					LastDataTime:   time.Time{}, // Will be enhanced later
					CreatedAt:      time.Now(),
				}
				topicDetails.Partitions = append(topicDetails.Partitions, partitionInfo)
			}
		}

		// Process flat schema format
		if configResp.MessageRecordType != nil {
			for _, field := range configResp.MessageRecordType.Fields {
				isKey := false
				for _, keyCol := range configResp.KeyColumns {
					if field.Name == keyCol {
						isKey = true
						break
					}
				}

				fieldType := "UNKNOWN"
				if field.Type != nil && field.Type.Kind != nil {
					fieldType = getFieldTypeName(field.Type)
				}

				schemaField := SchemaFieldInfo{
					Name: field.Name,
					Type: fieldType,
				}

				if isKey {
					topicDetails.KeySchema = append(topicDetails.KeySchema, schemaField)
				} else {
					topicDetails.ValueSchema = append(topicDetails.ValueSchema, schemaField)
				}
			}
		}

		// Get publishers information
		publishersResp, err := client.GetTopicPublishers(ctx, &mq_pb.GetTopicPublishersRequest{
			Topic: &schema_pb.Topic{
				Namespace: namespace,
				Name:      topicName,
			},
		})
		if err != nil {
			// Log error but don't fail the entire request
			glog.V(0).Infof("failed to get topic publishers for %s.%s: %v", namespace, topicName, err)
		} else {
			glog.V(1).Infof("got %d publishers for topic %s.%s", len(publishersResp.Publishers), namespace, topicName)
			topicDetails.Publishers = convertTopicPublishers(publishersResp.Publishers)
		}

		// Get subscribers information
		subscribersResp, err := client.GetTopicSubscribers(ctx, &mq_pb.GetTopicSubscribersRequest{
			Topic: &schema_pb.Topic{
				Namespace: namespace,
				Name:      topicName,
			},
		})
		if err != nil {
			// Log error but don't fail the entire request
			glog.V(0).Infof("failed to get topic subscribers for %s.%s: %v", namespace, topicName, err)
		} else {
			glog.V(1).Infof("got %d subscribers for topic %s.%s", len(subscribersResp.Subscribers), namespace, topicName)
			topicDetails.Subscribers = convertTopicSubscribers(subscribersResp.Subscribers)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Get consumer group offsets from the filer
	offsets, err := s.GetConsumerGroupOffsets(namespace, topicName)
	if err != nil {
		// Log error but don't fail the entire request
		glog.V(0).Infof("failed to get consumer group offsets for %s.%s: %v", namespace, topicName, err)
	} else {
		glog.V(1).Infof("got %d consumer group offsets for topic %s.%s", len(offsets), namespace, topicName)
		topicDetails.ConsumerGroupOffsets = offsets
	}

	return topicDetails, nil
}

// GetConsumerGroupOffsets retrieves consumer group offsets for a topic from the filer
func (s *AdminServer) GetConsumerGroupOffsets(namespace, topicName string) ([]ConsumerGroupOffsetInfo, error) {
	var offsets []ConsumerGroupOffsetInfo

	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Get the topic directory: /topics/namespace/topicName
		topicObj := topic.NewTopic(namespace, topicName)
		topicDir := topicObj.Dir()

		// List all version directories under the topic directory (e.g., v2025-07-10-05-44-34)
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

		// Process each version directory
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
				versionDir := filepath.Join(topicDir, versionResp.Entry.Name)

				// List all partition directories under the version directory (e.g., 0315-0630)
				partitionStream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
					Directory:          versionDir,
					Prefix:             "",
					StartFromFileName:  "",
					InclusiveStartFrom: false,
					Limit:              1000,
				})
				if err != nil {
					glog.Warningf("Failed to list version directory %s: %v", versionDir, err)
					continue
				}

				// Process each partition directory
				for {
					partitionResp, err := partitionStream.Recv()
					if err != nil {
						if err == io.EOF {
							break
						}
						glog.Warningf("Failed to receive partition entries: %v", err)
						break
					}

					// Only process directories that are partitions (format: NNNN-NNNN)
					if partitionResp.Entry.IsDirectory {
						// Parse partition range to get partition start ID (e.g., "0315-0630" -> 315)
						var partitionStart, partitionStop int32
						if n, err := fmt.Sscanf(partitionResp.Entry.Name, "%04d-%04d", &partitionStart, &partitionStop); n != 2 || err != nil {
							// Skip directories that don't match the partition format
							continue
						}

						partitionDir := filepath.Join(versionDir, partitionResp.Entry.Name)

						// List all .offset files in this partition directory
						offsetStream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
							Directory:          partitionDir,
							Prefix:             "",
							StartFromFileName:  "",
							InclusiveStartFrom: false,
							Limit:              1000,
						})
						if err != nil {
							glog.Warningf("Failed to list partition directory %s: %v", partitionDir, err)
							continue
						}

						// Process each offset file
						for {
							offsetResp, err := offsetStream.Recv()
							if err != nil {
								if err == io.EOF {
									break
								}
								glog.Warningf("Failed to receive offset entries: %v", err)
								break
							}

							// Only process .offset files
							if !offsetResp.Entry.IsDirectory && strings.HasSuffix(offsetResp.Entry.Name, ".offset") {
								consumerGroup := strings.TrimSuffix(offsetResp.Entry.Name, ".offset")

								// Read the offset value from the file
								offsetData, err := filer.ReadInsideFiler(client, partitionDir, offsetResp.Entry.Name)
								if err != nil {
									glog.Warningf("Failed to read offset file %s: %v", offsetResp.Entry.Name, err)
									continue
								}

								if len(offsetData) == 8 {
									offset := int64(util.BytesToUint64(offsetData))

									// Get the file modification time
									lastUpdated := time.Unix(offsetResp.Entry.Attributes.Mtime, 0)

									offsets = append(offsets, ConsumerGroupOffsetInfo{
										ConsumerGroup: consumerGroup,
										PartitionID:   partitionStart, // Use partition start as the ID
										Offset:        offset,
										LastUpdated:   lastUpdated,
									})
								}
							}
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group offsets: %w", err)
	}

	return offsets, nil
}

// convertRecordTypeToSchemaFields converts a protobuf RecordType to SchemaFieldInfo slice
func convertRecordTypeToSchemaFields(recordType *schema_pb.RecordType) []SchemaFieldInfo {
	var schemaFields []SchemaFieldInfo

	if recordType == nil || recordType.Fields == nil {
		return schemaFields
	}

	for _, field := range recordType.Fields {
		schemaField := SchemaFieldInfo{
			Name:     field.Name,
			Type:     getFieldTypeString(field.Type),
			Required: field.IsRequired,
		}
		schemaFields = append(schemaFields, schemaField)
	}

	return schemaFields
}

// getFieldTypeString converts a protobuf Type to a human-readable string
func getFieldTypeString(fieldType *schema_pb.Type) string {
	if fieldType == nil {
		return "unknown"
	}

	switch kind := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return getScalarTypeString(kind.ScalarType)
	case *schema_pb.Type_RecordType:
		return "record"
	case *schema_pb.Type_ListType:
		elementType := getFieldTypeString(kind.ListType.ElementType)
		return fmt.Sprintf("list<%s>", elementType)
	default:
		return "unknown"
	}
}

// getScalarTypeString converts a protobuf ScalarType to a string
func getScalarTypeString(scalarType schema_pb.ScalarType) string {
	switch scalarType {
	case schema_pb.ScalarType_BOOL:
		return "bool"
	case schema_pb.ScalarType_INT32:
		return "int32"
	case schema_pb.ScalarType_INT64:
		return "int64"
	case schema_pb.ScalarType_FLOAT:
		return "float"
	case schema_pb.ScalarType_DOUBLE:
		return "double"
	case schema_pb.ScalarType_BYTES:
		return "bytes"
	case schema_pb.ScalarType_STRING:
		return "string"
	default:
		return "unknown"
	}
}

// convertTopicPublishers converts protobuf TopicPublisher slice to PublisherInfo slice
func convertTopicPublishers(publishers []*mq_pb.TopicPublisher) []PublisherInfo {
	publisherInfos := make([]PublisherInfo, 0, len(publishers))

	for _, publisher := range publishers {
		publisherInfo := PublisherInfo{
			PublisherName:       publisher.PublisherName,
			ClientID:            publisher.ClientId,
			PartitionID:         publisher.Partition.RangeStart,
			Broker:              publisher.Broker,
			IsActive:            publisher.IsActive,
			LastPublishedOffset: publisher.LastPublishedOffset,
			LastAckedOffset:     publisher.LastAckedOffset,
		}

		// Convert timestamps
		if publisher.ConnectTimeNs > 0 {
			publisherInfo.ConnectTime = time.Unix(0, publisher.ConnectTimeNs)
		}
		if publisher.LastSeenTimeNs > 0 {
			publisherInfo.LastSeenTime = time.Unix(0, publisher.LastSeenTimeNs)
		}

		publisherInfos = append(publisherInfos, publisherInfo)
	}

	return publisherInfos
}

// convertTopicSubscribers converts protobuf TopicSubscriber slice to TopicSubscriberInfo slice
func convertTopicSubscribers(subscribers []*mq_pb.TopicSubscriber) []TopicSubscriberInfo {
	subscriberInfos := make([]TopicSubscriberInfo, 0, len(subscribers))

	for _, subscriber := range subscribers {
		subscriberInfo := TopicSubscriberInfo{
			ConsumerGroup:      subscriber.ConsumerGroup,
			ConsumerID:         subscriber.ConsumerId,
			ClientID:           subscriber.ClientId,
			PartitionID:        subscriber.Partition.RangeStart,
			Broker:             subscriber.Broker,
			IsActive:           subscriber.IsActive,
			CurrentOffset:      subscriber.CurrentOffset,
			LastReceivedOffset: subscriber.LastReceivedOffset,
		}

		// Convert timestamps
		if subscriber.ConnectTimeNs > 0 {
			subscriberInfo.ConnectTime = time.Unix(0, subscriber.ConnectTimeNs)
		}
		if subscriber.LastSeenTimeNs > 0 {
			subscriberInfo.LastSeenTime = time.Unix(0, subscriber.LastSeenTimeNs)
		}

		subscriberInfos = append(subscriberInfos, subscriberInfo)
	}

	return subscriberInfos
}

// findBrokerLeader finds the current broker leader
func (s *AdminServer) findBrokerLeader() (string, error) {
	// First, try to find any broker from the cluster
	var brokers []string
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
		})
		if err != nil {
			return err
		}

		for _, node := range resp.ClusterNodes {
			brokers = append(brokers, node.Address)
		}

		return nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to list brokers: %w", err)
	}

	if len(brokers) == 0 {
		return "", fmt.Errorf("no brokers found in cluster")
	}

	// Try each broker to find the leader
	for _, brokerAddr := range brokers {
		err := s.withBrokerClient(brokerAddr, func(client mq_pb.SeaweedMessagingClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Try to find broker leader
			_, err := client.FindBrokerLeader(ctx, &mq_pb.FindBrokerLeaderRequest{
				FilerGroup: "",
			})
			if err == nil {
				return nil // This broker is the leader
			}
			return err
		})
		if err == nil {
			return brokerAddr, nil
		}
	}

	return "", fmt.Errorf("no broker leader found")
}

// withBrokerClient connects to a message queue broker and executes a function
func (s *AdminServer) withBrokerClient(brokerAddress string, fn func(client mq_pb.SeaweedMessagingClient) error) error {
	return pb.WithBrokerGrpcClient(false, brokerAddress, s.grpcDialOption, fn)
}

// convertTopicRetention converts protobuf retention to TopicRetentionInfo
func convertTopicRetention(retention *mq_pb.TopicRetention) TopicRetentionInfo {
	if retention == nil || !retention.Enabled {
		return TopicRetentionInfo{
			Enabled:          false,
			RetentionSeconds: 0,
			DisplayValue:     0,
			DisplayUnit:      "days",
		}
	}

	// Convert seconds to human-readable format
	seconds := retention.RetentionSeconds
	var displayValue int32
	var displayUnit string

	if seconds >= 86400 { // >= 1 day
		displayValue = int32(seconds / 86400)
		displayUnit = "days"
	} else if seconds >= 3600 { // >= 1 hour
		displayValue = int32(seconds / 3600)
		displayUnit = "hours"
	} else {
		displayValue = int32(seconds)
		displayUnit = "seconds"
	}

	return TopicRetentionInfo{
		Enabled:          retention.Enabled,
		RetentionSeconds: seconds,
		DisplayValue:     displayValue,
		DisplayUnit:      displayUnit,
	}
}

// getFieldTypeName converts a schema_pb.Type to a human-readable type name
func getFieldTypeName(fieldType *schema_pb.Type) string {
	if fieldType.Kind == nil {
		return "UNKNOWN"
	}

	switch kind := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch kind.ScalarType {
		case schema_pb.ScalarType_BOOL:
			return "BOOLEAN"
		case schema_pb.ScalarType_INT32:
			return "INT32"
		case schema_pb.ScalarType_INT64:
			return "INT64"
		case schema_pb.ScalarType_FLOAT:
			return "FLOAT"
		case schema_pb.ScalarType_DOUBLE:
			return "DOUBLE"
		case schema_pb.ScalarType_BYTES:
			return "BYTES"
		case schema_pb.ScalarType_STRING:
			return "STRING"
		case schema_pb.ScalarType_TIMESTAMP:
			return "TIMESTAMP"
		case schema_pb.ScalarType_DATE:
			return "DATE"
		case schema_pb.ScalarType_TIME:
			return "TIME"
		case schema_pb.ScalarType_DECIMAL:
			return "DECIMAL"
		default:
			return "SCALAR"
		}
	case *schema_pb.Type_ListType:
		return "LIST"
	case *schema_pb.Type_RecordType:
		return "RECORD"
	default:
		return "UNKNOWN"
	}
}
