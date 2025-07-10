package broker

import (
	"context"
	"fmt"
	"strings"

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
	conf := &mq_pb.ConfigureTopicResponse{}
	ret.Topic = request.Topic
	if conf, err = b.fca.ReadTopicConfFromFiler(t); err != nil {
		glog.V(0).Infof("lookup topic %s conf: %v", request.Topic, err)
	} else {
		err = b.ensureTopicActiveAssignments(t, conf)
		ret.BrokerPartitionAssignments = conf.BrokerPartitionAssignments
	}

	return ret, err
}

func (b *MessageQueueBroker) ListTopics(ctx context.Context, request *mq_pb.ListTopicsRequest) (resp *mq_pb.ListTopicsResponse, err error) {
	if !b.isLockOwner() {
		proxyErr := b.withBrokerClient(false, pb.ServerAddress(b.lockAsBalancer.LockOwner()), func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ListTopics(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.ListTopicsResponse{}

	// Scan the filer directory structure to find all topics
	err = b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// List all namespaces under /topics
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory: filer.TopicsDir,
			Limit:     1000,
		})
		if err != nil {
			glog.V(0).Infof("list namespaces in %s: %v", filer.TopicsDir, err)
			return err
		}

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
			topicStream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
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
				confResp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
					Directory: topicPath,
					Name:      filer.TopicConfFile,
				})
				if err != nil {
					glog.V(0).Infof("lookup topic.conf in %s: %v", topicPath, err)
					continue
				}

				if confResp.Entry != nil {
					// This is a valid topic
					topic := &schema_pb.Topic{
						Namespace: namespaceName,
						Name:      topicName,
					}
					ret.Topics = append(ret.Topics, topic)
				}
			}
		}

		return nil
	})

	if err != nil {
		glog.V(0).Infof("list topics from filer: %v", err)
		// Return empty response on error
		return &mq_pb.ListTopicsResponse{}, nil
	}

	return ret, nil
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
		return nil, fmt.Errorf("failed to read topic configuration: %v", err)
	}

	// Ensure topic assignments are active
	err = b.ensureTopicActiveAssignments(t, conf)
	if err != nil {
		glog.V(0).Infof("ensure topic active assignments %s: %v", request.Topic, err)
		return nil, fmt.Errorf("failed to ensure topic assignments: %v", err)
	}

	// Build the response with complete configuration including metadata
	ret := &mq_pb.GetTopicConfigurationResponse{
		Topic:                      request.Topic,
		PartitionCount:             int32(len(conf.BrokerPartitionAssignments)),
		RecordType:                 conf.RecordType,
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
		return nil, fmt.Errorf("failed to read topic configuration: %v", err)
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
		return nil, fmt.Errorf("failed to read topic configuration: %v", err)
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
