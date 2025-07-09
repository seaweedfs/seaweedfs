package dash

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// GetTopics retrieves message queue topics data
func (s *AdminServer) GetTopics() (*TopicsData, error) {
	var topics []TopicInfo
	var totalMessages int64
	var totalSize int64

	// Find broker leader and get topics
	brokerLeader, err := s.findBrokerLeader()
	if err != nil {
		// If no broker leader found, return empty data
		return &TopicsData{
			Topics:        topics,
			TotalTopics:   len(topics),
			TotalMessages: 0,
			TotalSize:     0,
			LastUpdated:   time.Now(),
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

		// Convert protobuf topics to TopicInfo
		for _, pbTopic := range resp.Topics {
			topicInfo := TopicInfo{
				Name:         fmt.Sprintf("%s.%s", pbTopic.Namespace, pbTopic.Name),
				Partitions:   0,           // Will be populated when we look up topic brokers
				Subscribers:  0,           // Will be populated from broker stats
				MessageCount: 0,           // Will be populated from broker stats
				TotalSize:    0,           // Will be populated from broker stats
				LastMessage:  time.Time{}, // Will be populated from broker stats
				CreatedAt:    time.Now(),  // Default to now, could be enhanced
			}

			// Get topic configuration to get partition count
			lookupResp, err := client.LookupTopicBrokers(ctx, &mq_pb.LookupTopicBrokersRequest{
				Topic: pbTopic,
			})
			if err == nil {
				topicInfo.Partitions = len(lookupResp.BrokerPartitionAssignments)
			}

			topics = append(topics, topicInfo)
		}

		return nil
	})

	if err != nil {
		// If connection fails, return empty data
		return &TopicsData{
			Topics:        topics,
			TotalTopics:   len(topics),
			TotalMessages: 0,
			TotalSize:     0,
			LastUpdated:   time.Now(),
		}, nil
	}

	return &TopicsData{
		Topics:        topics,
		TotalTopics:   len(topics),
		TotalMessages: totalMessages,
		TotalSize:     totalSize,
		LastUpdated:   time.Now(),
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
		return "", fmt.Errorf("failed to list brokers: %v", err)
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
