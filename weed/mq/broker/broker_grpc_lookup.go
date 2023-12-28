package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FindTopicBrokers returns the brokers that are serving the topic
//
//  1. lock the topic
//
//  2. find the topic partitions on the filer
//     2.1 if the topic is not found, return error
//     2.1.1 if the request is_for_subscribe, return error not found
//     2.1.2 if the request is_for_publish, create the topic
//     2.2 if the topic is found, return the brokers
//
//  3. unlock the topic
func (b *MessageQueueBroker) LookupTopicBrokers(ctx context.Context, request *mq_pb.LookupTopicBrokersRequest) (resp *mq_pb.LookupTopicBrokersResponse, err error) {
	if b.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !b.lockAsBalancer.IsLocked() {
		proxyErr := b.withBrokerClient(false, b.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.LookupTopicBrokers(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.LookupTopicBrokersResponse{}
	ret.Topic = request.Topic
	ret.BrokerPartitionAssignments, err = b.Balancer.LookupOrAllocateTopicPartitions(ret.Topic, request.IsForPublish, 6)
	return ret, err
}

func (b *MessageQueueBroker) ListTopics(ctx context.Context, request *mq_pb.ListTopicsRequest) (resp *mq_pb.ListTopicsResponse, err error) {
	if b.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !b.lockAsBalancer.IsLocked() {
		proxyErr := b.withBrokerClient(false, b.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ListTopics(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.ListTopicsResponse{}
	knownTopics := make(map[string]struct{})
	for brokerStatsItem := range b.Balancer.Brokers.IterBuffered() {
		_, brokerStats := brokerStatsItem.Key, brokerStatsItem.Val
		for topicPartitionStatsItem := range brokerStats.TopicPartitionStats.IterBuffered() {
			topicPartitionStat := topicPartitionStatsItem.Val
			topic := &mq_pb.Topic{
				Namespace: topicPartitionStat.TopicPartition.Namespace,
				Name:      topicPartitionStat.TopicPartition.Name,
			}
			topicKey := fmt.Sprintf("%s/%s", topic.Namespace, topic.Name)
			if _, found := knownTopics[topicKey]; found {
				continue
			}
			knownTopics[topicKey] = struct{}{}
			ret.Topics = append(ret.Topics, topic)
		}
	}

	return ret, nil
}
