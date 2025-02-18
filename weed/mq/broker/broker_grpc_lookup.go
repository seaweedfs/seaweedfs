package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
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
	knownTopics := make(map[string]struct{})
	for brokerStatsItem := range b.PubBalancer.Brokers.IterBuffered() {
		_, brokerStats := brokerStatsItem.Key, brokerStatsItem.Val
		for topicPartitionStatsItem := range brokerStats.TopicPartitionStats.IterBuffered() {
			topicPartitionStat := topicPartitionStatsItem.Val
			topic := &schema_pb.Topic{
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

func (b *MessageQueueBroker) isLockOwner() bool {
	return b.lockAsBalancer.LockOwner() == b.option.BrokerAddress().String()
}
