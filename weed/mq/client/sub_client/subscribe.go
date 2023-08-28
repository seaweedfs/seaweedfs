package sub_client

import (
	cmap "github.com/orcaman/concurrent-map"
	"github.com/rdleal/intervalst/interval"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type SubscriberConfiguration struct {
}

type TopicSubscriber struct {
	namespace            string
	topic                string
	partition2Broker     *interval.SearchTree[string, int32]
	broker2PublishClient cmap.ConcurrentMap[string, mq_pb.SeaweedMessaging_PublishClient]
}

func NewTopicSubscriber(config *SubscriberConfiguration, namespace, topic string) *TopicSubscriber {
	return &TopicSubscriber{
		namespace: namespace,
		topic:     topic,
		partition2Broker: interval.NewSearchTree[string](func(a, b int32) int {
			return int(a - b)
		}),
		broker2PublishClient: cmap.New[mq_pb.SeaweedMessaging_PublishClient](),
	}
}
