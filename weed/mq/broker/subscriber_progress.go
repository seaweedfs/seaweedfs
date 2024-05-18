package broker

import "github.com/seaweedfs/seaweedfs/weed/mq/topic"

type SubscriberProgress struct {
	topic.Topic
	topic.Partition
	ConsumerGroup string
	Offset int64
}
