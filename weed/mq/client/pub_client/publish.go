package pub_client

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/broker"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (p *TopicPublisher) Publish(key, value []byte) error {
	hashKey := util.HashToInt32(key) % broker.MaxPartitionCount
	if hashKey < 0 {
		hashKey = -hashKey
	}
	brokerAddress, found := p.partition2Broker.Floor(hashKey, hashKey)
	if !found {
		return fmt.Errorf("no broker found for key %d", hashKey)
	}
	publishClient, found := p.broker2PublishClient.Get(brokerAddress)
	if !found {
		return fmt.Errorf("no publish client found for broker %s", brokerAddress)
	}
	if err := publishClient.Send(&mq_pb.PublishRequest{
		Message: &mq_pb.PublishRequest_Data{
			Data: &mq_pb.DataMessage{
				Key:   key,
				Value: value,
			},
		},
	}); err != nil {
		return fmt.Errorf("send publish request: %v", err)
	}
	return nil
}
