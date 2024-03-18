package pub_client

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (p *TopicPublisher) Publish(key, value []byte) error {
	hashKey := util.HashToInt32(key) % pub_balancer.MaxPartitionCount
	if hashKey < 0 {
		hashKey = -hashKey
	}
	inputBuffer, found := p.partition2Buffer.Floor(hashKey+1, hashKey+1)
	if !found {
		return fmt.Errorf("no input buffer found for key %d", hashKey)
	}

	return inputBuffer.Enqueue(&mq_pb.DataMessage{
		Key:   key,
		Value: value,
	})
}
