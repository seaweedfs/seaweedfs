package pub_client

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"time"
)

func (p *TopicPublisher) Publish(key, value []byte) error {
	if p.config.RecordType != nil {
		return fmt.Errorf("record type is set, use PublishRecord instead")
	}
	return p.doPublish(key, value)
}

func (p *TopicPublisher) doPublish(key, value []byte) error {
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
		TsNs:  time.Now().UnixNano(),
	})
}

func (p *TopicPublisher) PublishRecord(key []byte, recordValue *schema_pb.RecordValue) error {
	// serialize record value
	value, err := proto.Marshal(recordValue)
	if err != nil {
		return fmt.Errorf("failed to marshal record value: %v", err)
	}

	return p.doPublish(key, value)
}

func (p *TopicPublisher) FinishPublish() error {
	if inputBuffers, found := p.partition2Buffer.AllIntersections(0, pub_balancer.MaxPartitionCount); found {
		for _, inputBuffer := range inputBuffers {
			inputBuffer.Enqueue(&mq_pb.DataMessage{
				TsNs: time.Now().UnixNano(),
				Ctrl: &mq_pb.ControlMessage{
					IsClose:       true,
					PublisherName: p.config.PublisherName,
				},
			})
		}
	}

	return nil
}
