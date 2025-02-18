package sub_coordinator

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type ConsumerGroupInstanceId string

type ConsumerGroupInstance struct {
	InstanceId         ConsumerGroupInstanceId
	AssignedPartitions []topic.Partition
	ResponseChan       chan *mq_pb.SubscriberToSubCoordinatorResponse
	MaxPartitionCount  int32
}

func NewConsumerGroupInstance(instanceId string, maxPartitionCount int32) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:        ConsumerGroupInstanceId(instanceId),
		ResponseChan:      make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
		MaxPartitionCount: maxPartitionCount,
	}
}
