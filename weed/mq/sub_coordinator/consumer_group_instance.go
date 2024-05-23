package sub_coordinator

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type ConsumerGroupInstance struct {
	InstanceId string
	// the consumer group instance may not have an active partition
	Partitions        []*topic.Partition
	ResponseChan      chan *mq_pb.SubscriberToSubCoordinatorResponse
	MaxPartitionCount int32
}

func NewConsumerGroupInstance(instanceId string) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:   instanceId,
		ResponseChan: make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
	}
}

func (i ConsumerGroupInstance) AckUnAssignment(assignment *mq_pb.SubscriberToSubCoordinatorRequest_AckUnAssignmentMessage) {
	fmt.Printf("ack unassignment %v\n", assignment)
}
