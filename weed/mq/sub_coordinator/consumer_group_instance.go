package sub_coordinator

import (
	"fmt"
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

func NewConsumerGroupInstance(instanceId string) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:   ConsumerGroupInstanceId(instanceId),
		ResponseChan: make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
	}
}

func (i ConsumerGroupInstance) AckUnAssignment(assignment *mq_pb.SubscriberToSubCoordinatorRequest_AckUnAssignmentMessage) {
	fmt.Printf("ack unassignment %v\n", assignment)
}
