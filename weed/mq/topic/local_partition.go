package topic

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"time"
)

type LocalPartition struct {
	Partition
	isLeader        bool
	FollowerBrokers []pb.ServerAddress
	logBuffer       *log_buffer.LogBuffer
}

func (p LocalPartition) Publish(message *mq_pb.PublishRequest_DataMessage) {
	p.logBuffer.AddToBuffer(message.Key, message.Value, time.Now().UnixNano())
}

func FromPbBrokerPartitionsAssignment(self pb.ServerAddress, assignment *mq_pb.BrokerPartitionsAssignment) *LocalPartition {
	isLeaer := assignment.LeaderBroker == string(self)
	localPartition := &LocalPartition{
		Partition: Partition{
			RangeStart: assignment.PartitionStart,
			RangeStop:  assignment.PartitionStop,
			RingSize:   PartitionCount,
		},
		isLeader: isLeaer,
	}
	if !isLeaer {
		return localPartition
	}
	followers := make([]pb.ServerAddress, len(assignment.FollowerBrokers))
	for i, follower := range assignment.FollowerBrokers {
		followers[i] = pb.ServerAddress(follower)
	}
	localPartition.FollowerBrokers = followers
	return localPartition
}
