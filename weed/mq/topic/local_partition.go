package topic

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"time"
)

type LocalPartition struct {
	Partition
	isLeader        bool
	FollowerBrokers []pb.ServerAddress
	logBuffer       *log_buffer.LogBuffer
	ConsumerCount   int32
}

func NewLocalPartition(topic Topic, partition Partition, isLeader bool, followerBrokers []pb.ServerAddress) *LocalPartition {
	return &LocalPartition{
		Partition:       partition,
		isLeader:        isLeader,
		FollowerBrokers: followerBrokers,
		logBuffer: log_buffer.NewLogBuffer(
			fmt.Sprintf("%s/%s/%4d-%4d", topic.Namespace, topic.Name, partition.RangeStart, partition.RangeStop),
			2*time.Minute,
			func(startTime, stopTime time.Time, buf []byte) {

			},
			func() {

			},
		),
	}
}

type OnEachMessageFn func(logEntry *filer_pb.LogEntry) error

func (p LocalPartition) Publish(message *mq_pb.DataMessage) {
	p.logBuffer.AddToBuffer(message.Key, message.Value, time.Now().UnixNano())
}

func (p LocalPartition) Subscribe(clientName string, startReadTime time.Time, eachMessageFn OnEachMessageFn) {
	p.logBuffer.LoopProcessLogData(clientName, startReadTime, 0, func() bool {
		return true
	}, eachMessageFn)
}

func FromPbBrokerPartitionAssignment(self pb.ServerAddress, assignment *mq_pb.BrokerPartitionAssignment) *LocalPartition {
	isLeaer := assignment.LeaderBroker == string(self)
	localPartition := &LocalPartition{
		Partition: FromPbPartition(assignment.Partition),
		isLeader:  isLeaer,
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
