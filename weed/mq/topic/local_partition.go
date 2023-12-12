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
	isLeader          bool
	FollowerBrokers   []pb.ServerAddress
	logBuffer         *log_buffer.LogBuffer
	ConsumerCount     int32
	StopPublishersCh  chan struct{}
	Publishers        *LocalPartitionPublishers
	StopSubscribersCh chan struct{}
	Subscribers       *LocalPartitionSubscribers
}

func NewLocalPartition(partition Partition, isLeader bool, followerBrokers []pb.ServerAddress) *LocalPartition {
	return &LocalPartition{
		Partition:       partition,
		isLeader:        isLeader,
		FollowerBrokers: followerBrokers,
		logBuffer: log_buffer.NewLogBuffer(
			fmt.Sprintf("%d/%4d-%4d", partition.UnixTimeNs, partition.RangeStart, partition.RangeStop),
			2*time.Minute,
			func(startTime, stopTime time.Time, buf []byte) {

			},
			func() {

			},
		),
		Publishers:  NewLocalPartitionPublishers(),
		Subscribers: NewLocalPartitionSubscribers(),
	}
}

type OnEachMessageFn func(logEntry *filer_pb.LogEntry) error

func (p *LocalPartition) Publish(message *mq_pb.DataMessage) {
	p.logBuffer.AddToBuffer(message.Key, message.Value, time.Now().UnixNano())
}

func (p *LocalPartition) Subscribe(clientName string, startReadTime time.Time, onNoMessageFn func() bool, eachMessageFn OnEachMessageFn) {
	p.logBuffer.LoopProcessLogData(clientName, startReadTime, 0, onNoMessageFn, eachMessageFn)
}

func FromPbBrokerPartitionAssignment(self pb.ServerAddress, assignment *mq_pb.BrokerPartitionAssignment) *LocalPartition {
	isLeader := assignment.LeaderBroker == string(self)
	followers := make([]pb.ServerAddress, len(assignment.FollowerBrokers))
	for i, followerBroker := range assignment.FollowerBrokers {
		followers[i] = pb.ServerAddress(followerBroker)
	}
	return NewLocalPartition(FromPbPartition(assignment.Partition), isLeader, followers)
}

func (p *LocalPartition) closePublishers() {
	p.Publishers.SignalShutdown()
	close(p.StopPublishersCh)
}
func (p *LocalPartition) closeSubscribers() {
	p.Subscribers.SignalShutdown()
}

func (p *LocalPartition) WaitUntilNoPublishers() {
	for {
		if p.Publishers.IsEmpty() {
			return
		}
		time.Sleep(113 * time.Millisecond)
	}
}
