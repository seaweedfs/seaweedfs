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

var TIME_FORMAT = "2006-01-02-15-04-05"
func NewLocalPartition(partition Partition, isLeader bool, followerBrokers []pb.ServerAddress, logFlushFn log_buffer.LogFlushFuncType) *LocalPartition {
	return &LocalPartition{
		Partition:       partition,
		isLeader:        isLeader,
		FollowerBrokers: followerBrokers,
		logBuffer: log_buffer.NewLogBuffer(
			fmt.Sprintf("%d/%4d-%4d", partition.UnixTimeNs, partition.RangeStart, partition.RangeStop),
			2*time.Minute,
			logFlushFn,
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

func (p *LocalPartition) Subscribe(clientName string, startPosition log_buffer.MessagePosition, inMemoryOnly bool, onNoMessageFn func() bool, eachMessageFn OnEachMessageFn) {
	p.logBuffer.LoopProcessLogData(clientName, startPosition, inMemoryOnly, 0, onNoMessageFn, eachMessageFn)
}

func (p *LocalPartition) GetEarliestMessageTimeInMemory() time.Time {
	return p.logBuffer.GetEarliestTime()
}

func (p *LocalPartition) HasData() bool {
	return !p.logBuffer.GetEarliestTime().IsZero()
}

func (p *LocalPartition) GetEarliestInMemoryMessagePosition() log_buffer.MessagePosition {
	return p.logBuffer.GetEarliestPosition()
}

func FromPbBrokerPartitionAssignment(self pb.ServerAddress, assignment *mq_pb.BrokerPartitionAssignment, logFlushFn log_buffer.LogFlushFuncType) *LocalPartition {
	isLeader := assignment.LeaderBroker == string(self)
	followers := make([]pb.ServerAddress, len(assignment.FollowerBrokers))
	for i, followerBroker := range assignment.FollowerBrokers {
		followers[i] = pb.ServerAddress(followerBroker)
	}
	return NewLocalPartition(FromPbPartition(assignment.Partition), isLeader, followers, logFlushFn)
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
