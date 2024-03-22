package topic

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"sync/atomic"
	"time"
)

type LocalPartition struct {
	Partition
	isLeader        bool
	FollowerBrokers []pb.ServerAddress
	LogBuffer       *log_buffer.LogBuffer
	ConsumerCount   int32
	Publishers      *LocalPartitionPublishers
	Subscribers     *LocalPartitionSubscribers
	FollowerId      int32
}

var TIME_FORMAT = "2006-01-02-15-04-05"

func NewLocalPartition(partition Partition, isLeader bool, followerBrokers []pb.ServerAddress, logFlushFn log_buffer.LogFlushFuncType, readFromDiskFn log_buffer.LogReadFromDiskFuncType) *LocalPartition {
	return &LocalPartition{
		Partition:       partition,
		isLeader:        isLeader,
		FollowerBrokers: followerBrokers,
		LogBuffer: log_buffer.NewLogBuffer(fmt.Sprintf("%d/%04d-%04d", partition.UnixTimeNs, partition.RangeStart, partition.RangeStop),
			2*time.Minute, logFlushFn, readFromDiskFn, func() {}),
		Publishers:  NewLocalPartitionPublishers(),
		Subscribers: NewLocalPartitionSubscribers(),
	}
}

func (p *LocalPartition) Publish(message *mq_pb.DataMessage) {
	p.LogBuffer.AddToBuffer(message.Key, message.Value, time.Now().UnixNano())
}

func (p *LocalPartition) Subscribe(clientName string, startPosition log_buffer.MessagePosition,
	onNoMessageFn func() bool, eachMessageFn log_buffer.EachLogEntryFuncType) error {
	var processedPosition log_buffer.MessagePosition
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool

	for {
		processedPosition, isDone, readPersistedLogErr = p.LogBuffer.ReadFromDiskFn(startPosition, 0, eachMessageFn)
		if readPersistedLogErr != nil {
			glog.V(0).Infof("%s read %v persisted log: %v", clientName, p.Partition, readPersistedLogErr)
			return readPersistedLogErr
		}
		if isDone {
			return nil
		}

		startPosition = processedPosition
		processedPosition, isDone, readInMemoryLogErr = p.LogBuffer.LoopProcessLogData(clientName, startPosition, 0, onNoMessageFn, eachMessageFn)
		if isDone {
			return nil
		}
		startPosition = processedPosition

		if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
			continue
		}
		if readInMemoryLogErr != nil {
			glog.V(0).Infof("%s read %v in memory log: %v", clientName, p.Partition, readInMemoryLogErr)
			return readInMemoryLogErr
		}
	}
}

func (p *LocalPartition) GetEarliestMessageTimeInMemory() time.Time {
	return p.LogBuffer.GetEarliestTime()
}

func (p *LocalPartition) HasData() bool {
	return !p.LogBuffer.GetEarliestTime().IsZero()
}

func (p *LocalPartition) GetEarliestInMemoryMessagePosition() log_buffer.MessagePosition {
	return p.LogBuffer.GetEarliestPosition()
}

func FromPbBrokerPartitionAssignment(self pb.ServerAddress, partition Partition, assignment *mq_pb.BrokerPartitionAssignment, logFlushFn log_buffer.LogFlushFuncType, readFromDiskFn log_buffer.LogReadFromDiskFuncType) *LocalPartition {
	isLeader := assignment.LeaderBroker == string(self)
	followers := make([]pb.ServerAddress, len(assignment.FollowerBrokers))
	for i, followerBroker := range assignment.FollowerBrokers {
		followers[i] = pb.ServerAddress(followerBroker)
	}
	return NewLocalPartition(partition, isLeader, followers, logFlushFn, readFromDiskFn)
}

func (p *LocalPartition) closePublishers() {
	p.Publishers.SignalShutdown()
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

func (p *LocalPartition) MaybeShutdownLocalPartition() (hasShutdown bool) {
	if p.Publishers.IsEmpty() && p.Subscribers.IsEmpty() {
		p.LogBuffer.ShutdownLogBuffer()
		hasShutdown = true
	}
	return
}

func (p *LocalPartition) Shutdown() {
	p.closePublishers()
	p.closeSubscribers()
	p.LogBuffer.ShutdownLogBuffer()
	atomic.StoreInt32(&p.FollowerId, 0)
}
