package topic

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"sync/atomic"
	"time"
)

type LocalPartition struct {
	ListenersWaits int64
	AckTsNs        int64

	// notifying clients
	ListenersLock sync.Mutex
	ListenersCond *sync.Cond

	Partition
	LogBuffer   *log_buffer.LogBuffer
	Publishers  *LocalPartitionPublishers
	Subscribers *LocalPartitionSubscribers

	publishFolloweMeStream mq_pb.SeaweedMessaging_PublishFollowMeClient
	followerGrpcConnection *grpc.ClientConn
	Follower               string
}

var TIME_FORMAT = "2006-01-02-15-04-05"
var PartitionGenerationFormat = "v2006-01-02-15-04-05"

func NewLocalPartition(partition Partition, logFlushFn log_buffer.LogFlushFuncType, readFromDiskFn log_buffer.LogReadFromDiskFuncType) *LocalPartition {
	lp := &LocalPartition{
		Partition:   partition,
		Publishers:  NewLocalPartitionPublishers(),
		Subscribers: NewLocalPartitionSubscribers(),
	}
	lp.ListenersCond = sync.NewCond(&lp.ListenersLock)
	lp.LogBuffer = log_buffer.NewLogBuffer(fmt.Sprintf("%d/%04d-%04d", partition.UnixTimeNs, partition.RangeStart, partition.RangeStop),
		2*time.Minute, logFlushFn, readFromDiskFn, func() {
			if atomic.LoadInt64(&lp.ListenersWaits) > 0 {
				lp.ListenersCond.Broadcast()
			}
		})
	return lp
}

func (p *LocalPartition) Publish(message *mq_pb.DataMessage) error {
	p.LogBuffer.AddToBuffer(message)

	// maybe send to the follower
	if p.publishFolloweMeStream != nil {
		// println("recv", string(message.Key), message.TsNs)
		if followErr := p.publishFolloweMeStream.Send(&mq_pb.PublishFollowMeRequest{
			Message: &mq_pb.PublishFollowMeRequest_Data{
				Data: message,
			},
		}); followErr != nil {
			return fmt.Errorf("send to follower %s: %v", p.Follower, followErr)
		}
	} else {
		atomic.StoreInt64(&p.AckTsNs, message.TsNs)
	}

	return nil
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

func (p *LocalPartition) closePublishers() {
	p.Publishers.SignalShutdown()
}
func (p *LocalPartition) closeSubscribers() {
	p.Subscribers.SignalShutdown()
}

func (p *LocalPartition) WaitUntilNoPublishers() {
	for {
		if p.Publishers.Size() == 0 {
			return
		}
		time.Sleep(113 * time.Millisecond)
	}
}

func (p *LocalPartition) MaybeConnectToFollowers(initMessage *mq_pb.PublishMessageRequest_InitMessage, grpcDialOption grpc.DialOption) (err error) {
	if p.publishFolloweMeStream != nil {
		return nil
	}
	if initMessage.FollowerBroker == "" {
		return nil
	}

	p.Follower = initMessage.FollowerBroker
	ctx := context.Background()
	p.followerGrpcConnection, err = pb.GrpcDial(ctx, p.Follower, true, grpcDialOption)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", p.Follower, err)
	}
	followerClient := mq_pb.NewSeaweedMessagingClient(p.followerGrpcConnection)
	p.publishFolloweMeStream, err = followerClient.PublishFollowMe(ctx)
	if err != nil {
		return fmt.Errorf("fail to create publish client: %v", err)
	}
	if err = p.publishFolloweMeStream.Send(&mq_pb.PublishFollowMeRequest{
		Message: &mq_pb.PublishFollowMeRequest_Init{
			Init: &mq_pb.PublishFollowMeRequest_InitMessage{
				Topic:     initMessage.Topic,
				Partition: initMessage.Partition,
			},
		},
	}); err != nil {
		return err
	}

	// start receiving ack from follower
	go func() {
		defer func() {
			// println("stop receiving ack from follower")
		}()

		for {
			ack, err := p.publishFolloweMeStream.Recv()
			if err != nil {
				e, _ := status.FromError(err)
				if e.Code() == codes.Canceled {
					glog.V(0).Infof("local partition %v follower %v stopped", p.Partition, p.Follower)
					return
				}
				glog.Errorf("Receiving local partition %v  follower %s ack: %v", p.Partition, p.Follower, err)
				return
			}
			atomic.StoreInt64(&p.AckTsNs, ack.AckTsNs)
			// println("recv ack", ack.AckTsNs)
		}
	}()
	return nil
}

func (p *LocalPartition) MaybeShutdownLocalPartition() (hasShutdown bool) {

	if p.Publishers.Size() == 0 && p.Subscribers.Size() == 0 {
		p.LogBuffer.ShutdownLogBuffer()
		for !p.LogBuffer.IsAllFlushed() {
			time.Sleep(113 * time.Millisecond)
		}
		if p.publishFolloweMeStream != nil {
			// send close to the follower
			if followErr := p.publishFolloweMeStream.Send(&mq_pb.PublishFollowMeRequest{
				Message: &mq_pb.PublishFollowMeRequest_Close{
					Close: &mq_pb.PublishFollowMeRequest_CloseMessage{},
				},
			}); followErr != nil {
				glog.Errorf("Error closing follower stream: %v", followErr)
			}
			glog.V(4).Infof("closing grpcConnection to follower")
			p.followerGrpcConnection.Close()
			p.publishFolloweMeStream = nil
			p.Follower = ""
		}

		hasShutdown = true
	}

	glog.V(0).Infof("local partition %v Publisher:%d Subscriber:%d follower:%s shutdown %v", p.Partition, p.Publishers.Size(), p.Subscribers.Size(), p.Follower, hasShutdown)
	return
}

func (p *LocalPartition) Shutdown() {
	p.closePublishers()
	p.closeSubscribers()
	p.LogBuffer.ShutdownLogBuffer()
	glog.V(0).Infof("local partition %v shutting down", p.Partition)
}

func (p *LocalPartition) NotifyLogFlushed(flushTsNs int64) {
	if p.publishFolloweMeStream != nil {
		if followErr := p.publishFolloweMeStream.Send(&mq_pb.PublishFollowMeRequest{
			Message: &mq_pb.PublishFollowMeRequest_Flush{
				Flush: &mq_pb.PublishFollowMeRequest_FlushMessage{
					TsNs: flushTsNs,
				},
			},
		}); followErr != nil {
			glog.Errorf("send follower %s flush message: %v", p.Follower, followErr)
		}
		// println("notifying", p.Follower, "flushed at", flushTsNs)
	}
}
