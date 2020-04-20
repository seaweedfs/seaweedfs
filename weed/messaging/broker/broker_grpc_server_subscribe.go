package broker

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) Subscribe(stream messaging_pb.SeaweedMessaging_SubscribeServer) error {

	// process initial request
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	var messageCount int64
	subscriberId := in.Init.SubscriberId
	fmt.Printf("+ subscriber %s\n", subscriberId)
	defer func() {
		fmt.Printf("- subscriber %s: %d messages\n", subscriberId, messageCount)
	}()

	// TODO look it up
	topicConfig := &messaging_pb.TopicConfiguration{
		IsTransient: true,
	}

	if err = stream.Send(&messaging_pb.BrokerMessage{
		Redirect: nil,
	}); err != nil {
		return err
	}

	// get lock
	tp := TopicPartition{
		Namespace: in.Init.Namespace,
		Topic:     in.Init.Topic,
		Partition: in.Init.Partition,
	}
	lock := broker.topicLocks.RequestLock(tp, topicConfig, false)
	defer broker.topicLocks.ReleaseLock(tp, false)

	lastReadTime := time.Now()
	switch in.Init.StartPosition {
	case messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP:
		lastReadTime = time.Unix(0, in.Init.TimestampNs)
	case messaging_pb.SubscriberMessage_InitMessage_LATEST:
	case messaging_pb.SubscriberMessage_InitMessage_EARLIEST:
	}

	// how to process each message
	// an error returned will end the subscription
	eachMessageFn := func(m *messaging_pb.Message) error {
		err := stream.Send(&messaging_pb.BrokerMessage{
			Data: m,
		})
		if err != nil {
			glog.V(0).Infof("=> subscriber %v: %+v", subscriberId, err)
		}
		return err
	}

	messageCount, err = lock.logBuffer.LoopProcessLogData(lastReadTime, func() bool {
		lock.Mutex.Lock()
		lock.cond.Wait()
		lock.Mutex.Unlock()
		return true
	}, func(logEntry *filer_pb.LogEntry) error {
		m := &messaging_pb.Message{}
		if err = proto.Unmarshal(logEntry.Data, m); err != nil {
			glog.Errorf("unexpected unmarshal messaging_pb.Message: %v", err)
			return err
		}
		// fmt.Printf("sending : %d bytes\n", len(m.Value))
		if err = eachMessageFn(m); err != nil {
			glog.Errorf("sending %d bytes to %s: %s", len(m.Value), subscriberId, err)
			return err
		}
		return nil
	})

	return err

}
