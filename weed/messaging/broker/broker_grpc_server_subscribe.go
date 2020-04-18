package broker

import (
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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

	subscriberId := in.Init.SubscriberId

	// get lock
	tp := TopicPartition{
		Namespace: in.Init.Namespace,
		Topic:     in.Init.Topic,
		Partition: in.Init.Partition,
	}
	lock := broker.topicLocks.RequestSubscriberLock(tp)
	defer broker.topicLocks.ReleaseLock(tp, false)
	cond := sync.NewCond(&lock.Mutex)

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
			Data:     m,
		})
		if err != nil {
			glog.V(0).Infof("=> subscriber %v: %+v", subscriberId, err)
		}
		return err
	}

	// loop through all messages
	for {

		_, buf := lock.logBuffer.ReadFromBuffer(lastReadTime)

		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			entryData := buf[pos+4 : pos+4+int(size)]

			m := &messaging_pb.Message{}
			if err = proto.Unmarshal(entryData, m); err != nil {
				glog.Errorf("unexpected unmarshal messaging_pb.Message: %v", err)
				pos += 4 + int(size)
				continue
			}

			if err = eachMessageFn(m); err != nil {
				return err
			}

			lastReadTime = time.Unix(0, m.Timestamp)
			pos += 4 + int(size)
		}

		lock.Mutex.Lock()
		cond.Wait()
		lock.Mutex.Unlock()
	}

}
