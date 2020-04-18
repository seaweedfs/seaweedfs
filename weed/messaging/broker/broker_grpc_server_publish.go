package broker

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) Publish(stream messaging_pb.SeaweedMessaging_PublishServer) error {

	// process initial request
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	// TODO look it up
	topicConfig := &messaging_pb.TopicConfiguration{

	}

	// get lock
	tp := TopicPartition{
		Namespace: in.Init.Namespace,
		Topic:     in.Init.Topic,
		Partition: in.Init.Partition,
	}
	logBuffer := broker.topicLocks.RequestPublisherLock(tp, func(startTime, stopTime time.Time, buf []byte) {

		targetFile := fmt.Sprintf(
			"%s/%s/%s/%04d-%02d-%02d/%02d-%02d.part%02d",
			filer2.TopicsDir, tp.Namespace, tp.Topic,
			startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(),
			tp.Partition,
		)

		if err := broker.appendToFile(targetFile, topicConfig, buf); err != nil {
			glog.V(0).Infof("log write failed %s: %v", targetFile, err)
		}

	})
	defer broker.topicLocks.ReleaseLock(tp, true)

	updatesChan := make(chan int32)

	go func() {
		for update := range updatesChan {
			if err := stream.Send(&messaging_pb.PublishResponse{
				Config: &messaging_pb.PublishResponse_ConfigMessage{
					PartitionCount: update,
				},
			}); err != nil {
				glog.V(0).Infof("err sending publish response: %v", err)
				return
			}
		}
	}()


	// process each message
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if in.Data == nil {
			continue
		}

		m := &messaging_pb.Message{
			Timestamp: time.Now().UnixNano(),
			Key:       in.Data.Key,
			Value:     in.Data.Value,
			Headers:   in.Data.Headers,
		}

		data, err := proto.Marshal(m)
		if err != nil {
			glog.Errorf("marshall error: %v\n", err)
			continue
		}

		logBuffer.AddToBuffer(in.Data.Key, data)

	}
}
