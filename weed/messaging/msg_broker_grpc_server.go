package messaging

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/util/log_buffer"
)

func (broker *MessageBroker) Subscribe(server messaging_pb.SeaweedMessaging_SubscribeServer) error {
	panic("implement me")
}

func (broker *MessageBroker) Publish(stream messaging_pb.SeaweedMessaging_PublishServer) error {

	// process initial request
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	namespace, topic, partition := in.Init.Namespace, in.Init.Topic, in.Init.Partition

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

	logBuffer := log_buffer.NewLogBuffer(time.Minute, func(startTime, stopTime time.Time, buf []byte) {

		//targetFile :=
		fmt.Sprintf("%s/%s/%s/%04d-%02d-%02d/%02d-%02d.part%02d",
			filer2.TopicsDir, namespace, topic,
			startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(),
			partition,
		)

		/*
			if err := f.appendToFile(targetFile, buf); err != nil {
				glog.V(0).Infof("log write failed %s: %v", targetFile, err)
			}
		*/

	}, func() {
		// notify subscribers
	})

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
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

func (broker *MessageBroker) ConfigureTopic(c context.Context, request *messaging_pb.ConfigureTopicRequest) (*messaging_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) GetTopicConfiguration(c context.Context, request *messaging_pb.GetTopicConfigurationRequest) (*messaging_pb.GetTopicConfigurationResponse, error) {
	panic("implement me")
}
