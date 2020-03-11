package weed_server

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/queue_pb"
)

func (broker *MessageBroker) ConfigureTopic(context.Context, *queue_pb.ConfigureTopicRequest) (*queue_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) DeleteTopic(context.Context, *queue_pb.DeleteTopicRequest) (*queue_pb.DeleteTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) StreamWrite(queue_pb.SeaweedQueue_StreamWriteServer) error {
	panic("implement me")
}

func (broker *MessageBroker) StreamRead(*queue_pb.ReadMessageRequest, queue_pb.SeaweedQueue_StreamReadServer) error {
	panic("implement me")
}
