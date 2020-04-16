package weed_server

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) Subscribe(request *messaging_pb.SubscribeRequest, server messaging_pb.SeaweedMessaging_SubscribeServer) error {
	panic("implement me")
}

func (broker *MessageBroker) Publish(server messaging_pb.SeaweedMessaging_PublishServer) error {
	panic("implement me")
}

func (broker *MessageBroker) ConfigureTopic(c context.Context, request *messaging_pb.ConfigureTopicRequest) (*messaging_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) GetTopicConfiguration(c context.Context, request *messaging_pb.GetTopicConfigurationRequest) (*messaging_pb.GetTopicConfigurationResponse, error) {
	panic("implement me")
}
