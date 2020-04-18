package broker

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) ConfigureTopic(c context.Context, request *messaging_pb.ConfigureTopicRequest) (*messaging_pb.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (broker *MessageBroker) GetTopicConfiguration(c context.Context, request *messaging_pb.GetTopicConfigurationRequest) (*messaging_pb.GetTopicConfigurationResponse, error) {
	panic("implement me")
}
