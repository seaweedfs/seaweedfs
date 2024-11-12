package agent

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

type MessageQueueAgentOptions struct {
	SeedBrokers []pb.ServerAddress
}

type MessageQueueAgent struct {
	mq_pb.UnimplementedSeaweedMessagingServer
	option         *MessageQueueAgentOptions
	brokers        []pb.ServerAddress
	grpcDialOption grpc.DialOption
}

func NewMessageQueueAgent(option *MessageQueueAgentOptions, grpcDialOption grpc.DialOption) *MessageQueueAgent {

	// check masters to list all brokers

	return &MessageQueueAgent{
		option:         option,
		brokers:        []pb.ServerAddress{},
		grpcDialOption: grpcDialOption,
	}
}
