package agent

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc"
	"sync"
)

type SessionId int64
type SessionEntry[T any] struct {
	entry T
}

type MessageQueueAgentOptions struct {
	SeedBrokers []pb.ServerAddress
}

type MessageQueueAgent struct {
	mq_agent_pb.UnimplementedSeaweedMessagingAgentServer
	option          *MessageQueueAgentOptions
	brokers         []pb.ServerAddress
	grpcDialOption  grpc.DialOption
	publishers      map[SessionId]*SessionEntry[*pub_client.TopicPublisher]
	publishersLock  sync.RWMutex
	subscribers     map[SessionId]*SessionEntry[*sub_client.TopicSubscriber]
	subscribersLock sync.RWMutex
}

func NewMessageQueueAgent(option *MessageQueueAgentOptions, grpcDialOption grpc.DialOption) *MessageQueueAgent {

	// initialize brokers which may change later
	var brokers []pb.ServerAddress
	for _, broker := range option.SeedBrokers {
		brokers = append(brokers, broker)
	}

	return &MessageQueueAgent{
		option:         option,
		brokers:        brokers,
		grpcDialOption: grpcDialOption,
		publishers:     make(map[SessionId]*SessionEntry[*pub_client.TopicPublisher]),
		subscribers:    make(map[SessionId]*SessionEntry[*sub_client.TopicSubscriber]),
	}
}

func (a *MessageQueueAgent) brokersList() []string {
	var brokers []string
	for _, broker := range a.brokers {
		brokers = append(brokers, broker.String())
	}
	return brokers
}
