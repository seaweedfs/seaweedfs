package agent

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc"
	"sync"
)

type SessionId int64
type PublisherEntry struct {
	publisher      *pub_client.TopicPublisher
	lastActiveTsNs int64
}

type MessageQueueAgentOptions struct {
	SeedBrokers []pb.ServerAddress
}

type MessageQueueAgent struct {
	mq_agent_pb.UnimplementedSeaweedMessagingAgentServer
	option         *MessageQueueAgentOptions
	brokers        []pb.ServerAddress
	grpcDialOption grpc.DialOption
	publishers     map[SessionId]*PublisherEntry
	publishersLock sync.RWMutex
}

func NewMessageQueueAgent(option *MessageQueueAgentOptions, grpcDialOption grpc.DialOption) *MessageQueueAgent {

	// check masters to list all brokers

	return &MessageQueueAgent{
		option:         option,
		brokers:        []pb.ServerAddress{},
		grpcDialOption: grpcDialOption,
		publishers:     make(map[SessionId]*PublisherEntry),
	}
}

func (a *MessageQueueAgent) brokersList() []string {
	var brokers []string
	for _, broker := range a.brokers {
		brokers = append(brokers, broker.String())
	}
	return brokers
}
