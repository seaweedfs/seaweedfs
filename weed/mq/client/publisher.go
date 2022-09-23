package client

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/messages"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"time"
)

type PublishProcessor interface {
	AddMessage(m *messages.Message) error
	Shutdown() error
}

type PublisherOption struct {
	Masters string
	Topic   string
}

type Publisher struct {
	option    *PublisherOption
	masters   []pb.ServerAddress
	processor *PublishStreamProcessor
}

func NewPublisher(option *PublisherOption) *Publisher {
	p := &Publisher{
		masters:   pb.ServerAddresses(option.Masters).ToAddresses(),
		option:    option,
		processor: NewPublishStreamProcessor(3, 887*time.Millisecond),
	}
	return p
}

func (p Publisher) Publish(m *messages.Message) error {
	return p.processor.AddMessage(m)
}

func (p Publisher) Shutdown() error {
	return p.processor.Shutdown()
}
