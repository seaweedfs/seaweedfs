package client

import "github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"

type Publisher struct {
}

func (c *MessagingClient) NewPublisher(namespace, topic string) *Publisher {
	return &Publisher{}
}

func (p *Publisher) Publish(m *messaging_pb.RawData) error{
	return nil
}
