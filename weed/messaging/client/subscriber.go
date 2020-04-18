package client

import "github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"

type Subscriber struct {
}

func (c *MessagingClient) NewSubscriber(namespace, topic string) *Subscriber {
	return &Subscriber{}
}

func (p *Subscriber) Subscribe(processFn func(m *messaging_pb.Message)) error{
	return nil
}
