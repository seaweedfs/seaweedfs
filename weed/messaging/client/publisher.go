package client

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type Publisher struct {
	publishClient messaging_pb.SeaweedMessaging_PublishClient
}

func (mc *MessagingClient) NewPublisher(namespace, topic string) (*Publisher, error) {

	stream, err := messaging_pb.NewSeaweedMessagingClient(mc.grpcConnection).Publish(context.Background())
	if err != nil {
		return nil, err
	}

	// send init message
	err = stream.Send(&messaging_pb.PublishRequest{
		Init: &messaging_pb.PublishRequest_InitMessage{
			Namespace: namespace,
			Topic:     topic,
			Partition: 0,
		},
	})
	if err != nil {
		return nil, err
	}

	// process init response
	initResponse, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if initResponse.Redirect != nil {
		// TODO follow redirection
	}
	if initResponse.Config != nil {
	}

	// setup looks for control messages
	doneChan := make(chan error, 1)
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			if in.Redirect != nil{
			}
			if in.Config != nil{
			}
		}
	}()

	return &Publisher{
		publishClient: stream,
	}, nil
}

func (p *Publisher) Publish(m *messaging_pb.RawData) error {

	return p.publishClient.Send(&messaging_pb.PublishRequest{
		Data: m,
	})

}

func (p *Publisher) Shutdown() {

	p.publishClient.CloseSend()

}
