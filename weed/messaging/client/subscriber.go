package client

import (
	"context"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type Subscriber struct {
	subscriberClient messaging_pb.SeaweedMessaging_SubscribeClient
}

func (mc *MessagingClient) NewSubscriber(subscriberId, namespace, topic string) (*Subscriber, error) {
	stream, err := messaging_pb.NewSeaweedMessagingClient(mc.grpcConnection).Subscribe(context.Background())
	if err != nil {
		return nil, err
	}

	// send init message
	err = stream.Send(&messaging_pb.SubscriberMessage{
		Init: &messaging_pb.SubscriberMessage_InitMessage{
			Namespace:     namespace,
			Topic:         topic,
			Partition:     0,
			StartPosition: messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP,
			TimestampNs:   time.Now().UnixNano(),
			SubscriberId:  subscriberId,
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

	return &Subscriber{
		subscriberClient: stream,
	}, nil
}

func (s *Subscriber) Subscribe(processFn func(m *messaging_pb.Message)) error {
	for {
		resp, listenErr := s.subscriberClient.Recv()
		if listenErr == io.EOF {
			return nil
		}
		if listenErr != nil {
			return listenErr
		}
		processFn(resp.Data)
	}
}
