package msgclient

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type Subscriber struct {
	subscriberClients []messaging_pb.SeaweedMessaging_SubscribeClient
	subscriberId      string
}

/*
func (mc *MessagingClient) NewSubscriber(subscriberId, namespace, topic string, startTime time.Time) (*Subscriber, error) {
	// read topic configuration
	topicConfiguration := &messaging_pb.TopicConfiguration{
		PartitionCount: 4,
	}
	subscriberClients := make([]messaging_pb.SeaweedMessaging_SubscribeClient, topicConfiguration.PartitionCount)

	for i := 0; i < int(topicConfiguration.PartitionCount); i++ {
		client, err := mc.setupSubscriberClient(subscriberId, namespace, topic, int32(i), startTime)
		if err != nil {
			return nil, err
		}
		subscriberClients[i] = client
	}

	return &Subscriber{
		subscriberClients: subscriberClients,
		subscriberId:      subscriberId,
	}, nil
}

func (mc *MessagingClient) setupSubscriberClient(subscriberId, namespace, topic string, partition int32, startTime time.Time) (messaging_pb.SeaweedMessaging_SubscribeClient, error) {

	stream, err := setupSubscriberClient(subscriberId, namespace, topic, partition, startTime)
	if err != nil {
		return stream, err
	}
	if newBroker != nil {

	}

	return stream, nil

}
*/

func setupSubscriberClient(grpcConnection *grpc.ClientConn, subscriberId string, namespace string, topic string, partition int32, startTime time.Time) (stream messaging_pb.SeaweedMessaging_SubscribeClient, err error) {
	stream, err = messaging_pb.NewSeaweedMessagingClient(grpcConnection).Subscribe(context.Background())
	if err != nil {
		return
	}

	// send init message
	err = stream.Send(&messaging_pb.SubscriberMessage{
		Init: &messaging_pb.SubscriberMessage_InitMessage{
			Namespace:     namespace,
			Topic:         topic,
			Partition:     partition,
			StartPosition: messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP,
			TimestampNs:   startTime.UnixNano(),
			SubscriberId:  subscriberId,
		},
	})
	if err != nil {
		return
	}

	// process init response
	_, err = stream.Recv()
	if err != nil {
		return
	}
	return stream, nil
}

func (s *Subscriber) doSubscribe(partition int, processFn func(m *messaging_pb.Message)) error {
	for {
		resp, listenErr := s.subscriberClients[partition].Recv()
		if listenErr == io.EOF {
			return nil
		}
		if listenErr != nil {
			println(listenErr.Error())
			return listenErr
		}
		processFn(resp.Data)
	}
}

// Subscribe starts goroutines to process the messages
func (s *Subscriber) Subscribe(processFn func(m *messaging_pb.Message)) {
	for i := 0; i < len(s.subscriberClients); i++ {
		go s.doSubscribe(i, processFn)
	}
}
