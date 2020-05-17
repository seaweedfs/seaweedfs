package msgclient

import (
	"context"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"google.golang.org/grpc"
)

type Subscriber struct {
	subscriberClients []messaging_pb.SeaweedMessaging_SubscribeClient
	subscriberId      string
}

func (mc *MessagingClient) NewSubscriber(subscriberId, namespace, topic string, partitionId int, startTime time.Time) (*Subscriber, error) {
	// read topic configuration
	topicConfiguration := &messaging_pb.TopicConfiguration{
		PartitionCount: 4,
	}
	subscriberClients := make([]messaging_pb.SeaweedMessaging_SubscribeClient, topicConfiguration.PartitionCount)

	for i := 0; i < int(topicConfiguration.PartitionCount); i++ {
		if partitionId>=0 && i != partitionId {
			continue
		}
		tp := broker.TopicPartition{
			Namespace: namespace,
			Topic: topic,
			Partition: int32(i),
		}
		grpcClientConn, err := mc.findBroker(tp)
		if err != nil {
			return nil, err
		}
		client, err := setupSubscriberClient(grpcClientConn, tp, subscriberId, startTime)
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

func setupSubscriberClient(grpcConnection *grpc.ClientConn, tp broker.TopicPartition, subscriberId string, startTime time.Time) (stream messaging_pb.SeaweedMessaging_SubscribeClient, err error) {
	stream, err = messaging_pb.NewSeaweedMessagingClient(grpcConnection).Subscribe(context.Background())
	if err != nil {
		return
	}

	// send init message
	err = stream.Send(&messaging_pb.SubscriberMessage{
		Init: &messaging_pb.SubscriberMessage_InitMessage{
			Namespace:     tp.Namespace,
			Topic:         tp.Topic,
			Partition:     tp.Partition,
			StartPosition: messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP,
			TimestampNs:   startTime.UnixNano(),
			SubscriberId:  subscriberId,
		},
	})
	if err != nil {
		return
	}

	return stream, nil
}

func doSubscribe(subscriberClient messaging_pb.SeaweedMessaging_SubscribeClient, processFn func(m *messaging_pb.Message)) error {
	for {
		resp, listenErr := subscriberClient.Recv()
		if listenErr == io.EOF {
			return nil
		}
		if listenErr != nil {
			println(listenErr.Error())
			return listenErr
		}
		if resp.Data == nil {
			// this could be heartbeat from broker
			continue
		}
		processFn(resp.Data)
	}
}

// Subscribe starts goroutines to process the messages
func (s *Subscriber) Subscribe(processFn func(m *messaging_pb.Message)) {
	for i := 0; i < len(s.subscriberClients); i++ {
		if s.subscriberClients[i] != nil {
			go doSubscribe(s.subscriberClients[i], processFn)
		}
	}
}
