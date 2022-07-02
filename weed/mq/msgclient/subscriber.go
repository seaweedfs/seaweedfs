package msgclient

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/mq/broker"
	"github.com/chrislusf/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

type Subscriber struct {
	subscriberClients []mq_pb.SeaweedMessaging_SubscribeClient
	subscriberCancels []context.CancelFunc
	subscriberId      string
}

func (mc *MessagingClient) NewSubscriber(subscriberId, namespace, topic string, partitionId int, startTime time.Time) (*Subscriber, error) {
	// read topic configuration
	topicConfiguration := &mq_pb.TopicConfiguration{
		PartitionCount: 4,
	}
	subscriberClients := make([]mq_pb.SeaweedMessaging_SubscribeClient, topicConfiguration.PartitionCount)
	subscriberCancels := make([]context.CancelFunc, topicConfiguration.PartitionCount)

	for i := 0; i < int(topicConfiguration.PartitionCount); i++ {
		if partitionId >= 0 && i != partitionId {
			continue
		}
		tp := broker.TopicPartition{
			Namespace: namespace,
			Topic:     topic,
			Partition: int32(i),
		}
		grpcClientConn, err := mc.findBroker(tp)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		client, err := setupSubscriberClient(ctx, grpcClientConn, tp, subscriberId, startTime)
		if err != nil {
			return nil, err
		}
		subscriberClients[i] = client
		subscriberCancels[i] = cancel
	}

	return &Subscriber{
		subscriberClients: subscriberClients,
		subscriberCancels: subscriberCancels,
		subscriberId:      subscriberId,
	}, nil
}

func setupSubscriberClient(ctx context.Context, grpcConnection *grpc.ClientConn, tp broker.TopicPartition, subscriberId string, startTime time.Time) (stream mq_pb.SeaweedMessaging_SubscribeClient, err error) {
	stream, err = mq_pb.NewSeaweedMessagingClient(grpcConnection).Subscribe(ctx)
	if err != nil {
		return
	}

	// send init message
	err = stream.Send(&mq_pb.SubscriberMessage{
		Init: &mq_pb.SubscriberMessage_InitMessage{
			Namespace:     tp.Namespace,
			Topic:         tp.Topic,
			Partition:     tp.Partition,
			StartPosition: mq_pb.SubscriberMessage_InitMessage_TIMESTAMP,
			TimestampNs:   startTime.UnixNano(),
			SubscriberId:  subscriberId,
		},
	})
	if err != nil {
		return
	}

	return stream, nil
}

func doSubscribe(subscriberClient mq_pb.SeaweedMessaging_SubscribeClient, processFn func(m *mq_pb.Message)) error {
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
func (s *Subscriber) Subscribe(processFn func(m *mq_pb.Message)) {
	var wg sync.WaitGroup
	for i := 0; i < len(s.subscriberClients); i++ {
		if s.subscriberClients[i] != nil {
			wg.Add(1)
			go func(subscriberClient mq_pb.SeaweedMessaging_SubscribeClient) {
				defer wg.Done()
				doSubscribe(subscriberClient, processFn)
			}(s.subscriberClients[i])
		}
	}
	wg.Wait()
}

func (s *Subscriber) Shutdown() {
	for i := 0; i < len(s.subscriberClients); i++ {
		if s.subscriberCancels[i] != nil {
			s.subscriberCancels[i]()
		}
	}
}
