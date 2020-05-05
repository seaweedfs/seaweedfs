package msgclient

import (
	"io"
	"log"
	"time"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type PubChannel struct {
	client messaging_pb.SeaweedMessaging_PublishClient
}

func (mc *MessagingClient) NewPubChannel(chanName string) (*PubChannel, error) {
	tp := broker.TopicPartition{
		Namespace: "chan",
		Topic:     chanName,
		Partition: 0,
	}
	grpcConnection, err := mc.findBroker(tp)
	if err != nil {
		return nil, err
	}
	pc, err := setupPublisherClient(grpcConnection, tp)
	if err != nil {
		return nil, err
	}
	return &PubChannel{
		client: pc,
	}, nil
}

func (pc *PubChannel) Publish(m []byte) error {
	return pc.client.Send(&messaging_pb.PublishRequest{
		Data: &messaging_pb.Message{
			Value: m,
		},
	})
}
func (pc *PubChannel) Close() error {
	return pc.client.CloseSend()
}

type SubChannel struct {
	ch     chan []byte
	stream messaging_pb.SeaweedMessaging_SubscribeClient
}

func (mc *MessagingClient) NewSubChannel(chanName string) (*SubChannel, error) {
	tp := broker.TopicPartition{
		Namespace: "chan",
		Topic:     chanName,
		Partition: 0,
	}
	grpcConnection, err := mc.findBroker(tp)
	if err != nil {
		return nil, err
	}
	sc, err := setupSubscriberClient(grpcConnection, "", "chan", chanName, 0, time.Unix(0,0))
	if err != nil {
		return nil, err
	}

	t := &SubChannel{
		ch:     make(chan []byte),
		stream: sc,
	}

	go func() {
		for {
			resp, subErr := t.stream.Recv()
			if subErr == io.EOF {
				return
			}
			if subErr != nil {
				log.Printf("fail to receive from netchan %s: %v", chanName, subErr)
				return
			}
			if resp.IsClose {
				close(t.ch)
				return
			}
			if resp.Data != nil {
				t.ch <- resp.Data.Value
			}
		}
	}()

	return t, nil
}

func (sc *SubChannel) Channel() chan []byte {
	return sc.ch
}
