package msgclient

import (
	"io"
	"log"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type PubChannel struct {
	client         messaging_pb.SeaweedMessaging_PublishClient
	grpcConnection *grpc.ClientConn
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
		client:         pc,
		grpcConnection: grpcConnection,
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

	// println("send closing")
	if err := pc.client.Send(&messaging_pb.PublishRequest{
		Data: &messaging_pb.Message{
			IsClose: true,
		},
	}); err != nil {
		log.Printf("err send close: %v", err)
	}
	// println("receive closing")
	if _, err := pc.client.Recv(); err != nil && err != io.EOF {
		log.Printf("err receive close: %v", err)
	}
	// println("close connection")
	if err := pc.grpcConnection.Close(); err != nil {
		log.Printf("err connection close: %v", err)
	}
	return nil
}
