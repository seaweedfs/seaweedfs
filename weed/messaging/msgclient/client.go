package msgclient

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MessagingClient struct {
	bootstrapBrokers []string
	grpcConnections  map[broker.TopicPartition]*grpc.ClientConn
	grpcDialOption   grpc.DialOption
}

func NewMessagingClient(bootstrapBrokers ...string) *MessagingClient {
	return &MessagingClient{
		bootstrapBrokers: bootstrapBrokers,
		grpcConnections:  make(map[broker.TopicPartition]*grpc.ClientConn),
		grpcDialOption:   security.LoadClientTLS(util.GetViper(), "grpc.msg_client"),
	}
}


func (mc *MessagingClient) findBroker(tp broker.TopicPartition) (*grpc.ClientConn, error) {

	for _, broker := range mc.bootstrapBrokers {
		grpcConnection, err := pb.GrpcDial(context.Background(), broker, mc.grpcDialOption)
		if err != nil {
			log.Printf("dial broker %s: %v", broker, err)
			continue
		}
		defer grpcConnection.Close()

		resp, err := messaging_pb.NewSeaweedMessagingClient(grpcConnection).FindBroker(context.Background(),
			&messaging_pb.FindBrokerRequest{
				Namespace: tp.Namespace,
				Topic:     tp.Topic,
				Parition:  tp.Partition,
			})
		if err != nil {
			return nil, err
		}

		targetBroker := resp.Broker
		return pb.GrpcDial(context.Background(), targetBroker, mc.grpcDialOption)
	}
	return nil, fmt.Errorf("no broker found for %+v", tp)
}
