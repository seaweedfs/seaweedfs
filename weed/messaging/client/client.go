package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MessagingClient struct {
	bootstrapBrokers []string
	grpcConnection   *grpc.ClientConn
}

func NewMessagingClient(bootstrapBrokers []string) (*MessagingClient, error) {
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.msg_client")

	grpcConnection, err := pb.GrpcDial(context.Background(), "localhost:17777", grpcDialOption)
	if err != nil {
		return nil, err
	}

	return &MessagingClient{
		bootstrapBrokers: bootstrapBrokers,
		grpcConnection:   grpcConnection,
	}, nil
}
