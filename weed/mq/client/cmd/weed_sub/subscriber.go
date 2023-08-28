package main

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	err := pb.WithBrokerGrpcClient(true,
		"localhost:17777",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		func(client mq_pb.SeaweedMessagingClient) error {
			subClient, err := client.Subscribe(context.Background(), &mq_pb.SubscribeRequest{
				Init: &mq_pb.SubscribeRequest_InitMessage{
					Topic: &mq_pb.Topic{
						Namespace: "test",
						Name:      "test",
					},
				},
			})
			if err != nil {
				return err
			}

			for {
				resp, err := subClient.Recv()
				if err != nil {
					return err
				}
				if resp.GetCtrl() != nil {
					if resp.GetCtrl().Error != "" {
						return fmt.Errorf("ctrl error: %v", resp.GetCtrl().Error)
					}
				}
				if resp.GetData() != nil {
					println(string(resp.GetData().Key), "=>", string(resp.GetData().Value))
				}

			}
			return nil
		})

	if err != nil {
		fmt.Println(err)
	}
}
