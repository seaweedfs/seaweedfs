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
			pubClient, err := client.Publish(context.Background())
			if err != nil {
				return err
			}
			if initErr := pubClient.Send(&mq_pb.PublishRequest{
				Message: &mq_pb.PublishRequest_Init{
					Init: &mq_pb.PublishRequest_InitMessage{
						Topic: &mq_pb.Topic{
							Namespace: "test",
							Name:      "test",
						},
						Partition: &mq_pb.Partition{
							RangeStart: 0,
							RangeStop:  1,
							RingSize:   1,
						},
					},
				},
			}); initErr != nil {
				return initErr
			}

			for i := 0; i < 10; i++ {
				if dataErr := pubClient.Send(&mq_pb.PublishRequest{
					Message: &mq_pb.PublishRequest_Data{
						Data: &mq_pb.DataMessage{
							Key:   []byte(fmt.Sprintf("key-%d", i)),
							Value: []byte(fmt.Sprintf("value-%d", i)),
						},
					},
				}); dataErr != nil {
					return dataErr
				}
			}
			return nil
		})

	if err != nil {
		fmt.Println(err)
	}

}
