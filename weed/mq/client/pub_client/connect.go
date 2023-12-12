package pub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

// broker => publish client
// send init message
// save the publishing client
func (p *TopicPublisher) doConnect(partition *mq_pb.Partition, brokerAddress string) (publishClient *PublishClient, err error) {
	log.Printf("connecting to %v for topic partition %+v", brokerAddress, partition)

	grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, p.grpcDialOption)
	if err != nil {
		return publishClient, fmt.Errorf("dial broker %s: %v", brokerAddress, err)
	}
	brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
	stream, err := brokerClient.Publish(context.Background())
	if err != nil {
		return publishClient, fmt.Errorf("create publish client: %v", err)
	}
	publishClient = &PublishClient{
		SeaweedMessaging_PublishClient: stream,
		Broker:                         brokerAddress,
	}
	if err = publishClient.Send(&mq_pb.PublishRequest{
		Message: &mq_pb.PublishRequest_Init{
			Init: &mq_pb.PublishRequest_InitMessage{
				Topic: &mq_pb.Topic{
					Namespace: p.namespace,
					Name:      p.topic,
				},
				Partition: &mq_pb.Partition{
					RingSize:   partition.RingSize,
					RangeStart: partition.RangeStart,
					RangeStop:  partition.RangeStop,
				},
				AckInterval: 128,
			},
		},
	}); err != nil {
		return publishClient, fmt.Errorf("send init message: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return publishClient, fmt.Errorf("recv init response: %v", err)
	}
	if resp.Error != "" {
		return publishClient, fmt.Errorf("init response error: %v", resp.Error)
	}

	go func() {
		for {
			_, err := publishClient.Recv()
			if err != nil {
				e, ok := status.FromError(err)
				if ok && e.Code() == codes.Unknown && e.Message() == "EOF" {
					return
				}
				publishClient.Err = err
				fmt.Printf("publish to %s error: %v\n", publishClient.Broker, err)
				return
			}
		}
	}()
	return publishClient, nil
}
