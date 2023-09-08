package pub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *TopicPublisher) doLookup(brokerAddress string) error {
	err := pb.WithBrokerGrpcClient(true,
		brokerAddress,
		p.grpcDialOption,
		func(client mq_pb.SeaweedMessagingClient) error {
			lookupResp, err := client.LookupTopicBrokers(context.Background(),
				&mq_pb.LookupTopicBrokersRequest{
					Topic: &mq_pb.Topic{
						Namespace: p.namespace,
						Name:      p.topic,
					},
					IsForPublish: true,
				})
			if err != nil {
				return err
			}
			for _, brokerPartitionAssignment := range lookupResp.BrokerPartitionAssignments {
				// partition => publishClient
				publishClient, redirectTo, err := p.doConnect(brokerPartitionAssignment.Partition, brokerPartitionAssignment.LeaderBroker)
				if err != nil {
					return err
				}
				for redirectTo != "" {
					publishClient, redirectTo, err = p.doConnect(brokerPartitionAssignment.Partition, redirectTo)
					if err != nil {
						return err
					}
				}
				p.partition2Broker.Insert(
					brokerPartitionAssignment.Partition.RangeStart,
					brokerPartitionAssignment.Partition.RangeStop,
					publishClient)
			}
			return nil
		})

	if err != nil {
		return fmt.Errorf("lookup topic %s/%s: %v", p.namespace, p.topic, err)
	}
	return nil
}

// broker => publish client
// send init message
// save the publishing client
func (p *TopicPublisher) doConnect(partition *mq_pb.Partition, brokerAddress string) (publishClient *PublishClient, redirectTo string, err error) {
	grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, p.grpcDialOption)
	if err != nil {
		return publishClient, redirectTo, fmt.Errorf("dial broker %s: %v", brokerAddress, err)
	}
	brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
	stream, err := brokerClient.Publish(context.Background())
	if err != nil {
		return publishClient, redirectTo, fmt.Errorf("create publish client: %v", err)
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
		return publishClient, redirectTo, fmt.Errorf("send init message: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return publishClient, redirectTo, fmt.Errorf("recv init response: %v", err)
	}
	if resp.Error != "" {
		return publishClient, redirectTo, fmt.Errorf("init response error: %v", resp.Error)
	}
	if resp.RedirectToBroker != "" {
		redirectTo = resp.RedirectToBroker
		return publishClient, redirectTo, nil
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
	return publishClient, redirectTo, nil
}
