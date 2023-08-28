package pub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

func (p *TopicPublisher) doLookup(
	brokerAddress string, grpcDialOption grpc.DialOption) error {
	err := pb.WithBrokerGrpcClient(true,
		brokerAddress,
		grpcDialOption,
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
				// partition => broker
				p.partition2Broker.Insert(
					brokerPartitionAssignment.Partition.RangeStart,
					brokerPartitionAssignment.Partition.RangeStop,
					brokerPartitionAssignment.LeaderBroker)

				// broker => publish client
				// send init message
				// save the publishing client
				brokerAddress := brokerPartitionAssignment.LeaderBroker
				grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, grpcDialOption)
				if err != nil {
					return fmt.Errorf("dial broker %s: %v", brokerAddress, err)
				}
				brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
				publishClient, err := brokerClient.Publish(context.Background())
				if err != nil {
					return fmt.Errorf("create publish client: %v", err)
				}
				p.broker2PublishClient.Set(brokerAddress, publishClient)
				if err = publishClient.Send(&mq_pb.PublishRequest{
					Message: &mq_pb.PublishRequest_Init{
						Init: &mq_pb.PublishRequest_InitMessage{
							Topic: &mq_pb.Topic{
								Namespace: p.namespace,
								Name:      p.topic,
							},
							Partition: &mq_pb.Partition{
								RingSize:   brokerPartitionAssignment.Partition.RingSize,
								RangeStart: brokerPartitionAssignment.Partition.RangeStart,
								RangeStop:  brokerPartitionAssignment.Partition.RangeStop,
							},
						},
					},
				}); err != nil {
					return fmt.Errorf("send init message: %v", err)
				}
			}
			return nil
		})

	if err != nil {
		return fmt.Errorf("lookup topic %s/%s: %v", p.namespace, p.topic, err)
	}
	return nil
}
