package pub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (p *TopicPublisher) doLookupAndConnect(brokerAddress string) error {
	if p.config.CreateTopic {
		err := pb.WithBrokerGrpcClient(true,
			brokerAddress,
			p.grpcDialOption,
			func(client mq_pb.SeaweedMessagingClient) error {
				_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
					Topic: &mq_pb.Topic{
						Namespace: p.namespace,
						Name:      p.topic,
					},
					PartitionCount: p.config.CreateTopicPartitionCount,
				})
				return err
			})
		if err != nil {
			return fmt.Errorf("configure topic %s/%s: %v", p.namespace, p.topic, err)
		}
	}

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
			glog.V(0).Infof("lookup1 topic %s/%s: %v", p.namespace, p.topic, lookupResp)
			if p.config.CreateTopic && err != nil {
				_, err = client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
					Topic: &mq_pb.Topic{
						Namespace: p.namespace,
						Name:      p.topic,
					},
					PartitionCount: p.config.CreateTopicPartitionCount,
				})
				if err != nil {
					return err
				}
				lookupResp, err = client.LookupTopicBrokers(context.Background(),
					&mq_pb.LookupTopicBrokersRequest{
						Topic: &mq_pb.Topic{
							Namespace: p.namespace,
							Name:      p.topic,
						},
						IsForPublish: true,
					})
				glog.V(0).Infof("lookup2 topic %s/%s: %v", p.namespace, p.topic, lookupResp)
			}
			if err != nil {
				return err
			}

			for _, brokerPartitionAssignment := range lookupResp.BrokerPartitionAssignments {
				glog.V(0).Infof("topic %s/%s partition %v leader %s followers %v", p.namespace, p.topic, brokerPartitionAssignment.Partition, brokerPartitionAssignment.LeaderBroker, brokerPartitionAssignment.FollowerBrokers)
				// partition => publishClient
				publishClient, err := p.doConnect(brokerPartitionAssignment.Partition, brokerPartitionAssignment.LeaderBroker)
				if err != nil {
					return err
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
