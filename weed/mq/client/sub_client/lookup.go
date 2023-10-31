package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (sub *TopicSubscriber) doLookup(brokerAddress string) error {
	err := pb.WithBrokerGrpcClient(true,
		brokerAddress,
		sub.SubscriberConfig.GrpcDialOption,
		func(client mq_pb.SeaweedMessagingClient) error {
			lookupResp, err := client.LookupTopicBrokers(context.Background(),
				&mq_pb.LookupTopicBrokersRequest{
					Topic: &mq_pb.Topic{
						Namespace: sub.ContentConfig.Namespace,
						Name:      sub.ContentConfig.Topic,
					},
					IsForPublish: false,
				})
			if err != nil {
				return err
			}
			sub.brokerPartitionAssignments = lookupResp.BrokerPartitionAssignments
			return nil
		})

	if err != nil {
		return fmt.Errorf("lookup topic %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
	}
	return nil
}
