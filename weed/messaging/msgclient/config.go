package msgclient

import (
	"context"
	"log"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (mc *MessagingClient) configureTopic(tp broker.TopicPartition) error {

	return mc.withAnyBroker(func(client messaging_pb.SeaweedMessagingClient) error {
		_, err := client.ConfigureTopic(context.Background(),
			&messaging_pb.ConfigureTopicRequest{
				Namespace: tp.Namespace,
				Topic:     tp.Topic,
				Configuration: &messaging_pb.TopicConfiguration{
					PartitionCount: 0,
					Collection:     "",
					Replication:    "",
					IsTransient:    false,
					Partitoning:    0,
				},
			})
		return err
	})

}

func (mc *MessagingClient) DeleteTopic(namespace, topic string) error {

	return mc.withAnyBroker(func(client messaging_pb.SeaweedMessagingClient) error {
		_, err := client.DeleteTopic(context.Background(),
			&messaging_pb.DeleteTopicRequest{
				Namespace: namespace,
				Topic:     topic,
			})
		return err
	})
}

func (mc *MessagingClient) withAnyBroker(fn func(client messaging_pb.SeaweedMessagingClient) error) error {

	var lastErr error
	for _, broker := range mc.bootstrapBrokers {
		grpcConnection, err := pb.GrpcDial(context.Background(), broker, mc.grpcDialOption)
		if err != nil {
			log.Printf("dial broker %s: %v", broker, err)
			continue
		}
		defer grpcConnection.Close()

		err = fn(messaging_pb.NewSeaweedMessagingClient(grpcConnection))
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return lastErr
}
