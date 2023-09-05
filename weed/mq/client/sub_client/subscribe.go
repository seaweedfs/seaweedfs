package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync"
)

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {
	var wg sync.WaitGroup
	for _, brokerPartitionAssignment := range sub.brokerPartitionAssignments {
		brokerAddress := brokerPartitionAssignment.LeaderBroker
		grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, sub.SubscriberConfig.GrpcDialOption)
		if err != nil {
			return fmt.Errorf("dial broker %s: %v", brokerAddress, err)
		}
		brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
		subscribeClient, err := brokerClient.Subscribe(context.Background(), &mq_pb.SubscribeRequest{
			Consumer: &mq_pb.SubscribeRequest_Consumer{
				ConsumerGroup: sub.SubscriberConfig.GroupId,
				ConsumerId:    sub.SubscriberConfig.GroupInstanceId,
			},
			Cursor: &mq_pb.SubscribeRequest_Cursor{
				Topic: &mq_pb.Topic{
					Namespace: sub.ContentConfig.Namespace,
					Name:      sub.ContentConfig.Topic,
				},
				Partition: &mq_pb.Partition{
					RingSize:   brokerPartitionAssignment.Partition.RingSize,
					RangeStart: brokerPartitionAssignment.Partition.RangeStart,
					RangeStop:  brokerPartitionAssignment.Partition.RangeStop,
				},
				Filter: sub.ContentConfig.Filter,
			},
		})
		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sub.OnCompletionFunc != nil {
				defer sub.OnCompletionFunc()
			}
			for {
				resp, err := subscribeClient.Recv()
				if err != nil {
					fmt.Printf("subscribe error: %v\n", err)
					return
				}
				if resp.Message == nil {
					continue
				}
				switch m := resp.Message.(type) {
				case *mq_pb.SubscribeResponse_Data:
					if !sub.OnEachMessageFunc(m.Data.Key, m.Data.Value) {
						return
					}
				case *mq_pb.SubscribeResponse_Ctrl:
					// ignore
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
