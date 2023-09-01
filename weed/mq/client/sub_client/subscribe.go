package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync"
)

type EachMessageFunc func(key, value []byte) (shouldContinue bool)
type FinalFunc func()

func (sub *TopicSubscriber) Subscribe(eachMessageFn EachMessageFunc, finalFn FinalFunc) error {
	var wg sync.WaitGroup
	for _, brokerPartitionAssignment := range sub.brokerPartitionAssignments {
		brokerAddress := brokerPartitionAssignment.LeaderBroker
		grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, sub.grpcDialOption)
		if err != nil {
			return fmt.Errorf("dial broker %s: %v", brokerAddress, err)
		}
		brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
		subscribeClient, err := brokerClient.Subscribe(context.Background(), &mq_pb.SubscribeRequest{
			Consumer: &mq_pb.SubscribeRequest_Consumer{
				ConsumerGroup: sub.config.ConsumerGroup,
				ConsumerId:    sub.config.ConsumerId,
			},
			Cursor: &mq_pb.SubscribeRequest_Cursor{
				Topic: &mq_pb.Topic{
					Namespace: sub.namespace,
					Name:      sub.topic,
				},
				Partition: &mq_pb.Partition{
					RingSize:   brokerPartitionAssignment.Partition.RingSize,
					RangeStart: brokerPartitionAssignment.Partition.RangeStart,
					RangeStop:  brokerPartitionAssignment.Partition.RangeStop,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if finalFn != nil {
				defer finalFn()
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
					if !eachMessageFn(m.Data.Key, m.Data.Value) {
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
