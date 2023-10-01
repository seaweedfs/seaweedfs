package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync"
)

func (sub *TopicSubscriber) doProcess() error {
	var wg sync.WaitGroup
	for _, brokerPartitionAssignment := range sub.brokerPartitionAssignments {
		brokerAddress := brokerPartitionAssignment.LeaderBroker
		grpcConnection, err := pb.GrpcDial(context.Background(), brokerAddress, true, sub.SubscriberConfig.GrpcDialOption)
		if err != nil {
			return fmt.Errorf("dial broker %s: %v", brokerAddress, err)
		}
		brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
		subscribeClient, err := brokerClient.Subscribe(context.Background(), &mq_pb.SubscribeRequest{
			Message: &mq_pb.SubscribeRequest_Init{
				Init: &mq_pb.SubscribeRequest_InitMessage{
					ConsumerGroup: sub.SubscriberConfig.GroupId,
					ConsumerId:    sub.SubscriberConfig.GroupInstanceId,
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
			defer func() {
				subscribeClient.SendMsg(&mq_pb.SubscribeRequest{
					Message: &mq_pb.SubscribeRequest_Ack{
						Ack: &mq_pb.SubscribeRequest_AckMessage{
							Sequence: 0,
						},
					},
				})
				subscribeClient.CloseSend()
			}()
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
					if m.Ctrl.IsEndOfStream || m.Ctrl.IsEndOfTopic {
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
