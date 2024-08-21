package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
)

func (sub *TopicSubscriber) onEachPartition(assigned *mq_pb.BrokerPartitionAssignment, stopCh chan struct{}) error {
	// connect to the partition broker
	return pb.WithBrokerGrpcClient(true, assigned.LeaderBroker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {

		subscribeClient, err := client.SubscribeMessage(context.Background())
		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}

		perPartitionConcurrency := sub.SubscriberConfig.PerPartitionConcurrency
		if perPartitionConcurrency <= 0 {
			perPartitionConcurrency = 1
		}

		if err = subscribeClient.Send(&mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Init{
				Init: &mq_pb.SubscribeMessageRequest_InitMessage{
					ConsumerGroup: sub.SubscriberConfig.ConsumerGroup,
					ConsumerId:    sub.SubscriberConfig.ConsumerGroupInstanceId,
					Topic:         sub.ContentConfig.Topic.ToPbTopic(),
					PartitionOffset: &mq_pb.PartitionOffset{
						Partition: assigned.Partition,
						StartType: mq_pb.PartitionOffsetStartType_EARLIEST_IN_MEMORY,
					},
					Filter:         sub.ContentConfig.Filter,
					FollowerBroker: assigned.FollowerBroker,
					Concurrency:    perPartitionConcurrency,
				},
			},
		}); err != nil {
			glog.V(0).Infof("subscriber %s connected to partition %+v at %v: %v", sub.ContentConfig.Topic, assigned.Partition, assigned.LeaderBroker, err)
		}

		glog.V(0).Infof("subscriber %s connected to partition %+v at %v", sub.ContentConfig.Topic, assigned.Partition, assigned.LeaderBroker)

		if sub.OnCompletionFunc != nil {
			defer sub.OnCompletionFunc()
		}

		type KeyedOffset struct {
			Key    []byte
			Offset int64
		}

		partitionOffsetChan := make(chan KeyedOffset, 1024)
		defer func() {
			close(partitionOffsetChan)
		}()
		executors := util.NewLimitedConcurrentExecutor(int(perPartitionConcurrency))

		go func() {
			for {
				select {
				case <-stopCh:
					subscribeClient.CloseSend()
					return
				case ack, ok := <-partitionOffsetChan:
					if !ok {
						subscribeClient.CloseSend()
						return
					}
					subscribeClient.SendMsg(&mq_pb.SubscribeMessageRequest{
						Message: &mq_pb.SubscribeMessageRequest_Ack{
							Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
								Key:      ack.Key,
								Sequence: ack.Offset,
							},
						},
					})
				}
			}
		}()

		var lastErr error

		for lastErr == nil {
			// glog.V(0).Infof("subscriber %s/%s/%s waiting for message", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
			resp, err := subscribeClient.Recv()
			if err != nil {
				return fmt.Errorf("subscribe recv: %v", err)
			}
			if resp.Message == nil {
				glog.V(0).Infof("subscriber %s/%s received nil message", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
				continue
			}
			switch m := resp.Message.(type) {
			case *mq_pb.SubscribeMessageResponse_Data:
				if m.Data.Ctrl != nil {
					glog.V(2).Infof("subscriber %s received control from producer:%s isClose:%v", sub.SubscriberConfig.ConsumerGroup, m.Data.Ctrl.PublisherName, m.Data.Ctrl.IsClose)
					continue
				}
				executors.Execute(func() {
					processErr := sub.OnEachMessageFunc(m.Data.Key, m.Data.Value)
					if processErr == nil {
						partitionOffsetChan <- KeyedOffset{
							Key:    m.Data.Key,
							Offset: m.Data.TsNs,
						}
					} else {
						lastErr = processErr
					}
				})
			case *mq_pb.SubscribeMessageResponse_Ctrl:
				// glog.V(0).Infof("subscriber %s/%s/%s received control %+v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, m.Ctrl)
				if m.Ctrl.IsEndOfStream || m.Ctrl.IsEndOfTopic {
					return io.EOF
				}
			}
		}

		return lastErr
	})
}
