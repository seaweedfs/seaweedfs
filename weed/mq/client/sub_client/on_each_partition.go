package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"io"
	"reflect"
	"time"
)

type KeyedOffset struct {
	Key    []byte
	Offset int64
}

func (sub *TopicSubscriber) onEachPartition(assigned *mq_pb.BrokerPartitionAssignment, stopCh chan struct{}, onDataMessageFn OnDataMessageFn) error {
	// connect to the partition broker
	return pb.WithBrokerGrpcClient(true, assigned.LeaderBroker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {

		subscribeClient, err := client.SubscribeMessage(context.Background())
		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}

		slidingWindowSize := sub.SubscriberConfig.SlidingWindowSize
		if slidingWindowSize <= 0 {
			slidingWindowSize = 1
		}

		po := findPartitionOffset(sub.ContentConfig.PartitionOffsets, assigned.Partition)
		if po == nil {
			po = &schema_pb.PartitionOffset{
				Partition: assigned.Partition,
				StartTsNs: time.Now().UnixNano(),
				StartType: schema_pb.PartitionOffsetStartType_EARLIEST_IN_MEMORY,
			}
		}

		if err = subscribeClient.Send(&mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Init{
				Init: &mq_pb.SubscribeMessageRequest_InitMessage{
					ConsumerGroup:     sub.SubscriberConfig.ConsumerGroup,
					ConsumerId:        sub.SubscriberConfig.ConsumerGroupInstanceId,
					Topic:             sub.ContentConfig.Topic.ToPbTopic(),
					PartitionOffset:   po,
					Filter:            sub.ContentConfig.Filter,
					FollowerBroker:    assigned.FollowerBroker,
					SlidingWindowSize: slidingWindowSize,
				},
			},
		}); err != nil {
			glog.V(0).Infof("subscriber %s connected to partition %+v at %v: %v", sub.ContentConfig.Topic, assigned.Partition, assigned.LeaderBroker, err)
		}

		glog.V(0).Infof("subscriber %s connected to partition %+v at %v", sub.ContentConfig.Topic, assigned.Partition, assigned.LeaderBroker)

		if sub.OnCompletionFunc != nil {
			defer sub.OnCompletionFunc()
		}

		go func() {
			for {
				select {
				case <-stopCh:
					subscribeClient.CloseSend()
					return
				case ack, ok := <-sub.PartitionOffsetChan:
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

		for {
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
				if len(m.Data.Key) == 0 {
					fmt.Printf("empty key %+v, type %v\n", m, reflect.TypeOf(m))
					continue
				}
				onDataMessageFn(m)
			case *mq_pb.SubscribeMessageResponse_Ctrl:
				// glog.V(0).Infof("subscriber %s/%s/%s received control %+v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, m.Ctrl)
				if m.Ctrl.IsEndOfStream || m.Ctrl.IsEndOfTopic {
					return io.EOF
				}
			}
		}

	})
}

func findPartitionOffset(partitionOffsets []*schema_pb.PartitionOffset, partition *schema_pb.Partition) *schema_pb.PartitionOffset {
	for _, po := range partitionOffsets {
		if po.Partition == partition {
			return po
		}
	}
	return nil
}
