package sub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
	"sync"
	"time"
)

func (sub *TopicSubscriber) doKeepConnectedToSubCoordinator() {
	waitTime := 1 * time.Second
	for {
		for _, broker := range sub.bootstrapBrokers {
			// TODO find the balancer
			// connect to the balancer
			pb.WithBrokerGrpcClient(true, broker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				stream, err := client.SubscriberToSubCoordinator(ctx)
				if err != nil {
					glog.V(1).Infof("subscriber %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
					return err
				}
				waitTime = 1 * time.Second

				// Maybe later: subscribe to multiple topics instead of just one

				if err := stream.Send(&mq_pb.SubscriberToSubCoordinatorRequest{
					Message: &mq_pb.SubscriberToSubCoordinatorRequest_Init{
						Init: &mq_pb.SubscriberToSubCoordinatorRequest_InitMessage{
							ConsumerGroup:      sub.SubscriberConfig.ConsumerGroup,
							ConsumerGroupInstanceId: sub.SubscriberConfig.ConsumerGroupInstanceId,
							Topic: &mq_pb.Topic{
								Namespace: sub.ContentConfig.Namespace,
								Name:      sub.ContentConfig.Topic,
							},
						},
					},
				}); err != nil {
					glog.V(1).Infof("subscriber %s/%s send init: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
					return err
				}

				// keep receiving messages from the sub coordinator
				for {
					resp, err := stream.Recv()
					if err != nil {
						glog.V(1).Infof("subscriber %s/%s receive: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
						return err
					}
					assignment := resp.GetAssignment()
					if assignment != nil {
						glog.V(0).Infof("subscriber %s/%s receive assignment: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, assignment)
					}
					sub.onEachAssignment(assignment)
				}

				return nil
			})
		}
		glog.V(4).Infof("subscriber %s/%s/%s waiting for more assignments", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
		if waitTime < 10*time.Second {
			waitTime += 1 * time.Second
		}
		time.Sleep(waitTime)
	}
}

func (sub *TopicSubscriber) onEachAssignment(assignment *mq_pb.SubscriberToSubCoordinatorResponse_Assignment) {
	if assignment == nil {
		return
	}
	// process each partition, with a concurrency limit
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, sub.ProcessorConfig.ConcurrentPartitionLimit)

	for _, assigned := range assignment.AssignedPartitions {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(partition *mq_pb.Partition, broker string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			glog.V(1).Infof("subscriber %s/%s/%s assigned partition %+v at %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, partition, broker)
			err := sub.onEachPartition(partition, broker)
			if err != nil {
				glog.V(0).Infof("subscriber %s/%s/%s partition %+v at %v: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, partition, broker, err)
			}
		}(assigned.Partition, assigned.Broker)
	}

	wg.Wait()
}

func (sub *TopicSubscriber) onEachPartition(partition *mq_pb.Partition, broker string) error {
	// connect to the partition broker
	return pb.WithBrokerGrpcClient(true, broker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		subscribeClient, err := client.SubscribeMessage(context.Background(), &mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Init{
				Init: &mq_pb.SubscribeMessageRequest_InitMessage{
					ConsumerGroup: sub.SubscriberConfig.ConsumerGroup,
					ConsumerId:    sub.SubscriberConfig.ConsumerGroupInstanceId,
					Topic: &mq_pb.Topic{
						Namespace: sub.ContentConfig.Namespace,
						Name:      sub.ContentConfig.Topic,
					},
					Partition: &mq_pb.Partition{
						RingSize:   partition.RingSize,
						RangeStart: partition.RangeStart,
						RangeStop:  partition.RangeStop,
					},
					Filter: sub.ContentConfig.Filter,
					Offset: &mq_pb.SubscribeMessageRequest_InitMessage_StartTimestampNs{
						StartTimestampNs: sub.alreadyProcessedTsNs,
					},
				},
			},
		})

		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}

		glog.V(1).Infof("subscriber %s/%s/%s connected to partition %+v at %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, partition, broker)

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
			glog.V(3).Infof("subscriber %s/%s/%s waiting for message", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
			resp, err := subscribeClient.Recv()
			if err != nil {
				return fmt.Errorf("subscribe recv: %v", err)
			}
			if resp.Message == nil {
				continue
			}
			switch m := resp.Message.(type) {
			case *mq_pb.SubscribeResponse_Data:
				shouldContinue, processErr := sub.OnEachMessageFunc(m.Data.Key, m.Data.Value)
				if processErr != nil {
					return fmt.Errorf("process error: %v", processErr)
				}
				sub.alreadyProcessedTsNs = m.Data.TsNs
				if !shouldContinue {
					return nil
				}
			case *mq_pb.SubscribeResponse_Ctrl:
				if m.Ctrl.IsEndOfStream || m.Ctrl.IsEndOfTopic {
					return io.EOF
				}
			}
		}

		return nil
	})
}
