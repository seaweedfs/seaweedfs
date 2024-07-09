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
			// lookup topic brokers
			var brokerLeader string
			err := pb.WithBrokerGrpcClient(false, broker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
				resp, err := client.FindBrokerLeader(context.Background(), &mq_pb.FindBrokerLeaderRequest{})
				if err != nil {
					return err
				}
				brokerLeader = resp.Broker
				return nil
			})
			if err != nil {
				glog.V(0).Infof("broker coordinator on %s: %v", broker, err)
				continue
			}
			glog.V(0).Infof("found broker coordinator: %v", brokerLeader)

			// connect to the balancer
			pb.WithBrokerGrpcClient(true, brokerLeader, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				stream, err := client.SubscriberToSubCoordinator(ctx)
				if err != nil {
					glog.V(0).Infof("subscriber %s: %v", sub.ContentConfig.Topic, err)
					return err
				}
				waitTime = 1 * time.Second

				// Maybe later: subscribe to multiple topics instead of just one

				if err := stream.Send(&mq_pb.SubscriberToSubCoordinatorRequest{
					Message: &mq_pb.SubscriberToSubCoordinatorRequest_Init{
						Init: &mq_pb.SubscriberToSubCoordinatorRequest_InitMessage{
							ConsumerGroup:           sub.SubscriberConfig.ConsumerGroup,
							ConsumerGroupInstanceId: sub.SubscriberConfig.ConsumerGroupInstanceId,
							Topic:                   sub.ContentConfig.Topic.ToPbTopic(),
						},
					},
				}); err != nil {
					glog.V(0).Infof("subscriber %s send init: %v", sub.ContentConfig.Topic, err)
					return err
				}

				// keep receiving messages from the sub coordinator
				for {
					resp, err := stream.Recv()
					if err != nil {
						glog.V(0).Infof("subscriber %s receive: %v", sub.ContentConfig.Topic, err)
						return err
					}
					assignment := resp.GetAssignment()
					if assignment != nil {
						glog.V(0).Infof("subscriber %s receive assignment: %v", sub.ContentConfig.Topic, assignment)
					}
					sub.onEachAssignment(assignment)
				}

				return nil
			})
		}
		glog.V(0).Infof("subscriber %s/%s waiting for more assignments", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup)
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

	for _, assigned := range assignment.PartitionAssignments {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(assigned *mq_pb.BrokerPartitionAssignment) {
			defer wg.Done()
			defer func() { <-semaphore }()
			glog.V(0).Infof("subscriber %s/%s assigned partition %+v at %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker)
			err := sub.onEachPartition(assigned)
			if err != nil {
				glog.V(0).Infof("subscriber %s/%s partition %+v at %v: %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker, err)
			}
		}(assigned)
	}

	wg.Wait()
}

func (sub *TopicSubscriber) onEachPartition(assigned *mq_pb.BrokerPartitionAssignment) error {
	// connect to the partition broker
	return pb.WithBrokerGrpcClient(true, assigned.LeaderBroker, sub.SubscriberConfig.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		subscribeClient, err := client.SubscribeMessage(context.Background(), &mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Init{
				Init: &mq_pb.SubscribeMessageRequest_InitMessage{
					ConsumerGroup: sub.SubscriberConfig.ConsumerGroup,
					ConsumerId:    sub.SubscriberConfig.ConsumerGroupInstanceId,
					Topic:         sub.ContentConfig.Topic.ToPbTopic(),
					PartitionOffset: &mq_pb.PartitionOffset{
						Partition: assigned.Partition,
						StartTsNs: sub.alreadyProcessedTsNs,
						StartType: mq_pb.PartitionOffsetStartType_EARLIEST_IN_MEMORY,
					},
					Filter:          sub.ContentConfig.Filter,
					FollowerBrokers: assigned.FollowerBrokers,
				},
			},
		})

		if err != nil {
			return fmt.Errorf("create subscribe client: %v", err)
		}

		glog.V(0).Infof("subscriber %s/%s connected to partition %+v at %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker)

		if sub.OnCompletionFunc != nil {
			defer sub.OnCompletionFunc()
		}
		defer func() {
			subscribeClient.SendMsg(&mq_pb.SubscribeMessageRequest{
				Message: &mq_pb.SubscribeMessageRequest_Ack{
					Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
						Sequence: 0,
					},
				},
			})
			subscribeClient.CloseSend()
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
				shouldContinue, processErr := sub.OnEachMessageFunc(m.Data.Key, m.Data.Value)
				if processErr != nil {
					return fmt.Errorf("process error: %v", processErr)
				}
				sub.alreadyProcessedTsNs = m.Data.TsNs
				if !shouldContinue {
					return nil
				}
			case *mq_pb.SubscribeMessageResponse_Ctrl:
				// glog.V(0).Infof("subscriber %s/%s/%s received control %+v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, m.Ctrl)
				if m.Ctrl.IsEndOfStream || m.Ctrl.IsEndOfTopic {
					return io.EOF
				}
			}
		}

		return nil
	})
}
