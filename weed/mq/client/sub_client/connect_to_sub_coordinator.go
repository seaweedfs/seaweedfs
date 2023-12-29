package sub_client

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync"
	"time"
)

func (sub *TopicSubscriber) doKeepConnectedToSubCoordinator() {
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
		print("z")
		time.Sleep(3 * time.Second)
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
			glog.V(0).Infof("subscriber %s/%s/%s assigned partition %+v at %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, partition, broker)
			sub.onEachPartition(partition, broker)
		}(assigned.Partition, assigned.Broker)
	}

	wg.Wait()
}

func (sub *TopicSubscriber) onEachPartition(partition *mq_pb.Partition, broker string) {
}
