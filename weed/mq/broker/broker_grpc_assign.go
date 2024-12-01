package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"sync"
)

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (b *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}

	// drain existing topic partition subscriptions
	for _, assignment := range request.BrokerPartitionAssignments {
		t := topic.FromPbTopic(request.Topic)
		partition := topic.FromPbPartition(assignment.Partition)
		b.accessLock.Lock()
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions
			b.localTopicManager.RemoveLocalPartition(t, partition)
		} else {
			var localPartition *topic.LocalPartition
			if localPartition = b.localTopicManager.GetLocalPartition(t, partition); localPartition == nil {
				localPartition = topic.NewLocalPartition(partition, b.genLogFlushFunc(t, partition), logstore.GenMergedReadFunc(b, t, partition))
				b.localTopicManager.AddLocalPartition(t, localPartition)
			}
		}
		b.accessLock.Unlock()
	}

	// if is leader, notify the followers to drain existing topic partition subscriptions
	if request.IsLeader {
		for _, brokerPartition := range request.BrokerPartitionAssignments {
			if follower := brokerPartition.FollowerBroker; follower != "" {
				err := pb.WithBrokerGrpcClient(false, follower, b.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
					_, err := client.AssignTopicPartitions(context.Background(), request)
					return err
				})
				if err != nil {
					return ret, err
				}
			}
		}
	}

	glog.V(0).Infof("AssignTopicPartitions: topic %s partition assignments: %v", request.Topic, request.BrokerPartitionAssignments)
	return ret, nil
}

// called by broker leader to drain existing partitions.
// new/updated partitions will be detected by broker from the filer
func (b *MessageQueueBroker) assignTopicPartitionsToBrokers(ctx context.Context, t *schema_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment, isAdd bool) error {
	// notify the brokers to create the topic partitions in parallel
	var wg sync.WaitGroup
	for _, bpa := range assignments {
		wg.Add(1)
		go func(bpa *mq_pb.BrokerPartitionAssignment) {
			defer wg.Done()
			if doCreateErr := b.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
				_, doCreateErr := client.AssignTopicPartitions(ctx, &mq_pb.AssignTopicPartitionsRequest{
					Topic: t,
					BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
						{
							Partition: bpa.Partition,
						},
					},
					IsLeader:   true,
					IsDraining: !isAdd,
				})
				if doCreateErr != nil {
					if !isAdd {
						return fmt.Errorf("drain topic %s %v on %s: %v", t, bpa.LeaderBroker, bpa.Partition, doCreateErr)
					} else {
						return fmt.Errorf("create topic %s %v on %s: %v", t, bpa.LeaderBroker, bpa.Partition, doCreateErr)
					}
				}
				brokerStats, found := b.PubBalancer.Brokers.Get(bpa.LeaderBroker)
				if !found {
					brokerStats = pub_balancer.NewBrokerStats()
					if !b.PubBalancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
						brokerStats, _ = b.PubBalancer.Brokers.Get(bpa.LeaderBroker)
					}
				}
				brokerStats.RegisterAssignment(t, bpa.Partition, isAdd)
				return nil
			}); doCreateErr != nil {
				glog.Errorf("create topic %s partition %+v on %s: %v", t, bpa.Partition, bpa.LeaderBroker, doCreateErr)
			}
		}(bpa)
	}
	wg.Wait()

	return nil
}
