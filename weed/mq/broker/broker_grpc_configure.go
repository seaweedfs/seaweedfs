package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConfigureTopic Runs on any broker, but proxied to the balancer if not the balancer
// It generates an assignments based on existing allocations,
// and then assign the partitions to the brokers.
func (b *MessageQueueBroker) ConfigureTopic(ctx context.Context, request *mq_pb.ConfigureTopicRequest) (resp *mq_pb.ConfigureTopicResponse, err error) {
	if b.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !b.lockAsBalancer.IsLocked() {
		proxyErr := b.withBrokerClient(false, b.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ConfigureTopic(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	t := topic.FromPbTopic(request.Topic)
	resp, err = b.readTopicConfFromFiler(t)
	if err != nil {
		glog.V(0).Infof("read topic %s conf: %v", request.Topic, err)
	} else {
		err = b.ensureTopicActiveAssignments(t, resp)
	}
	if err == nil && len(resp.BrokerPartitionAssignments) == int(request.PartitionCount) {
		glog.V(0).Infof("existing topic partitions %d: %+v", len(resp.BrokerPartitionAssignments), resp.BrokerPartitionAssignments)
	} else {
		resp = &mq_pb.ConfigureTopicResponse{}
		if b.Balancer.Brokers.IsEmpty()	{
			return nil, status.Errorf(codes.Unavailable, pub_balancer.ErrNoBroker.Error())
		}
		resp.BrokerPartitionAssignments = pub_balancer.AllocateTopicPartitions(b.Balancer.Brokers, request.PartitionCount)

		// save the topic configuration on filer
		if err := b.saveTopicConfToFiler(request.Topic, resp); err != nil {
			return nil, fmt.Errorf("configure topic: %v", err)
		}

		b.Balancer.OnPartitionChange(request.Topic, resp.BrokerPartitionAssignments)
	}

	if assignErr := b.assignTopicPartitionsToBrokers(ctx, request.Topic, resp.BrokerPartitionAssignments); assignErr != nil {
		return nil, assignErr
	}

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, err
}

func (b *MessageQueueBroker) assignTopicPartitionsToBrokers(ctx context.Context, t *mq_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment) error {
	for _, bpa := range assignments {
		fmt.Printf("create topic %s partition %+v on %s\n", t, bpa.Partition, bpa.LeaderBroker)
		if doCreateErr := b.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
			_, doCreateErr := client.AssignTopicPartitions(ctx, &mq_pb.AssignTopicPartitionsRequest{
				Topic: t,
				BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
					{
						Partition: bpa.Partition,
					},
				},
				IsLeader:   true,
				IsDraining: false,
			})
			if doCreateErr != nil {
				return fmt.Errorf("do create topic %s on %s: %v", t, bpa.LeaderBroker, doCreateErr)
			}
			brokerStats, found := b.Balancer.Brokers.Get(bpa.LeaderBroker)
			if !found {
				brokerStats = pub_balancer.NewBrokerStats()
				if !b.Balancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
					brokerStats, _ = b.Balancer.Brokers.Get(bpa.LeaderBroker)
				}
			}
			brokerStats.RegisterAssignment(t, bpa.Partition)
			return nil
		}); doCreateErr != nil {
			return doCreateErr
		}
	}
	return nil
}

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (b *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", b.option.Ip, b.option.Port))

	// drain existing topic partition subscriptions
	for _, assignment := range request.BrokerPartitionAssignments {
		t := topic.FromPbTopic(request.Topic)
		partition := topic.FromPbPartition(assignment.Partition)
		b.accessLock.Lock()
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions
			b.localTopicManager.RemoveTopicPartition(t, partition)
		} else {
			var localPartition *topic.LocalPartition
			if localPartition = b.localTopicManager.GetTopicPartition(t, partition); localPartition == nil {
				localPartition = topic.FromPbBrokerPartitionAssignment(self, partition, assignment, b.genLogFlushFunc(t, assignment.Partition), b.genLogOnDiskReadFunc(t, assignment.Partition))
				b.localTopicManager.AddTopicPartition(t, localPartition)
			}
		}
		b.accessLock.Unlock()
	}

	// if is leader, notify the followers to drain existing topic partition subscriptions
	if request.IsLeader {
		for _, brokerPartition := range request.BrokerPartitionAssignments {
			for _, follower := range brokerPartition.FollowerBrokers {
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
