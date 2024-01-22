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
	"sync"
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
		// no need to assign directly.
		// The added or updated assignees will read from filer directly.
		// The gone assignees will die by themselves.
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

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, resp.BrokerPartitionAssignments)

	return resp, err
}

func (b *MessageQueueBroker) assignTopicPartitionsToBrokers(ctx context.Context, t *mq_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment, isAdd bool) error {
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
				brokerStats, found := b.Balancer.Brokers.Get(bpa.LeaderBroker)
				if !found {
					brokerStats = pub_balancer.NewBrokerStats()
					if !b.Balancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
						brokerStats, _ = b.Balancer.Brokers.Get(bpa.LeaderBroker)
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
