package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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

	ret := &mq_pb.ConfigureTopicResponse{}
	ret.BrokerPartitionAssignments, err = b.Balancer.LookupOrAllocateTopicPartitions(request.Topic, true, request.PartitionCount)

	for _, bpa := range ret.BrokerPartitionAssignments {
		fmt.Printf("create topic %s partition %+v on %s\n", request.Topic, bpa.Partition, bpa.LeaderBroker)
		if doCreateErr := b.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
			_, doCreateErr := client.AssignTopicPartitions(ctx, &mq_pb.AssignTopicPartitionsRequest{
				Topic: request.Topic,
				BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
					{
						Partition: bpa.Partition,
					},
				},
				IsLeader:   true,
				IsDraining: false,
			})
			if doCreateErr != nil {
				return fmt.Errorf("do create topic %s on %s: %v", request.Topic, bpa.LeaderBroker, doCreateErr)
			}
			brokerStats, found := b.Balancer.Brokers.Get(bpa.LeaderBroker)
			if !found {
				brokerStats = pub_balancer.NewBrokerStats()
				if !b.Balancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
					brokerStats, _ = b.Balancer.Brokers.Get(bpa.LeaderBroker)
				}
			}
			brokerStats.RegisterAssignment(request.Topic, bpa.Partition)
			return nil
		}); doCreateErr != nil {
			return nil, doCreateErr
		}
	}

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, ret.BrokerPartitionAssignments)

	return ret, err
}

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (b *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", b.option.Ip, b.option.Port))

	// drain existing topic partition subscriptions
	for _, assignment := range request.BrokerPartitionAssignments {
		localPartition := topic.FromPbBrokerPartitionAssignment(self, assignment, b.genLogFlushFunc(request.Topic, assignment.Partition))
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions

			b.localTopicManager.RemoveTopicPartition(
				topic.FromPbTopic(request.Topic),
				localPartition.Partition)
		} else {
			b.localTopicManager.AddTopicPartition(
				topic.FromPbTopic(request.Topic),
				localPartition)
		}
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

func (b *MessageQueueBroker) genLogFlushFunc(t *mq_pb.Topic, partition *mq_pb.Partition) log_buffer.LogFlushFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

	return func(startTime, stopTime time.Time, buf []byte) {
		if len(buf) == 0 {
			return
		}

		startTime, stopTime = startTime.UTC(), stopTime.UTC()
		fileName := startTime.Format(topic.TIME_FORMAT)

		targetFile := fmt.Sprintf("%s/%s",partitionDir, fileName)

		for {
			if err := b.appendToFile(targetFile, buf); err != nil {
				glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
				time.Sleep(737 * time.Millisecond)
			} else {
				break
			}
		}
	}
}
