package sub_coordinator

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"time"
)

type ConsumerGroup struct {
	topic topic.Topic
	// map a consumer group instance id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	Market                 *Market
	reBalanceTimer         *time.Timer
	filerClientAccessor    *filer_client.FilerClientAccessor
	stopCh                 chan struct{}
}

func NewConsumerGroup(t *schema_pb.Topic, reblanceSeconds int32, filerClientAccessor *filer_client.FilerClientAccessor) *ConsumerGroup {
	cg := &ConsumerGroup{
		topic:                  topic.FromPbTopic(t),
		ConsumerGroupInstances: cmap.New[*ConsumerGroupInstance](),
		filerClientAccessor:    filerClientAccessor,
		stopCh:                 make(chan struct{}),
	}
	if conf, err := cg.filerClientAccessor.ReadTopicConfFromFiler(cg.topic); err == nil {
		var partitions []topic.Partition
		for _, assignment := range conf.BrokerPartitionAssignments {
			partitions = append(partitions, topic.FromPbPartition(assignment.Partition))
		}
		cg.Market = NewMarket(partitions, time.Duration(reblanceSeconds)*time.Second)
	} else {
		glog.V(0).Infof("fail to read topic conf from filer: %v", err)
		return nil
	}

	go func() {
		for {
			select {
			case adjustment := <-cg.Market.AdjustmentChan:
				cgi, found := cg.ConsumerGroupInstances.Get(string(adjustment.consumer))
				if !found {
					glog.V(0).Infof("consumer group instance %s not found", adjustment.consumer)
					continue
				}
				if adjustment.isAssign {
					if conf, err := cg.filerClientAccessor.ReadTopicConfFromFiler(cg.topic); err == nil {
						for _, assignment := range conf.BrokerPartitionAssignments {
							if adjustment.partition.Equals(topic.FromPbPartition(assignment.Partition)) {
								cgi.ResponseChan <- &mq_pb.SubscriberToSubCoordinatorResponse{
									Message: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment_{
										Assignment: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment{
											PartitionAssignment: &mq_pb.BrokerPartitionAssignment{
												Partition:      adjustment.partition.ToPbPartition(),
												LeaderBroker:   assignment.LeaderBroker,
												FollowerBroker: assignment.FollowerBroker,
											},
										},
									},
								}
								glog.V(0).Infof("send assignment %v to %s", adjustment.partition, adjustment.consumer)
								break
							}
						}
					}
				} else {
					cgi.ResponseChan <- &mq_pb.SubscriberToSubCoordinatorResponse{
						Message: &mq_pb.SubscriberToSubCoordinatorResponse_UnAssignment_{
							UnAssignment: &mq_pb.SubscriberToSubCoordinatorResponse_UnAssignment{
								Partition: adjustment.partition.ToPbPartition(),
							},
						},
					}
					glog.V(0).Infof("send unassignment %v to %s", adjustment.partition, adjustment.consumer)
				}
			case <-cg.stopCh:
				return
			}
		}
	}()

	return cg
}

func (cg *ConsumerGroup) AckAssignment(cgi *ConsumerGroupInstance, assignment *mq_pb.SubscriberToSubCoordinatorRequest_AckAssignmentMessage) {
	fmt.Printf("ack assignment %v\n", assignment)
	cg.Market.ConfirmAdjustment(&Adjustment{
		consumer:  cgi.InstanceId,
		partition: topic.FromPbPartition(assignment.Partition),
		isAssign:  true,
	})
}
func (cg *ConsumerGroup) AckUnAssignment(cgi *ConsumerGroupInstance, assignment *mq_pb.SubscriberToSubCoordinatorRequest_AckUnAssignmentMessage) {
	fmt.Printf("ack unassignment %v\n", assignment)
	cg.Market.ConfirmAdjustment(&Adjustment{
		consumer:  cgi.InstanceId,
		partition: topic.FromPbPartition(assignment.Partition),
		isAssign:  false,
	})
}

func (cg *ConsumerGroup) OnPartitionListChange(assignments []*mq_pb.BrokerPartitionAssignment) {
}

func (cg *ConsumerGroup) Shutdown() {
	close(cg.stopCh)
}
