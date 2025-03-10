package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"sync"
	"time"
)

type ProcessorState struct {
	stopCh chan struct{}
}

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {

	go sub.startProcessors()

	// loop forever
	// TODO shutdown the subscriber when not needed anymore
	sub.doKeepConnectedToSubCoordinator()

	return nil
}

func (sub *TopicSubscriber) startProcessors() {
	// listen to the messages from the sub coordinator
	// start one processor per partition
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, sub.SubscriberConfig.MaxPartitionCount)

	for message := range sub.brokerPartitionAssignmentChan {
		if assigned := message.GetAssignment(); assigned != nil {
			wg.Add(1)
			semaphore <- struct{}{}

			topicPartition := topic.FromPbPartition(assigned.PartitionAssignment.Partition)

			// wait until no covering partition is still in progress
			sub.waitUntilNoOverlappingPartitionInFlight(topicPartition)

			// start a processors
			stopChan := make(chan struct{})
			sub.activeProcessorsLock.Lock()
			sub.activeProcessors[topicPartition] = &ProcessorState{
				stopCh: stopChan,
			}
			sub.activeProcessorsLock.Unlock()

			go func(assigned *mq_pb.BrokerPartitionAssignment, topicPartition topic.Partition) {
				defer func() {
					sub.activeProcessorsLock.Lock()
					delete(sub.activeProcessors, topicPartition)
					sub.activeProcessorsLock.Unlock()

					<-semaphore
					wg.Done()
				}()
				glog.V(0).Infof("subscriber %s/%s assigned partition %+v at %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker)
				sub.brokerPartitionAssignmentAckChan <- &mq_pb.SubscriberToSubCoordinatorRequest{
					Message: &mq_pb.SubscriberToSubCoordinatorRequest_AckAssignment{
						AckAssignment: &mq_pb.SubscriberToSubCoordinatorRequest_AckAssignmentMessage{
							Partition: assigned.Partition,
						},
					},
				}

				executors := util.NewLimitedConcurrentExecutor(int(sub.SubscriberConfig.SlidingWindowSize))
				onDataMessageFn := func(m *mq_pb.SubscribeMessageResponse_Data) {
					executors.Execute(func() {
						if sub.OnDataMessageFunc != nil {
							sub.OnDataMessageFunc(m)
						}
						sub.PartitionOffsetChan <- KeyedOffset{
							Key:    m.Data.Key,
							Offset: m.Data.TsNs,
						}
					})
				}

				err := sub.onEachPartition(assigned, stopChan, onDataMessageFn)
				if err != nil {
					glog.V(0).Infof("subscriber %s/%s partition %+v at %v: %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker, err)
				} else {
					glog.V(0).Infof("subscriber %s/%s partition %+v at %v completed", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker)
				}
				sub.brokerPartitionAssignmentAckChan <- &mq_pb.SubscriberToSubCoordinatorRequest{
					Message: &mq_pb.SubscriberToSubCoordinatorRequest_AckUnAssignment{
						AckUnAssignment: &mq_pb.SubscriberToSubCoordinatorRequest_AckUnAssignmentMessage{
							Partition: assigned.Partition,
						},
					},
				}
			}(assigned.PartitionAssignment, topicPartition)
		}
		if unAssignment := message.GetUnAssignment(); unAssignment != nil {
			topicPartition := topic.FromPbPartition(unAssignment.Partition)
			sub.activeProcessorsLock.Lock()
			if processor, found := sub.activeProcessors[topicPartition]; found {
				close(processor.stopCh)
				delete(sub.activeProcessors, topicPartition)
			}
			sub.activeProcessorsLock.Unlock()
		}
	}

	wg.Wait()

}

func (sub *TopicSubscriber) waitUntilNoOverlappingPartitionInFlight(topicPartition topic.Partition) {
	foundOverlapping := true
	for foundOverlapping {
		sub.activeProcessorsLock.Lock()
		foundOverlapping = false
		var overlappedPartition topic.Partition
		for partition, _ := range sub.activeProcessors {
			if partition.Overlaps(topicPartition) {
				if partition.Equals(topicPartition) {
					continue
				}
				foundOverlapping = true
				overlappedPartition = partition
				break
			}
		}
		sub.activeProcessorsLock.Unlock()
		if foundOverlapping {
			glog.V(0).Infof("subscriber %s new partition %v waiting for partition %+v to complete", sub.ContentConfig.Topic, topicPartition, overlappedPartition)
			time.Sleep(1 * time.Second)
		}
	}
}
