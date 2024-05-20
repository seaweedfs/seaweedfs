package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync"
	"time"
)

type ProcessorState struct {

}

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {

	go sub.startProcessors()

	// loop forever
	sub.doKeepConnectedToSubCoordinator()

	return nil
}

func (sub *TopicSubscriber) startProcessors() {
	// listen to the messages from the sub coordinator
	// start one processor per partition
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, sub.ProcessorConfig.MaxPartitionCount)

	for assigned := range sub.brokerPartitionAssignmentChan {
		wg.Add(1)
		semaphore <- struct{}{}

		topicPartition := topic.FromPbPartition(assigned.Partition)

		// wait until no covering partition is still in progress
		sub.waitUntilNoOverlappingPartitionInFlight(topicPartition)

		// start a processors
		sub.activeProcessorsLock.Lock()
		sub.activeProcessors[topicPartition] = &ProcessorState{}
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
			err := sub.onEachPartition(assigned)
			if err != nil {
				glog.V(0).Infof("subscriber %s/%s partition %+v at %v: %v", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker, err)
			} else {
				glog.V(0).Infof("subscriber %s/%s partition %+v at %v completed", sub.ContentConfig.Topic, sub.SubscriberConfig.ConsumerGroup, assigned.Partition, assigned.LeaderBroker)
			}
		}(assigned, topicPartition)
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
