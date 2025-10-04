package topic

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type LocalTopic struct {
	Topic
	Partitions    []*LocalPartition
	partitionLock sync.RWMutex
}

func NewLocalTopic(topic Topic) *LocalTopic {
	return &LocalTopic{
		Topic:      topic,
		Partitions: make([]*LocalPartition, 0),
	}
}

func (localTopic *LocalTopic) findPartition(partition Partition) *LocalPartition {
	localTopic.partitionLock.RLock()
	defer localTopic.partitionLock.RUnlock()

	glog.V(4).Infof("findPartition searching for %s in %d partitions", partition.String(), len(localTopic.Partitions))
	for i, localPartition := range localTopic.Partitions {
		glog.V(4).Infof("Comparing partition[%d]: %s with target %s", i, localPartition.Partition.String(), partition.String())
		if localPartition.Partition.LogicalEquals(partition) {
			glog.V(4).Infof("Found matching partition at index %d", i)
			return localPartition
		}
	}
	glog.V(4).Infof("No matching partition found for %s", partition.String())
	return nil
}
func (localTopic *LocalTopic) removePartition(partition Partition) bool {
	localTopic.partitionLock.Lock()
	defer localTopic.partitionLock.Unlock()

	foundPartitionIndex := -1
	for i, localPartition := range localTopic.Partitions {
		if localPartition.Partition.LogicalEquals(partition) {
			foundPartitionIndex = i
			localPartition.Shutdown()
			break
		}
	}
	if foundPartitionIndex == -1 {
		return false
	}
	localTopic.Partitions = append(localTopic.Partitions[:foundPartitionIndex], localTopic.Partitions[foundPartitionIndex+1:]...)
	return true
}
func (localTopic *LocalTopic) addPartition(localPartition *LocalPartition) {
	localTopic.partitionLock.Lock()
	defer localTopic.partitionLock.Unlock()
	for _, partition := range localTopic.Partitions {
		if localPartition.Partition.LogicalEquals(partition.Partition) {
			return
		}
	}
	localTopic.Partitions = append(localTopic.Partitions, localPartition)
}

func (localTopic *LocalTopic) closePartitionPublishers(unixTsNs int64) bool {
	var wg sync.WaitGroup
	for _, localPartition := range localTopic.Partitions {
		if localPartition.UnixTimeNs != unixTsNs {
			continue
		}
		wg.Add(1)
		go func(localPartition *LocalPartition) {
			defer wg.Done()
			localPartition.closePublishers()
		}(localPartition)
	}
	wg.Wait()
	return true
}

func (localTopic *LocalTopic) closePartitionSubscribers(unixTsNs int64) bool {
	var wg sync.WaitGroup
	for _, localPartition := range localTopic.Partitions {
		if localPartition.UnixTimeNs != unixTsNs {
			continue
		}
		wg.Add(1)
		go func(localPartition *LocalPartition) {
			defer wg.Done()
			localPartition.closeSubscribers()
		}(localPartition)
	}
	wg.Wait()
	return true
}

func (localTopic *LocalTopic) WaitUntilNoPublishers() {
	for {
		var wg sync.WaitGroup
		for _, localPartition := range localTopic.Partitions {
			wg.Add(1)
			go func(localPartition *LocalPartition) {
				defer wg.Done()
				localPartition.WaitUntilNoPublishers()
			}(localPartition)
		}
		wg.Wait()
		if len(localTopic.Partitions) == 0 {
			return
		}
	}
}
