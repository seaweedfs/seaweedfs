package topic

import "sync"

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

	for _, localPartition := range localTopic.Partitions {
		if localPartition.Partition.Equals(partition) {
			return localPartition
		}
	}
	return nil
}
func (localTopic *LocalTopic) removePartition(partition Partition) bool {
	localTopic.partitionLock.Lock()
	defer localTopic.partitionLock.Unlock()

	foundPartitionIndex := -1
	for i, localPartition := range localTopic.Partitions {
		if localPartition.Partition.Equals(partition) {
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
		if localPartition.Partition.Equals(partition.Partition) {
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
