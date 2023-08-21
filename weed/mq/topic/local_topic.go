package topic

type LocalTopic struct {
	Topic
	Partitions []*LocalPartition
}

func (localTopic *LocalTopic) findPartition(partition Partition) *LocalPartition {
	for _, localPartition := range localTopic.Partitions {
		if localPartition.Partition.Equals(partition) {
			return localPartition
		}
	}
	return nil
}
func (localTopic *LocalTopic) removePartition(partition Partition) bool {
	foundPartitionIndex := -1
	for i, localPartition := range localTopic.Partitions {
		if localPartition.Partition.Equals(partition) {
			foundPartitionIndex = i
			break
		}
	}
	if foundPartitionIndex == -1 {
		return false
	}
	localTopic.Partitions = append(localTopic.Partitions[:foundPartitionIndex], localTopic.Partitions[foundPartitionIndex+1:]...)
	return true
}
