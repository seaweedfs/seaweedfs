package topic

import "fmt"

type TopicPartition struct {
	Topic
	Partition
}

func (tp *TopicPartition) TopicPartitionId() string {
	return fmt.Sprintf("%v.%v-%04d-%04d", tp.Namespace, tp.Topic, tp.RangeStart, tp.RangeStop)
}
