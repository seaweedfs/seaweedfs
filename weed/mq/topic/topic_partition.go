package topic

import "fmt"

type TopicPartition struct {
	Namespace  string
	Topic      string
	RangeStart int32
	RangeStop  int32
}

func (tp *TopicPartition) String() string {
	return fmt.Sprintf("%v.%v-%04d-%04d", tp.Namespace, tp.Topic, tp.RangeStart, tp.RangeStop)
}
