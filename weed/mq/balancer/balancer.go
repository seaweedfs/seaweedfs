package balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type Balancer struct {
	Brokers cmap.ConcurrentMap[string, *BrokerStats]
}
type BrokerStats struct {
	TopicPartitionCount int32
	MessageCount        int64
	BytesCount          int64
	CpuUsagePercent     int32
}

type TopicPartition struct {
	Topic      string
	RangeStart int32
	RangeStop  int32
}

type TopicPartitionStats struct {
	TopicPartition
	Throughput          int64
	ConsumerCount       int64
	TopicPartitionCount int64
}

func NewBalancer() *Balancer {
	return &Balancer{
		Brokers: cmap.New[*BrokerStats](),
	}
}

func NewBrokerStats() *BrokerStats {
	return &BrokerStats{}
}

func (tp *TopicPartition) String() string {
	return fmt.Sprintf("%v-%04d-%04d", tp.Topic, tp.RangeStart, tp.RangeStop)
}
