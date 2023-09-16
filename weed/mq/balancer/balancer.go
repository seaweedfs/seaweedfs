package balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
)

type Balancer struct {
	Brokers cmap.ConcurrentMap[string, *BrokerStats]
}
type BrokerStats struct {
	TopicPartitionCount int32
	ConsumerCount       int32
	CpuUsagePercent     int32
}

func NewBalancer() *Balancer {
	return &Balancer{
		Brokers: cmap.New[*BrokerStats](),
	}
}

func NewBrokerStats() *BrokerStats {
	return &BrokerStats{}
}
