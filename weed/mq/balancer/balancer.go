package balancer

import cmap "github.com/orcaman/concurrent-map"

type Balancer struct {
	brokers cmap.ConcurrentMap[string, *BrokerStats]
}
type BrokerStats struct {
	stats map[TopicPartition]*TopicPartitionStats
}

type TopicPartition struct {
	Topic      string
	RangeStart int32
	RangeStop  int32
}

type TopicPartitionStats struct {
	Throughput int64
}
