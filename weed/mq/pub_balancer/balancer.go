package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

const (
	MaxPartitionCount  = 8 * 9 * 5 * 7 //2520
	LockBrokerBalancer = "broker_balancer"
)

// Balancer collects stats from all brokers.
//
//	When publishers wants to create topics, it picks brokers to assign the topic partitions.
//	When consumers wants to subscribe topics, it tells which brokers are serving the topic partitions.
//
// When a partition needs to be split or merged, or a partition needs to be moved to another broker,
// the balancer will let the broker tell the consumer instance to stop processing the partition.
// The existing consumer instance will flush the internal state, and then stop processing.
// Then the balancer will tell the brokers to start sending new messages in the new/moved partition to the consumer instances.
//
// Failover to standby consumer instances:
//
//	A consumer group can have min and max number of consumer instances.
//	For consumer instances joined after the max number, they will be in standby mode.
//
//	When a consumer instance is down, the broker will notice this and inform the balancer.
//	The balancer will then tell the broker to send the partition to another standby consumer instance.
type Balancer struct {
	Brokers cmap.ConcurrentMap[string, *BrokerStats] // key: broker address
	// Collected from all brokers when they connect to the broker leader
	TopicToBrokers cmap.ConcurrentMap[string, *PartitionSlotToBrokerList] // key: topic name
}

func NewBalancer() *Balancer {
	return &Balancer{
		Brokers:        cmap.New[*BrokerStats](),
		TopicToBrokers: cmap.New[*PartitionSlotToBrokerList](),
	}
}

func (balancer *Balancer) OnBrokerConnected(broker string) (brokerStats *BrokerStats) {
	var found bool
	brokerStats, found = balancer.Brokers.Get(broker)
	if !found {
		brokerStats = NewBrokerStats()
		if !balancer.Brokers.SetIfAbsent(broker, brokerStats) {
			brokerStats, _ = balancer.Brokers.Get(broker)
		}
	}
	return brokerStats
}

func (balancer *Balancer) OnBrokerDisconnected(broker string, stats *BrokerStats) {
	balancer.Brokers.Remove(broker)

	// update TopicToBrokers
	for _, topic := range stats.Topics {
		partitionSlotToBrokerList, found := balancer.TopicToBrokers.Get(topic.String())
		if !found {
			continue
		}
		partitionSlotToBrokerList.RemoveBroker(broker)
	}
}

func (balancer *Balancer) OnBrokerStatsUpdated(broker string, brokerStats *BrokerStats, receivedStats *mq_pb.BrokerStats) {
	brokerStats.UpdateStats(receivedStats)

	// update TopicToBrokers
	for _, topicPartitionStats := range receivedStats.Stats {
		topic := topicPartitionStats.Topic
		partition := topicPartitionStats.Partition
		partitionSlotToBrokerList, found := balancer.TopicToBrokers.Get(topic.String())
		if !found {
			partitionSlotToBrokerList = NewPartitionSlotToBrokerList(MaxPartitionCount)
			if !balancer.TopicToBrokers.SetIfAbsent(topic.String(), partitionSlotToBrokerList) {
				partitionSlotToBrokerList, _ = balancer.TopicToBrokers.Get(topic.String())
			}
		}
		partitionSlotToBrokerList.AddBroker(partition, broker)
	}
}
