package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"math/rand"
	"modernc.org/mathutil"
	"sort"
)

func (balancer *PubBalancer) RepairTopics() []BalanceAction {
	action := BalanceTopicPartitionOnBrokers(balancer.Brokers)
	return []BalanceAction{action}
}

type TopicPartitionInfo struct {
	Broker string
}

// RepairMissingTopicPartitions check the stats of all brokers,
// and repair the missing topic partitions on the brokers.
func RepairMissingTopicPartitions(brokers cmap.ConcurrentMap[string, *BrokerStats]) (actions []BalanceAction) {

	// find all topic partitions
	topicToTopicPartitions := make(map[topic.Topic]map[topic.Partition]*TopicPartitionInfo)
	for brokerStatsItem := range brokers.IterBuffered() {
		broker, brokerStats := brokerStatsItem.Key, brokerStatsItem.Val
		for topicPartitionStatsItem := range brokerStats.TopicPartitionStats.IterBuffered() {
			topicPartitionStat := topicPartitionStatsItem.Val
			topicPartitionToInfo, found := topicToTopicPartitions[topicPartitionStat.Topic]
			if !found {
				topicPartitionToInfo = make(map[topic.Partition]*TopicPartitionInfo)
				topicToTopicPartitions[topicPartitionStat.Topic] = topicPartitionToInfo
			}
			tpi, found := topicPartitionToInfo[topicPartitionStat.Partition]
			if !found {
				tpi = &TopicPartitionInfo{}
				topicPartitionToInfo[topicPartitionStat.Partition] = tpi
			}
			tpi.Broker = broker
		}
	}

	// collect all brokers as candidates
	candidates := make([]string, 0, brokers.Count())
	for brokerStatsItem := range brokers.IterBuffered() {
		candidates = append(candidates, brokerStatsItem.Key)
	}

	// find the missing topic partitions
	for t, topicPartitionToInfo := range topicToTopicPartitions {
		missingPartitions := EachTopicRepairMissingTopicPartitions(t, topicPartitionToInfo)
		for _, partition := range missingPartitions {
			actions = append(actions, BalanceActionCreate{
				TopicPartition: topic.TopicPartition{
					Topic:     t,
					Partition: partition,
				},
				TargetBroker: candidates[rand.Intn(len(candidates))],
			})
		}
	}

	return actions
}

func EachTopicRepairMissingTopicPartitions(t topic.Topic, info map[topic.Partition]*TopicPartitionInfo) (missingPartitions []topic.Partition) {

	// find the missing topic partitions
	var partitions []topic.Partition
	for partition := range info {
		partitions = append(partitions, partition)
	}
	return findMissingPartitions(partitions, MaxPartitionCount)
}

// findMissingPartitions find the missing partitions
func findMissingPartitions(partitions []topic.Partition, ringSize int32) (missingPartitions []topic.Partition) {
	// sort the partitions by range start
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].RangeStart < partitions[j].RangeStart
	})

	// calculate the average partition size
	var covered int32
	for _, partition := range partitions {
		covered += partition.RangeStop - partition.RangeStart
	}
	averagePartitionSize := covered / int32(len(partitions))

	// find the missing partitions
	var coveredWatermark int32
	i := 0
	for i < len(partitions) {
		partition := partitions[i]
		if partition.RangeStart > coveredWatermark {
			upperBound := mathutil.MinInt32(coveredWatermark+averagePartitionSize, partition.RangeStart)
			missingPartitions = append(missingPartitions, topic.Partition{
				RangeStart: coveredWatermark,
				RangeStop:  upperBound,
				RingSize:   ringSize,
			})
			coveredWatermark = upperBound
			if coveredWatermark == partition.RangeStop {
				i++
			}
		} else {
			coveredWatermark = partition.RangeStop
			i++
		}
	}
	for coveredWatermark < ringSize {
		upperBound := mathutil.MinInt32(coveredWatermark+averagePartitionSize, ringSize)
		missingPartitions = append(missingPartitions, topic.Partition{
			RangeStart: coveredWatermark,
			RangeStop:  upperBound,
			RingSize:   ringSize,
		})
		coveredWatermark = upperBound
	}
	return missingPartitions
}
