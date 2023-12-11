package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"math/rand"
)

func BalanceTopicPartitionOnBrokers(brokers cmap.ConcurrentMap[string, *BrokerStats]) BalanceAction {
	// 1. calculate the average number of partitions per broker
	var totalPartitionCount int32
	var totalBrokerCount int32
	for brokerStats := range brokers.IterBuffered() {
		totalBrokerCount++
		totalPartitionCount += brokerStats.Val.TopicPartitionCount
	}
	averagePartitionCountPerBroker := totalPartitionCount / totalBrokerCount
	minPartitionCountPerBroker := averagePartitionCountPerBroker
	maxPartitionCountPerBroker := averagePartitionCountPerBroker
	var sourceBroker, targetBroker string
	var candidatePartition *topic.TopicPartition
	for brokerStats := range brokers.IterBuffered() {
		if minPartitionCountPerBroker > brokerStats.Val.TopicPartitionCount {
			minPartitionCountPerBroker = brokerStats.Val.TopicPartitionCount
			targetBroker = brokerStats.Key
		}
		if maxPartitionCountPerBroker < brokerStats.Val.TopicPartitionCount {
			maxPartitionCountPerBroker = brokerStats.Val.TopicPartitionCount
			sourceBroker = brokerStats.Key
			// select a random partition from the source broker
			randomePartitionIndex := rand.Intn(int(brokerStats.Val.TopicPartitionCount))
			index := 0
			for topicPartitionStats := range brokerStats.Val.TopicPartitionStats.IterBuffered() {
				if index == randomePartitionIndex {
					candidatePartition = &topicPartitionStats.Val.TopicPartition
					break
				} else {
					index++
				}
			}
		}
	}
	if minPartitionCountPerBroker >= maxPartitionCountPerBroker-1 {
		return nil
	}
	// 2. move the partitions from the source broker to the target broker
	return &BalanceActionMove{
		TopicPartition: *candidatePartition,
		SourceBroker:   sourceBroker,
		TargetBroker:   targetBroker,
	}
}
