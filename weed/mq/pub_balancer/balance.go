package pub_balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"google.golang.org/grpc"
)

/*
* Assuming a topic has [x,y] number of partitions when publishing, and there are b number of brokers.
* and p is the number of partitions per topic.
* if the broker number b <= x, then p = x.
* if the broker number x < b < y, then x <= p <= b.
* if the broker number b >= y, x <= p <= y

Balance topic partitions to brokers
===================================

When the goal is to make sure that low traffic partitions can be merged, (and p >= x, and after last rebalance interval):
1. Calculate the average load(throughput) of partitions per topic.
2. If any two neighboring partitions have a load that is less than the average load, merge them.
3. If min(b, y) < p, then merge two neighboring partitions that have the least combined load.

When the goal is to make sure that high traffic partitions can be split, (and p < y and p < b, and after last rebalance interval):
1. Calculate the average number of partitions per broker.
2. If any partition has a load that is more than the average load, split it into two partitions.

When the goal is to make sure that each broker has the same number of partitions:
1. Calculate the average number of partitions per broker.
2. For the brokers that have more than the average number of partitions, move the partitions to the brokers that have less than the average number of partitions.

*/

type BalanceAction interface {
}
type BalanceActionMerge struct {
	Before []topic.TopicPartition
	After  topic.TopicPartition
}
type BalanceActionSplit struct {
	Before topic.TopicPartition
	After  []topic.TopicPartition
}

type BalanceActionMove struct {
	TopicPartition topic.TopicPartition
	SourceBroker   string
	TargetBroker   string
}

type BalanceActionCreate struct {
	TopicPartition topic.TopicPartition
	TargetBroker   string
}

// BalancePublishers check the stats of all brokers,
// and balance the publishers to the brokers.
func (balancer *PubBalancer) BalancePublishers() []BalanceAction {
	action := BalanceTopicPartitionOnBrokers(balancer.Brokers)
	return []BalanceAction{action}
}

func (balancer *PubBalancer) ExecuteBalanceAction(actions []BalanceAction, grpcDialOption grpc.DialOption) (err error) {
	for _, action := range actions {
		switch action.(type) {
		case *BalanceActionMove:
			err = balancer.ExecuteBalanceActionMove(action.(*BalanceActionMove), grpcDialOption)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
