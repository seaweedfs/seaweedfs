package pub_balancer

import ()

func (balancer *PubBalancer) RepairTopics() []BalanceAction {
	action := BalanceTopicPartitionOnBrokers(balancer.Brokers)
	return []BalanceAction{action}
}

type TopicPartitionInfo struct {
	Broker string
}
