package mq

const LAST_MINUTES = 10

type TopicStat struct {
	MessageCounts [LAST_MINUTES]int64
	ByteCounts    [LAST_MINUTES]int64
}

func NewTopicStat() *TopicStat {
	return &TopicStat{}
}
