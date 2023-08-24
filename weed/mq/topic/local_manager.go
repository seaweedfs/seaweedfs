package topic

import (
	cmap "github.com/orcaman/concurrent-map/v2"
)

// LocalTopicManager manages topics on local broker
type LocalTopicManager struct {
	topics cmap.ConcurrentMap[string, *LocalTopic]
}

// NewLocalTopicManager creates a new LocalTopicManager
func NewLocalTopicManager() *LocalTopicManager {
	return &LocalTopicManager{
		topics: cmap.New[*LocalTopic](),
	}
}

// AddTopic adds a topic to the local topic manager
func (manager *LocalTopicManager) AddTopicPartition(topic Topic, localPartition *LocalPartition) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		localTopic = &LocalTopic{
			Topic:      topic,
			Partitions: make([]*LocalPartition, 0),
		}
	}
	if localTopic.findPartition(localPartition.Partition) != nil {
		return
	}
	localTopic.Partitions = append(localTopic.Partitions, localPartition)
}

// GetTopic gets a topic from the local topic manager
func (manager *LocalTopicManager) GetTopicPartition(topic Topic, partition Partition) *LocalPartition {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return nil
	}
	return localTopic.findPartition(partition)
}

// RemoveTopic removes a topic from the local topic manager
func (manager *LocalTopicManager) RemoveTopic(topic Topic) {
	manager.topics.Remove(topic.String())
}

func (manager *LocalTopicManager) RemoveTopicPartition(topic Topic, partition Partition) (removed bool) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return false
	}
	return localTopic.removePartition(partition)
}
