package topic

import (
	"context"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/shirou/gopsutil/v4/cpu"
)

// LocalTopicManager manages topics on local broker
type LocalTopicManager struct {
	topics       cmap.ConcurrentMap[string, *LocalTopic]
	cleanupDone  chan struct{} // Signal cleanup goroutine to stop
	cleanupTimer *time.Ticker
}

// NewLocalTopicManager creates a new LocalTopicManager
func NewLocalTopicManager() *LocalTopicManager {
	return &LocalTopicManager{
		topics:      cmap.New[*LocalTopic](),
		cleanupDone: make(chan struct{}),
	}
}

// StartIdlePartitionCleanup starts a background goroutine that periodically
// cleans up idle partitions (partitions with no publishers and no subscribers)
func (manager *LocalTopicManager) StartIdlePartitionCleanup(ctx context.Context, checkInterval, idleTimeout time.Duration) {
	manager.cleanupTimer = time.NewTicker(checkInterval)

	go func() {
		defer close(manager.cleanupDone)
		defer manager.cleanupTimer.Stop()

		glog.V(1).Infof("Idle partition cleanup started: check every %v, cleanup after %v idle", checkInterval, idleTimeout)

		for {
			select {
			case <-ctx.Done():
				glog.V(1).Info("Idle partition cleanup stopped")
				return
			case <-manager.cleanupTimer.C:
				manager.cleanupIdlePartitions(idleTimeout)
			}
		}
	}()
}

// cleanupIdlePartitions removes idle partitions from memory
func (manager *LocalTopicManager) cleanupIdlePartitions(idleTimeout time.Duration) {
	cleanedCount := 0

	// Iterate through all topics
	manager.topics.IterCb(func(topicKey string, localTopic *LocalTopic) {
		localTopic.partitionLock.Lock()
		defer localTopic.partitionLock.Unlock()

		// Check each partition
		for i := len(localTopic.Partitions) - 1; i >= 0; i-- {
			partition := localTopic.Partitions[i]

			if partition.ShouldCleanup(idleTimeout) {
				glog.V(1).Infof("Cleaning up idle partition %s (idle for %v, publishers=%d, subscribers=%d)",
					partition.Partition.String(),
					partition.GetIdleDuration(),
					partition.Publishers.Size(),
					partition.Subscribers.Size())

				// Shutdown the partition (closes LogBuffer, etc.)
				partition.Shutdown()

				// Remove from slice
				localTopic.Partitions = append(localTopic.Partitions[:i], localTopic.Partitions[i+1:]...)
				cleanedCount++
			}
		}

		// If topic has no partitions left, remove it
		if len(localTopic.Partitions) == 0 {
			glog.V(1).Infof("Removing empty topic %s", topicKey)
			manager.topics.Remove(topicKey)
		}
	})

	if cleanedCount > 0 {
		glog.V(0).Infof("Cleaned up %d idle partition(s)", cleanedCount)
	}
}

// WaitForCleanupShutdown waits for the cleanup goroutine to finish
func (manager *LocalTopicManager) WaitForCleanupShutdown() {
	<-manager.cleanupDone
	glog.V(1).Info("Idle partition cleanup shutdown complete")
}

// AddLocalPartition adds a topic to the local topic manager
func (manager *LocalTopicManager) AddLocalPartition(topic Topic, localPartition *LocalPartition) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		localTopic = NewLocalTopic(topic)
	}
	if !manager.topics.SetIfAbsent(topic.String(), localTopic) {
		localTopic, _ = manager.topics.Get(topic.String())
	}
	localTopic.addPartition(localPartition)
}

// GetLocalPartition gets a topic from the local topic manager
func (manager *LocalTopicManager) GetLocalPartition(topic Topic, partition Partition) *LocalPartition {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return nil
	}
	result := localTopic.findPartition(partition)
	return result
}

// RemoveTopic removes a topic from the local topic manager
func (manager *LocalTopicManager) RemoveTopic(topic Topic) {
	manager.topics.Remove(topic.String())
}

func (manager *LocalTopicManager) RemoveLocalPartition(topic Topic, partition Partition) (removed bool) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return false
	}
	return localTopic.removePartition(partition)
}

func (manager *LocalTopicManager) ClosePublishers(topic Topic, unixTsNs int64) (removed bool) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return false
	}
	return localTopic.closePartitionPublishers(unixTsNs)
}

func (manager *LocalTopicManager) CloseSubscribers(topic Topic, unixTsNs int64) (removed bool) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return false
	}
	return localTopic.closePartitionSubscribers(unixTsNs)
}

// ListTopicsInMemory returns all topics currently tracked in memory
func (manager *LocalTopicManager) ListTopicsInMemory() []Topic {
	var topics []Topic
	for item := range manager.topics.IterBuffered() {
		topics = append(topics, item.Val.Topic)
	}
	return topics
}

// TopicExistsInMemory checks if a topic exists in memory (not flushed data)
func (manager *LocalTopicManager) TopicExistsInMemory(topic Topic) bool {
	_, exists := manager.topics.Get(topic.String())
	return exists
}

func (manager *LocalTopicManager) CollectStats(duration time.Duration) *mq_pb.BrokerStats {
	stats := &mq_pb.BrokerStats{
		Stats: make(map[string]*mq_pb.TopicPartitionStats),
	}

	// collect current broker's cpu usage
	// this needs to be in front, so the following stats can be more accurate
	usages, err := cpu.Percent(duration, false)
	if err == nil && len(usages) > 0 {
		stats.CpuUsagePercent = int32(usages[0])
	}

	// collect current broker's topics and partitions
	manager.topics.IterCb(func(topic string, localTopic *LocalTopic) {
		for _, localPartition := range localTopic.Partitions {
			topicPartition := &TopicPartition{
				Topic:     Topic{Namespace: localTopic.Namespace, Name: localTopic.Name},
				Partition: localPartition.Partition,
			}
			stats.Stats[topicPartition.TopicPartitionId()] = &mq_pb.TopicPartitionStats{
				Topic: &schema_pb.Topic{
					Namespace: string(localTopic.Namespace),
					Name:      localTopic.Name,
				},
				Partition:       localPartition.Partition.ToPbPartition(),
				PublisherCount:  int32(localPartition.Publishers.Size()),
				SubscriberCount: int32(localPartition.Subscribers.Size()),
				Follower:        localPartition.Follower,
			}
			// fmt.Printf("collect topic %+v partition %+v\n", topicPartition, localPartition.Partition)
		}
	})

	return stats

}

func (manager *LocalTopicManager) WaitUntilNoPublishers(topic Topic) {
	localTopic, ok := manager.topics.Get(topic.String())
	if !ok {
		return
	}
	localTopic.WaitUntilNoPublishers()
}
