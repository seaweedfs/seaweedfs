package topic

import (
	"testing"
	"time"
)

// cleanupIdlePartitions used to call manager.topics.Remove from inside
// manager.topics.IterCb. IterCb holds the shard's read lock while running the
// callback, and Remove takes the same shard's write lock, so removing an
// emptied topic deadlocked the cleanup goroutine and permanently wedged the
// shard: every later ListTopicsInMemory / TopicExistsInMemory blocked behind
// the stuck writer. Observed in CI as the Kafka gateway's Metadata requests
// timing out forever once any topic went idle mid-test.
func TestCleanupIdlePartitionsRemovesEmptyTopicWithoutDeadlock(t *testing.T) {
	manager := NewLocalTopicManager()

	tp := NewTopic("test", "idle-topic")
	localTopic := NewLocalTopic(tp)
	localPartition := NewLocalPartition(Partition{RingSize: 2520, RangeStart: 0, RangeStop: 2520}, 1, nil, nil)
	localTopic.Partitions = append(localTopic.Partitions, localPartition)
	manager.topics.Set(tp.String(), localTopic)

	// Make the partition idle beyond any timeout.
	localPartition.lastActivityTime.Store(time.Now().Add(-time.Hour).UnixNano())

	done := make(chan struct{})
	go func() {
		manager.cleanupIdlePartitions(time.Minute)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cleanupIdlePartitions deadlocked while removing an emptied topic")
	}

	if manager.topics.Has(tp.String()) {
		t.Errorf("emptied topic %s should have been removed", tp.String())
	}

	// The map must still be fully usable afterwards — with the deadlock the
	// wedged shard made this call block forever.
	listed := make(chan []Topic, 1)
	go func() { listed <- manager.ListTopicsInMemory() }()
	select {
	case topics := <-listed:
		if len(topics) != 0 {
			t.Errorf("expected no topics in memory, got %v", topics)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ListTopicsInMemory blocked after cleanup — shard still wedged")
	}
}

// A topic that gains a partition between the cleanup scan and the removal must
// be kept: removal re-checks emptiness under the shard write lock.
func TestCleanupIdlePartitionsKeepsActiveTopic(t *testing.T) {
	manager := NewLocalTopicManager()

	tp := NewTopic("test", "active-topic")
	localPartition := NewLocalPartition(Partition{RingSize: 2520, RangeStart: 0, RangeStop: 2520}, 1, nil, nil)
	manager.AddLocalPartition(tp, localPartition)

	// Fresh activity: nothing should be cleaned up.
	manager.cleanupIdlePartitions(time.Minute)

	if !manager.topics.Has(tp.String()) {
		t.Errorf("active topic %s should not have been removed", tp.String())
	}
	if manager.GetLocalPartition(tp, localPartition.Partition) == nil {
		t.Errorf("active partition should still be registered")
	}
}
