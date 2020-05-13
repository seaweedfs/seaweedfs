package broker

import (
	"sync"
)

type TopicPartitionSubscription struct {
	sync.Mutex
	name         string
	lastReadTsNs int64
	cond         *sync.Cond
}

func NewTopicPartitionSubscription(name string) *TopicPartitionSubscription {
	t := &TopicPartitionSubscription{
		name: name,
	}
	t.cond = sync.NewCond(t)
	return t
}

func (s *TopicPartitionSubscription) Wait() {
	s.Mutex.Lock()
	s.cond.Wait()
	s.Mutex.Unlock()
}

func (s *TopicPartitionSubscription) NotifyOne() {
	// notify one waiting goroutine
	s.cond.Signal()
}

type TopicPartitionSubscriptions struct {
	sync.Mutex
	cond              *sync.Cond
	subscriptions     map[string]*TopicPartitionSubscription
	subscriptionsLock sync.RWMutex
}

func NewTopicPartitionSubscriptions() *TopicPartitionSubscriptions {
	m := &TopicPartitionSubscriptions{
		subscriptions: make(map[string]*TopicPartitionSubscription),
	}
	m.cond = sync.NewCond(m)
	return m
}

func (m *TopicPartitionSubscriptions) AddSubscription(subscription string) *TopicPartitionSubscription {
	m.subscriptionsLock.Lock()
	defer m.subscriptionsLock.Unlock()

	if s, found := m.subscriptions[subscription]; found {
		return s
	}

	s := NewTopicPartitionSubscription(subscription)
	m.subscriptions[subscription] = s

	return s

}

func (m *TopicPartitionSubscriptions) NotifyAll() {

	m.subscriptionsLock.RLock()
	defer m.subscriptionsLock.RUnlock()

	for name, tps := range m.subscriptions {
		println("notifying", name)
		tps.NotifyOne()
	}

}

func (m *TopicPartitionSubscriptions) Wait() {
	m.Mutex.Lock()
	m.cond.Wait()
	for _, tps := range m.subscriptions {
		tps.NotifyOne()
	}
	m.Mutex.Unlock()
}
