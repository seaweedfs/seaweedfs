package topic

import (
	"sync"
	"sync/atomic"
	"time"
)

type LocalPartitionSubscribers struct {
	Subscribers     map[string]*LocalSubscriber
	SubscribersLock sync.RWMutex
}
type LocalSubscriber struct {
	connectTimeNs      int64 // accessed atomically
	lastSeenTimeNs     int64 // accessed atomically
	lastReceivedOffset int64 // accessed atomically - offset of last message received
	lastAckedOffset    int64 // accessed atomically - offset of last message acknowledged
	stopCh             chan struct{}
}

func NewLocalSubscriber() *LocalSubscriber {
	now := time.Now().UnixNano()
	subscriber := &LocalSubscriber{
		stopCh: make(chan struct{}, 1),
	}
	atomic.StoreInt64(&subscriber.connectTimeNs, now)
	atomic.StoreInt64(&subscriber.lastSeenTimeNs, now)
	atomic.StoreInt64(&subscriber.lastReceivedOffset, 0)
	atomic.StoreInt64(&subscriber.lastAckedOffset, 0)
	return subscriber
}
func (p *LocalSubscriber) SignalShutdown() {
	close(p.stopCh)
}

// UpdateLastSeen updates the last activity time for this subscriber
func (p *LocalSubscriber) UpdateLastSeen() {
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// UpdateReceivedOffset updates the offset of the last message received by this subscriber
func (p *LocalSubscriber) UpdateReceivedOffset(offset int64) {
	atomic.StoreInt64(&p.lastReceivedOffset, offset)
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// UpdateAckedOffset updates the offset of the last message acknowledged by this subscriber
func (p *LocalSubscriber) UpdateAckedOffset(offset int64) {
	atomic.StoreInt64(&p.lastAckedOffset, offset)
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// GetTimestamps returns the connect and last seen timestamps safely
func (p *LocalSubscriber) GetTimestamps() (connectTimeNs, lastSeenTimeNs int64) {
	return atomic.LoadInt64(&p.connectTimeNs), atomic.LoadInt64(&p.lastSeenTimeNs)
}

// GetOffsets returns the received and acknowledged offsets safely
func (p *LocalSubscriber) GetOffsets() (lastReceivedOffset, lastAckedOffset int64) {
	return atomic.LoadInt64(&p.lastReceivedOffset), atomic.LoadInt64(&p.lastAckedOffset)
}

// GetCurrentOffset returns the acknowledged offset (for compatibility)
func (p *LocalSubscriber) GetCurrentOffset() int64 {
	return atomic.LoadInt64(&p.lastAckedOffset)
}

func NewLocalPartitionSubscribers() *LocalPartitionSubscribers {
	return &LocalPartitionSubscribers{
		Subscribers: make(map[string]*LocalSubscriber),
	}
}

func (p *LocalPartitionSubscribers) AddSubscriber(clientName string, Subscriber *LocalSubscriber) {
	p.SubscribersLock.Lock()
	defer p.SubscribersLock.Unlock()

	p.Subscribers[clientName] = Subscriber
}

func (p *LocalPartitionSubscribers) RemoveSubscriber(clientName string) {
	p.SubscribersLock.Lock()
	defer p.SubscribersLock.Unlock()

	delete(p.Subscribers, clientName)
}

func (p *LocalPartitionSubscribers) SignalShutdown() {
	p.SubscribersLock.RLock()
	defer p.SubscribersLock.RUnlock()

	for _, Subscriber := range p.Subscribers {
		Subscriber.SignalShutdown()
	}
}

func (p *LocalPartitionSubscribers) Size() int {
	p.SubscribersLock.RLock()
	defer p.SubscribersLock.RUnlock()

	return len(p.Subscribers)
}

// GetSubscriberNames returns the names of all subscribers
func (p *LocalPartitionSubscribers) GetSubscriberNames() []string {
	p.SubscribersLock.RLock()
	defer p.SubscribersLock.RUnlock()

	names := make([]string, 0, len(p.Subscribers))
	for name := range p.Subscribers {
		names = append(names, name)
	}
	return names
}

// ForEachSubscriber iterates over all subscribers
func (p *LocalPartitionSubscribers) ForEachSubscriber(fn func(name string, subscriber *LocalSubscriber)) {
	p.SubscribersLock.RLock()
	defer p.SubscribersLock.RUnlock()

	for name, subscriber := range p.Subscribers {
		fn(name, subscriber)
	}
}
