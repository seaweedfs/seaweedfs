package topic

import (
	"sync"
	"sync/atomic"
	"time"
)

type LocalPartitionPublishers struct {
	publishers     map[string]*LocalPublisher
	publishersLock sync.RWMutex
}
type LocalPublisher struct {
	connectTimeNs       int64 // accessed atomically
	lastSeenTimeNs      int64 // accessed atomically
	lastPublishedOffset int64 // accessed atomically - offset of last message published
	lastAckedOffset     int64 // accessed atomically - offset of last message acknowledged by broker
}

func NewLocalPublisher() *LocalPublisher {
	now := time.Now().UnixNano()
	publisher := &LocalPublisher{}
	atomic.StoreInt64(&publisher.connectTimeNs, now)
	atomic.StoreInt64(&publisher.lastSeenTimeNs, now)
	atomic.StoreInt64(&publisher.lastPublishedOffset, 0)
	atomic.StoreInt64(&publisher.lastAckedOffset, 0)
	return publisher
}
func (p *LocalPublisher) SignalShutdown() {
}

// UpdateLastSeen updates the last activity time for this publisher
func (p *LocalPublisher) UpdateLastSeen() {
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// UpdatePublishedOffset updates the offset of the last message published by this publisher
func (p *LocalPublisher) UpdatePublishedOffset(offset int64) {
	atomic.StoreInt64(&p.lastPublishedOffset, offset)
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// UpdateAckedOffset updates the offset of the last message acknowledged by the broker for this publisher
func (p *LocalPublisher) UpdateAckedOffset(offset int64) {
	atomic.StoreInt64(&p.lastAckedOffset, offset)
	atomic.StoreInt64(&p.lastSeenTimeNs, time.Now().UnixNano())
}

// GetTimestamps returns the connect and last seen timestamps safely
func (p *LocalPublisher) GetTimestamps() (connectTimeNs, lastSeenTimeNs int64) {
	return atomic.LoadInt64(&p.connectTimeNs), atomic.LoadInt64(&p.lastSeenTimeNs)
}

// GetOffsets returns the published and acknowledged offsets safely
func (p *LocalPublisher) GetOffsets() (lastPublishedOffset, lastAckedOffset int64) {
	return atomic.LoadInt64(&p.lastPublishedOffset), atomic.LoadInt64(&p.lastAckedOffset)
}

func NewLocalPartitionPublishers() *LocalPartitionPublishers {
	return &LocalPartitionPublishers{
		publishers: make(map[string]*LocalPublisher),
	}
}

func (p *LocalPartitionPublishers) AddPublisher(clientName string, publisher *LocalPublisher) {
	p.publishersLock.Lock()
	defer p.publishersLock.Unlock()

	p.publishers[clientName] = publisher
}

func (p *LocalPartitionPublishers) RemovePublisher(clientName string) {
	p.publishersLock.Lock()
	defer p.publishersLock.Unlock()

	delete(p.publishers, clientName)
}

func (p *LocalPartitionPublishers) SignalShutdown() {
	p.publishersLock.RLock()
	defer p.publishersLock.RUnlock()

	for _, publisher := range p.publishers {
		publisher.SignalShutdown()
	}
}

func (p *LocalPartitionPublishers) Size() int {
	p.publishersLock.RLock()
	defer p.publishersLock.RUnlock()

	return len(p.publishers)
}

// GetPublisherNames returns the names of all publishers
func (p *LocalPartitionPublishers) GetPublisherNames() []string {
	p.publishersLock.RLock()
	defer p.publishersLock.RUnlock()

	names := make([]string, 0, len(p.publishers))
	for name := range p.publishers {
		names = append(names, name)
	}
	return names
}

// ForEachPublisher iterates over all publishers
func (p *LocalPartitionPublishers) ForEachPublisher(fn func(name string, publisher *LocalPublisher)) {
	p.publishersLock.RLock()
	defer p.publishersLock.RUnlock()

	for name, publisher := range p.publishers {
		fn(name, publisher)
	}
}
