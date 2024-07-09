package topic

import "sync"

type LocalPartitionPublishers struct {
	publishers     map[string]*LocalPublisher
	publishersLock sync.RWMutex
}
type LocalPublisher struct {
}

func NewLocalPublisher() *LocalPublisher {
	return &LocalPublisher{}
}
func (p *LocalPublisher) SignalShutdown() {
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
