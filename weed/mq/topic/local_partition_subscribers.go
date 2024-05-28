package topic

import "sync"

type LocalPartitionSubscribers struct {
	Subscribers     map[string]*LocalSubscriber
	SubscribersLock sync.RWMutex
}
type LocalSubscriber struct {
	stopCh chan struct{}
}

func NewLocalSubscriber() *LocalSubscriber {
	return &LocalSubscriber{
		stopCh: make(chan struct{}, 1),
	}
}
func (p *LocalSubscriber) SignalShutdown() {
	close(p.stopCh)
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
