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
