package util

import (
	"errors"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	TimeoutErr = errors.New("timeout")
)

// A bufferedChan implemented by a buffered channel
type ResourcePool struct {
	sync.Mutex
	bufferedChan  chan interface{}
	poolSizeLimit int
	inuse         int
	newFn         func() (interface{}, error)
}

func NewResourcePool(poolSizeLimit int, newFn func() (interface{}, error)) *ResourcePool {
	p := &ResourcePool{
		poolSizeLimit: poolSizeLimit,
		newFn:         newFn,
		bufferedChan:  make(chan interface{}, poolSizeLimit),
	}
	return p
}

func (p *ResourcePool) Size() int {
	p.Lock()
	defer p.Unlock()
	return len(p.bufferedChan) + p.inuse
}

func (p *ResourcePool) Free() int {
	p.Lock()
	defer p.Unlock()
	return p.poolSizeLimit - p.inuse
}

func (p *ResourcePool) Get(timeout time.Duration) (interface{}, error) {
	d, err := p.get(timeout)
	if err != nil {
		return nil, err
	}
	if d == nil && p.newFn != nil {
		var err error
		d, err = p.newFn()
		if err != nil {
			return nil, err
		}
	}
	p.Lock()
	defer p.Unlock()
	p.inuse++
	return d, nil
}

func (p *ResourcePool) Release(v interface{}) {

	p.Lock()
	defer p.Unlock()
	if p.inuse == 0 {
		glog.V(0).Infof("released too many times?")
		return
	}
	p.bufferedChan <- v
	p.inuse--
}

func (p *ResourcePool) get(timeout time.Duration) (interface{}, error) {

	select {
	case v := <-p.bufferedChan:
		return v, nil
	default:
	}

	if p.Free() > 0 {
		d, err := p.newFn()
		if err != nil {
			return nil, err
		}
		return d, nil
	}

	// wait for an freed item
	select {
	case v := <-p.bufferedChan:
		return v, nil
	case <-time.After(timeout):
	}
	return nil, TimeoutErr
}
