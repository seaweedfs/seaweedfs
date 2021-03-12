package resource_pool

import (
	"fmt"
	"sync"

	"errors"
)

// A resource pool implementation that manages multiple resource location
// entries.  The handles to each resource location entry acts independently.
// For example "tcp localhost:11211" could act as memcache
// shard 0 and "tcp localhost:11212" could act as memcache shard 1.
type multiResourcePool struct {
	options Options

	createPool func(Options) ResourcePool

	rwMutex    sync.RWMutex
	isLameDuck bool // guarded by rwMutex
	// NOTE: the locationPools is guarded by rwMutex, but the pool entries
	// are not.
	locationPools map[string]ResourcePool
}

// This returns a MultiResourcePool, which manages multiple
// resource location entries.  The handles to each resource location
// entry acts independently.
//
// When createPool is nil, NewSimpleResourcePool is used as default.
func NewMultiResourcePool(
	options Options,
	createPool func(Options) ResourcePool) ResourcePool {

	if createPool == nil {
		createPool = NewSimpleResourcePool
	}

	return &multiResourcePool{
		options:       options,
		createPool:    createPool,
		rwMutex:       sync.RWMutex{},
		isLameDuck:    false,
		locationPools: make(map[string]ResourcePool),
	}
}

// See ResourcePool for documentation.
func (p *multiResourcePool) NumActive() int32 {
	total := int32(0)

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, pool := range p.locationPools {
		total += pool.NumActive()
	}
	return total
}

// See ResourcePool for documentation.
func (p *multiResourcePool) ActiveHighWaterMark() int32 {
	high := int32(0)

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, pool := range p.locationPools {
		val := pool.ActiveHighWaterMark()
		if val > high {
			high = val
		}
	}
	return high
}

// See ResourcePool for documentation.
func (p *multiResourcePool) NumIdle() int {
	total := 0

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, pool := range p.locationPools {
		total += pool.NumIdle()
	}
	return total
}

// See ResourcePool for documentation.
func (p *multiResourcePool) Register(resourceLocation string) error {
	if resourceLocation == "" {
		return errors.New("Registering invalid resource location")
	}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.isLameDuck {
		return fmt.Errorf(
			"Cannot register %s to lame duck resource pool",
			resourceLocation)
	}

	if _, inMap := p.locationPools[resourceLocation]; inMap {
		return nil
	}

	pool := p.createPool(p.options)
	if err := pool.Register(resourceLocation); err != nil {
		return err
	}

	p.locationPools[resourceLocation] = pool
	return nil
}

// See ResourcePool for documentation.
func (p *multiResourcePool) Unregister(resourceLocation string) error {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if pool, inMap := p.locationPools[resourceLocation]; inMap {
		_ = pool.Unregister("")
		pool.EnterLameDuckMode()
		delete(p.locationPools, resourceLocation)
	}
	return nil
}

func (p *multiResourcePool) ListRegistered() []string {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	result := make([]string, 0, len(p.locationPools))
	for key, _ := range p.locationPools {
		result = append(result, key)
	}

	return result
}

// See ResourcePool for documentation.
func (p *multiResourcePool) Get(
	resourceLocation string) (ManagedHandle, error) {

	pool := p.getPool(resourceLocation)
	if pool == nil {
		return nil, fmt.Errorf(
			"%s is not registered in the resource pool",
			resourceLocation)
	}
	return pool.Get(resourceLocation)
}

// See ResourcePool for documentation.
func (p *multiResourcePool) Release(handle ManagedHandle) error {
	pool := p.getPool(handle.ResourceLocation())
	if pool == nil {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	return pool.Release(handle)
}

// See ResourcePool for documentation.
func (p *multiResourcePool) Discard(handle ManagedHandle) error {
	pool := p.getPool(handle.ResourceLocation())
	if pool == nil {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	return pool.Discard(handle)
}

// See ResourcePool for documentation.
func (p *multiResourcePool) EnterLameDuckMode() {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	p.isLameDuck = true

	for _, pool := range p.locationPools {
		pool.EnterLameDuckMode()
	}
}

func (p *multiResourcePool) getPool(resourceLocation string) ResourcePool {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if pool, inMap := p.locationPools[resourceLocation]; inMap {
		return pool
	}
	return nil
}
