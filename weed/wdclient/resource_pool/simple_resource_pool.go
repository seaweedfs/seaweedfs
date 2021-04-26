package resource_pool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type idleHandle struct {
	handle    interface{}
	keepUntil *time.Time
}

type TooManyHandles struct {
	location string
}

func (t TooManyHandles) Error() string {
	return fmt.Sprintf("Too many handles to %s", t.location)
}

type OpenHandleError struct {
	location string
	err      error
}

func (o OpenHandleError) Error() string {
	return fmt.Sprintf("Failed to open resource handle: %s (%v)", o.location, o.err)
}

// A resource pool implementation where all handles are associated to the
// same resource location.
type simpleResourcePool struct {
	options Options

	numActive *int32 // atomic counter

	activeHighWaterMark *int32 // atomic / monotonically increasing value

	openTokens Semaphore

	mutex       sync.Mutex
	location    string        // guard by mutex
	idleHandles []*idleHandle // guarded by mutex
	isLameDuck  bool          // guarded by mutex
}

// This returns a SimpleResourcePool, where all handles are associated to a
// single resource location.
func NewSimpleResourcePool(options Options) ResourcePool {
	numActive := new(int32)
	atomic.StoreInt32(numActive, 0)

	activeHighWaterMark := new(int32)
	atomic.StoreInt32(activeHighWaterMark, 0)

	var tokens Semaphore
	if options.OpenMaxConcurrency > 0 {
		tokens = NewBoundedSemaphore(uint(options.OpenMaxConcurrency))
	}

	return &simpleResourcePool{
		location:            "",
		options:             options,
		numActive:           numActive,
		activeHighWaterMark: activeHighWaterMark,
		openTokens:          tokens,
		mutex:               sync.Mutex{},
		idleHandles:         make([]*idleHandle, 0, 0),
		isLameDuck:          false,
	}
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) NumActive() int32 {
	return atomic.LoadInt32(p.numActive)
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) ActiveHighWaterMark() int32 {
	return atomic.LoadInt32(p.activeHighWaterMark)
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) NumIdle() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.idleHandles)
}

// SimpleResourcePool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *simpleResourcePool) Register(resourceLocation string) error {
	if resourceLocation == "" {
		return errors.New("Invalid resource location")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isLameDuck {
		return fmt.Errorf(
			"cannot register %s to lame duck resource pool",
			resourceLocation)
	}

	if p.location == "" {
		p.location = resourceLocation
		return nil
	}
	return errors.New("SimpleResourcePool can only register one location")
}

// SimpleResourcePool will enter lame duck mode upon calling Unregister.
func (p *simpleResourcePool) Unregister(resourceLocation string) error {
	p.EnterLameDuckMode()
	return nil
}

func (p *simpleResourcePool) ListRegistered() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.location != "" {
		return []string{p.location}
	}
	return []string{}
}

func (p *simpleResourcePool) getLocation() (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.location == "" {
		return "", fmt.Errorf(
			"resource location is not set for SimpleResourcePool")
	}

	if p.isLameDuck {
		return "", fmt.Errorf(
			"lame duck resource pool cannot return handles to %s",
			p.location)
	}

	return p.location, nil
}

// This gets an active resource from the resource pool.  Note that the
// resourceLocation argument is ignored (The handles are associated to the
// resource location provided by the first Register call).
func (p *simpleResourcePool) Get(unused string) (ManagedHandle, error) {
	activeCount := atomic.AddInt32(p.numActive, 1)
	if p.options.MaxActiveHandles > 0 &&
		activeCount > p.options.MaxActiveHandles {

		atomic.AddInt32(p.numActive, -1)
		return nil, TooManyHandles{p.location}
	}

	highest := atomic.LoadInt32(p.activeHighWaterMark)
	for activeCount > highest &&
		!atomic.CompareAndSwapInt32(
			p.activeHighWaterMark,
			highest,
			activeCount) {

		highest = atomic.LoadInt32(p.activeHighWaterMark)
	}

	if h := p.getIdleHandle(); h != nil {
		return h, nil
	}

	location, err := p.getLocation()
	if err != nil {
		atomic.AddInt32(p.numActive, -1)
		return nil, err
	}

	if p.openTokens != nil {
		// Current implementation does not wait for tokens to become available.
		// If that causes availability hits, we could increase the wait,
		// similar to simple_pool.go.
		if p.openTokens.TryAcquire(0) {
			defer p.openTokens.Release()
		} else {
			// We could not immediately acquire a token.
			// Instead of waiting
			atomic.AddInt32(p.numActive, -1)
			return nil, OpenHandleError{
				p.location, errors.New("Open Error: reached OpenMaxConcurrency")}
		}
	}

	handle, err := p.options.Open(location)
	if err != nil {
		atomic.AddInt32(p.numActive, -1)
		return nil, OpenHandleError{p.location, err}
	}

	return NewManagedHandle(p.location, handle, p, p.options), nil
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) Release(handle ManagedHandle) error {
	if pool, ok := handle.Owner().(*simpleResourcePool); !ok || pool != p {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		// We can unref either before or after queuing the idle handle.
		// The advantage of unref-ing before queuing is that there is
		// a higher chance of successful Get when number of active handles
		// is close to the limit (but potentially more handle creation).
		// The advantage of queuing before unref-ing is that there's a
		// higher chance of reusing handle (but potentially more Get failures).
		atomic.AddInt32(p.numActive, -1)
		p.queueIdleHandles(h)
	}

	return nil
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) Discard(handle ManagedHandle) error {
	if pool, ok := handle.Owner().(*simpleResourcePool); !ok || pool != p {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		atomic.AddInt32(p.numActive, -1)
		if err := p.options.Close(h); err != nil {
			return fmt.Errorf("failed to close resource handle: %v", err)
		}
	}
	return nil
}

// See ResourcePool for documentation.
func (p *simpleResourcePool) EnterLameDuckMode() {
	p.mutex.Lock()

	toClose := p.idleHandles
	p.isLameDuck = true
	p.idleHandles = []*idleHandle{}

	p.mutex.Unlock()

	p.closeHandles(toClose)
}

// This returns an idle resource, if there is one.
func (p *simpleResourcePool) getIdleHandle() ManagedHandle {
	var toClose []*idleHandle
	defer func() {
		// NOTE: Must keep the closure around to late bind the toClose slice.
		p.closeHandles(toClose)
	}()

	now := p.options.getCurrentTime()

	p.mutex.Lock()
	defer p.mutex.Unlock()

	var i int
	for i = 0; i < len(p.idleHandles); i++ {
		idle := p.idleHandles[i]
		if idle.keepUntil == nil || now.Before(*idle.keepUntil) {
			break
		}
	}
	if i > 0 {
		toClose = p.idleHandles[0:i]
	}

	if i < len(p.idleHandles) {
		idle := p.idleHandles[i]
		p.idleHandles = p.idleHandles[i+1:]
		return NewManagedHandle(p.location, idle.handle, p, p.options)
	}

	if len(p.idleHandles) > 0 {
		p.idleHandles = []*idleHandle{}
	}
	return nil
}

// This adds an idle resource to the pool.
func (p *simpleResourcePool) queueIdleHandles(handle interface{}) {
	var toClose []*idleHandle
	defer func() {
		// NOTE: Must keep the closure around to late bind the toClose slice.
		p.closeHandles(toClose)
	}()

	now := p.options.getCurrentTime()
	var keepUntil *time.Time
	if p.options.MaxIdleTime != nil {
		// NOTE: Assign to temp variable first to work around compiler bug
		x := now.Add(*p.options.MaxIdleTime)
		keepUntil = &x
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isLameDuck {
		toClose = []*idleHandle{
			{handle: handle},
		}
		return
	}

	p.idleHandles = append(
		p.idleHandles,
		&idleHandle{
			handle:    handle,
			keepUntil: keepUntil,
		})

	nIdleHandles := uint32(len(p.idleHandles))
	if nIdleHandles > p.options.MaxIdleHandles {
		handlesToClose := nIdleHandles - p.options.MaxIdleHandles
		toClose = p.idleHandles[0:handlesToClose]
		p.idleHandles = p.idleHandles[handlesToClose:nIdleHandles]
	}
}

// Closes resources, at this point it is assumed that this resources
// are no longer referenced from the main idleHandles slice.
func (p *simpleResourcePool) closeHandles(handles []*idleHandle) {
	for _, handle := range handles {
		_ = p.options.Close(handle.handle)
	}
}
