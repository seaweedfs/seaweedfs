package resource_pool

import (
	"sync/atomic"

	"errors"
)

// A resource handle managed by a resource pool.
type ManagedHandle interface {
	// This returns the handle's resource location.
	ResourceLocation() string

	// This returns the underlying resource handle (or error if the handle
	// is no longer active).
	Handle() (interface{}, error)

	// This returns the resource pool which owns this handle.
	Owner() ResourcePool

	// The releases the underlying resource handle to the caller and marks the
	// managed handle as inactive.  The caller is responsible for cleaning up
	// the released handle.  This returns nil if the managed handle no longer
	// owns the resource.
	ReleaseUnderlyingHandle() interface{}

	// This indicates a user is done with the handle and releases the handle
	// back to the resource pool.
	Release() error

	// This indicates the handle is an invalid state, and that the
	// connection should be discarded from the connection pool.
	Discard() error
}

// A physical implementation of ManagedHandle
type managedHandleImpl struct {
	location string
	handle   interface{}
	pool     ResourcePool
	isActive int32 // atomic bool
	options  Options
}

// This creates a managed handle wrapper.
func NewManagedHandle(
	resourceLocation string,
	handle interface{},
	pool ResourcePool,
	options Options) ManagedHandle {

	h := &managedHandleImpl{
		location: resourceLocation,
		handle:   handle,
		pool:     pool,
		options:  options,
	}
	atomic.StoreInt32(&h.isActive, 1)

	return h
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) ResourceLocation() string {
	return c.location
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) Handle() (interface{}, error) {
	if atomic.LoadInt32(&c.isActive) == 0 {
		return c.handle, errors.New("Resource handle is no longer valid")
	}
	return c.handle, nil
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) Owner() ResourcePool {
	return c.pool
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) ReleaseUnderlyingHandle() interface{} {
	if atomic.CompareAndSwapInt32(&c.isActive, 1, 0) {
		return c.handle
	}
	return nil
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) Release() error {
	return c.pool.Release(c)
}

// See ManagedHandle for documentation.
func (c *managedHandleImpl) Discard() error {
	return c.pool.Discard(c)
}
