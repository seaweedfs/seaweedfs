package resource_pool

import (
	"time"
)

type Options struct {
	// The maximum number of active resource handles per resource location.  (A
	// non-positive value indicates the number of active resource handles is
	// unbounded).
	MaxActiveHandles int32

	// The maximum number of idle resource handles per resource location that
	// are kept alive by the resource pool.
	MaxIdleHandles uint32

	// The maximum amount of time an idle resource handle can remain alive (if
	// specified).
	MaxIdleTime *time.Duration

	// This limits the number of concurrent Open calls (there's no limit when
	// OpenMaxConcurrency is non-positive).
	OpenMaxConcurrency int

	// This function creates a resource handle (e.g., a connection) for a
	// resource location.  The function must be thread-safe.
	Open func(resourceLocation string) (
		handle interface{},
		err error)

	// This function destroys a resource handle and performs the necessary
	// cleanup to free up resources.  The function must be thread-safe.
	Close func(handle interface{}) error

	// This specifies the now time function.  When the function is non-nil, the
	// resource pool will use the specified function instead of time.Now to
	// generate the current time.
	NowFunc func() time.Time
}

func (o Options) getCurrentTime() time.Time {
	if o.NowFunc == nil {
		return time.Now()
	} else {
		return o.NowFunc()
	}
}

// A generic interface for managed resource pool.  All resource pool
// implementations must be threadsafe.
type ResourcePool interface {
	// This returns the number of active resource handles.
	NumActive() int32

	// This returns the highest number of actives handles for the entire
	// lifetime of the pool.  If the pool contains multiple sub-pools, the
	// high water mark is the max of the sub-pools' high water marks.
	ActiveHighWaterMark() int32

	// This returns the number of alive idle handles.  NOTE: This is only used
	// for testing.
	NumIdle() int

	// This associates a resource location to the resource pool; afterwhich,
	// the user can get resource handles for the resource location.
	Register(resourceLocation string) error

	// This dissociates a resource location from the resource pool; afterwhich,
	// the user can no longer get resource handles for the resource location.
	// If the given resource location corresponds to a sub-pool, the unregistered
	// sub-pool will enter lame duck mode.
	Unregister(resourceLocation string) error

	// This returns the list of registered resource location entries.
	ListRegistered() []string

	// This gets an active resource handle from the resource pool.  The
	// handle will remain active until one of the following is called:
	//  1. handle.Release()
	//  2. handle.Discard()
	//  3. pool.Release(handle)
	//  4. pool.Discard(handle)
	Get(key string) (ManagedHandle, error)

	// This releases an active resource handle back to the resource pool.
	Release(handle ManagedHandle) error

	// This discards an active resource from the resource pool.
	Discard(handle ManagedHandle) error

	// Enter the resource pool into lame duck mode.  The resource pool
	// will no longer return resource handles, and all idle resource handles
	// are closed immediately (including active resource handles that are
	// released back to the pool afterward).
	EnterLameDuckMode()
}
