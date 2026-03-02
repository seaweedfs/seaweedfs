package blockvol

import (
	"sync/atomic"
	"time"
)

// Lease tracks a runtime-only lease expiry for write fencing.
// Zero-value is an expired (invalid) lease. Not persisted.
type Lease struct {
	expiry atomic.Value // stores time.Time
}

// Grant sets the lease to expire after ttl from now.
func (l *Lease) Grant(ttl time.Duration) {
	l.expiry.Store(time.Now().Add(ttl))
}

// IsValid returns true if the lease has not expired.
func (l *Lease) IsValid() bool {
	v := l.expiry.Load()
	if v == nil {
		return false
	}
	return time.Now().Before(v.(time.Time))
}

// Revoke immediately invalidates the lease.
func (l *Lease) Revoke() {
	l.expiry.Store(time.Time{}) // zero time is always in the past
}
