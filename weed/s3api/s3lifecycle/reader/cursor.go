package reader

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// Cursor tracks the meta-log position for each ActionKey within one shard.
// Position = tsNs of the last event the shard's reader+downstream considers
// "fully resolved" for this ActionKey. Subscription resumes at the minimum
// position across all keys.
//
// A frozen key is pinned at its current position (BLOCKED outcome): Advance
// is a no-op until Unfreeze. The meta-log retention bounds how long a freeze
// can last before the entry expires from the log; operator action via the
// blocker-resolve flow lifts the freeze.
type Cursor struct {
	mu     sync.RWMutex
	state  map[s3lifecycle.ActionKey]int64
	frozen map[s3lifecycle.ActionKey]struct{}
}

func NewCursor() *Cursor {
	return &Cursor{
		state:  map[s3lifecycle.ActionKey]int64{},
		frozen: map[s3lifecycle.ActionKey]struct{}{},
	}
}

// MinTsNs returns the smallest position across all keys; zero if empty.
// Used as the resume point for the meta-log subscription.
func (c *Cursor) MinTsNs() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.state) == 0 {
		return 0
	}
	var min int64 = -1
	for _, v := range c.state {
		if min < 0 || v < min {
			min = v
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Get returns the current position for key, or 0 if unset.
func (c *Cursor) Get(key s3lifecycle.ActionKey) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state[key]
}

// Advance moves key's position forward to tsNs. No-op if key is frozen, if
// tsNs <= current, or if tsNs <= 0.
func (c *Cursor) Advance(key s3lifecycle.ActionKey, tsNs int64) {
	if tsNs <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, isFrozen := c.frozen[key]; isFrozen {
		return
	}
	if cur := c.state[key]; tsNs > cur {
		c.state[key] = tsNs
	}
}

// Freeze pins key at its current position. If key has no recorded position
// yet, it is set to tsNs.
func (c *Cursor) Freeze(key s3lifecycle.ActionKey, tsNs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.state[key]; !exists {
		c.state[key] = tsNs
	}
	c.frozen[key] = struct{}{}
}

// Unfreeze releases a freeze. Subsequent Advance calls take effect.
func (c *Cursor) Unfreeze(key s3lifecycle.ActionKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.frozen, key)
}

// IsFrozen reports whether key is currently pinned.
func (c *Cursor) IsFrozen(key s3lifecycle.ActionKey) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.frozen[key]
	return ok
}

// Snapshot copies the cursor map for persistence. Frozen keys are not
// distinguished in the snapshot; callers persist freezes via the separate
// blocker-record store.
func (c *Cursor) Snapshot() map[s3lifecycle.ActionKey]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[s3lifecycle.ActionKey]int64, len(c.state))
	for k, v := range c.state {
		out[k] = v
	}
	return out
}

// Restore replaces the cursor map. Freezes are not restored here; the
// caller re-applies them from blocker records on startup.
func (c *Cursor) Restore(state map[s3lifecycle.ActionKey]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = make(map[s3lifecycle.ActionKey]int64, len(state))
	for k, v := range state {
		c.state[k] = v
	}
	c.frozen = map[s3lifecycle.ActionKey]struct{}{}
}
