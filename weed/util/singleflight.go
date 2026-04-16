package util

import (
	"sync"
)

// call represents an in-flight or completed function call.
type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

// SingleFlightGroup provides deduplication of concurrent function calls
// keyed by a string. If multiple goroutines call Do with the same key
// concurrently, only one executes the function; the others wait and
// receive the same result.
//
// After a call completes, the key is removed so that subsequent calls
// trigger a fresh execution.
type SingleFlightGroup struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do executes fn once for a given key, even if called concurrently.
// All callers for the same key block until fn returns and then receive
// the same result.
func (g *SingleFlightGroup) Do(key string, fn func() ([]byte, error)) ([]byte, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()

	// Hold the lock while signalling completion and removing the key.
	// This ensures that any goroutine currently in Do either:
	//   - holds a reference to c and will receive the result via wg.Wait, or
	//   - acquires the lock after delete and starts a fresh call.
	// Without the lock, a new caller could find the key missing (after
	// delete) and start a duplicate fn before wg.Done wakes existing
	// waiters.
	g.mu.Lock()
	delete(g.m, key)
	c.wg.Done()
	g.mu.Unlock()

	return c.val, c.err
}
