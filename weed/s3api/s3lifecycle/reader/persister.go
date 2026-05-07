package reader

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// Persister loads and stores per-shard cursor state. The contract:
//   - Load returns an empty map (not an error) when no prior state exists.
//   - Save replaces the prior state atomically; partial writes must not be
//     observable on a later Load.
//   - Concurrent Save calls for the same shardID are serialized by the
//     caller (one Reader per shard at a time).
//
// The filer-backed implementation lives in the worker package alongside the
// rest of the persistence wiring; this package only ships the in-memory
// variant used by tests.
type Persister interface {
	Load(ctx context.Context, shardID int) (map[s3lifecycle.ActionKey]int64, error)
	Save(ctx context.Context, shardID int, state map[s3lifecycle.ActionKey]int64) error
}

// InMemoryPersister is a Persister for tests. Save copies the input so
// later mutations by the caller don't leak into stored state.
type InMemoryPersister struct {
	mu      sync.Mutex
	storage map[int]map[s3lifecycle.ActionKey]int64
}

func NewInMemoryPersister() *InMemoryPersister {
	return &InMemoryPersister{storage: map[int]map[s3lifecycle.ActionKey]int64{}}
}

func (p *InMemoryPersister) Load(ctx context.Context, shardID int) (map[s3lifecycle.ActionKey]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[s3lifecycle.ActionKey]int64{}
	for k, v := range p.storage[shardID] {
		out[k] = v
	}
	return out, nil
}

func (p *InMemoryPersister) Save(ctx context.Context, shardID int, state map[s3lifecycle.ActionKey]int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := make(map[s3lifecycle.ActionKey]int64, len(state))
	for k, v := range state {
		copied[k] = v
	}
	p.storage[shardID] = copied
	return nil
}
