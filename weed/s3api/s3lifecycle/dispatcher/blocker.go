package dispatcher

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// BlockerRecord persists the state needed to re-freeze a cursor on worker
// restart. Stored under /etc/s3/lifecycle/blockers/<shard>/<rule_hash>/<kind>.
//
// Operator action via the blocker-resolve flow either:
//   - Quarantine: keep the freeze; mark the (object, version) skipped via a
//     side-table so subsequent re-evaluations don't trip the same blocker.
//   - Retry: clear the record and Unfreeze; the cursor advances on the next
//     successful dispatch.
type BlockerRecord struct {
	ShardID    int
	Key        s3lifecycle.ActionKey
	FrozenAtNs int64  // tsNs of the event that tripped the blocker
	Reason     string // RPC outcome reason
	CreatedAt  time.Time
}

// BlockerStore persists BlockerRecords. The contract:
//   - Put replaces any existing record for (ShardID, Key) atomically.
//   - Delete is idempotent (no error on missing).
//   - List returns all records for ShardID for restart-time freeze replay.
type BlockerStore interface {
	Put(ctx context.Context, rec BlockerRecord) error
	Delete(ctx context.Context, shardID int, key s3lifecycle.ActionKey) error
	List(ctx context.Context, shardID int) ([]BlockerRecord, error)
}

// InMemoryBlockerStore is a BlockerStore for tests.
type InMemoryBlockerStore struct {
	mu      sync.Mutex
	records map[int]map[s3lifecycle.ActionKey]BlockerRecord
}

func NewInMemoryBlockerStore() *InMemoryBlockerStore {
	return &InMemoryBlockerStore{records: map[int]map[s3lifecycle.ActionKey]BlockerRecord{}}
}

func (s *InMemoryBlockerStore) Put(ctx context.Context, rec BlockerRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.records[rec.ShardID]
	if !ok {
		m = map[s3lifecycle.ActionKey]BlockerRecord{}
		s.records[rec.ShardID] = m
	}
	m[rec.Key] = rec
	return nil
}

func (s *InMemoryBlockerStore) Delete(ctx context.Context, shardID int, key s3lifecycle.ActionKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.records[shardID]; ok {
		delete(m, key)
	}
	return nil
}

func (s *InMemoryBlockerStore) List(ctx context.Context, shardID int) ([]BlockerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.records[shardID]
	out := make([]BlockerRecord, 0, len(m))
	for _, rec := range m {
		out = append(out, rec)
	}
	return out, nil
}
