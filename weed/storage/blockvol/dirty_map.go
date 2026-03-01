package blockvol

import "sync"

// dirtyEntry tracks a single LBA's location in the WAL.
type dirtyEntry struct {
	walOffset uint64
	lsn       uint64
	length    uint32
}

// dirtyShard is one shard of the sharded dirty map.
type dirtyShard struct {
	mu sync.RWMutex
	m  map[uint64]dirtyEntry
}

// DirtyMap is a concurrency-safe map from LBA to WAL offset.
// It uses sharding to reduce lock contention on concurrent workloads.
type DirtyMap struct {
	shards []dirtyShard
	mask   uint64 // numShards - 1, for fast modulo
}

// NewDirtyMap creates an empty dirty map with the given number of shards.
// numShards must be a power-of-2. Use 1 for a single-shard (unsharded) map.
func NewDirtyMap(numShards int) *DirtyMap {
	if numShards <= 0 {
		numShards = 1
	}
	shards := make([]dirtyShard, numShards)
	for i := range shards {
		shards[i].m = make(map[uint64]dirtyEntry)
	}
	return &DirtyMap{
		shards: shards,
		mask:   uint64(numShards - 1),
	}
}

// shard returns the shard for the given LBA.
func (d *DirtyMap) shard(lba uint64) *dirtyShard {
	return &d.shards[lba&d.mask]
}

// Put records that the given LBA has dirty data at walOffset.
func (d *DirtyMap) Put(lba uint64, walOffset uint64, lsn uint64, length uint32) {
	s := d.shard(lba)
	s.mu.Lock()
	s.m[lba] = dirtyEntry{walOffset: walOffset, lsn: lsn, length: length}
	s.mu.Unlock()
}

// Get returns the dirty entry for lba. ok is false if lba is not dirty.
func (d *DirtyMap) Get(lba uint64) (walOffset uint64, lsn uint64, length uint32, ok bool) {
	s := d.shard(lba)
	s.mu.RLock()
	e, found := s.m[lba]
	s.mu.RUnlock()
	if !found {
		return 0, 0, 0, false
	}
	return e.walOffset, e.lsn, e.length, true
}

// Delete removes a single LBA from the dirty map.
func (d *DirtyMap) Delete(lba uint64) {
	s := d.shard(lba)
	s.mu.Lock()
	delete(s.m, lba)
	s.mu.Unlock()
}

// rangeEntry is a snapshot of one dirty entry for lock-free iteration.
type rangeEntry struct {
	lba       uint64
	walOffset uint64
	lsn       uint64
	length    uint32
}

// Range calls fn for each dirty entry with LBA in [start, start+count).
// Entries are copied under lock (one shard at a time), then fn is called
// without holding any lock, so fn may safely call back into DirtyMap.
func (d *DirtyMap) Range(start uint64, count uint32, fn func(lba, walOffset, lsn uint64, length uint32)) {
	end := start + uint64(count)

	var entries []rangeEntry
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.RLock()
		for lba, e := range s.m {
			if lba >= start && lba < end {
				entries = append(entries, rangeEntry{lba, e.walOffset, e.lsn, e.length})
			}
		}
		s.mu.RUnlock()
	}

	for _, e := range entries {
		fn(e.lba, e.walOffset, e.lsn, e.length)
	}
}

// Len returns the number of dirty entries across all shards.
func (d *DirtyMap) Len() int {
	n := 0
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}

// SnapshotEntry is an exported snapshot of one dirty entry.
type SnapshotEntry struct {
	Lba       uint64
	WalOffset uint64
	Lsn       uint64
	Length    uint32
}

// Snapshot returns a copy of all dirty entries. Each shard is locked
// individually, and the result is returned without holding any lock.
func (d *DirtyMap) Snapshot() []SnapshotEntry {
	var entries []SnapshotEntry
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.RLock()
		for lba, e := range s.m {
			entries = append(entries, SnapshotEntry{
				Lba:       lba,
				WalOffset: e.walOffset,
				Lsn:       e.lsn,
				Length:    e.length,
			})
		}
		s.mu.RUnlock()
	}
	return entries
}
