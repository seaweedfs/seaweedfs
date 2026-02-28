package blockvol

import "sync"

// dirtyEntry tracks a single LBA's location in the WAL.
type dirtyEntry struct {
	walOffset uint64
	lsn       uint64
	length    uint32
}

// DirtyMap is a concurrency-safe map from LBA to WAL offset.
// Phase 1 uses a single shard; Phase 3 upgrades to 256 shards.
type DirtyMap struct {
	mu sync.RWMutex
	m  map[uint64]dirtyEntry
}

// NewDirtyMap creates an empty dirty map.
func NewDirtyMap() *DirtyMap {
	return &DirtyMap{m: make(map[uint64]dirtyEntry)}
}

// Put records that the given LBA has dirty data at walOffset.
func (d *DirtyMap) Put(lba uint64, walOffset uint64, lsn uint64, length uint32) {
	d.mu.Lock()
	d.m[lba] = dirtyEntry{walOffset: walOffset, lsn: lsn, length: length}
	d.mu.Unlock()
}

// Get returns the dirty entry for lba. ok is false if lba is not dirty.
func (d *DirtyMap) Get(lba uint64) (walOffset uint64, lsn uint64, length uint32, ok bool) {
	d.mu.RLock()
	e, found := d.m[lba]
	d.mu.RUnlock()
	if !found {
		return 0, 0, 0, false
	}
	return e.walOffset, e.lsn, e.length, true
}

// Delete removes a single LBA from the dirty map.
func (d *DirtyMap) Delete(lba uint64) {
	d.mu.Lock()
	delete(d.m, lba)
	d.mu.Unlock()
}

// rangeEntry is a snapshot of one dirty entry for lock-free iteration.
type rangeEntry struct {
	lba       uint64
	walOffset uint64
	lsn       uint64
	length    uint32
}

// Range calls fn for each dirty entry with LBA in [start, start+count).
// Entries are copied under lock, then fn is called without holding the lock,
// so fn may safely call back into DirtyMap (Put, Delete, etc.).
func (d *DirtyMap) Range(start uint64, count uint32, fn func(lba, walOffset, lsn uint64, length uint32)) {
	end := start + uint64(count)

	// Snapshot matching entries under lock.
	d.mu.RLock()
	entries := make([]rangeEntry, 0, len(d.m))
	for lba, e := range d.m {
		if lba >= start && lba < end {
			entries = append(entries, rangeEntry{lba, e.walOffset, e.lsn, e.length})
		}
	}
	d.mu.RUnlock()

	// Call fn without lock.
	for _, e := range entries {
		fn(e.lba, e.walOffset, e.lsn, e.length)
	}
}

// Len returns the number of dirty entries.
func (d *DirtyMap) Len() int {
	d.mu.RLock()
	n := len(d.m)
	d.mu.RUnlock()
	return n
}

// SnapshotEntry is an exported snapshot of one dirty entry.
type SnapshotEntry struct {
	Lba       uint64
	WalOffset uint64
	Lsn       uint64
	Length    uint32
}

// Snapshot returns a copy of all dirty entries. The snapshot is taken under
// read lock but returned without holding the lock.
func (d *DirtyMap) Snapshot() []SnapshotEntry {
	d.mu.RLock()
	entries := make([]SnapshotEntry, 0, len(d.m))
	for lba, e := range d.m {
		entries = append(entries, SnapshotEntry{
			Lba:       lba,
			WalOffset: e.walOffset,
			Lsn:       e.lsn,
			Length:    e.length,
		})
	}
	d.mu.RUnlock()
	return entries
}
