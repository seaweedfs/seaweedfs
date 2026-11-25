package filer

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

const DefaultReaderCacheMemoryLimit = 256 << 20

type ReaderCacheBudget struct {
	sync.Mutex
	limit        int64
	used         int64
	reservations map[*SingleChunkCacher]int64
	idle         list.List
	idleEntries  map[*SingleChunkCacher]*list.Element
	changed      chan struct{}
}

func NewReaderCacheBudget(limit int64) *ReaderCacheBudget {
	if limit <= 0 {
		limit = DefaultReaderCacheMemoryLimit
	}
	return &ReaderCacheBudget{
		limit:        limit,
		reservations: make(map[*SingleChunkCacher]int64),
		idleEntries:  make(map[*SingleChunkCacher]*list.Element),
		changed:      make(chan struct{}),
	}
}

func (b *ReaderCacheBudget) reserve(s *SingleChunkCacher) error {
	if s.chunkSize < 0 {
		return fmt.Errorf("invalid chunk size %d", s.chunkSize)
	}
	if b == nil {
		return nil
	}
	size := int64(mem.AllocationSize(s.chunkSize))
	if size > b.limit {
		return fmt.Errorf("chunk buffer needs %d bytes, exceeding reader cache budget %d; increase the readerCacheSizeMB budget", size, b.limit)
	}
	for {
		// a cancelled download must not evict a chunk for itself
		if err := s.ctx.Err(); err != nil {
			return err
		}

		b.Lock()
		if size <= b.limit-b.used {
			b.used += size
			b.reservations[s] = size
			b.Unlock()
			return nil
		}
		// Prefer evicting an idle chunk no stream is positioned in; fall back
		// to the oldest pinned one so abandoned pins cannot block the budget.
		var victim *SingleChunkCacher
		var entry *list.Element
		pinnedVictim := false
		for e := b.idle.Front(); e != nil; e = e.Next() {
			c := e.Value.(*SingleChunkCacher)
			if atomic.LoadInt32(&c.pins) == 0 {
				victim, entry = c, e
				pinnedVictim = false
				break
			}
			if victim == nil {
				victim, entry = c, e
				pinnedVictim = true
			}
		}
		if entry != nil {
			b.idle.Remove(entry)
			delete(b.idleEntries, victim)
			b.Unlock()
			if !victim.parent.evictForBudget(victim, pinnedVictim) {
				// The victim was pinned between selection and removal: keep it
				// evictable so a pin abandoned in that gap cannot wedge the
				// budget, then retry the selection.
				b.Lock()
				if _, ok := b.reservations[victim]; ok && b.idleEntries[victim] == nil {
					b.idleEntries[victim] = b.idle.PushBack(victim)
				}
				b.Unlock()
			}
			continue
		}
		changed := b.changed
		b.Unlock()
		select {
		case <-changed:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

// canFit reports whether a whole-chunk buffer of this size can ever be
// reserved. A chunk bigger than the budget cannot be read through the
// whole-chunk path at all, so callers must fall back to range fetches.
func (b *ReaderCacheBudget) canFit(size int) bool {
	return b == nil || int64(mem.AllocationSize(size)) <= b.limit
}

// TryReserveScratch takes size bytes for a buffer that a hedged chunk read
// borrows while it races the download holding the chunk's own reservation.
// Like a download it evicts idle chunks no stream is positioned in, but it
// never waits and never evicts a pinned chunk: a hedge that does not fit is
// simply not sent. A nil budget has room for anything.
func (b *ReaderCacheBudget) TryReserveScratch(size int) bool {
	if b == nil {
		return true
	}
	n := int64(mem.AllocationSize(size))
	if n > b.limit {
		return false
	}
	for {
		b.Lock()
		if n <= b.limit-b.used {
			b.used += n
			b.Unlock()
			return true
		}
		var victim *SingleChunkCacher
		var entry *list.Element
		for e := b.idle.Front(); e != nil; e = e.Next() {
			if c := e.Value.(*SingleChunkCacher); atomic.LoadInt32(&c.pins) == 0 {
				victim, entry = c, e
				break
			}
		}
		if entry == nil {
			b.Unlock()
			return false
		}
		b.idle.Remove(entry)
		delete(b.idleEntries, victim)
		b.Unlock()
		if !victim.parent.evictForBudget(victim, false) {
			// pinned meanwhile: keep it evictable, as reserve does, and give up
			b.Lock()
			if _, ok := b.reservations[victim]; ok && b.idleEntries[victim] == nil {
				b.idleEntries[victim] = b.idle.PushBack(victim)
			}
			b.Unlock()
			return false
		}
	}
}

// ReleaseScratch returns what TryReserveScratch took.
func (b *ReaderCacheBudget) ReleaseScratch(size int) {
	if b == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	b.used -= int64(mem.AllocationSize(size))
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *ReaderCacheBudget) complete(s *SingleChunkCacher) {
	if b == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	if _, found := b.reservations[s]; found && b.idleEntries[s] == nil {
		b.idleEntries[s] = b.idle.PushBack(s)
		close(b.changed)
		b.changed = make(chan struct{})
	}
}

func (b *ReaderCacheBudget) release(s *SingleChunkCacher) {
	if b == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	if size, found := b.reservations[s]; found {
		b.used -= size
		delete(b.reservations, s)
		if entry := b.idleEntries[s]; entry != nil {
			b.idle.Remove(entry)
			delete(b.idleEntries, s)
		}
		close(b.changed)
		b.changed = make(chan struct{})
	}
}
