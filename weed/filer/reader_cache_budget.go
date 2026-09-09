package filer

import (
	"container/list"
	"fmt"
	"sync"

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
	size := int64(mem.AllocationSize(s.chunkSize))
	if size > b.limit {
		return fmt.Errorf("chunk buffer needs %d bytes, exceeding reader cache budget %d; increase -readerCacheSizeMB", size, b.limit)
	}
	for {
		b.Lock()
		if size <= b.limit-b.used {
			b.used += size
			b.reservations[s] = size
			b.Unlock()
			return nil
		}
		if entry := b.idle.Front(); entry != nil {
			victim := entry.Value.(*SingleChunkCacher)
			b.idle.Remove(entry)
			delete(b.idleEntries, victim)
			b.Unlock()
			victim.parent.remove(victim)
			continue
		}
		changed := b.changed
		b.Unlock()
		<-changed
	}
}

func (b *ReaderCacheBudget) complete(s *SingleChunkCacher) {
	b.Lock()
	defer b.Unlock()
	if _, found := b.reservations[s]; found && b.idleEntries[s] == nil {
		b.idleEntries[s] = b.idle.PushBack(s)
		close(b.changed)
		b.changed = make(chan struct{})
	}
}

func (b *ReaderCacheBudget) release(s *SingleChunkCacher) {
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
