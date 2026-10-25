package storage

import (
	"os"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

// Read-only volumes — cloud-tiered ones above all — outnumber writable ones by
// orders of magnitude on a large server, and each one used to pin its .idx and
// .sdx descriptors for the life of the process. At ~600K volumes per server
// that alone exhausts any fd limit. Neither file is needed except while a
// lookup is in flight, so SortedFileNeedleMap borrows them from this bounded
// pool: an idle volume holds no descriptor at all, while a busy one keeps its
// handles hot instead of paying an open() per needle.
const maxPooledIndexFiles = 1024

type pooledFile struct {
	file    *os.File
	refs    int
	dropped bool // left the pool; close once the last borrower is done
}

type indexFilePool struct {
	sync.Mutex
	lru *simplelru.LRU[string, *pooledFile]
}

var pooledIndexFiles = newIndexFilePool(maxPooledIndexFiles)

func newIndexFilePool(size int) *indexFilePool {
	p := &indexFilePool{}
	// simplelru is not thread safe on its own; every access below holds
	// p.Mutex, and this eviction callback runs inline under it.
	p.lru, _ = simplelru.NewLRU(size, func(_ string, f *pooledFile) {
		f.dropped = true
		f.closeIfUnused()
	})
	return p
}

// closeIfUnused closes a handle that has left the pool once no borrower is
// still reading through it. Callers hold indexFilePool.Mutex.
func (f *pooledFile) closeIfUnused() {
	if f.dropped && f.refs == 0 && f.file != nil {
		f.file.Close()
		f.file = nil
	}
}

// Writable and read-only handles for the same path are pooled separately so a
// read never depends on the .idx being openable for write — a volume served off
// a read-only mount still answers lookups.
func poolKey(name string, writable bool) string {
	if writable {
		return name + "\x00rw"
	}
	return name
}

// borrow hands out an open handle for name, reusing the pooled one when there
// is one. The caller must release it exactly once.
func (p *indexFilePool) borrow(name string, writable bool) (*pooledFile, error) {
	key := poolKey(name, writable)

	p.Lock()
	if f, found := p.lru.Get(key); found {
		f.refs++
		p.Unlock()
		return f, nil
	}
	p.Unlock()

	flag := os.O_RDONLY
	if writable {
		flag = os.O_RDWR
	}
	// Opened outside the lock: a cold open blocks on disk, and holding a
	// process-wide mutex across it would serialize every volume's lookups.
	file, err := backend.OpenVolumeFile(name, flag)
	if err != nil {
		return nil, err
	}

	p.Lock()
	defer p.Unlock()
	if f, found := p.lru.Get(key); found { // another borrower won the race
		f.refs++
		file.Close()
		return f, nil
	}
	f := &pooledFile{file: file, refs: 1}
	p.lru.Add(key, f)
	return f, nil
}

func (p *indexFilePool) release(f *pooledFile) {
	p.Lock()
	defer p.Unlock()
	f.refs--
	f.closeIfUnused()
}

// discard forgets the pooled handles for name, so a later rename or delete of
// that path cannot be served from a descriptor on the old inode.
func (p *indexFilePool) discard(name string) {
	p.Lock()
	defer p.Unlock()
	p.lru.Remove(poolKey(name, false))
	p.lru.Remove(poolKey(name, true))
}
