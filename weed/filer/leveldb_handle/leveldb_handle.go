// Package leveldb_handle wraps a goleveldb database so a write that fails on
// a storage error reopens it. goleveldb latches the first journal write error
// and returns it for every later write until the database is reopened, so one
// transient ENOSPC or EIO would otherwise fail writes until a restart.
package leveldb_handle

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	reopenInterval     = time.Second
	busyReopenInterval = 10 * time.Second
	drainTimeout       = time.Second
	reopenWait         = 5 * time.Second
	probeFile          = storage.FileDesc{Type: storage.TypeTemp, Num: 1 << 40}
	probeData          = make([]byte, 64*1024)
)

// DB offers the leveldb.DB methods the filer stores use. Every call holds mu
// shared, an iterator until it is released. A reopen stops admitting calls
// for at most drainTimeout and takes mu with TryLock only, so a call that
// nests another one delays the reopen instead of deadlocking.
type DB struct {
	name        string
	stor        storage.Storage
	ownsStorage bool
	opts        *opt.Options
	closed      atomic.Bool
	nextReopen  atomic.Int64
	reopening   chan struct{}
	draining    atomic.Pointer[chan struct{}]

	mu      sync.RWMutex
	db      *leveldb.DB
	openErr error
}

func OpenFile(path string, o *opt.Options) (*DB, error) {
	stor, err := storage.OpenFile(path, o.GetReadOnly())
	if err != nil {
		return nil, err
	}
	d, err := Open(stor, o)
	if err != nil {
		stor.Close()
		return nil, err
	}
	d.name = path
	d.ownsStorage = true
	return d, nil
}

func Open(stor storage.Storage, o *opt.Options) (*DB, error) {
	db, err := open(stor, o)
	if err != nil {
		return nil, err
	}
	return &DB{stor: stor, opts: o, db: db, reopening: make(chan struct{}, 1)}, nil
}

func open(stor storage.Storage, o *opt.Options) (*leveldb.DB, error) {
	db, err := leveldb.Open(stor, o)
	if leveldb_errors.IsCorrupted(err) {
		db, err = leveldb.Recover(stor, o)
	}
	return db, err
}

func (d *DB) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	db, err := d.acquire()
	if err != nil {
		return nil, err
	}
	defer d.mu.RUnlock()
	return db.Get(key, ro)
}

func (d *DB) NewIterator(slice *leveldb_util.Range, ro *opt.ReadOptions) iterator.Iterator {
	db, err := d.acquire()
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	return &heldIterator{Iterator: db.NewIterator(slice, ro), unlock: d.mu.RUnlock}
}

func (d *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return d.write(func(db *leveldb.DB) error { return db.Put(key, value, wo) })
}

func (d *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return d.write(func(db *leveldb.DB) error { return db.Delete(key, wo) })
}

func (d *DB) Write(batch *leveldb.Batch, wo *opt.WriteOptions) error {
	return d.write(func(db *leveldb.DB) error { return db.Write(batch, wo) })
}

// Close does not wait for in-flight calls, matching leveldb.DB.Close.
func (d *DB) Close() error {
	if d.closed.Swap(true) {
		return leveldb.ErrClosed
	}
	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()
	var err error
	if db != nil {
		err = db.Close()
	}
	if d.ownsStorage {
		if closeErr := d.stor.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (d *DB) write(fn func(*leveldb.DB) error) error {
	db, err := d.acquire()
	if err != nil {
		return err
	}
	err = fn(db)
	d.mu.RUnlock()
	if !isStorageError(err) || !d.reopen(db) {
		return err
	}
	if db, err = d.acquire(); err != nil {
		return err
	}
	defer d.mu.RUnlock()
	return fn(db)
}

// acquire returns the open database with mu held shared.
func (d *DB) acquire() (*leveldb.DB, error) {
	for attempt := 0; ; attempt++ {
		if draining := d.draining.Load(); draining != nil {
			<-*draining
		}
		d.mu.RLock()
		if d.closed.Load() {
			d.mu.RUnlock()
			return nil, leveldb.ErrClosed
		}
		if d.db != nil {
			return d.db, nil
		}
		err := d.openErr
		d.mu.RUnlock()
		if attempt > 0 || !d.reopen(nil) {
			return nil, err
		}
	}
}

// reopen replaces failed with a freshly opened database once no call holds
// it, unless another caller already did. It waits at most reopenWait for a
// running reopen and reports whether a retry can use an open database.
func (d *DB) reopen(failed *leveldb.DB) bool {
	select {
	case d.reopening <- struct{}{}:
	case <-time.After(reopenWait):
		return false
	}
	defer func() { <-d.reopening }()
	d.mu.RLock()
	current := d.db
	d.mu.RUnlock()
	if current != failed {
		return current != nil
	}
	if d.closed.Load() || time.Now().UnixNano() < d.nextReopen.Load() {
		return false
	}
	d.holdOffReopen(reopenInterval)
	// Reopening on storage that still fails would also lose reads.
	if err := probe(d.stor); err != nil {
		glog.Warningf("leveldb %s: storage still failing, not reopening: %v", d.name, err)
		return false
	}
	if !d.lockWhenIdle() {
		glog.Warningf("leveldb %s: calls still running after %v, not reopening", d.name, drainTimeout)
		d.holdOffReopen(busyReopenInterval)
		return false
	}
	defer d.mu.Unlock()
	if d.closed.Load() {
		return false
	}
	glog.Warningf("leveldb %s: reopening after a storage write error", d.name)
	if failed != nil {
		failed.Close()
	}
	d.db, d.openErr = open(d.stor, d.opts)
	if d.openErr != nil {
		glog.Errorf("leveldb %s: reopen: %v", d.name, d.openErr)
		return false
	}
	return true
}

func (d *DB) holdOffReopen(interval time.Duration) {
	d.nextReopen.Store(time.Now().Add(interval).UnixNano())
}

// lockWhenIdle holds new calls back while the running ones finish, then
// takes mu, giving up after drainTimeout.
func (d *DB) lockWhenIdle() bool {
	draining := make(chan struct{})
	d.draining.Store(&draining)
	defer func() {
		d.draining.Store(nil)
		close(draining)
	}()
	deadline := time.Now().Add(drainTimeout)
	for !d.mu.TryLock() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

func probe(stor storage.Storage) error {
	w, err := stor.Create(probeFile)
	if err != nil {
		return err
	}
	_, err = w.Write(probeData)
	if err == nil {
		err = w.Sync()
	}
	if closeErr := w.Close(); err == nil {
		err = closeErr
	}
	stor.Remove(probeFile)
	return err
}

func isStorageError(err error) bool {
	var errno syscall.Errno
	return errors.As(err, &errno)
}

type heldIterator struct {
	iterator.Iterator
	unlock func()
}

func (it *heldIterator) Release() {
	it.Iterator.Release()
	if it.unlock != nil {
		it.unlock()
		it.unlock = nil
	}
}
