package leveldb_handle

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type failingStorage struct {
	storage.Storage
	fail         atomic.Bool
	failOpen     atomic.Bool
	probeStarted chan struct{}
	probeRelease chan struct{}
}

func (f *failingStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	if f.failOpen.Load() && fd.Type != storage.TypeTemp {
		return nil, syscall.ENOSPC
	}
	if fd.Type == storage.TypeTemp && f.probeRelease != nil {
		f.probeStarted <- struct{}{}
		<-f.probeRelease
	}
	w, err := f.Storage.Create(fd)
	if err != nil {
		return nil, err
	}
	return &failingWriter{Writer: w, s: f}, nil
}

type failingWriter struct {
	storage.Writer
	s *failingStorage
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.s.fail.Load() {
		return 0, syscall.ENOSPC
	}
	return w.Writer.Write(p)
}

func openFailing(t *testing.T) (*DB, *failingStorage) {
	t.Helper()
	interval, busy, timeout, wait := reopenInterval, busyReopenInterval, drainTimeout, reopenWait
	reopenInterval, busyReopenInterval, drainTimeout, reopenWait = 0, 0, 50*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() {
		reopenInterval, busyReopenInterval, drainTimeout, reopenWait = interval, busy, timeout, wait
	})
	st := &failingStorage{Storage: storage.NewMemStorage()}
	db, err := Open(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, st
}

func expectValue(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key), nil)
	if err != nil || string(got) != want {
		t.Fatalf("get %s = %q, %v; want %q", key, got, err, want)
	}
}

func TestWriteRecoversOnceStorageRecovers(t *testing.T) {
	db, st := openFailing(t)
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}

	st.fail.Store(true)
	if err := db.Put([]byte("b"), []byte("2"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put while storage fails: %v", err)
	}
	expectValue(t, db, "a", "1")

	st.fail.Store(false)
	if err := db.Put([]byte("c"), []byte("3"), nil); err != nil {
		t.Fatalf("put after storage recovered: %v", err)
	}
	batch := new(leveldb.Batch)
	batch.Delete([]byte("c"))
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("batch after storage recovered: %v", err)
	}
	expectValue(t, db, "a", "1")
	if _, err := db.Get([]byte("c"), nil); err != leveldb.ErrNotFound {
		t.Fatalf("get deleted key: %v", err)
	}
}

func TestStalledProbeKeepsReadsAvailable(t *testing.T) {
	db, st := openFailing(t)
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	st.probeStarted, st.probeRelease = make(chan struct{}, 1), make(chan struct{})
	st.fail.Store(true)
	put := make(chan error, 1)
	go func() { put <- db.Put([]byte("b"), []byte("2"), nil) }()
	<-st.probeStarted

	read := make(chan struct{})
	go func() {
		defer close(read)
		expectValue(t, db, "a", "1")
		db.NewIterator(nil, nil).Release()
	}()
	select {
	case <-read:
	case <-time.After(5 * time.Second):
		t.Error("reads blocked behind a stalled probe")
	}
	close(st.probeRelease)
	if err := <-put; !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put while storage fails: %v", err)
	}
	<-read
}

func TestWritesDoNotQueueBehindAStalledProbe(t *testing.T) {
	db, st := openFailing(t)
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	st.probeStarted, st.probeRelease = make(chan struct{}, 1), make(chan struct{})
	st.fail.Store(true)
	first := make(chan error, 1)
	go func() { first <- db.Put([]byte("b"), []byte("2"), nil) }()
	<-st.probeStarted

	second := make(chan error, 1)
	go func() { second <- db.Put([]byte("c"), []byte("3"), nil) }()
	select {
	case err := <-second:
		if !errors.Is(err, syscall.ENOSPC) {
			t.Errorf("put behind a stalled probe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("put queued behind a stalled probe")
	}
	close(st.probeRelease)
	if err := <-first; !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put while storage fails: %v", err)
	}
}

func TestReopenWaitsForOpenIterator(t *testing.T) {
	db, st := openFailing(t)
	for _, k := range []string{"a", "b"} {
		if err := db.Put([]byte(k), []byte(k), nil); err != nil {
			t.Fatal(err)
		}
	}
	st.fail.Store(true)
	if err := db.Put([]byte("x"), []byte("x"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)
	busyReopenInterval = time.Hour

	iter := db.NewIterator(nil, nil)
	if !iter.Next() || string(iter.Key()) != "a" {
		t.Fatalf("first key %q", iter.Key())
	}
	if err := db.Put([]byte("y"), []byte("y"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put with an open iterator: %v", err)
	}
	if !iter.Next() || string(iter.Key()) != "b" || iter.Next() {
		t.Fatalf("iterator after a refused reopen: key %q, err %v", iter.Key(), iter.Error())
	}
	iter.Release()
	iter.Release()

	if err := db.Put([]byte("y"), []byte("y"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put right after a refused reopen: %v", err)
	}
	db.nextReopen.Store(0)
	if err := db.Put([]byte("y"), []byte("y"), nil); err != nil {
		t.Fatalf("put after the iterator was released: %v", err)
	}
	expectValue(t, db, "b", "b")
}

func TestNestedReadWaitsOutTheDrain(t *testing.T) {
	db, st := openFailing(t)
	drainTimeout = 500 * time.Millisecond
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	if err := db.Put([]byte("x"), []byte("x"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)
	st.probeStarted, st.probeRelease = make(chan struct{}, 1), make(chan struct{})

	iter := db.NewIterator(nil, nil)
	put := make(chan error, 1)
	go func() { put <- db.Put([]byte("y"), []byte("y"), nil) }()
	<-st.probeStarted
	close(st.probeRelease)
	for db.draining.Load() == nil {
		select {
		case err := <-put:
			t.Fatalf("put returned before its drain was seen: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	expectValue(t, db, "a", "1")
	iter.Release()
	if err := <-put; !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put with an open iterator: %v", err)
	}
}

func TestReopenIsNotStarvedByOverlappingReaders(t *testing.T) {
	db, st := openFailing(t)
	drainTimeout = 5 * time.Second
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	if err := db.Put([]byte("x"), []byte("x"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)

	stop := make(chan struct{})
	var readers, started sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		started.Add(1)
		go func() {
			defer readers.Done()
			for first := true; ; first = false {
				select {
				case <-stop:
					return
				default:
				}
				iter := db.NewIterator(nil, nil)
				if first {
					started.Done()
				}
				time.Sleep(2 * time.Millisecond)
				iter.Release()
			}
		}()
		time.Sleep(500 * time.Microsecond)
	}
	started.Wait()
	err := db.Put([]byte("y"), []byte("y"), nil)
	close(stop)
	readers.Wait()
	if err != nil {
		t.Fatalf("put while readers overlap: %v", err)
	}
	expectValue(t, db, "y", "y")
}

func TestCloseIsNotUndoneByReopen(t *testing.T) {
	db, st := openFailing(t)
	st.fail.Store(true)
	if err := db.Put([]byte("a"), []byte("1"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)
	db.Close()
	if err := db.Put([]byte("a"), []byte("1"), nil); err != leveldb.ErrClosed {
		t.Fatalf("put after close: %v", err)
	}
	if iter := db.NewIterator(nil, nil); iter.Next() || iter.Error() != leveldb.ErrClosed {
		t.Fatalf("iterator after close: %v", iter.Error())
	}
}

func TestReopenKeepsFileDatabase(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if !db.reopen(db.db) {
		t.Fatal("reopen failed")
	}
	if err := db.Put([]byte("b"), []byte("2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectValue(t, db, "a", "1")
	expectValue(t, db, "b", "2")
}

func TestReopenAttemptsAreRateLimited(t *testing.T) {
	db, st := openFailing(t)
	reopenInterval = time.Hour
	st.fail.Store(true)
	if err := db.Put([]byte("a"), []byte("1"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)
	if err := db.Put([]byte("a"), []byte("1"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("put within the reopen interval: %v", err)
	}
	db.nextReopen.Store(0)
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatalf("put after the reopen interval: %v", err)
	}
}

func TestFailedReopenReportsErrorsUntilOpen(t *testing.T) {
	db, st := openFailing(t)
	if err := db.Put([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	if err := db.Put([]byte("b"), []byte("2"), nil); err == nil {
		t.Fatal("put while storage fails succeeded")
	}
	st.fail.Store(false)
	st.failOpen.Store(true)
	if err := db.Put([]byte("b"), []byte("2"), nil); err == nil {
		t.Fatal("put with a failed reopen succeeded")
	}
	if _, err := db.Get([]byte("a"), nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("get with a failed reopen: %v", err)
	}
	iter := db.NewIterator(nil, nil)
	if iter.Next() || !errors.Is(iter.Error(), syscall.ENOSPC) {
		t.Fatalf("iterator with a failed reopen: %v", iter.Error())
	}
	iter.Release()

	st.failOpen.Store(false)
	expectValue(t, db, "a", "1")
	if err := db.Put([]byte("b"), []byte("2"), nil); err != nil {
		t.Fatalf("put after the reopen succeeded: %v", err)
	}
}
