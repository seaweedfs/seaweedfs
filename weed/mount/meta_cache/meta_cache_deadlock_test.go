package meta_cache

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestApplyLoopInvalidateDoesNotDeadlockWithLockHoldingEnqueuer reproduces the
// mount hang seen in production (all readdirs stuck, FUSE waiting>0):
//
//   - A FUSE flush handler (flushMetadataToFiler) holds the file handle's
//     exclusive lock in wfs.fhLockTable and then enqueues a local metadata
//     event, waiting for the apply loop to process it.
//   - Concurrently the apply loop processes a subscriber event whose
//     invalidateFunc (weedfs.go) acquires the same file handle lock.
//
// The single-goroutine apply loop blocks on the fh lock while the fh lock
// holder blocks on the apply loop: a permanent ABBA deadlock. Every later
// EnsureVisited/BeginDirectoryBuild then backs up behind the dead loop, FUSE
// handler goroutines exhaust, and the kernel queue (waiting) grows.
//
// The contract under test: the apply loop must never block on locks that can
// be held by goroutines waiting on the apply loop.
func TestApplyLoopInvalidateDoesNotDeadlockWithLockHoldingEnqueuer(t *testing.T) {
	mapper, err := NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}

	// Stand-in for wfs.fhLockTable, keyed like FileHandleId.
	fhLockTable := util.NewLockTable[uint64]()
	const fhKey uint64 = 42

	invalidateEntered := make(chan struct{})
	var enteredOnce sync.Once

	var cachedMu sync.Mutex
	cached := map[util.FullPath]bool{"/": true, "/dir": true}

	mc := NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		mapper,
		util.FullPath("/"),
		false,
		func(path util.FullPath) {
			cachedMu.Lock()
			defer cachedMu.Unlock()
			cached[path] = true
		},
		func(path util.FullPath) bool {
			cachedMu.Lock()
			defer cachedMu.Unlock()
			return cached[path]
		},
		func(path util.FullPath, entry *filer_pb.Entry) {
			// Mirrors the wfs invalidateFunc: it takes the open file
			// handle's exclusive lock before refreshing the handle entry.
			enteredOnce.Do(func() { close(invalidateEntered) })
			lock := fhLockTable.AcquireLock("invalidateFunc", fhKey, util.ExclusiveLock)
			fhLockTable.ReleaseLock(fhKey, lock)
		},
		func(dir util.FullPath) {},
	)
	defer func() {
		// Only safe to shut down once the apply loop is unwedged.
		mc.Shutdown()
	}()

	subscriberEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    1,
					FileMode: 0100644,
				},
			},
		},
	}
	localEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file.txt"},
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    2,
					FileMode: 0100644,
				},
			},
			NewParentPath: "/dir",
		},
	}

	// Step 1: the "flush handler" takes the fh exclusive lock
	// (flushMetadataToFiler does this before talking to the filer).
	flushLock := fhLockTable.AcquireLock("doFlush", fhKey, util.ExclusiveLock)
	flushLockReleased := false
	releaseFlushLock := func() {
		if !flushLockReleased {
			flushLockReleased = true
			fhLockTable.ReleaseLock(fhKey, flushLock)
		}
	}
	defer releaseFlushLock()

	// Step 2: a subscriber event arrives; the apply loop will run
	// invalidateFunc, which wants the fh lock held above.
	subscriberDone := make(chan error, 1)
	go func() {
		subscriberDone <- mc.ApplyMetadataResponse(context.Background(), subscriberEvent, SubscriberMetadataResponseApplyOptions)
	}()

	select {
	case <-invalidateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("invalidateFunc was never invoked for the subscriber event")
	}
	// Give the invalidate goroutine a moment to actually block on AcquireLock.
	time.Sleep(50 * time.Millisecond)

	// Step 3: still holding the fh lock, the flush handler applies its local
	// metadata event and waits for the apply loop — exactly what
	// flushMetadataToFiler does after streamCreateEntry succeeds.
	flushApplyDone := make(chan error, 1)
	go func() {
		flushApplyDone <- mc.ApplyMetadataResponseOwned(context.Background(), localEvent, LocalMetadataResponseApplyOptions)
	}()

	select {
	case err := <-flushApplyDone:
		if err != nil {
			t.Fatalf("local metadata apply: %v", err)
		}
	case <-time.After(3 * time.Second):
		// Unwedge the apply loop so Shutdown in the deferred cleanup
		// doesn't hang the whole test binary.
		releaseFlushLock()
		t.Fatal("deadlock: apply loop blocked on fh lock while fh lock holder waits on apply loop")
	}

	releaseFlushLock()

	select {
	case err := <-subscriberDone:
		if err != nil {
			t.Fatalf("subscriber apply: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber apply never completed")
	}
}
