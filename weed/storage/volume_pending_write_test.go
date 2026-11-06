package storage

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/require"
)

func waitForPendingWrites(t *testing.T, v *Volume, id NeedleId, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		v.pendingWrites.Lock()
		got := len(v.pendingWrites.byId[id])
		v.pendingWrites.Unlock()
		if got >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d writes of needle %d registered", got, count, id)
		}
		time.Sleep(time.Millisecond)
	}
}

func requireNeedleDeleted(t *testing.T, v *Volume, id NeedleId) {
	t.Helper()
	_, err := v.readNeedle(&needle.Needle{Id: id}, nil, nil)
	require.ErrorIs(t, err, ErrorDeleted)
}

// A replicate delete that finds nothing can still have the write it undoes
// queued behind the volume locks. That write must not land live afterwards.
func TestDeleteOfMissingNeedleCancelsQueuedWrite(t *testing.T) {
	for _, fsync := range []bool{false, true} {
		t.Run(fmt.Sprintf("fsync=%v", fsync), func(t *testing.T) {
			dir := t.TempDir()
			v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
			require.NoError(t, err)

			n := newRandomNeedle(1)
			v.dataFileSyncLock.Lock()
			written := make(chan error, 1)
			go func() {
				_, _, _, err := v.writeNeedle2(n, true, fsync, false)
				written <- err
			}()
			waitForPendingWrites(t, v, n.Id, 1)

			_, err = v.readNeedle(&needle.Needle{Id: n.Id}, nil, nil)
			require.ErrorIs(t, err, ErrorNotFound)
			v.CancelPendingWrites(n.Id, n.Cookie)
			v.dataFileSyncLock.Unlock()

			require.ErrorIs(t, <-written, ErrorDeleted)
			requireNeedleDeleted(t, v, n.Id)
			require.Empty(t, v.pendingWrites.byId)

			v.Close()
			v, err = NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
			require.NoError(t, err)
			defer v.Close()
			requireNeedleDeleted(t, v, n.Id)
		})
	}
}

// A delete that finds a copy also cancels a retry of the same write that is
// still queued, so the retry does not bring the needle back.
func TestDeleteCancelsQueuedRewrite(t *testing.T) {
	v := newWorkerTestVolume(t)
	n := newRandomNeedle(1)
	_, _, _, err := v.writeNeedle2(n, true, false, false)
	require.NoError(t, err)

	rewrite := newRandomNeedle(1)
	rewrite.Cookie = n.Cookie
	// holds the durable rewrite after it registers, before it reaches the worker
	v.asyncWorkerLock.Lock()
	written := make(chan error, 1)
	go func() {
		_, _, _, err := v.writeNeedle2(rewrite, true, true, false)
		written <- err
	}()
	waitForPendingWrites(t, v, n.Id, 1)

	_, err = v.deleteNeedle2(&needle.Needle{Id: n.Id, Cookie: n.Cookie})
	require.NoError(t, err)
	requireNeedleDeleted(t, v, n.Id)
	v.asyncWorkerLock.Unlock()

	require.ErrorIs(t, <-written, ErrorDeleted)
	requireNeedleDeleted(t, v, n.Id)
	require.Empty(t, v.pendingWrites.byId)
}

// Only writes already in flight are cancelled: writing the needle again after
// the delete works as before.
func TestWriteAfterDeleteIsNotCancelled(t *testing.T) {
	v := newWorkerTestVolume(t)
	n := newRandomNeedle(1)
	v.CancelPendingWrites(n.Id, n.Cookie)
	_, err := v.deleteNeedle2(&needle.Needle{Id: n.Id, Cookie: n.Cookie})
	require.NoError(t, err)

	_, _, _, err = v.writeNeedle2(n, true, false, false)
	require.NoError(t, err)
	got := &needle.Needle{Id: n.Id}
	_, err = v.readNeedle(got, nil, nil)
	require.NoError(t, err)
	require.Equal(t, n.Data, got.Data)
}

// A delete only cancels writes that carry its cookie.
func TestCancelWithOtherCookieKeepsWrite(t *testing.T) {
	v := newWorkerTestVolume(t)
	n := newRandomNeedle(1)
	v.dataFileSyncLock.Lock()
	written := make(chan error, 1)
	go func() {
		_, _, _, err := v.writeNeedle2(n, true, false, false)
		written <- err
	}()
	waitForPendingWrites(t, v, n.Id, 1)
	v.CancelPendingWrites(n.Id, n.Cookie+1)
	v.dataFileSyncLock.Unlock()

	require.NoError(t, <-written)
	got := &needle.Needle{Id: n.Id}
	_, err := v.readNeedle(got, nil, nil)
	require.NoError(t, err)
	require.Equal(t, n.Data, got.Data)
}

// The cancelled write is taken back before the next write of the needle runs,
// so an identical write sent after the delete appends its own copy instead of
// matching the record about to be deleted, even in the same batch.
func TestWriteAfterCancelledWriteStaysLive(t *testing.T) {
	v := newWorkerTestVolume(t)
	v.location = &DiskLocation{syncBatch: SyncBatchOption{
		MinBytes:    1 << 30,
		MinRequests: syncBatchMaxRequests,
		Wait:        time.Minute,
	}}
	armed := make(chan chan time.Time, 1)
	prevAfter := syncBatchAfter
	syncBatchAfter = func(time.Duration) <-chan time.Time {
		window := make(chan time.Time, 1)
		armed <- window
		return window
	}
	defer func() { syncBatchAfter = prevAfter }()

	queue := func(n *needle.Needle) *needle.AsyncRequest {
		w := v.trackPendingWrite(n)
		t.Cleanup(func() { v.untrackPendingWrite(w) })
		r := needle.NewAsyncRequest(n, true)
		r.ActualSize = needle.GetActualSize(Size(len(n.Data)), v.Version())
		require.True(t, v.asyncRequestAppend(r))
		return r
	}

	n := newRandomNeedle(1)
	first := queue(n)
	window := <-armed
	v.CancelPendingWrites(n.Id, n.Cookie)
	retry := queue(&needle.Needle{Id: n.Id, Cookie: n.Cookie, Data: n.Data, Checksum: n.Checksum})
	require.Eventually(t, func() bool { return len(v.asyncRequestsChan) == 0 }, 10*time.Second, time.Millisecond)
	window <- time.Now()

	_, _, _, err := first.WaitComplete()
	require.ErrorIs(t, err, ErrorDeleted)
	_, _, isUnchanged, err := retry.WaitComplete()
	require.NoError(t, err)
	require.False(t, isUnchanged, "the retry matched the cancelled write")
	got := &needle.Needle{Id: n.Id}
	_, err = v.readNeedle(got, nil, nil)
	require.NoError(t, err)
	require.Equal(t, n.Data, got.Data)
}

type syncedSizeBackend struct {
	backend.BackendStorageFile
	synced atomic.Int64
}

func (b *syncedSizeBackend) Sync() error {
	size, _, err := b.GetStat()
	if err != nil {
		return err
	}
	b.synced.Store(size)
	return b.BackendStorageFile.Sync()
}

// A durable write taken back during its fsync appends the tombstone after
// that fsync; it must be flushed before the .idx row that points at it.
func TestTakenBackTombstoneIsSynced(t *testing.T) {
	for _, isStopping := range []bool{false, true} {
		t.Run(fmt.Sprintf("inline=%v", isStopping), func(t *testing.T) {
			v := newWorkerTestVolume(t)
			synced := &syncedSizeBackend{BackendStorageFile: v.DataBackend}
			v.DataBackend = synced

			n := newRandomNeedle(1)
			v.dataFileSyncLock.Lock()
			written := make(chan error, 1)
			go func() {
				_, _, _, err := v.writeNeedle2(n, true, true, isStopping)
				written <- err
			}()
			waitForPendingWrites(t, v, n.Id, 1)
			v.CancelPendingWrites(n.Id, n.Cookie)
			v.dataFileSyncLock.Unlock()

			require.ErrorIs(t, <-written, ErrorDeleted)
			size, _, err := v.DataBackend.GetStat()
			require.NoError(t, err)
			require.Equal(t, size, synced.synced.Load(), "the tombstone was not flushed")
		})
	}
}
