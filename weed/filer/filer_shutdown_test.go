package filer

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

type shutdownStore struct {
	VirtualFilerStore
	closed chan struct{}
}

func (s *shutdownStore) Shutdown() {
	close(s.closed)
}

func TestShutdownKeepsStoreOpenUntilMetadataIsFlushed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &shutdownStore{closed: make(chan struct{})}
		releaseFlush := make(chan struct{})
		flushed := false
		lb := log_buffer.NewLogBuffer("filer shutdown", time.Hour,
			func(_ *log_buffer.LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {
				<-releaseFlush
				select {
				case <-store.closed:
					t.Error("metadata store closed before the pending log write")
				default:
					flushed = true
				}
			}, nil, nil)
		f := &Filer{Store: store, LocalMetaLogBuffer: lb, deletionQuit: make(chan struct{})}
		if err := lb.AddDataToBuffer(nil, []byte("last metadata event"), 0); err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			f.Shutdown()
			close(done)
		}()
		synctest.Wait()
		select {
		case <-store.closed:
			t.Error("metadata store closed while the log write was blocked")
		default:
		}

		close(releaseFlush)
		<-done
		lb.WaitForShutdown()
		if !flushed {
			t.Error("shutdown returned without persisting the pending metadata")
		}
		select {
		case <-store.closed:
		default:
			t.Error("metadata store was not closed after the log write finished")
		}
	})
}

func TestShutdownBoundsBlockedMetadataFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &shutdownStore{closed: make(chan struct{})}
		f := &Filer{Store: store, deletionQuit: make(chan struct{})}
		f.flushCtx, f.flushCancel = context.WithCancel(context.Background())

		var flushStarted atomic.Bool
		lb := log_buffer.NewLogBuffer("blocked flush", time.Hour,
			func(lb *log_buffer.LogBuffer, _, _ time.Time, buf []byte, _, _ int64) {
				flushStarted.Store(true)
				// A dead cluster makes append retries hang here; the shutdown
				// deadline cancels the shared flush context to unblock it.
				<-f.flushCtx.Done()
				lb.NoteFlushDropped(len(buf))
			}, nil, nil)
		f.LocalMetaLogBuffer = lb
		if err := lb.AddDataToBuffer(nil, []byte("last metadata event"), 0); err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			f.Shutdown()
			close(done)
		}()

		<-done
		if !flushStarted.Load() {
			t.Error("shutdown finished without running the pending flush")
		}
		if ts := lb.GetLastFlushTsNs(); ts != 0 {
			t.Errorf("dropped flush advanced the flushed watermark to %d", ts)
		}
		select {
		case <-store.closed:
		default:
			t.Error("metadata store was not closed after the bounded wait")
		}
	})
}
