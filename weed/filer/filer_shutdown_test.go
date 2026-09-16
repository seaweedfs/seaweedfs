package filer

import (
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
