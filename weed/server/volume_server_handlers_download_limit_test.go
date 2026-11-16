package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

func TestCheckDownloadLimitLocalReadModeWaitsInsteadOfProxying(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	require.NoError(t, store.AddVolume(1, "", storage.NeedleMapInMemory, "001", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	vs := &VolumeServer{
		store:                         store,
		ReadMode:                      "local",
		concurrentDownloadLimit:       1,
		inFlightDownloadDataSize:      2,
		inflightDownloadDataTimeout:   50 * time.Millisecond,
		inFlightDownloadDataLimitCond: sync.NewCond(new(sync.Mutex)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/1,0123456789abcdef", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	require.False(t, vs.checkDownloadLimit(w, r))
	require.Equal(t, http.StatusTooManyRequests, w.Code)
}

func TestWaitForDownloadSlotSeesReleaseBeforeWaiting(t *testing.T) {
	vs := &VolumeServer{
		concurrentDownloadLimit:       1,
		inFlightDownloadDataSize:      2,
		inflightDownloadDataTimeout:   5 * time.Second,
		inFlightDownloadDataLimitCond: sync.NewCond(new(sync.Mutex)),
	}
	r := httptest.NewRequest(http.MethodGet, "/1,0123456789abcdef", nil)
	w := httptest.NewRecorder()

	vs.inFlightDownloadDataLimitCond.L.Lock()
	got := make(chan bool, 1)
	go func() { got <- vs.waitForDownloadSlot(w, r) }()
	time.Sleep(100 * time.Millisecond)
	atomic.AddInt64(&vs.inFlightDownloadDataSize, -2)
	vs.inFlightDownloadDataLimitCond.Broadcast()
	vs.inFlightDownloadDataLimitCond.L.Unlock()

	select {
	case ok := <-got:
		require.True(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("waiter missed the released download slot")
	}
}
