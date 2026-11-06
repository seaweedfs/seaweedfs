package weed_server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// heldSyncBackend parks the first .dat fsync until released: the window where
// a durable write is appended but not indexed yet.
type heldSyncBackend struct {
	backend.BackendStorageFile
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *heldSyncBackend) Sync() error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return b.BackendStorageFile.Sync()
}

// A replica delete that arrives while the write it undoes is in its fsync
// finds nothing, yet the write must not land live afterwards. A plain delete
// that finds nothing is not sent on to the replicas, so it leaves the write.
func TestDeleteHandlerCancelsAWriteInItsFsyncWindow(t *testing.T) {
	for _, tc := range []struct {
		query string
		live  bool
	}{
		{"?type=replicate", false},
		{"", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
				[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
				[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
			t.Cleanup(store.Close)
			require.NoError(t, store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0))
			v := store.GetVolume(1)
			held := &heldSyncBackend{BackendStorageFile: v.DataBackend, entered: make(chan struct{}), release: make(chan struct{})}
			v.DataBackend = held

			n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: []byte("late write")}
			n.Checksum = needle.NewCRC(n.Data)
			fid := needle.NewFileIdFromNeedle(1, n).String()
			written := make(chan error, 1)
			go func() {
				_, err := store.WriteVolumeNeedle(1, n, true, true)
				written <- err
			}()
			<-held.entered

			resp := httptest.NewRecorder()
			vs := &VolumeServer{store: store, guard: security.NewGuard(nil, "", 0, "", 0)}
			vs.DeleteHandler(resp, httptest.NewRequest(http.MethodDelete, "/"+fid+tc.query, nil))
			require.Equal(t, http.StatusNotFound, resp.Code)
			close(held.release)

			err := <-written
			_, readErr := store.ReadVolumeNeedle(1, &needle.Needle{Id: n.Id}, nil, nil)
			if tc.live {
				require.NoError(t, err)
				require.NoError(t, readErr)
			} else {
				require.ErrorIs(t, err, storage.ErrorDeleted)
				require.ErrorIs(t, readErr, storage.ErrorDeleted)
			}
		})
	}
}
