package weed_server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestBatchDelete_DeniesNonWhitelistedPeer verifies that BatchDelete refuses
// callers that fall outside the configured admin whitelist, matching the
// behaviour of the other destructive volume-server RPCs.
func TestBatchDelete_DeniesNonWhitelistedPeer(t *testing.T) {
	vs := &VolumeServer{
		guard: security.NewGuard([]string{"10.0.0.1"}, "", 0, "", 0, "", ""),
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	resp, err := vs.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{"1,deadbeef"},
		SkipCookieCheck: true,
	})
	if err == nil {
		t.Fatalf("expected PermissionDenied, got nil error (resp=%+v)", resp)
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("expected codes.PermissionDenied, got %v (err=%v)", got, err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response even on auth failure")
	}
	if len(resp.Results) != 0 {
		t.Fatalf("expected no per-file results on auth failure, got %d", len(resp.Results))
	}
}

type syncCountingBackend struct {
	backend.BackendStorageFile
	syncs atomic.Int32
}

func (b *syncCountingBackend) Sync() error {
	b.syncs.Add(1)
	return b.BackendStorageFile.Sync()
}

func TestBatchDeleteFsyncsOnlyWhenAsked(t *testing.T) {
	for _, fsync := range []bool{false, true} {
		t.Run(fmt.Sprint(fsync), func(t *testing.T) {
			store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
				[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
				[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
			t.Cleanup(store.Close)
			require.NoError(t, store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0))
			n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: []byte("chunk")}
			n.Checksum = needle.NewCRC(n.Data)
			_, err := store.WriteVolumeNeedle(1, n, true, false)
			require.NoError(t, err)

			v := store.GetVolume(1)
			counting := &syncCountingBackend{BackendStorageFile: v.DataBackend}
			v.DataBackend = counting

			resp, err := (&VolumeServer{store: store}).BatchDelete(context.Background(), &volume_server_pb.BatchDeleteRequest{
				FileIds: []string{needle.NewFileIdFromNeedle(1, n).String()},
				Fsync:   fsync,
			})
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
			require.Equal(t, int32(http.StatusAccepted), resp.Results[0].Status, resp.Results[0].Error)
			if fsync {
				require.Positive(t, counting.syncs.Load(), "the tombstone was not synced")
			} else {
				require.Zero(t, counting.syncs.Load(), "a delete that did not ask for fsync paid for one")
			}
		})
	}
}
