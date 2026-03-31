package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindLockOwnerExpiredLockReturnsNotFound(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{Host: "filer1:8888"},
		filer: &filer.Filer{
			Dlm: lock_manager.NewDistributedLockManager("filer1:8888"),
		},
	}
	fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"}, 0)
	fs.filer.Dlm.InsertLock("expired-lock", time.Now().Add(-time.Second).UnixNano(), "token1", "owner1", 5, 2)

	resp, err := fs.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
		Name: "expired-lock",
	})
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}
