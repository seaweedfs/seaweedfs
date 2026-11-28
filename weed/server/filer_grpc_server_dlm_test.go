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
	fs.filer.Dlm.InsertLock("expired-lock", time.Now().Add(-time.Second).UnixNano(), "token1", "", "owner1", 5, 2)

	resp, err := fs.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
		Name: "expired-lock",
	})
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestForwardedLockAcquired(t *testing.T) {
	tests := []struct {
		name string
		resp *filer_pb.LockResponse
		want bool
	}{
		{name: "nil response"},
		{name: "empty token", resp: &filer_pb.LockResponse{}},
		{name: "application error", resp: &filer_pb.LockResponse{RenewToken: "token", Error: "lock already owned"}},
		{name: "acquired", resp: &filer_pb.LockResponse{RenewToken: "token"}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, forwardedLockAcquired(tt.resp))
		})
	}
}

func TestReplicateLockReleaseBeforeAcquire(t *testing.T) {
	fs := &FilerServer{filer: &filer.Filer{
		Dlm: lock_manager.NewDistributedLockManager("backup:8888"),
	}}
	expires := time.Now().Add(time.Minute).UnixNano()
	release := &filer_pb.ReplicateLockRequest{
		Name: "key", RenewToken: "released/renewed", ExpiredAtNs: expires,
		Generation: 20, Seq: 3, IsUnlock: true,
	}
	_, err := fs.ReplicateLock(context.Background(), release)
	require.NoError(t, err)
	_, err = fs.ReplicateLock(context.Background(), &filer_pb.ReplicateLockRequest{
		Name: "key", RenewToken: "released/original", ExpiredAtNs: expires,
		Generation: 20, Seq: 1,
	})
	require.NoError(t, err)
	_, found := fs.filer.Dlm.GetLock("key")
	require.False(t, found, "the RPC handler must preserve release identity and expiry")

	_, err = fs.ReplicateLock(context.Background(), &filer_pb.ReplicateLockRequest{
		Name: "key", RenewToken: "new-lease/secret", ExpiredAtNs: expires,
		Generation: 1, Seq: 1,
	})
	require.NoError(t, err)
	_, err = fs.ReplicateLock(context.Background(), release)
	require.NoError(t, err)
	lock, found := fs.filer.Dlm.GetLock("key")
	require.True(t, found)
	require.Equal(t, "new-lease/secret", lock.Token)
}
