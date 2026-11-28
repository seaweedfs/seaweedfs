package weed_server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newDLMHandoffServer(t *testing.T, interceptor grpc.UnaryServerInterceptor) (*FilerServer, pb.ServerAddress) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pb.NewServerAddressWithGrpcPort(listener.Addr().String(), listener.Addr().(*net.TCPAddr).Port)
	fs := &FilerServer{
		filer:  &filer.Filer{Dlm: lock_manager.NewDistributedLockManager(addr)},
		option: &FilerOption{Host: addr}, grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	var options []grpc.ServerOption
	if interceptor != nil {
		options = append(options, grpc.UnaryInterceptor(interceptor))
	}
	server := grpc.NewServer(options...)
	filer_pb.RegisterSeaweedFilerServer(server, fs)
	go server.Serve(listener)
	fs.SetupDlmReplication()
	t.Cleanup(func() { fs.dlmHandoffCancel(); server.Stop(); pb.InvalidateGrpcConnection(addr.ToGrpcAddress()) })
	return fs, addr
}

// Delay a real transfer RPC after its source snapshot is taken, then release
// through the old primary while the request is in flight. Exercise both the
// demotion-to-backup and removal-after-transfer topology branches.
func TestDLM_ReleaseDuringHandoff(t *testing.T) {
	for _, keepBackup := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep_backup=%v", keepBackup), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			captured := make(chan *filer_pb.TransferLocksRequest, 1)
			proceed := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			serve := func(delay bool) (*FilerServer, pb.ServerAddress) {
				return newDLMHandoffServer(t, func(c context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					if transfer, ok := req.(*filer_pb.TransferLocksRequest); ok && delay {
						captured <- transfer
						select {
						case <-proceed:
						case <-c.Done():
							return nil, c.Err()
						}
					}
					return handler(c, req)
				})
			}
			source, a := serve(false)
			target, b := serve(true)
			members := []pb.ServerAddress{a, b}
			if !keepBackup {
				_, c := serve(false)
				members = append(members, c)
			}
			source.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{a}, 1)
			target.filer.Dlm.LockRing.SetSnapshot(members, 2)
			key := ""
			for i := 0; i < 10000; i++ {
				candidate := fmt.Sprintf("handoff-%d", i)
				primary, backup := target.filer.Dlm.LockRing.GetPrimaryAndBackup(candidate)
				if primary == b && (backup == a) == keepBackup {
					key = candidate
					break
				}
			}
			require.NotEmpty(t, key)
			acquired, err := source.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, SecondsToLock: 60, Owner: "alice"})
			require.NoError(t, err)
			require.Empty(t, acquired.Error)
			require.NotEmpty(t, acquired.RenewToken)

			source.filer.Dlm.LockRing.SetTakeSnapshotCallback(source.OnDlmChangeSnapshot)
			transferred := make(chan struct{})
			go func() { source.filer.Dlm.LockRing.SetSnapshot(members, 2); close(transferred) }()
			var request *filer_pb.TransferLocksRequest
			select {
			case request = <-captured:
			case <-ctx.Done():
				t.Fatal("handoff did not start")
			}
			type unlockResult struct {
				response *filer_pb.UnlockResponse
				err      error
			}
			released := make(chan unlockResult, 1)
			go func() {
				response, err := source.DistributedUnlock(ctx, &filer_pb.UnlockRequest{Name: key, RenewToken: acquired.RenewToken})
				released <- unlockResult{response, err}
			}()
			// Same-key admission ordering is established with a contention hook
			// in TestDLM_UnlockWaitsForHandoffAdmission. Here test the real RPCs.
			unblock()
			select {
			case <-transferred:
			case <-ctx.Done():
				t.Fatal("handoff did not complete")
			}
			var result unlockResult
			select {
			case result = <-released:
			case <-ctx.Done():
				t.Fatal("release did not complete after handoff")
			}
			require.NoError(t, result.err)
			require.NotNil(t, result.response)
			require.Empty(t, result.response.Error)
			_, found := target.filer.Dlm.GetLock(key)
			require.False(t, found, "completed release was undone by the delayed transfer")

			// Replay the captured transfer after the release has completed.
			_, err = target.TransferLocks(ctx, request)
			require.NoError(t, err)
			_, found = target.filer.Dlm.GetLock(key)
			require.False(t, found)
			next, err := target.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, SecondsToLock: 60, Owner: "bob"})
			require.NoError(t, err)
			require.Empty(t, next.Error)
			require.NotEmpty(t, next.RenewToken)
		})
	}
}

func TestDLM_ReleaseBeforeHandoffReachesPromotedBackup(t *testing.T) {
	source, a := newDLMHandoffServer(t, nil)
	target, b := newDLMHandoffServer(t, nil)
	backup, c := newDLMHandoffServer(t, nil)
	initial, next := []pb.ServerAddress{a, b}, []pb.ServerAddress{b, c}
	source.filer.Dlm.LockRing.SetSnapshot(initial, 1)
	target.filer.Dlm.LockRing.SetSnapshot(initial, 1)
	nextRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	nextRing.SetServers(next)
	key := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("pre-handoff-%d", i)
		if source.filer.Dlm.LockRing.GetPrimary(candidate) == a && nextRing.GetPrimary(candidate) == b {
			key = candidate
			break
		}
	}
	require.NotEmpty(t, key)
	ctx := context.Background()
	lease, err := source.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, Owner: "alice", SecondsToLock: 60})
	require.NoError(t, err)
	require.Empty(t, lease.Error)
	require.Eventually(t, func() bool { _, ok := target.filer.Dlm.GetLock(key); return ok }, 5*time.Second, time.Millisecond)
	backup.filer.Dlm.LockRing.SetSnapshot(next, 2)
	target.filer.Dlm.LockRing.SetTakeSnapshotCallback(target.OnDlmChangeSnapshot)
	target.filer.Dlm.LockRing.SetSnapshot(next, 2)
	// Move routing before the source's topology callback starts: unlock wins
	// admission while B already has the old lease and C is its new backup.
	source.filer.Dlm.LockRing.SetSnapshot(next, 2)
	response, err := source.DistributedUnlock(ctx, &filer_pb.UnlockRequest{Name: key, RenewToken: lease.RenewToken})
	require.NoError(t, err)
	require.Empty(t, response.Error)
	source.OnDlmChangeSnapshot(next)
	_, found := target.filer.Dlm.GetLock(key)
	require.False(t, found)
	acquired, err := target.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, Owner: "bob", SecondsToLock: 60})
	require.NoError(t, err)
	require.Empty(t, acquired.Error)
	require.NotEmpty(t, acquired.RenewToken)
}

func TestDLM_HandoffRetriesWithoutMembershipChange(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(fmt.Sprintf("installed_before_error=%v", installed), func(t *testing.T) {
			source, a := newDLMHandoffServer(t, nil)
			var attempts atomic.Int32
			target, b := newDLMHandoffServer(t, func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if _, transfer := req.(*filer_pb.TransferLocksRequest); transfer && attempts.Add(1) == 1 {
					if installed {
						if _, err := handler(ctx, req); err != nil {
							return nil, err
						}
					}
					return nil, status.Error(codes.Unavailable, "injected handoff failure")
				}
				return handler(ctx, req)
			})
			next := []pb.ServerAddress{a, b}
			source.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{a}, 1)
			target.filer.Dlm.LockRing.SetSnapshot(next, 2)
			key := ""
			for i := 0; i < 10000; i++ {
				candidate := fmt.Sprintf("retry-%d", i)
				if target.filer.Dlm.LockRing.GetPrimary(candidate) == b {
					key = candidate
					break
				}
			}
			require.NotEmpty(t, key)
			ctx := context.Background()
			lease, err := source.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, Owner: "alice", SecondsToLock: 60})
			require.NoError(t, err)
			require.Empty(t, lease.Error)
			source.filer.Dlm.LockRing.SetTakeSnapshotCallback(source.OnDlmChangeSnapshot)
			source.filer.Dlm.LockRing.SetSnapshot(next, 2)
			require.Eventually(t, func() bool {
				lock, ok := target.filer.Dlm.GetLock(key)
				return attempts.Load() >= 2 && ok && lock.Token == lease.RenewToken
			}, 5*time.Second, time.Millisecond)
			require.EqualValues(t, 2, source.filer.Dlm.LockRing.Version())
			renewed, err := source.DistributedLock(ctx, &filer_pb.LockRequest{Name: key, Owner: "alice", RenewToken: lease.RenewToken, SecondsToLock: 60})
			require.NoError(t, err)
			require.Empty(t, renewed.Error)
			require.Equal(t, string(b), renewed.LockHostMovedTo, "renewal authority stays at the new primary")
			require.NotEqual(t, lease.RenewToken, renewed.RenewToken)
		})
	}
}

func TestDLM_HandoffRetryStops(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%v", shutdown), func(t *testing.T) {
			fs, addr := newDLMHandoffServer(t, nil)
			fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{addr}, 1)
			expiry := time.Now().Add(time.Second).UnixNano()
			if shutdown {
				expiry = time.Now().Add(time.Minute).UnixNano()
			}
			_, _, _, _, err := fs.filer.Dlm.LockWithTimeout("key", expiry, "", "owner")
			require.NoError(t, err)
			fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"target:8888"}, 2)
			fs.filer.Dlm.TransferFn = func(pb.ServerAddress, *lock_manager.Lock) error {
				return status.Error(codes.Unavailable, "target unavailable")
			}
			require.Error(t, fs.filer.Dlm.TransferLockToOwner("key"))
			fs.retryDlmHandoff("key")
			if shutdown {
				fs.dlmHandoffCancel()
			}
			require.Eventually(t, func() bool {
				fs.dlmHandoffMu.Lock()
				defer fs.dlmHandoffMu.Unlock()
				return len(fs.dlmHandoffRetries) == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}
