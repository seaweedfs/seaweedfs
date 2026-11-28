package weed_server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
			dial := grpc.WithTransportCredentials(insecure.NewCredentials())
			serve := func(delay bool) (*FilerServer, pb.ServerAddress) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				addr := pb.NewServerAddressWithGrpcPort(listener.Addr().String(), listener.Addr().(*net.TCPAddr).Port)
				fs := &FilerServer{
					filer:  &filer.Filer{Dlm: lock_manager.NewDistributedLockManager(addr)},
					option: &FilerOption{Host: addr}, grpcDialOption: dial,
				}
				server := grpc.NewServer(grpc.UnaryInterceptor(func(c context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					if transfer, ok := req.(*filer_pb.TransferLocksRequest); ok && delay {
						captured <- transfer
						select {
						case <-proceed:
						case <-c.Done():
							return nil, c.Err()
						}
					}
					return handler(c, req)
				}))
				filer_pb.RegisterSeaweedFilerServer(server, fs)
				go server.Serve(listener)
				t.Cleanup(func() { server.Stop(); pb.InvalidateGrpcConnection(addr.ToGrpcAddress()) })
				fs.SetupDlmReplication()
				return fs, addr
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
			// An acknowledged release cannot precede delivery of the handoff.
			var result unlockResult
			early := false
			select {
			case result = <-released:
				early = true
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			select {
			case <-transferred:
			case <-ctx.Done():
				t.Fatal("handoff did not complete")
			}
			if !early {
				select {
				case result = <-released:
				case <-ctx.Done():
					t.Fatal("release did not complete after handoff")
				}
			}
			require.NoError(t, result.err)
			require.NotNil(t, result.response)
			require.Empty(t, result.response.Error)
			_, found := target.filer.Dlm.GetLock(key)
			require.False(t, found, "completed release was undone by the delayed transfer")
			require.False(t, early, "release must wait for handoff and then reach the new primary")

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
