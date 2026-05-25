package weed_server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newPosixTestServer() *FilerServer {
	fs, _ := newTxnTestServer(nil)
	fs.posixLocks = posixlock.NewManager()
	return fs
}

func pbLock(start, end uint64, typ uint32, sid, owner uint64, pid uint32, flock bool) *filer_pb.PosixLockRange {
	return &filer_pb.PosixLockRange{Start: start, End: end, Type: typ, Sid: sid, Owner: owner, Pid: pid, IsFlock: flock}
}

func posixOp(t *testing.T, fs *FilerServer, op filer_pb.PosixLockOp, lk *filer_pb.PosixLockRange) *filer_pb.PosixLockResponse {
	t.Helper()
	resp, err := fs.PosixLock(context.Background(), &filer_pb.PosixLockRequest{Key: "s3.fuse.lock:/x", Op: op, Lock: lk})
	if err != nil {
		t.Fatalf("PosixLock op=%v: %v", op, err)
	}
	return resp
}

func TestPosixLockGrantAndConflict(t *testing.T) {
	fs := newPosixTestServer()

	if r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 1, 7, false)); !r.Granted {
		t.Fatal("first lock should be granted")
	}
	// Conflicting writer from another session: rejected, conflict reported.
	r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(50, 149, posixlock.Write, 2, 1, 8, false))
	if r.Granted {
		t.Fatal("overlapping lock from another session should conflict")
	}
	if !r.HasConflict || r.Conflict.GetPid() != 7 || r.Conflict.GetStart() != 0 || r.Conflict.GetEnd() != 99 {
		t.Fatalf("conflict not reported correctly: %+v", r.Conflict)
	}
}

func TestPosixLockUnlockThenReacquire(t *testing.T) {
	fs := newPosixTestServer()
	posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 1, 7, false))
	posixOp(t, fs, filer_pb.PosixLockOp_UNLOCK, pbLock(0, 99, posixlock.Unlock, 1, 1, 7, false))
	if r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 2, 1, 8, false)); !r.Granted {
		t.Fatal("lock should be grantable after the holder unlocked")
	}
}

func TestPosixLockGetLk(t *testing.T) {
	fs := newPosixTestServer()
	posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(10, 50, posixlock.Write, 1, 1, 7, false))

	r := posixOp(t, fs, filer_pb.PosixLockOp_GET_LK, pbLock(30, 70, posixlock.Read, 2, 1, 8, false))
	if !r.HasConflict || r.Conflict.GetPid() != 7 {
		t.Fatalf("GET_LK should report the holder, got %+v", r)
	}
	// No conflict for the same owner.
	r = posixOp(t, fs, filer_pb.PosixLockOp_GET_LK, pbLock(30, 70, posixlock.Read, 1, 1, 7, false))
	if r.HasConflict {
		t.Fatal("an owner should not conflict with itself")
	}
}

func TestPosixLockReleasePosixOwnerKeepsFlock(t *testing.T) {
	fs := newPosixTestServer()
	posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 1, 7, false))
	posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 1<<63, posixlock.Write, 1, 1, 7, true))

	posixOp(t, fs, filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, pbLock(0, 0, posixlock.Unlock, 1, 1, 7, false))

	// fcntl gone, flock remains.
	if r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 2, 1, 8, false)); !r.Granted {
		t.Fatal("fcntl lock should be gone after RELEASE_POSIX_OWNER")
	}
	if r := posixOp(t, fs, filer_pb.PosixLockOp_GET_LK, pbLock(0, 10, posixlock.Write, 3, 3, 9, true)); !r.HasConflict {
		t.Fatal("flock lock should survive RELEASE_POSIX_OWNER")
	}
}

func TestPosixLockKeepAlive(t *testing.T) {
	fs := newPosixTestServer()
	resp, err := fs.PosixLock(context.Background(), &filer_pb.PosixLockRequest{
		Key: "s3.fuse.lock:/x", Op: filer_pb.PosixLockOp_KEEP_ALIVE,
		Lock: pbLock(0, 0, posixlock.Unlock, 7, 0, 0, false),
	})
	if err != nil || resp == nil {
		t.Fatalf("keep_alive should succeed: err=%v", err)
	}
	// A renewed session that goes stale is reaped; a never-renewed one is not.
	fs.posixLocks.TryLock("s3.fuse.lock:/x", posixlock.Range{Start: 0, End: 9, Type: posixlock.Write, Sid: 7, Owner: 1})
	if reaped := fs.posixLocks.ReapExpired(0); len(reaped) != 1 || reaped[0] != 7 {
		t.Fatalf("renewed session 7 should be reapable at ttl=0, got %v", reaped)
	}
}

// A request whose key is owned by another filer is forwarded to it; the owner
// applies it and the sender does not. The owner's ring points back at the bogus
// sender, so without is_moved on the forwarded hop it would re-forward and fail.
func TestPosixLockForwardsToOwner(t *testing.T) {
	const key = "s3.fuse.lock:/x"
	owner := newPosixTestServer()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	ownerAddr := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), port)
	sender := pb.ServerAddress("127.0.0.1:1")

	withRing(owner, ownerAddr, sender)
	owner.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, owner)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	self := newPosixTestServer()
	withRing(self, sender, ownerAddr)
	self.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := self.PosixLock(ctx, &filer_pb.PosixLockRequest{
		Key: key, Op: filer_pb.PosixLockOp_TRY_LOCK,
		Lock: pbLock(0, 99, posixlock.Write, 1, 1, 7, false),
	})
	if err != nil || !resp.GetGranted() {
		t.Fatalf("forwarded TRY_LOCK: err=%v granted=%v", err, resp.GetGranted())
	}

	// The lock landed on the owner: a conflicting acquire there is rejected.
	if _, granted := owner.posixLocks.TryLock(key, posixlock.Range{Start: 50, End: 149, Type: posixlock.Write, Sid: 2, Owner: 1}); granted {
		t.Fatal("owner should hold the forwarded lock")
	}
	// The sender did not apply locally.
	if _, granted := self.posixLocks.TryLock(key, posixlock.Range{Start: 0, End: 99, Type: posixlock.Write, Sid: 9, Owner: 9}); !granted {
		t.Fatal("sender must forward, not apply locally")
	}
}

func TestPosixLockWarmupDefersGrants(t *testing.T) {
	fs := newPosixTestServer()
	fs.posixLockReadyAt.Store(time.Now().UnixNano()) // warming up

	// A would-be grant is deferred (not granted, no conflict) so the client retries.
	r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 1, 7, false))
	if r.Granted {
		t.Fatal("grant should be deferred during warm-up")
	}
	if r.HasConflict {
		t.Fatalf("deferred grant should report no conflict, got %+v", r.Conflict)
	}

	// A lock the owner already knows about is still reported as a conflict.
	fs.posixLocks.TryLock("s3.fuse.lock:/x", posixlock.Range{Start: 0, End: 99, Type: posixlock.Write, Sid: 9, Owner: 1, Pid: 5})
	if r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(50, 60, posixlock.Write, 1, 1, 7, false)); r.Granted || !r.HasConflict {
		t.Fatalf("known conflict should be reported during warm-up: %+v", r)
	}

	// After warm-up, grants resume.
	posixOp(t, fs, filer_pb.PosixLockOp_UNLOCK, pbLock(0, 99, posixlock.Unlock, 9, 1, 5, false))
	fs.posixLockReadyAt.Store(time.Now().Add(-2 * posixLockWarmup).UnixNano())
	if r := posixOp(t, fs, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 1, 7, false)); !r.Granted {
		t.Fatal("grant should succeed after warm-up")
	}
}
