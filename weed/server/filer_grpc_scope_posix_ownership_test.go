package weed_server

import (
	"context"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const posixOwnershipKey = scopePosixLockKeyPrefix + "/locks/file"

type posixTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *posixTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// posixOwnershipHarness serves a real FilerServer PosixLock handler behind the
// scope interceptor, with the lock manager on a controllable clock.
type posixOwnershipHarness struct {
	t     *testing.T
	fs    *FilerServer
	clock *posixTestClock
	start time.Time
	addr  string
	keys  atomic.Value // [2]string: current and previous verification key
}

func newPosixOwnershipHarness(t *testing.T, leeway time.Duration) *posixOwnershipHarness {
	t.Helper()
	h := &posixOwnershipHarness{t: t, clock: &posixTestClock{now: time.Now()}}
	h.start = h.clock.Now()
	h.fs = newPosixTestServer(t)
	h.fs.posixLocks = posixlock.NewManagerWithClock(h.clock.Now)
	h.keys.Store([2]string{scopeTestKey, ""})
	sc := newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			keys := h.keys.Load().([2]string)
			return security.SigningKey(keys[0]), security.SigningKey(keys[1])
		},
		leeway: func() time.Duration { return leeway },
	}, h.fs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(sc.ServerOptions()...)
	filer_pb.RegisterSeaweedFilerServer(srv, h.fs)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	h.addr = lis.Addr().String()
	return h
}

// at moves the lock manager's clock to d after the harness start.
func (h *posixOwnershipHarness) at(d time.Duration) {
	h.clock.mu.Lock()
	defer h.clock.mu.Unlock()
	h.clock.now = h.start.Add(d)
}

func (h *posixOwnershipHarness) client(token string) filer_pb.SeaweedFilerClient {
	h.t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+h.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(scopeToken(token)))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = conn.Close() })
	return filer_pb.NewSeaweedFilerClient(conn)
}

func (h *posixOwnershipHarness) call(c filer_pb.SeaweedFilerClient, key string, op filer_pb.PosixLockOp, lk *filer_pb.PosixLockRange, held ...*filer_pb.PosixLockRange) (*filer_pb.PosixLockResponse, error) {
	return c.PosixLock(scopeCtx(h.t), &filer_pb.PosixLockRequest{Key: key, Op: op, Lock: lk, Locks: held})
}

func (h *posixOwnershipHarness) must(c filer_pb.SeaweedFilerClient, key string, op filer_pb.PosixLockOp, lk *filer_pb.PosixLockRange, held ...*filer_pb.PosixLockRange) *filer_pb.PosixLockResponse {
	h.t.Helper()
	resp, err := h.call(c, key, op, lk, held...)
	if err != nil {
		h.t.Fatalf("PosixLock %v: %v", op, err)
	}
	return resp
}

func (h *posixOwnershipHarness) held(key string) []posixlock.Range {
	return h.fs.posixLocks.Snapshot()[key]
}

// grantable reports whether a fresh scoped holder could take lk's range now.
func (h *posixOwnershipHarness) grantable(key string, flock bool) bool {
	h.t.Helper()
	probe := h.client(scopedToken("newcomer", time.Hour, []string{"/locks"}, nil))
	resp := h.must(probe, key, filer_pb.PosixLockOp_GET_LK, pbLock(0, 99, posixlock.Write, 50, 51, 9, flock))
	return !resp.GetHasConflict()
}

func expectPosixCode(t *testing.T, what string, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("%s: got %v, want %v", what, err, code)
	}
}

func TestScopedPosixLockSurvivesReplayOfItsDisclosedIdentity(t *testing.T) {
	intruders := []struct {
		name  string
		token func() string
	}{
		{name: "another scoped token", token: func() string { return scopedToken("intruder", time.Hour, []string{"/locks"}, nil) }},
		{name: "tokenless", token: func() string { return "" }},
		{name: "unscoped token", token: unscopedToken},
	}
	attacks := []struct {
		name string
		op   filer_pb.PosixLockOp
		lock func(known *filer_pb.PosixLockRange) *filer_pb.PosixLockRange
		held bool
	}{
		{name: "partial unlock", op: filer_pb.PosixLockOp_UNLOCK, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange {
			return pbLock(10, 20, posixlock.Unlock, k.Sid, k.Owner, 8, k.IsFlock)
		}},
		{name: "release posix owner", op: filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange { return k }},
		{name: "release flock owner", op: filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange { return k }},
		{name: "bare keepalive", op: filer_pb.PosixLockOp_KEEP_ALIVE, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange {
			return &filer_pb.PosixLockRange{Sid: k.Sid}
		}},
		{name: "reassertion", op: filer_pb.PosixLockOp_KEEP_ALIVE, held: true, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange {
			return &filer_pb.PosixLockRange{Sid: k.Sid}
		}},
		{name: "try lock", op: filer_pb.PosixLockOp_TRY_LOCK, lock: func(k *filer_pb.PosixLockRange) *filer_pb.PosixLockRange {
			return pbLock(0, 99, posixlock.Write, k.Sid, k.Owner, 8, k.IsFlock)
		}},
	}
	for _, flock := range []bool{false, true} {
		for _, intruder := range intruders {
			for _, attack := range attacks {
				name := map[bool]string{false: "fcntl", true: "flock"}[flock] + "/" + intruder.name + "/" + attack.name
				t.Run(name, func(t *testing.T) {
					h := newPosixOwnershipHarness(t, 0)
					holder := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
					if !h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, flock)).Granted {
						t.Fatal("holder's acquisition refused")
					}
					before := h.held(posixOwnershipKey)

					c := h.client(intruder.token())
					query := pbLock(0, 99, posixlock.Write, 3, 4, 8, flock)
					known := h.must(c, posixOwnershipKey, filer_pb.PosixLockOp_GET_LK, query).GetConflict()
					if known.GetSid() != 1 || known.GetOwner() != 2 || known.GetPid() != 7 {
						t.Fatalf("GET_LK should disclose the holder, got %+v", known)
					}

					h.at(10 * time.Second)
					var held []*filer_pb.PosixLockRange
					if attack.held {
						held = []*filer_pb.PosixLockRange{pbLock(0, 99, posixlock.Write, known.Sid, known.Owner, 8, flock)}
					}
					resp := h.must(c, posixOwnershipKey, attack.op, attack.lock(known), held...)
					if resp.GetGranted() {
						t.Fatal("the holder's identity must not grant another caller its lock")
					}
					if got := h.held(posixOwnershipKey); !reflect.DeepEqual(got, before) {
						t.Fatalf("holder's locks changed:\n got %+v\nwant %+v", got, before)
					}
					if !h.must(c, posixOwnershipKey, filer_pb.PosixLockOp_GET_LK, pbLock(0, 99, posixlock.Write, known.Sid, known.Owner, 8, flock)).GetHasConflict() {
						t.Fatal("the disclosed identity must still conflict for another caller")
					}

					// The intruder renewed nothing: the holder's lease still ends 15s
					// after its acquisition.
					h.at(15*time.Second - time.Nanosecond)
					if h.grantable(posixOwnershipKey, flock) {
						t.Fatal("holder's lease must last until its deadline")
					}
					h.at(15 * time.Second)
					if !h.grantable(posixOwnershipKey, flock) {
						t.Fatal("holder's lease was extended by another caller")
					}
				})
			}
		}
	}
}

func TestScopedPosixLockHolderUnlocksReleasesAndRenewsItsOwnLocks(t *testing.T) {
	for _, flock := range []bool{false, true} {
		t.Run(map[bool]string{false: "fcntl", true: "flock"}[flock], func(t *testing.T) {
			h := newPosixOwnershipHarness(t, 0)
			holder := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
			h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, flock))

			h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_UNLOCK, pbLock(10, 20, posixlock.Unlock, 1, 2, 7, flock))
			if got := h.held(posixOwnershipKey); len(got) != 2 || got[0].End != 9 || got[1].Start != 21 {
				t.Fatalf("partial unlock should split the holder's range, got %+v", got)
			}
			release := filer_pb.PosixLockOp_RELEASE_POSIX_OWNER
			if flock {
				release = filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER
			}
			h.must(holder, posixOwnershipKey, release, pbLock(0, 0, posixlock.Unlock, 1, 2, 7, flock))
			if got := h.held(posixOwnershipKey); len(got) != 0 {
				t.Fatalf("owner release should drop the holder's locks, got %+v", got)
			}

			h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, flock))
			h.at(10 * time.Second)
			h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})
			h.at(20 * time.Second)
			if h.grantable(posixOwnershipKey, flock) {
				t.Fatal("a bare keepalive should renew the holder's lease")
			}
			h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, pbLock(0, 99, posixlock.Write, 1, 2, 7, flock))
			h.at(35*time.Second - time.Nanosecond)
			if h.grantable(posixOwnershipKey, flock) {
				t.Fatal("a reassertion should renew the holder's lease")
			}
			h.at(35 * time.Second)
			if !h.grantable(posixOwnershipKey, flock) {
				t.Fatal("the lease should end 15s after the last renewal")
			}
		})
	}
}

func TestScopedPosixLockLeaseEndsAtTokenExpiryPlusLeeway(t *testing.T) {
	const leeway = 2 * time.Second
	h := newPosixOwnershipHarness(t, leeway)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	h.start = exp.Add(-5 * time.Second)
	h.at(0)
	holder := h.client(signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
		AllowedPrefixes:  []string{"/locks"},
		RegisteredClaims: jwt.RegisteredClaims{ID: "holder", ExpiresAt: jwt.NewNumericDate(exp)},
	}))
	h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	h.at(time.Second)
	h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})

	h.at(5*time.Second + leeway - time.Nanosecond)
	if h.grantable(posixOwnershipKey, false) {
		t.Fatal("the lease should last until the token's expiry plus leeway")
	}
	h.at(5*time.Second + leeway)
	if !h.grantable(posixOwnershipKey, false) {
		t.Fatal("a renewal must not extend the lease past the token's expiry plus leeway")
	}
}

func TestScopedPosixLockRenewalWithNarrowerGrantLeavesOtherKeysToExpire(t *testing.T) {
	keyA := scopePosixLockKeyPrefix + "/locks/a"
	keyB := scopePosixLockKeyPrefix + "/locks/b"
	h := newPosixOwnershipHarness(t, 0)
	wide := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	h.must(wide, keyA, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	h.must(wide, keyB, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))

	h.at(10 * time.Second)
	narrow := h.client(scopedToken("holder", time.Hour, []string{"/locks/a"}, nil))
	h.must(narrow, keyA, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	_, err := h.call(narrow, keyB, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})
	expectPosixCode(t, "bare keepalive outside the renewed grant", err, codes.PermissionDenied)
	_, err = h.call(narrow, keyB, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	expectPosixCode(t, "reassertion outside the renewed grant", err, codes.PermissionDenied)

	h.at(15 * time.Second)
	if !h.grantable(keyB, false) {
		t.Fatal("key b keeps only its previous deadline")
	}
	if h.grantable(keyA, false) {
		t.Fatal("key a was renewed under the narrower grant")
	}
	h.at(25 * time.Second)
	if !h.grantable(keyA, false) {
		t.Fatal("key a's lease ends 15s after its renewal")
	}
}

func TestScopedPosixLockTokenWithNewIdCannotAdoptPreviousLocks(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	old := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	h.must(old, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	before := h.held(posixOwnershipKey)

	h.at(10 * time.Second)
	renewed := h.client(scopedToken("holder-renewed", time.Hour, []string{"/locks"}, nil))
	h.must(renewed, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	h.must(renewed, posixOwnershipKey, filer_pb.PosixLockOp_UNLOCK, pbLock(0, 99, posixlock.Unlock, 1, 2, 7, false))
	if got := h.held(posixOwnershipKey); !reflect.DeepEqual(got, before) {
		t.Fatalf("a new token id adopted or released the previous locks: %+v", got)
	}

	h.at(15 * time.Second)
	if !h.must(renewed, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false)).Granted {
		t.Fatal("after expiry the renewed token should reacquire cleanly")
	}
}

// Ownership is per token id: tokens sharing an id, such as a remount reusing
// its credential, act for the same holder, even under a fresh sid.
func TestScopedPosixLockTokensSharingAnIdShareItsLocks(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	first := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	h.must(first, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))

	remount := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	if h.must(remount, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 5, 2, 8, false)).Granted {
		t.Fatal("a fresh sid is a different owner and must conflict")
	}
	h.must(remount, posixOwnershipKey, filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, pbLock(0, 0, posixlock.Unlock, 1, 2, 8, false))
	if got := h.held(posixOwnershipKey); len(got) != 0 {
		t.Fatalf("a token with the same id controls the holder's locks, got %+v", got)
	}
}

func TestScopedPosixLockGetLkReportsConflictWithoutTransferringOwnership(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	holder := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(10, 50, posixlock.Write, 1, 2, 7, false))

	if h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_GET_LK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false)).GetHasConflict() {
		t.Fatal("the holder must not conflict with itself")
	}
	reader := h.client(scopedToken("reader", time.Hour, nil, []string{"/locks"}))
	got := h.must(reader, posixOwnershipKey, filer_pb.PosixLockOp_GET_LK, pbLock(0, 99, posixlock.Read, 1, 2, 9, false)).GetConflict()
	want := pbLock(10, 50, posixlock.Write, 1, 2, 7, false)
	if got.GetStart() != want.Start || got.GetEnd() != want.End || got.GetType() != want.Type || got.GetSid() != want.Sid || got.GetOwner() != want.Owner || got.GetPid() != want.Pid {
		t.Fatalf("GET_LK with the holder's identity should report the holder, got %+v", got)
	}
	// A peer's cooling probe carries no principal, so it also sees the conflict.
	probe, err := h.fs.PosixLock(context.Background(), &filer_pb.PosixLockRequest{
		Key: posixOwnershipKey, IsMoved: true, CoolingProbe: true, Op: filer_pb.PosixLockOp_GET_LK,
		Lock: pbLock(0, 99, posixlock.Write, 1, 2, 7, false),
	})
	if err != nil || !probe.GetHasConflict() {
		t.Fatalf("a cooling probe must report the scoped lock: %+v, %v", probe, err)
	}
	h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_UNLOCK, pbLock(0, 99, posixlock.Unlock, 1, 2, 7, false))
	if got := h.held(posixOwnershipKey); len(got) != 0 {
		t.Fatalf("queries must not transfer ownership away from the holder: %+v", got)
	}
}

func TestScopedPosixLockRefusedRequestsLeaveStateUnchanged(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	holder := h.client(scopedToken("holder", time.Hour, []string{"/locks"}, nil))
	h.must(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))
	before := h.held(posixOwnershipKey)

	h.at(10 * time.Second)
	valid := pbLock(0, 99, posixlock.Write, 1, 2, 7, false)
	_, err := h.call(holder, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, valid, pbLock(200, 299, posixlock.Unlock, 1, 2, 7, false))
	expectPosixCode(t, "reassertion with an unlock range", err, codes.InvalidArgument)
	_, err = h.call(holder, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, valid, pbLock(300, 200, posixlock.Write, 1, 2, 7, false))
	expectPosixCode(t, "reassertion with an inverted range", err, codes.InvalidArgument)
	_, err = h.call(holder, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(200, 299, posixlock.Unlock, 1, 2, 7, false))
	expectPosixCode(t, "try lock with an unlock range", err, codes.InvalidArgument)

	expired := h.client(signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
		AllowedPrefixes:  []string{"/locks"},
		RegisteredClaims: jwt.RegisteredClaims{ID: "holder", ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))},
	}))
	_, err = h.call(expired, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1}, valid)
	expectPosixCode(t, "keepalive with an expired token", err, codes.Unauthenticated)
	readOnly := h.client(scopedToken("holder", time.Hour, nil, []string{"/locks"}))
	_, err = h.call(readOnly, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})
	expectPosixCode(t, "keepalive with a read-only grant", err, codes.PermissionDenied)

	if got := h.held(posixOwnershipKey); !reflect.DeepEqual(got, before) {
		t.Fatalf("a refused request changed the holder's locks: %+v", got)
	}
	h.at(15 * time.Second)
	if !h.grantable(posixOwnershipKey, false) {
		t.Fatal("a refused request must not renew the lease")
	}
}

func TestUnscopedPosixLocksKeepTheirBehaviour(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	tokenless := h.client("")
	unscoped := h.client(unscopedToken())
	scoped := h.client(scopedToken("scoped", time.Hour, []string{"/locks"}, nil))
	h.must(tokenless, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))

	// Unscoped callers share one namespace in which sid and owner identify the
	// lock, as before.
	if h.must(unscoped, posixOwnershipKey, filer_pb.PosixLockOp_GET_LK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false)).GetHasConflict() {
		t.Fatal("an unscoped caller naming the owner should not conflict with it")
	}
	h.must(unscoped, posixOwnershipKey, filer_pb.PosixLockOp_UNLOCK, pbLock(10, 20, posixlock.Unlock, 1, 2, 7, false))
	if got := h.held(posixOwnershipKey); len(got) != 2 {
		t.Fatalf("an unscoped caller should still split an unscoped lock, got %+v", got)
	}
	before := h.held(posixOwnershipKey)

	// A scoped token's verified id never matches an unscoped lock.
	h.must(scoped, posixOwnershipKey, filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, pbLock(0, 0, posixlock.Unlock, 1, 2, 7, false))
	if h.must(scoped, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false)).Granted {
		t.Fatal("a scoped token must not take over an unscoped lock")
	}
	if got := h.held(posixOwnershipKey); !reflect.DeepEqual(got, before) {
		t.Fatalf("a scoped token changed an unscoped lock: %+v", got)
	}

	// Unscoped sessions are leased only once they renew, as a whole.
	h.at(time.Hour)
	if reaped := h.fs.posixLocks.ReapExpired(posixLockSessionTTL); len(reaped) != 0 {
		t.Fatalf("a never-renewed unscoped session must not be reaped, got %v", reaped)
	}
	h.must(scoped, posixOwnershipKey, filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})
	h.at(time.Hour + posixLockSessionTTL)
	if reaped := h.fs.posixLocks.ReapExpired(posixLockSessionTTL); len(reaped) != 0 {
		t.Fatalf("a scoped keepalive must not lease an unscoped session, got %v", reaped)
	}
	h.must(tokenless, scopePosixLockKeyPrefix+"/elsewhere", filer_pb.PosixLockOp_KEEP_ALIVE, &filer_pb.PosixLockRange{Sid: 1})
	h.at(2*time.Hour + posixLockSessionTTL)
	if reaped := h.fs.posixLocks.ReapExpired(posixLockSessionTTL); len(reaped) != 1 || reaped[0] != 1 {
		t.Fatalf("a renewed unscoped session should be reaped after the ttl, got %v", reaped)
	}
	if got := h.held(posixOwnershipKey); len(got) != 0 {
		t.Fatalf("the reaped unscoped session's locks should be gone, got %+v", got)
	}
}

func TestScopedPosixLockOwnerSurvivesSigningKeyRotation(t *testing.T) {
	h := newPosixOwnershipHarness(t, 0)
	claims := func() security.SeaweedFilerClaims {
		return security.SeaweedFilerClaims{
			AllowedPrefixes:  []string{"/locks"},
			RegisteredClaims: jwt.RegisteredClaims{ID: "holder", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		}
	}
	before := h.client(signedScopeClaims(t, scopeTestKey, claims()))
	h.must(before, posixOwnershipKey, filer_pb.PosixLockOp_TRY_LOCK, pbLock(0, 99, posixlock.Write, 1, 2, 7, false))

	const rotated = "rotated-scope-test-key"
	h.keys.Store([2]string{rotated, scopeTestKey})
	h.must(before, posixOwnershipKey, filer_pb.PosixLockOp_UNLOCK, pbLock(10, 20, posixlock.Unlock, 1, 2, 7, false))
	after := h.client(signedScopeClaims(t, rotated, claims()))
	h.must(after, posixOwnershipKey, filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, pbLock(0, 0, posixlock.Unlock, 1, 2, 7, false))
	if got := h.held(posixOwnershipKey); len(got) != 0 {
		t.Fatalf("the token id, not the signing key, owns the lock; got %+v", got)
	}
}
