package weed_server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func newAdminLockMaster() *MasterServer {
	ms := newLeaderMaster()
	ms.adminLocks = NewAdminLocks()
	return ms
}

func leaseAdminLock(ms *MasterServer, client string, prev *master_pb.LeaseAdminTokenResponse) (*master_pb.LeaseAdminTokenResponse, error) {
	req := &master_pb.LeaseAdminTokenRequest{LockName: "admin", ClientName: client}
	if prev != nil {
		req.PreviousToken, req.PreviousLockTime = prev.Token, prev.LockTsNs
	}
	return ms.LeaseAdminToken(context.Background(), req)
}

func releaseAdminLock(ms *MasterServer, lease *master_pb.LeaseAdminTokenResponse) error {
	_, err := ms.ReleaseAdminToken(context.Background(), &master_pb.ReleaseAdminTokenRequest{
		LockName: "admin", PreviousToken: lease.Token, PreviousLockTime: lease.LockTsNs,
	})
	return err
}

// runTogether starts fns at the same moment and waits for all of them. A panic
// in any of them is returned as an error instead of crashing the test binary,
// the same way it would take down the master (gRPC has no recovery there).
func runTogether(fns ...func()) (panicErr error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	start := make(chan struct{})
	for _, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					panicErr = fmt.Errorf("panic: %v", r)
					mu.Unlock()
				}
			}()
			<-start
			fn()
		}()
	}
	close(start)
	wg.Wait()
	return panicErr
}

func TestAdminLockLeaseRenewRelease(t *testing.T) {
	ms := newAdminLockMaster()

	a, err := leaseAdminLock(ms, "shellA", nil)
	require.NoError(t, err)

	_, err = leaseAdminLock(ms, "shellB", nil)
	require.Error(t, err, "second client must be refused while the lock is held")

	renewed, err := leaseAdminLock(ms, "shellA", a)
	require.NoError(t, err)

	_, err = leaseAdminLock(ms, "shellB", a)
	require.Error(t, err, "a token replaced by a renewal must not renew again")

	require.NoError(t, releaseAdminLock(ms, a))
	_, err = leaseAdminLock(ms, "shellB", nil)
	require.Error(t, err, "releasing with a stale token must keep the current holder")

	require.NoError(t, releaseAdminLock(ms, renewed))
	require.NoError(t, releaseAdminLock(ms, renewed), "releasing twice is a no-op")

	_, err = leaseAdminLock(ms, "shellB", nil)
	require.NoError(t, err, "lock is free after release")
}

func TestAdminLockReleaseUnknownLock(t *testing.T) {
	ms := newAdminLockMaster()
	require.NoError(t, releaseAdminLock(ms, &master_pb.LeaseAdminTokenResponse{Token: 1, LockTsNs: 1}))
}

// Two releases of the same token (a shell retrying on timeout) both passed the
// token check before either deleted the lock; the second one then dereferenced
// the missing entry and panicked.
func TestAdminLockConcurrentReleaseOfSameToken(t *testing.T) {
	ms := newAdminLockMaster()
	for i := 0; i < 10000; i++ {
		lease, err := leaseAdminLock(ms, "shell", nil)
		require.NoError(t, err)
		release := func() { _ = releaseAdminLock(ms, lease) }
		require.NoError(t, runTogether(release, release), "iteration %d", i)
		_, _, locked := ms.adminLocks.isLocked("admin")
		require.False(t, locked, "iteration %d", i)
	}
}

// Two clients leasing a free lock at the same time must not both get it.
func TestAdminLockConcurrentLeaseGrantsOnce(t *testing.T) {
	ms := newAdminLockMaster()
	for i := 0; i < 2000; i++ {
		ms.adminLocks = NewAdminLocks()
		var mu sync.Mutex
		granted := 0
		lease := func(client string) func() {
			return func() {
				if _, err := leaseAdminLock(ms, client, nil); err == nil {
					mu.Lock()
					granted++
					mu.Unlock()
				}
			}
		}
		require.NoError(t, runTogether(lease("shellA"), lease("shellB"), lease("shellC")))
		require.Equal(t, 1, granted, "iteration %d: exclusive lock granted %d times", i, granted)
	}
}

// Concurrent renewals with the same token must not all succeed: only one new
// token can be the valid one.
func TestAdminLockConcurrentRenewalOfSameToken(t *testing.T) {
	ms := newAdminLockMaster()
	for i := 0; i < 2000; i++ {
		ms.adminLocks = NewAdminLocks()
		held, err := leaseAdminLock(ms, "shell", nil)
		require.NoError(t, err)
		var mu sync.Mutex
		renewed := 0
		renew := func() {
			if _, err := leaseAdminLock(ms, "shell", held); err == nil {
				mu.Lock()
				renewed++
				mu.Unlock()
			}
		}
		require.NoError(t, runTogether(renew, renew, renew, renew))
		require.Equal(t, 1, renewed, "iteration %d: one token renewed %d times", i, renewed)
	}
}
