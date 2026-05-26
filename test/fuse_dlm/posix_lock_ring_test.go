package fuse_dlm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// posixLockKey returns the routed-lock key a mount uses for a file at the mount
// root. Mounts run with -filer.path=/, so the file's filer path is "/"+name; the
// prefix must match mount.posixLockKeyForInode ("s3.fuse.lock:").
func posixLockKey(name string) string { return "s3.fuse.lock:/" + name }

// These tests exercise cross-mount POSIX advisory locks (flock), which the
// mounts route to the inode's owner filer because they run with -dlm
// (crossMountLocks() == lockClient != nil). They reuse the dlmTestCluster:
// master + volume + 2 filers (forming the lock ring) + 2 mounts on filer0.
//
// All flock opens are O_RDONLY on purpose: a write open (O_RDWR/O_WRONLY) would
// also take the -dlm whole-file write lock (held until close), which is a
// different mechanism — opening read-only isolates the POSIX advisory flock.

// requireForwardedLocks skips on platforms where the kernel does not forward
// advisory locks to the FUSE server. Only Linux forwards flock/fcntl (SETLK) to
// the filesystem; macFUSE handles flock in-kernel per mount, so cross-mount
// coordination can't be observed there even though the routed path is correct.
func requireForwardedLocks(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("advisory locks are only forwarded to the FUSE server on Linux (GOOS=%s)", runtime.GOOS)
	}
}

// openFlock opens path read-only and takes a flock of type how (e.g. LOCK_EX).
// The file must already exist. The returned file holds the lock until closed.
func openFlock(path string, how int) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// createFile creates an empty file; the brief -dlm write lock it takes is
// released on close, before any flock test runs.
func createFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err, "create %s", path)
	require.NoError(t, f.Close())
}

// waitVisible waits for path to appear (cross-mount metadata propagation).
func waitVisible(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, 15*time.Second, 200*time.Millisecond, "%s never became visible", path)
}

// tryExclusiveFlock attempts a non-blocking exclusive flock on path and
// classifies the outcome: acquired (and released again), blocked by another
// owner (EWOULDBLOCK/EAGAIN), or an unexpected error. It distinguishes a real
// "held by another" from incidental errors so the latter can't masquerade as a
// satisfied "must be blocked" assertion.
func tryExclusiveFlock(path string) (acquired, blocked bool, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return false, false, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return false, true, nil
		}
		return false, false, err
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return true, false, nil
}

// requireEventuallyBlocked asserts path becomes held by another owner. It polls
// because a routed lock RPC can hit a transient EIO (a cold or forwarded gRPC
// call under load) even while the lock is genuinely held; it still fails if the
// lock is never blocked — whether it can be acquired (a double-grant) or errors
// persistently — so an incidental error can't masquerade as "held".
func requireEventuallyBlocked(t *testing.T, path string, timeout time.Duration, msg string) {
	t.Helper()
	require.Eventuallyf(t, func() bool { return isBlocked(path) },
		timeout, 500*time.Millisecond, "%s", msg)
}

// isBlocked / isAcquirable are lenient predicates for polling, where transient
// errors during a migration are expected and simply mean "not yet".
func isBlocked(path string) bool { _, b, _ := tryExclusiveFlock(path); return b }
func isAcquirable(path string) bool {
	a, _, _ := tryExclusiveFlock(path)
	return a
}

// stopFiler stops filer idx and clears its command so cluster teardown does not
// try to stop it again.
func (c *dlmTestCluster) stopFiler(idx int) {
	stopCmd(c.filerCmds[idx])
	c.filerCmds[idx] = nil
}

// TestPosixLockCrossMount verifies a flock taken on one mount is seen by the
// other mount of the same cluster (the routed-to-owner-filer path end to end).
func TestPosixLockCrossMount(t *testing.T) {
	requireForwardedLocks(t)
	c := startDLMTestCluster(t)

	name := "posix-xmount.lock"
	path0 := filepath.Join(c.mountPoints[0], name)
	path1 := filepath.Join(c.mountPoints[1], name)

	createFile(t, path0)
	waitVisible(t, path1)

	held, err := openFlock(path0, syscall.LOCK_EX)
	require.NoError(t, err, "mount0 should acquire the exclusive flock")
	defer held.Close()

	requireEventuallyBlocked(t, path1, 15*time.Second, "mount1 must be blocked while mount0 holds the flock")

	require.NoError(t, syscall.Flock(int(held.Fd()), syscall.LOCK_UN))
	require.Eventually(t, func() bool { return isAcquirable(path1) },
		15*time.Second, 500*time.Millisecond,
		"mount1 should acquire the flock after mount0 releases it")
}

// TestPosixLockSurvivesFilerLoss verifies advisory locks held across mounts
// survive a filer leaving the ring: ownership of the affected keys migrates to
// the surviving filer and the holding mount re-asserts them there, so the locks
// stay honored. It locks many files so that — with 2 filers — several are owned
// by filer1 and thus actually migrate when filer1 stops.
func TestPosixLockSurvivesFilerLoss(t *testing.T) {
	requireForwardedLocks(t)
	c := startDLMTestCluster(t)

	const n = 12

	// Select files via the same ring the filers run so the locked set provably
	// spans both filers: filer1-owned keys must migrate when filer1 stops, while
	// filer0-owned keys must keep working. Choosing by ownership (instead of
	// hoping a sequential set happens to spread) keeps the migration path
	// exercised on every run, independent of the cluster's dynamic ports.
	ring := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	ring.SetServers([]pb.ServerAddress{
		pb.ServerAddress(c.filerAddress(0)),
		pb.ServerAddress(c.filerAddress(1)),
	})
	filer1 := pb.ServerAddress(c.filerAddress(1))
	var onFiler1, onFiler0 []string
	for i := 0; (len(onFiler1) < n/2 || len(onFiler0) < n/2) && i < 1000; i++ {
		name := fmt.Sprintf("posix-migrate-%d.lock", i)
		if ring.GetPrimary(posixLockKey(name)) == filer1 {
			onFiler1 = append(onFiler1, name)
		} else {
			onFiler0 = append(onFiler0, name)
		}
	}
	require.GreaterOrEqualf(t, len(onFiler1), n/2, "need %d filer1-owned keys to exercise migration", n/2)
	require.GreaterOrEqualf(t, len(onFiler0), n/2, "need %d filer0-owned keys", n/2)
	names := append(onFiler1[:n/2:n/2], onFiler0[:n/2]...)

	held := make([]*os.File, n)
	for i, name := range names {
		createFile(t, filepath.Join(c.mountPoints[0], name))
		waitVisible(t, filepath.Join(c.mountPoints[1], name))
		f, err := openFlock(filepath.Join(c.mountPoints[0], name), syscall.LOCK_EX)
		require.NoError(t, err, "mount0 should acquire flock %s", name)
		held[i] = f
		defer held[i].Close()
	}

	require.Eventually(t, func() bool {
		for _, name := range names {
			if !isBlocked(filepath.Join(c.mountPoints[1], name)) {
				return false
			}
		}
		return true
	}, 20*time.Second, time.Second,
		"every lock must be held on mount1 before the ring change")

	// Drop filer1 from the ring; keys it owned migrate to filer0.
	c.stopFiler(1)
	require.NoError(t, c.waitForFilerCount(1, 30*time.Second), "ring should drop to one filer")

	// Poll until every lock is honored again on the surviving filer: the ring
	// must propagate and the holding mount must re-assert its locks (keepalive is
	// 5s). We assert only the settled state — the transient migration window is
	// covered by unit tests.
	require.Eventually(t, func() bool {
		for _, name := range names {
			if !isBlocked(filepath.Join(c.mountPoints[1], name)) {
				return false
			}
		}
		return true
	}, 45*time.Second, time.Second,
		"all locks must survive filer1 leaving the ring (migrate to filer0)")

	// Releasing on mount0 frees them all: mount1 can then acquire each.
	for _, f := range held {
		require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_UN))
	}
	require.Eventually(t, func() bool {
		for _, name := range names {
			if !isAcquirable(filepath.Join(c.mountPoints[1], name)) {
				return false
			}
		}
		return true
	}, 20*time.Second, 500*time.Millisecond,
		"mount1 should acquire every lock after mount0 releases them post-migration")
}
