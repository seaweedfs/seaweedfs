package fuse_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fcntlSetLk is a helper that performs a non-blocking fcntl F_SETLK on a file.
func fcntlSetLk(f *os.File, typ int16, start, len int64) error {
	flk := syscall.Flock_t{
		Type:   typ,
		Whence: 0, // SEEK_SET
		Start:  start,
		Len:    len, // 0 means to EOF
	}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &flk)
}

// fcntlSetLkw is a helper that performs a blocking fcntl F_SETLKW on a file.
func fcntlSetLkw(f *os.File, typ int16, start, len int64) error {
	flk := syscall.Flock_t{
		Type:   typ,
		Whence: 0,
		Start:  start,
		Len:    len,
	}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &flk)
}

// fcntlGetLk queries for a conflicting lock via F_GETLK.
// If no conflict, the returned Flock_t.Type will be F_UNLCK.
func fcntlGetLk(f *os.File, typ int16, start, len int64) (syscall.Flock_t, error) {
	flk := syscall.Flock_t{
		Type:   typ,
		Whence: 0,
		Start:  start,
		Len:    len,
	}
	err := syscall.FcntlFlock(f.Fd(), syscall.F_GETLK, &flk)
	return flk, err
}

// TestPosixFileLocking tests POSIX advisory file locking over the FUSE mount.
func TestPosixFileLocking(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("FlockExclusive", func(t *testing.T) {
		testFlockExclusive(t, framework)
	})
	t.Run("FlockShared", func(t *testing.T) {
		testFlockShared(t, framework)
	})
	t.Run("FlockUpgrade", func(t *testing.T) {
		testFlockUpgrade(t, framework)
	})
	t.Run("FlockReleaseOnClose", func(t *testing.T) {
		testFlockReleaseOnClose(t, framework)
	})
	t.Run("FcntlWriteLockConflict", func(t *testing.T) {
		testFcntlWriteLockConflict(t, framework)
	})
	t.Run("FcntlReadLocksShared", func(t *testing.T) {
		testFcntlReadLocksShared(t, framework)
	})
	t.Run("FcntlGetLk", func(t *testing.T) {
		testFcntlGetLk(t, framework)
	})
	t.Run("FcntlByteRangePartial", func(t *testing.T) {
		testFcntlByteRangePartial(t, framework)
	})
	t.Run("FcntlSetLkwBlocks", func(t *testing.T) {
		testFcntlSetLkwBlocks(t, framework)
	})
	t.Run("FcntlReleaseOnClose", func(t *testing.T) {
		testFcntlReleaseOnClose(t, framework)
	})
	t.Run("ConcurrentLockContention", func(t *testing.T) {
		testConcurrentLockContention(t, framework)
	})
}

// --- flock() tests ---

// testFlockExclusive verifies that two file descriptors cannot hold an
// exclusive flock simultaneously.
func testFlockExclusive(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_exclusive.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	// f1 takes exclusive lock.
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	// f2 exclusive should fail with EWOULDBLOCK.
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK, "second exclusive flock should fail")

	// Unlock f1, then f2 should succeed.
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

// testFlockShared verifies that multiple file descriptors can hold a shared
// flock, but an exclusive lock conflicts with a shared lock.
func testFlockShared(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_shared.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	// Both should succeed with shared locks.
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))

	// Exclusive should fail while shared locks are held.
	f3, err := os.Open(path)
	require.NoError(t, err)
	defer f3.Close()
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

// testFlockUpgrade verifies that a shared flock can be upgraded to exclusive.
func testFlockUpgrade(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_upgrade.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	// Acquire shared, then upgrade to exclusive.
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	// Another fd should not be able to acquire any lock.
	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
}

// testFlockReleaseOnClose verifies that closing a file descriptor releases its
// flock, allowing another fd to acquire the lock.
func testFlockReleaseOnClose(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_close.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	// Close releases the lock.
	f1.Close()

	// Now another fd should be able to lock.
	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

// --- fcntl() POSIX lock tests ---

// testFcntlWriteLockConflict verifies that two fds cannot hold overlapping
// write locks on the same byte range.
func testFcntlWriteLockConflict(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_write.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()

	// f1 locks bytes [0, 10).
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 10))

	// f2 overlapping write lock should fail with EAGAIN.
	err = fcntlSetLk(f2, syscall.F_WRLCK, 5, 10)
	assert.ErrorIs(t, err, syscall.EAGAIN, "overlapping write lock should fail with EAGAIN")

	// Unlock f1.
	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 10))

	// Now f2 should succeed.
	require.NoError(t, fcntlSetLk(f2, syscall.F_WRLCK, 5, 10))
	require.NoError(t, fcntlSetLk(f2, syscall.F_UNLCK, 5, 10))
}

// testFcntlReadLocksShared verifies that multiple fds can hold overlapping
// read locks on the same byte range.
func testFcntlReadLocksShared(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_shared.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()

	// Both should succeed with read locks on overlapping ranges.
	require.NoError(t, fcntlSetLk(f1, syscall.F_RDLCK, 0, 10))
	require.NoError(t, fcntlSetLk(f2, syscall.F_RDLCK, 0, 10))

	// A write lock should fail.
	f3, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f3.Close()
	err = fcntlSetLk(f3, syscall.F_WRLCK, 0, 10)
	assert.ErrorIs(t, err, syscall.EAGAIN, "write lock should fail with EAGAIN while read locks are held")

	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 10))
	require.NoError(t, fcntlSetLk(f2, syscall.F_UNLCK, 0, 10))
}

// testFcntlGetLk verifies that F_GETLK reports conflicting lock details.
func testFcntlGetLk(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_getlk.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()

	// f1 takes a write lock on [0, 5).
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 5))

	// f2 queries for conflicting lock on [0, 10).
	flk, err := fcntlGetLk(f2, syscall.F_WRLCK, 0, 10)
	require.NoError(t, err)
	assert.NotEqual(t, syscall.F_UNLCK, flk.Type, "should report a conflict")

	// Query on a non-overlapping range should report no conflict.
	flk, err = fcntlGetLk(f2, syscall.F_WRLCK, 5, 5)
	require.NoError(t, err)
	assert.Equal(t, int16(syscall.F_UNLCK), flk.Type, "no conflict expected on non-overlapping range")

	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 5))
}

// testFcntlByteRangePartial verifies that non-overlapping byte ranges can be
// locked independently by different file descriptors.
func testFcntlByteRangePartial(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_range.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789ABCDEF"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()

	// f1 locks [0, 8), f2 locks [8, 16). Non-overlapping, both should succeed.
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 8))
	require.NoError(t, fcntlSetLk(f2, syscall.F_WRLCK, 8, 8))

	// f2 trying to extend into f1's range should fail.
	err = fcntlSetLk(f2, syscall.F_WRLCK, 4, 8)
	assert.ErrorIs(t, err, syscall.EAGAIN, "extending into another fd's locked range should fail with EAGAIN")

	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 8))
	require.NoError(t, fcntlSetLk(f2, syscall.F_UNLCK, 8, 8))
}

// testFcntlSetLkwBlocks verifies that F_SETLKW blocks until the conflicting
// lock is released, then succeeds.
func testFcntlSetLkwBlocks(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_setlkw.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// f1 takes exclusive lock.
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 0))

	// Launch goroutine that blocks on F_SETLKW.
	done := make(chan error, 1)
	go func() {
		f2, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			done <- err
			return
		}
		defer f2.Close()
		// This should block until f1 releases.
		err = fcntlSetLkw(f2, syscall.F_WRLCK, 0, 0)
		if err != nil {
			done <- err
			return
		}
		// Release our lock and signal success.
		err = fcntlSetLk(f2, syscall.F_UNLCK, 0, 0)
		done <- err
	}()

	// Give the goroutine time to block.
	time.Sleep(200 * time.Millisecond)

	// Verify it hasn't completed yet.
	select {
	case err := <-done:
		t.Fatalf("F_SETLKW should be blocking, but returned: %v", err)
	default:
		// Expected — still blocking.
	}

	// Release f1's lock to unblock the waiter.
	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 0))

	select {
	case err := <-done:
		require.NoError(t, err, "F_SETLKW should succeed after lock is released")
	case <-time.After(5 * time.Second):
		t.Fatal("F_SETLKW did not unblock within timeout")
	}
}

// testFcntlReleaseOnClose verifies that closing a file descriptor releases
// any fcntl locks held on it.
func testFcntlReleaseOnClose(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_close.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 0))
	f1.Close()

	// A new fd should now be able to acquire the lock.
	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()
	require.NoError(t, fcntlSetLk(f2, syscall.F_WRLCK, 0, 0))
	require.NoError(t, fcntlSetLk(f2, syscall.F_UNLCK, 0, 0))
}

// --- Concurrency test ---

// testConcurrentLockContention has multiple goroutines competing for an
// exclusive lock, each writing to the file while holding it.
func testConcurrentLockContention(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "lock_contention.txt")
	require.NoError(t, os.WriteFile(path, []byte(""), 0644))

	numWorkers := 8
	writesPerWorker := 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error

	addError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errors = append(errors, err)
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
				if err != nil {
					addError(fmt.Errorf("worker %d open: %v", id, err))
					return
				}

				// Blocking lock — will wait until available.
				if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
					f.Close()
					addError(fmt.Errorf("worker %d flock: %v", id, err))
					return
				}

				// Write while holding the lock.
				msg := fmt.Sprintf("worker %d write %d\n", id, j)
				if _, err := f.WriteString(msg); err != nil {
					syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
					f.Close()
					addError(fmt.Errorf("worker %d write: %v", id, err))
					return
				}

				syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				f.Close()
			}
		}(i)
	}

	wg.Wait()
	require.Empty(t, errors, "concurrent lock contention errors: %v", errors)

	// Verify all writes were preserved.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	expectedLines := numWorkers * writesPerWorker
	actualLines := bytes.Count(data, []byte("\n"))
	assert.Equal(t, expectedLines, actualLines,
		"file should contain exactly %d lines from all workers", expectedLines)
}
