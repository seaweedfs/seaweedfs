package fuse_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fcntl helpers (used by both test process and subprocess) ---

func fcntlSetLk(f *os.File, typ int16, start, len int64) error {
	flk := syscall.Flock_t{
		Type:   typ,
		Whence: 0, // SEEK_SET
		Start:  start,
		Len:    len, // 0 means to EOF
	}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &flk)
}

func fcntlSetLkw(f *os.File, typ int16, start, len int64) error {
	flk := syscall.Flock_t{
		Type:   typ,
		Whence: 0,
		Start:  start,
		Len:    len,
	}
	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &flk)
}

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

// --- Subprocess entry point for fcntl lock tests ---
//
// POSIX fcntl locks are per-process (the kernel uses the process's files_struct
// as lock owner). To test inter-process lock conflicts over FUSE, contending
// locks must come from separate processes.
//
// Actions:
//   hold          — acquire F_SETLK, print "LOCKED\n", wait for stdin close, exit
//   hold-blocking — acquire F_SETLKW, print "LOCKED\n", wait for stdin close, exit
//   try           — try F_SETLK, print "OK\n" or "EAGAIN\n", exit
//   getlk         — do F_GETLK, print lock type as integer, exit

func TestFcntlLockHelper(t *testing.T) {
	action := os.Getenv("LOCK_ACTION")
	if action == "" {
		t.Skip("subprocess helper — not invoked directly")
	}

	filePath := os.Getenv("LOCK_FILE")
	lockType := parseLockType(os.Getenv("LOCK_TYPE"))
	start, _ := strconv.ParseInt(os.Getenv("LOCK_START"), 10, 64)
	length, _ := strconv.ParseInt(os.Getenv("LOCK_LEN"), 10, 64)

	f, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	switch action {
	case "hold":
		if err := fcntlSetLk(f, lockType, start, length); err != nil {
			fmt.Fprintf(os.Stderr, "setlk: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("LOCKED")
		os.Stdout.Sync()
		io.ReadAll(os.Stdin) // block until parent closes pipe

	case "hold-blocking":
		if err := fcntlSetLkw(f, lockType, start, length); err != nil {
			fmt.Fprintf(os.Stderr, "setlkw: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("LOCKED")
		os.Stdout.Sync()
		io.ReadAll(os.Stdin)

	case "try":
		if err := fcntlSetLk(f, lockType, start, length); err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EACCES) {
				fmt.Println("EAGAIN")
			} else {
				fmt.Fprintf(os.Stderr, "unexpected setlk error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println("OK")
		}
		os.Stdout.Sync()

	case "getlk":
		flk := syscall.Flock_t{Type: lockType, Whence: 0, Start: start, Len: length}
		if err := syscall.FcntlFlock(f.Fd(), syscall.F_GETLK, &flk); err != nil {
			fmt.Fprintf(os.Stderr, "getlk: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(flk.Type)
		os.Stdout.Sync()

	default:
		fmt.Fprintf(os.Stderr, "unknown action: %s\n", action)
		os.Exit(1)
	}

	os.Exit(0)
}

func parseLockType(s string) int16 {
	switch s {
	case "F_RDLCK":
		return syscall.F_RDLCK
	case "F_WRLCK":
		return syscall.F_WRLCK
	default:
		return syscall.F_WRLCK
	}
}

// --- Subprocess coordination helpers ---

// lockHolder is a subprocess holding an fcntl lock. A background goroutine
// reads stdout and signals the locked channel when "LOCKED" is received,
// avoiding races between IsLocked and WaitLocked.
type lockHolder struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	locked chan struct{} // closed when subprocess prints "LOCKED"
}

// startLockHolder launches a subprocess that acquires an fcntl lock and holds
// it until Release is called.
func startLockHolder(t *testing.T, path string, lockType string, start, length int64) *lockHolder {
	t.Helper()
	return startLockProcess(t, "hold", path, lockType, start, length)
}

// startBlockingLockHolder launches a subprocess that tries F_SETLKW.
// It blocks until the lock is available, then holds it until Release is called.
func startBlockingLockHolder(t *testing.T, path string, lockType string, start, length int64) *lockHolder {
	t.Helper()
	return startLockProcess(t, "hold-blocking", path, lockType, start, length)
}

func startLockProcess(t *testing.T, action, path, lockType string, start, length int64) *lockHolder {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestFcntlLockHelper$")
	cmd.Env = append(os.Environ(),
		"LOCK_ACTION="+action,
		"LOCK_FILE="+path,
		"LOCK_TYPE="+lockType,
		fmt.Sprintf("LOCK_START=%d", start),
		fmt.Sprintf("LOCK_LEN=%d", length),
	)

	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)

	stdoutPipe, err := cmd.StdoutPipe()
	require.NoError(t, err)

	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	locked := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		if scanner.Scan() && strings.TrimSpace(scanner.Text()) == "LOCKED" {
			close(locked)
		}
	}()

	return &lockHolder{cmd: cmd, stdin: stdin, locked: locked}
}

// WaitLocked waits for the subprocess to acquire the lock and signal "LOCKED".
func (h *lockHolder) WaitLocked(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.locked:
		// OK
	case <-time.After(timeout):
		t.Fatal("timed out waiting for subprocess to acquire lock")
	}
}

// IsLocked checks whether the subprocess has signaled "LOCKED" within 200ms.
func (h *lockHolder) IsLocked() bool {
	select {
	case <-h.locked:
		return true
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

// Release signals the subprocess to exit and waits for it.
func (h *lockHolder) Release(t *testing.T) {
	t.Helper()
	h.stdin.Close()
	_ = h.cmd.Wait()
}

// tryLockInSubprocess tries a non-blocking fcntl lock in a subprocess.
// Returns nil if the lock was acquired, syscall.EAGAIN if it was denied.
func tryLockInSubprocess(t *testing.T, path, lockType string, start, length int64) error {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestFcntlLockHelper$")
	cmd.Env = append(os.Environ(),
		"LOCK_ACTION=try",
		"LOCK_FILE="+path,
		"LOCK_TYPE="+lockType,
		fmt.Sprintf("LOCK_START=%d", start),
		fmt.Sprintf("LOCK_LEN=%d", length),
	)
	cmd.Stderr = os.Stderr

	out, err := cmd.Output()
	require.NoError(t, err, "subprocess failed to run")

	result := strings.TrimSpace(string(out))
	if result == "EAGAIN" {
		return syscall.EAGAIN
	}
	require.Equal(t, "OK", result, "unexpected subprocess output")
	return nil
}

// getLkInSubprocess does F_GETLK in a subprocess and returns the lock type.
func getLkInSubprocess(t *testing.T, path, lockType string, start, length int64) int16 {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestFcntlLockHelper$")
	cmd.Env = append(os.Environ(),
		"LOCK_ACTION=getlk",
		"LOCK_FILE="+path,
		"LOCK_TYPE="+lockType,
		fmt.Sprintf("LOCK_START=%d", start),
		fmt.Sprintf("LOCK_LEN=%d", length),
	)
	cmd.Stderr = os.Stderr

	out, err := cmd.Output()
	require.NoError(t, err, "subprocess failed to run")

	val, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 16)
	require.NoError(t, err, "failed to parse lock type from subprocess")
	return int16(val)
}

// --- Test runner ---

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

// --- flock() tests (same-process is fine — flock uses per-fd owners) ---

func testFlockExclusive(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_exclusive.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK, "second exclusive flock should fail")

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

func testFlockShared(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_shared.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))

	f3, err := os.Open(path)
	require.NoError(t, err)
	defer f3.Close()
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

func testFlockUpgrade(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_upgrade.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)
	defer f1.Close()

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	assert.ErrorIs(t, err, syscall.EWOULDBLOCK)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_UN))
}

func testFlockReleaseOnClose(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "flock_close.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.Open(path)
	require.NoError(t, err)

	require.NoError(t, syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	f1.Close()

	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	require.NoError(t, syscall.Flock(int(f2.Fd()), syscall.LOCK_UN))
}

// --- fcntl() POSIX lock tests (use subprocesses for inter-process semantics) ---

// testFcntlWriteLockConflict: test process holds a write lock, subprocess
// contender is denied, then succeeds after release.
func testFcntlWriteLockConflict(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_write.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// Test process locks [0, 10).
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 10))

	// Subprocess: overlapping write lock should fail with EAGAIN.
	err = tryLockInSubprocess(t, path, "F_WRLCK", 5, 10)
	assert.ErrorIs(t, err, syscall.EAGAIN, "overlapping write lock should fail with EAGAIN")

	// Unlock, then subprocess should succeed.
	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 10))

	err = tryLockInSubprocess(t, path, "F_WRLCK", 5, 10)
	assert.NoError(t, err, "lock should succeed after release")
}

// testFcntlReadLocksShared: test process holds a read lock, subprocess read
// lock succeeds (shared), subprocess write lock is denied.
func testFcntlReadLocksShared(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_shared.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// Test process holds a read lock.
	require.NoError(t, fcntlSetLk(f1, syscall.F_RDLCK, 0, 10))

	// Subprocess: read lock should succeed (shared).
	err = tryLockInSubprocess(t, path, "F_RDLCK", 0, 10)
	assert.NoError(t, err, "shared read lock should succeed")

	// Subprocess: write lock should fail.
	err = tryLockInSubprocess(t, path, "F_WRLCK", 0, 10)
	assert.ErrorIs(t, err, syscall.EAGAIN, "write lock should fail with EAGAIN while read lock is held")

	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 10))
}

// testFcntlGetLk: test process holds a write lock, subprocess queries via
// F_GETLK and sees the conflict.
func testFcntlGetLk(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_getlk.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// Test process takes a write lock on [0, 5).
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 5))

	// Subprocess: F_GETLK on overlapping range should report conflict.
	typ := getLkInSubprocess(t, path, "F_WRLCK", 0, 10)
	assert.NotEqual(t, int16(syscall.F_UNLCK), typ, "should report a conflict")

	// Subprocess: F_GETLK on non-overlapping range should report no conflict.
	typ = getLkInSubprocess(t, path, "F_WRLCK", 5, 5)
	assert.Equal(t, int16(syscall.F_UNLCK), typ, "no conflict expected on non-overlapping range")

	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 5))
}

// testFcntlByteRangePartial: test process and subprocess lock non-overlapping
// ranges independently; extending into the other's range is denied.
func testFcntlByteRangePartial(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_range.txt")
	require.NoError(t, os.WriteFile(path, []byte("0123456789ABCDEF"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// Test process locks [0, 8).
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 8))

	// Subprocess locks [8, 16) — non-overlapping, should succeed.
	holder := startLockHolder(t, path, "F_WRLCK", 8, 8)
	holder.WaitLocked(t, 5*time.Second)

	// Subprocess: extending into test process's range should fail.
	err = tryLockInSubprocess(t, path, "F_WRLCK", 4, 8)
	assert.ErrorIs(t, err, syscall.EAGAIN, "extending into another process's locked range should fail with EAGAIN")

	holder.Release(t)
	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 8))
}

// testFcntlSetLkwBlocks: test process holds exclusive lock, subprocess blocks
// on F_SETLKW, then unblocks when lock is released.
func testFcntlSetLkwBlocks(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_setlkw.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	f1, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f1.Close()

	// Test process takes exclusive lock.
	require.NoError(t, fcntlSetLk(f1, syscall.F_WRLCK, 0, 0))

	// Launch subprocess that will block on F_SETLKW.
	blocker := startBlockingLockHolder(t, path, "F_WRLCK", 0, 0)

	// Verify it hasn't acquired the lock yet (still blocking).
	assert.False(t, blocker.IsLocked(), "F_SETLKW should be blocking")

	// Release our lock to unblock the subprocess.
	require.NoError(t, fcntlSetLk(f1, syscall.F_UNLCK, 0, 0))

	// Subprocess should now acquire and signal "LOCKED".
	blocker.WaitLocked(t, 5*time.Second)
	blocker.Release(t)
}

// testFcntlReleaseOnClose: subprocess holds a lock then exits (fd closes),
// test process verifies it can now acquire the lock.
func testFcntlReleaseOnClose(t *testing.T, fw *FuseTestFramework) {
	path := filepath.Join(fw.GetMountPoint(), "fcntl_close.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	// Subprocess acquires exclusive lock and holds it.
	holder := startLockHolder(t, path, "F_WRLCK", 0, 0)
	holder.WaitLocked(t, 5*time.Second)

	// Another subprocess trying to lock should be denied while holder is active.
	err := tryLockInSubprocess(t, path, "F_WRLCK", 0, 0)
	assert.ErrorIs(t, err, syscall.EAGAIN, "lock should be held by subprocess")

	// Release subprocess → fd closes → lock released.
	holder.Release(t)

	// Test process should now succeed.
	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f2.Close()
	require.NoError(t, fcntlSetLk(f2, syscall.F_WRLCK, 0, 0))
	require.NoError(t, fcntlSetLk(f2, syscall.F_UNLCK, 0, 0))
}

// --- Concurrency test (uses flock which has per-fd owners) ---

// testConcurrentLockContention has multiple goroutines competing for an
// exclusive flock, each appending to the file while holding it.
// Uses non-blocking flock with retry to avoid exhausting the go-fuse server's
// reader goroutines (blocking FUSE SETLKW can starve unlock processing).
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

				// Non-blocking flock with retry to avoid FUSE server thread starvation.
				locked := false
				for attempt := 0; attempt < 500; attempt++ {
					if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
						locked = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if !locked {
					f.Close()
					addError(fmt.Errorf("worker %d: failed to acquire flock after retries", id))
					return
				}

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

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	expectedLines := numWorkers * writesPerWorker
	actualLines := bytes.Count(data, []byte("\n"))
	assert.Equal(t, expectedLines, actualLines,
		"file should contain exactly %d lines from all workers", expectedLines)
}
