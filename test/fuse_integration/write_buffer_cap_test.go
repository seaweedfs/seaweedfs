package fuse_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mountDebugPort holds the debug/pprof port the test passed to the
// mount process via -debug.port. It is set once at TestWriteBufferCap
// entry and consulted from the write-timeout paths to fetch the mount
// process's goroutine dump (the test's own dumpAllGoroutines only
// covers the test process).
var mountDebugPort int

// fetchMountGoroutines pulls a full goroutine dump from the mount
// process's pprof endpoint. If the mount debug port isn't configured
// or the HTTP call fails, a short explanation is returned instead of
// an error — this is diagnostic best-effort, not a test assertion.
func fetchMountGoroutines() string {
	if mountDebugPort == 0 {
		return "(mount debug port not configured)"
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=2", mountDebugPort)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Sprintf("(failed to reach mount pprof at %s: %v)", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("(failed to read mount pprof body: %v)", err)
	}
	return string(body)
}

// dumpAllGoroutines returns a full stack trace of every live goroutine.
// Used on write-timeout to give CI actionable diagnosis if the write
// buffer cap ever re-regresses into a hang.
func dumpAllGoroutines() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// writeBufferCapConfig returns a TestConfig that exercises the new
// -writeBufferSizeMB flag. The cap is set below the aggregate in-flight
// write demand of the subtests below, so every new chunk has to pass
// through the Reserve/Release backpressure path at least some of the
// time. The cap is intentionally NOT minimal — over-tight settings
// interact with the per-file writable-chunk limit and the FUSE MaxWrite
// batching in ways that starve single-handle writers on slow CI.
//
// Also enables the mount's pprof debug endpoint so the test can fetch
// mount-process goroutine dumps on write-timeout, which is the only
// way to actually diagnose a backpressure deadlock (the test process
// itself is just blocked in syscall.Write waiting on FUSE).
func writeBufferCapConfig(debugPort int) *TestConfig {
	return &TestConfig{
		Collection:  "",
		Replication: "000",
		ChunkSizeMB: 2,   // 2 MiB chunks
		CacheSizeMB: 100, // read cache (unrelated)
		NumVolumes:  3,
		EnableDebug: false,
		MountOptions: []string{
			// 16 MiB total write buffer ⇒ up to 8 chunks in flight
			// across every open file handle on this mount. Large
			// enough to avoid starving a single handle on a slow
			// CI runner, small enough that the concurrent test
			// below still has to drain through it.
			"-writeBufferSizeMB=16",
			"-debug=true",
			fmt.Sprintf("-debug.port=%d", debugPort),
		},
		SkipCleanup: false,
	}
}

// writeWithTimeout wraps os.WriteFile with a hard deadline so a stuck
// write fails the test fast instead of consuming the full job budget.
// This is belt-and-braces around the 45-minute workflow timeout and
// makes write-buffer regressions surface as an actionable failure.
func writeWithTimeout(t *testing.T, path string, data []byte, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- os.WriteFile(path, data, 0644) }()
	select {
	case err := <-done:
		require.NoError(t, err, "write %s", path)
	case <-time.After(timeout):
		t.Logf("write %s did not finish within %v — dumping TEST goroutines:\n%s", path, timeout, dumpAllGoroutines())
		t.Logf("dumping MOUNT goroutines:\n%s", fetchMountGoroutines())
		t.Fatalf("write %s timed out — write buffer cap is likely leaking or deadlocking", path)
	}
}

// runSubtestWithWatchdog runs body on the current (subtest main)
// goroutine and starts a watcher goroutine that logs diagnostics and
// fails the test if body doesn't return within timeout.
//
// body must run on the main goroutine because test helpers inside it
// (require.NoError, writeWithTimeout's own t.Fatalf on its internal
// timeout) need t.Fatal / t.FailNow, which Go's testing docs restrict
// to the goroutine running the test function. The watcher goroutine
// only calls goroutine-safe t methods (t.Log, t.Logf, t.Errorf) so it
// can mark the test failed and dump diagnostics without violating
// that contract. If body is stuck past timeout the watcher still
// surfaces the wedge (test + mount goroutine dumps + a FAIL mark);
// body itself gets unblocked either by its own inner writeWithTimeout
// firing t.Fatalf or by Go test's global -timeout.
func runSubtestWithWatchdog(t *testing.T, timeout time.Duration, body func(t *testing.T)) {
	t.Helper()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-stop:
			return
		case <-time.After(timeout):
			t.Logf("subtest exceeded %v watchdog — dumping TEST goroutines:\n%s", timeout, dumpAllGoroutines())
			t.Logf("dumping MOUNT goroutines:\n%s", fetchMountGoroutines())
			t.Errorf("subtest exceeded %v watchdog — see goroutine dumps above", timeout)
		}
	}()
	body(t)
}

// TestWriteBufferCap exercises the end-to-end write-buffer cap on a
// real FUSE mount. Without the cap, a volume-server stall would let
// the swap file grow without bound (issue #8777). With the cap, writers
// must serialize through a bounded budget while still producing correct
// output — that correctness (and the absence of deadlocks) is what
// this test verifies.
//
// Note: this test deliberately does not assert that Reserve *blocked*
// at some observed used-byte peak. The mount runs as a subprocess so
// its in-process WriteBufferAccountant state is not reachable from the
// test without adding a metrics/RPC surface to the mount binary. The
// deterministic peak-vs-cap assertion instead lives in the in-package
// unit test TestWriteBufferCap_SharedAcrossPipelines, which drives a
// controlled gated uploader and samples Used() throughout the run.
func TestWriteBufferCap(t *testing.T) {
	mountDebugPort = freePort(t)
	config := writeBufferCapConfig(mountDebugPort)
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	const subtestTimeout = 3 * time.Minute

	t.Run("ConcurrentWritesUnderCap", func(t *testing.T) {
		runSubtestWithWatchdog(t, subtestTimeout, func(t *testing.T) {
			testConcurrentWritesUnderCap(t, framework)
		})
	})

	t.Run("LargeFileUnderCap", func(t *testing.T) {
		runSubtestWithWatchdog(t, subtestTimeout, func(t *testing.T) {
			testLargeFileUnderCap(t, framework)
		})
	})

	t.Run("DoesNotDeadlockAfterPressure", func(t *testing.T) {
		runSubtestWithWatchdog(t, subtestTimeout, func(t *testing.T) {
			testWriteBufferNoDeadlockAfterPressure(t, framework)
		})
	})
}

// testConcurrentWritesUnderCap opens several files in parallel with
// aggregate demand that exceeds the 16 MiB write buffer cap, then
// verifies every byte survived the round trip.
func testConcurrentWritesUnderCap(t *testing.T, framework *FuseTestFramework) {
	const (
		numFiles = 4
		fileSize = 8 * 1024 * 1024 // 8 MiB per file ⇒ 32 MiB total vs 16 MiB cap
	)

	dir := "write_buffer_cap_concurrent"
	framework.CreateTestDir(dir)

	payloads := make([][]byte, numFiles)
	for i := range payloads {
		buf := make([]byte, fileSize)
		_, err := rand.Read(buf)
		require.NoError(t, err)
		payloads[i] = buf
	}

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, numFiles)
	timedOut := make(chan struct{}, numFiles)
	for i := 0; i < numFiles; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("file_%02d.bin", i)
			path := filepath.Join(framework.GetMountPoint(), dir, name)
			done := make(chan error, 1)
			go func() { done <- os.WriteFile(path, payloads[i], 0644) }()
			select {
			case err := <-done:
				if err != nil {
					errs <- fmt.Errorf("writer %d: %w", i, err)
				}
			case <-time.After(90 * time.Second):
				timedOut <- struct{}{}
				errs <- fmt.Errorf("writer %d: timed out after 90s", i)
			}
		}()
	}
	wg.Wait()
	close(errs)
	// If any writer timed out, dump every live goroutine so CI shows the
	// wedge instead of just a walltime.
	select {
	case <-timedOut:
		t.Logf("at least one concurrent writer timed out — dumping goroutines:\n%s", dumpAllGoroutines())
	default:
	}
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("wrote %d × %d MiB under 16 MiB cap in %v", numFiles, fileSize/(1024*1024), time.Since(start))

	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file_%02d.bin", i)
		path := filepath.Join(framework.GetMountPoint(), dir, name)
		got, err := os.ReadFile(path)
		require.NoError(t, err, "read %s", name)
		require.Equal(t, len(payloads[i]), len(got), "size mismatch for %s", name)
		if !bytes.Equal(payloads[i], got) {
			t.Fatalf("content mismatch for %s", name)
		}
	}
}

// testLargeFileUnderCap writes a single file whose size exceeds the
// 16 MiB cap through a single handle, verifying that the pipeline
// drains its own earlier chunks and makes forward progress rather than
// self-deadlocking when the global budget is already full of its own
// earlier sealed chunks.
func testLargeFileUnderCap(t *testing.T, framework *FuseTestFramework) {
	const fileSize = 20 * 1024 * 1024 // 20 MiB ⇒ 10 chunks vs 8-slot budget

	payload := make([]byte, fileSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	name := "write_buffer_cap_large.bin"
	path := filepath.Join(framework.GetMountPoint(), name)

	start := time.Now()
	writeWithTimeout(t, path, payload, 90*time.Second)
	t.Logf("wrote %d MiB through one handle under 16 MiB cap in %v", fileSize/(1024*1024), time.Since(start))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, len(payload), len(got))
	if !bytes.Equal(payload, got) {
		t.Fatal("content mismatch on large single-handle write")
	}
}

// testWriteBufferNoDeadlockAfterPressure verifies the mount is still
// healthy after being driven against the cap. A budget-slot leak would
// eventually cause every new chunk allocation to hang; a quick canary
// write catches that as a hard failure.
func testWriteBufferNoDeadlockAfterPressure(t *testing.T, framework *FuseTestFramework) {
	name := "write_buffer_cap_canary.txt"
	path := filepath.Join(framework.GetMountPoint(), name)
	content := []byte("write buffer cap canary — mount still healthy")

	writeWithTimeout(t, path, content, 30*time.Second)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, got)
}
