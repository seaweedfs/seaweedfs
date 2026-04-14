package fuse_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeBufferCapConfig returns a TestConfig that exercises the new
// -writeBufferSizeMB flag. The cap is deliberately much smaller than
// the total write demand driven by the tests below, so every test path
// must make progress through Reserve/Release backpressure rather than
// by fitting everything in the buffer at once.
func writeBufferCapConfig() *TestConfig {
	return &TestConfig{
		Collection:  "",
		Replication: "000",
		ChunkSizeMB: 2,   // 2 MiB chunks
		CacheSizeMB: 100, // read cache (unrelated to write buffer)
		NumVolumes:  3,
		EnableDebug: false,
		MountOptions: []string{
			// 4 MiB total write buffer ⇒ at most 2 chunks in flight
			// across every open file handle on this mount.
			"-writeBufferSizeMB=4",
			// Low concurrentWriters so backpressure is reachable
			// without requiring huge file sizes.
			"-concurrentWriters=4",
		},
		SkipCleanup: false,
	}
}

// TestWriteBufferCap exercises the end-to-end write-buffer cap on a real
// FUSE mount. Without the cap, a volume-server stall would let the swap
// file grow without bound (issue #8777). With the cap, writers must
// serialize through a bounded budget while still producing correct
// output — that correctness is what this test verifies.
func TestWriteBufferCap(t *testing.T) {
	config := writeBufferCapConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	t.Run("ConcurrentLargeWrites", func(t *testing.T) {
		testConcurrentLargeWritesUnderCap(t, framework)
	})

	t.Run("SingleFileExceedingCap", func(t *testing.T) {
		testSingleFileExceedingCap(t, framework)
	})

	t.Run("DoesNotDeadlockAfterPressure", func(t *testing.T) {
		testWriteBufferNoDeadlockAfterPressure(t, framework)
	})
}

// testConcurrentLargeWritesUnderCap opens several large files in
// parallel so the cumulative in-flight chunk count vastly exceeds the
// -writeBufferSizeMB budget, then verifies every byte survived the
// round trip. The only mechanism that can serialize those writes within
// a 4 MiB budget is Reserve/Release blocking inside the upload pipeline.
func testConcurrentLargeWritesUnderCap(t *testing.T, framework *FuseTestFramework) {
	const (
		numFiles = 6
		fileSize = 6 * 1024 * 1024 // 6 MiB ⇒ 3 chunks per file
	)

	dir := "write_buffer_cap_concurrent"
	framework.CreateTestDir(dir)

	// Pre-generate distinct random payloads so a content mismatch
	// unambiguously points at a specific writer.
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
	for i := 0; i < numFiles; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("file_%02d.bin", i)
			path := filepath.Join(framework.GetMountPoint(), dir, name)
			if err := os.WriteFile(path, payloads[i], 0644); err != nil {
				errs <- fmt.Errorf("writer %d: %w", i, err)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("wrote %d × %d MiB under 4 MiB cap in %v", numFiles, fileSize/(1024*1024), time.Since(start))

	// Read every file back and verify byte-for-byte equality.
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

// testSingleFileExceedingCap writes one file much larger than the write
// buffer through a single file handle. Every chunk past the first few
// must go through Reserve/Release on the same pipeline, verifying that
// the per-file path drains and does not self-deadlock when the budget
// is already full of its own earlier chunks.
func testSingleFileExceedingCap(t *testing.T, framework *FuseTestFramework) {
	const fileSize = 20 * 1024 * 1024 // 20 MiB ⇒ 10 chunks, cap holds 2

	payload := make([]byte, fileSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	name := "write_buffer_cap_single_large.bin"
	path := filepath.Join(framework.GetMountPoint(), name)

	start := time.Now()
	require.NoError(t, os.WriteFile(path, payload, 0644))
	t.Logf("wrote %d MiB through one handle under 4 MiB cap in %v", fileSize/(1024*1024), time.Since(start))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, len(payload), len(got))
	if !bytes.Equal(payload, got) {
		t.Fatal("content mismatch on large single-handle write")
	}
}

// testWriteBufferNoDeadlockAfterPressure verifies the mount is still
// healthy after being driven against the cap. If Reserve/Release had
// leaked a slot across the previous subtests, the accountant would
// eventually refuse every new chunk and writes would hang. A final
// small write must still complete promptly.
func testWriteBufferNoDeadlockAfterPressure(t *testing.T, framework *FuseTestFramework) {
	name := "write_buffer_cap_canary.txt"
	path := filepath.Join(framework.GetMountPoint(), name)
	content := []byte("write buffer cap canary — mount still healthy")

	done := make(chan error, 1)
	go func() { done <- os.WriteFile(path, content, 0644) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("canary write hung — write budget likely leaked")
	}

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, got)
}
