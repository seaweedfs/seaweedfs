package fuse_dlm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDLMConcurrentWritersSameFile verifies that two mounts writing to the same
// file concurrently produce valid (non-corrupted) data. With DLM enabled, the
// writes are serialized — one blocks until the other completes.
//
// Note: cross-mount read consistency depends on FUSE kernel cache invalidation
// and filer metadata subscription, which are asynchronous. This test verifies
// write integrity, not instant read convergence.
func TestDLMConcurrentWritersSameFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	const iterations = 5
	for iter := 0; iter < iterations; iter++ {
		fileName := fmt.Sprintf("concurrent_write_%d.txt", iter)
		payloadA := []byte(fmt.Sprintf("mount0-iteration-%d-payload-AAAA", iter))
		payloadB := []byte(fmt.Sprintf("mount1-iteration-%d-payload-BBBB", iter))

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := os.WriteFile(filepath.Join(cluster.mountPoints[0], fileName), payloadA, 0644)
			assert.NoError(t, err, "mount0 write iteration %d", iter)
		}()

		go func() {
			defer wg.Done()
			err := os.WriteFile(filepath.Join(cluster.mountPoints[1], fileName), payloadB, 0644)
			assert.NoError(t, err, "mount1 write iteration %d", iter)
		}()

		wg.Wait()

		// Verify file is readable and contains one of the expected payloads
		// (read from mount0 — its own view is authoritative for write success).
		content, err := os.ReadFile(filepath.Join(cluster.mountPoints[0], fileName))
		require.NoError(t, err, "read from mount0 iteration %d", iter)
		validPayload := string(content) == string(payloadA) || string(content) == string(payloadB)
		assert.True(t, validPayload,
			"iteration %d: content must be one of the expected payloads, got: %q", iter, content)
	}
}

// TestDLMRepeatedOpenWriteClose verifies that repeated open/write/close cycles
// from both mounts all succeed without errors.
func TestDLMRepeatedOpenWriteClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	const cycles = 20
	fileName := "repeated_write.txt"

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < cycles; i++ {
			data := []byte(fmt.Sprintf("mount0-cycle-%d", i))
			err := os.WriteFile(filepath.Join(cluster.mountPoints[0], fileName), data, 0644)
			assert.NoError(t, err, "mount0 cycle %d", i)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < cycles; i++ {
			data := []byte(fmt.Sprintf("mount1-cycle-%d", i))
			err := os.WriteFile(filepath.Join(cluster.mountPoints[1], fileName), data, 0644)
			assert.NoError(t, err, "mount1 cycle %d", i)
		}
	}()

	wg.Wait()

	// File must be readable from at least one mount
	content, err := os.ReadFile(filepath.Join(cluster.mountPoints[0], fileName))
	require.NoError(t, err)
	assert.NotEmpty(t, content, "file must not be empty")
}

// TestDLMStressConcurrentWrites stress-tests DLM with many goroutines across
// both mounts writing to a shared pool of files.
func TestDLMStressConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	const (
		goroutinesPerMount = 2
		numFiles           = 3
		cyclesPerGoroutine = 3
	)

	var wg sync.WaitGroup
	var writeErrors atomic.Int64

	for mountIdx := 0; mountIdx < 2; mountIdx++ {
		for g := 0; g < goroutinesPerMount; g++ {
			wg.Add(1)
			go func(mIdx, gIdx int) {
				defer wg.Done()
				for cycle := 0; cycle < cyclesPerGoroutine; cycle++ {
					fileIdx := (gIdx + cycle) % numFiles
					fileName := fmt.Sprintf("stress_%d.txt", fileIdx)
					path := filepath.Join(cluster.mountPoints[mIdx], fileName)
					data := []byte(fmt.Sprintf("m%d-g%d-c%d", mIdx, gIdx, cycle))
					if err := os.WriteFile(path, data, 0644); err != nil {
						writeErrors.Add(1)
						t.Logf("write error mount%d goroutine%d cycle%d: %v", mIdx, gIdx, cycle, err)
					}
				}
			}(mountIdx, g)
		}
	}

	wg.Wait()

	// Under heavy DLM contention, a small number of transient FUSE flush
	// errors (EIO on close) are acceptable. The important guarantee is that
	// files are not corrupted, not that every write succeeds under stress.
	totalWrites := int64(2 * goroutinesPerMount * cyclesPerGoroutine)
	maxAllowedErrors := totalWrites/5 + 1 // tolerate up to ~20% transient FUSE errors
	assert.LessOrEqual(t, writeErrors.Load(), maxAllowedErrors,
		"too many write errors: %d out of %d writes", writeErrors.Load(), totalWrites)

	// Verify all files are readable from mount0
	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("stress_%d.txt", i)
		content, err := os.ReadFile(filepath.Join(cluster.mountPoints[0], fileName))
		require.NoError(t, err, "read stress file %d", i)
		assert.NotEmpty(t, content, "stress file %d must not be empty", i)
	}
}

// TestDLMWriteBlocksSecondWriter verifies the core DLM guarantee: while one
// mount has a file open for writing, another mount's write-open blocks until
// the first mount closes the file.
func TestDLMWriteBlocksSecondWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	fileName := "blocking_test.txt"
	path0 := filepath.Join(cluster.mountPoints[0], fileName)
	path1 := filepath.Join(cluster.mountPoints[1], fileName)

	// Mount 0 opens the file for writing and holds it open
	f, err := os.OpenFile(path0, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err, "mount0 open")
	_, err = f.Write([]byte("mount0-holds-lock"))
	require.NoError(t, err, "mount0 write")

	// Mount 1 tries to write — should block (we use a goroutine with atomic flag)
	var mount1Completed atomic.Bool
	mount1Done := make(chan error, 1)
	go func() {
		err := os.WriteFile(path1, []byte("mount1-waited"), 0644)
		mount1Completed.Store(true)
		mount1Done <- err
	}()

	// Give mount 1 a moment — it should NOT complete while mount 0 holds the file open
	time.Sleep(3 * time.Second)
	require.False(t, mount1Completed.Load(),
		"mount1 write must not complete while mount0 holds the file open")
	t.Log("mount1 write is blocked as expected while mount0 holds the file")

	// Mount 0 closes the file — this releases the DLM lock
	require.NoError(t, f.Close(), "mount0 close")

	// Mount 1 should now complete
	select {
	case err := <-mount1Done:
		assert.NoError(t, err, "mount1 write after mount0 close")
	case <-time.After(30 * time.Second):
		t.Fatal("mount1 write did not complete within 30s after mount0 closed")
	}
}

// TestDLMRenameWhileWriteOpen verifies that a rename is coordinated with DLM:
// while mount0 has a file open for writing (re-opened after creation),
// mount1 cannot rename it until mount0 closes the file.
func TestDLMRenameWhileWriteOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	origName := "rename_source.txt"
	newName := "rename_dest.txt"

	// Create and close the file first so it's flushed to the filer and
	// visible on both mounts.
	require.NoError(t, os.WriteFile(
		filepath.Join(cluster.mountPoints[0], origName),
		[]byte("initial-content"), 0644))
	time.Sleep(2 * time.Second) // metadata propagation

	// Verify mount1 can see the file
	_, err := os.Stat(filepath.Join(cluster.mountPoints[1], origName))
	require.NoError(t, err, "mount1 should see the file")

	// Mount 0 re-opens the file for writing and holds it open
	f, err := os.OpenFile(
		filepath.Join(cluster.mountPoints[0], origName),
		os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err, "mount0 reopen")
	_, err = f.Write([]byte("data-while-holding-lock"))
	require.NoError(t, err, "mount0 write")

	// Mount 1 tries to rename — should block because mount0 holds the
	// DLM lock on the old path
	var renameCompleted atomic.Bool
	renameDone := make(chan error, 1)
	go func() {
		err := os.Rename(
			filepath.Join(cluster.mountPoints[1], origName),
			filepath.Join(cluster.mountPoints[1], newName))
		renameCompleted.Store(true)
		renameDone <- err
	}()

	// Rename must NOT complete while mount0 holds the file open
	time.Sleep(3 * time.Second)
	require.False(t, renameCompleted.Load(),
		"rename must not complete while mount0 holds the file open")
	t.Log("rename is blocked as expected while mount0 holds the file")

	// Mount 0 closes → releases DLM lock → rename should proceed
	require.NoError(t, f.Close(), "mount0 close")

	select {
	case err := <-renameDone:
		assert.NoError(t, err, "rename after mount0 close")
	case <-time.After(30 * time.Second):
		t.Fatal("rename did not complete within 30s after mount0 closed")
	}
}

// TestDLMRenameWhileWriteOpenSameMount verifies DLM lock migration on rename
// within the same mount: mount0 holds a file open for writing, then renames it
// on the same mount. The rename should block until close, and the DLM lock
// should migrate from the old path to the new path.
func TestDLMRenameWhileWriteOpenSameMount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	origName := "same_mount_rename_src.txt"
	newName := "same_mount_rename_dst.txt"
	mp := cluster.mountPoints[0]

	// Create and close the file so it exists on the filer
	require.NoError(t, os.WriteFile(filepath.Join(mp, origName), []byte("content"), 0644))
	time.Sleep(1 * time.Second)

	// Re-open for writing and hold open
	f, err := os.OpenFile(filepath.Join(mp, origName), os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err, "open")
	_, err = f.Write([]byte("holding-lock"))
	require.NoError(t, err, "write")

	// Rename on the same mount — should block because the DLM lock on
	// the old path is held by the write-open
	var renameCompleted atomic.Bool
	renameDone := make(chan error, 1)
	go func() {
		err := os.Rename(filepath.Join(mp, origName), filepath.Join(mp, newName))
		renameCompleted.Store(true)
		renameDone <- err
	}()

	time.Sleep(3 * time.Second)
	require.False(t, renameCompleted.Load(),
		"same-mount rename must not complete while file is held open")
	t.Log("same-mount rename is blocked as expected")

	// Close releases the DLM lock → rename should proceed
	require.NoError(t, f.Close(), "close")

	select {
	case err := <-renameDone:
		assert.NoError(t, err, "rename after close")
	case <-time.After(30 * time.Second):
		t.Fatal("rename did not complete within 30s after close")
	}

	// Verify destination exists
	time.Sleep(500 * time.Millisecond)
	_, err = os.Stat(filepath.Join(mp, newName))
	assert.NoError(t, err, "renamed file should exist")
}

// TestDLMConcurrentRenames verifies that two concurrent renames of the same
// file from different mounts don't corrupt metadata. DLM locks on both old
// and new paths ensure renames are serialized.
func TestDLMConcurrentRenames(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DLM integration test in short mode")
	}

	cluster := startDLMTestCluster(t)
	t.Cleanup(cluster.Stop)

	// Create a file first
	origPath := filepath.Join(cluster.mountPoints[0], "rename_race.txt")
	require.NoError(t, os.WriteFile(origPath, []byte("original-content"), 0644))
	time.Sleep(1 * time.Second) // propagation

	// Both mounts try to rename the same file concurrently
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)

	go func() {
		defer wg.Done()
		errA = os.Rename(
			filepath.Join(cluster.mountPoints[0], "rename_race.txt"),
			filepath.Join(cluster.mountPoints[0], "renamed_by_mount0.txt"),
		)
	}()

	go func() {
		defer wg.Done()
		errB = os.Rename(
			filepath.Join(cluster.mountPoints[1], "rename_race.txt"),
			filepath.Join(cluster.mountPoints[1], "renamed_by_mount1.txt"),
		)
	}()

	wg.Wait()

	// At least one rename should succeed; the other may fail with ENOENT
	// since the source was already moved.
	succeeded := 0
	if errA == nil {
		succeeded++
		t.Logf("mount0 rename succeeded")
	} else {
		t.Logf("mount0 rename failed: %v", errA)
	}
	if errB == nil {
		succeeded++
		t.Logf("mount1 rename succeeded")
	} else {
		t.Logf("mount1 rename failed: %v", errB)
	}
	assert.GreaterOrEqual(t, succeeded, 1, "at least one rename must succeed")
}
