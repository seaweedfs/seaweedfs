package fuse_test

import (
	"bytes"
	"crypto/rand"
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

// writebackConfig returns a TestConfig with writebackCache enabled
func writebackConfig() *TestConfig {
	return &TestConfig{
		Collection:  "",
		Replication: "000",
		ChunkSizeMB: 2,
		CacheSizeMB: 100,
		NumVolumes:  3,
		EnableDebug: false,
		MountOptions: []string{
			"-writebackCache",
		},
		SkipCleanup: false,
	}
}

// waitForFileContent polls until a file has the expected content or timeout expires.
// This is needed because writebackCache defers data upload to background goroutines,
// so there is a brief window after close() where the file may not yet be readable.
func waitForFileContent(t *testing.T, path string, expected []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		actual, err := os.ReadFile(path)
		if err == nil && bytes.Equal(expected, actual) {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("content mismatch: got %d bytes, want %d bytes", len(actual), len(expected))
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("file %s did not have expected content within %v: %v", path, timeout, lastErr)
}

// waitForFileSize polls until a file reports the expected size or timeout expires.
func waitForFileSize(t *testing.T, path string, expectedSize int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path)
		if err == nil && info.Size() == expectedSize {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("file %s did not reach expected size %d within %v", path, expectedSize, timeout)
}

// TestWritebackCacheBasicOperations tests fundamental file I/O with writebackCache enabled
func TestWritebackCacheBasicOperations(t *testing.T) {
	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	t.Run("WriteAndReadBack", func(t *testing.T) {
		testWritebackWriteAndReadBack(t, framework)
	})

	t.Run("MultipleFilesSequential", func(t *testing.T) {
		testWritebackMultipleFilesSequential(t, framework)
	})

	t.Run("LargeFile", func(t *testing.T) {
		testWritebackLargeFile(t, framework)
	})

	t.Run("EmptyFile", func(t *testing.T) {
		testWritebackEmptyFile(t, framework)
	})

	t.Run("OverwriteExistingFile", func(t *testing.T) {
		testWritebackOverwriteFile(t, framework)
	})
}

// testWritebackWriteAndReadBack writes a file and verifies it can be read back
// after the async flush completes.
func testWritebackWriteAndReadBack(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_basic.txt"
	content := []byte("Hello from writebackCache test!")
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Write file — close() returns immediately with async flush
	require.NoError(t, os.WriteFile(mountPath, content, 0644))

	// Wait for async flush to complete and verify content
	waitForFileContent(t, mountPath, content, 30*time.Second)
}

// testWritebackMultipleFilesSequential writes multiple files sequentially
// and verifies all are readable after async flushes complete.
func testWritebackMultipleFilesSequential(t *testing.T, framework *FuseTestFramework) {
	dir := "writeback_sequential"
	framework.CreateTestDir(dir)

	numFiles := 50
	files := make(map[string][]byte, numFiles)

	// Write files sequentially — each close() returns immediately
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("file_%03d.txt", i)
		content := []byte(fmt.Sprintf("Sequential file %d content: %s", i, time.Now().Format(time.RFC3339Nano)))
		path := filepath.Join(framework.GetMountPoint(), dir, filename)
		require.NoError(t, os.WriteFile(path, content, 0644))
		files[filename] = content
	}

	// Verify all files after a brief wait for async flushes
	for filename, expectedContent := range files {
		path := filepath.Join(framework.GetMountPoint(), dir, filename)
		waitForFileContent(t, path, expectedContent, 30*time.Second)
	}
}

// testWritebackLargeFile writes a large file (multi-chunk) with writebackCache
func testWritebackLargeFile(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_large.bin"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// 8MB file (spans multiple 2MB chunks)
	content := make([]byte, 8*1024*1024)
	_, err := rand.Read(content)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(mountPath, content, 0644))

	// Wait for file to be fully flushed
	waitForFileContent(t, mountPath, content, 60*time.Second)
}

// testWritebackEmptyFile creates an empty file with writebackCache
func testWritebackEmptyFile(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_empty.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create empty file
	f, err := os.Create(mountPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Should exist and be empty
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())
}

// testWritebackOverwriteFile tests overwriting an existing file
func testWritebackOverwriteFile(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_overwrite.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// First write
	content1 := []byte("First version of the file")
	require.NoError(t, os.WriteFile(mountPath, content1, 0644))
	waitForFileContent(t, mountPath, content1, 30*time.Second)

	// Overwrite with different content
	content2 := []byte("Second version — overwritten content that is longer than the first")
	require.NoError(t, os.WriteFile(mountPath, content2, 0644))
	waitForFileContent(t, mountPath, content2, 30*time.Second)
}

// TestWritebackCacheFsync tests that fsync still forces synchronous flush
// even when writebackCache is enabled
func TestWritebackCacheFsync(t *testing.T) {
	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	t.Run("FsyncForcesFlush", func(t *testing.T) {
		testFsyncForcesFlush(t, framework)
	})

	t.Run("FsyncThenRead", func(t *testing.T) {
		testFsyncThenRead(t, framework)
	})
}

// testFsyncForcesFlush verifies that calling fsync before close ensures
// data is immediately available for reading, bypassing the async path.
func testFsyncForcesFlush(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_fsync.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	content := []byte("Data that must be flushed synchronously via fsync")

	// Open, write, fsync, close
	f, err := os.OpenFile(mountPath, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)

	_, err = f.Write(content)
	require.NoError(t, err)

	// fsync forces synchronous data+metadata flush
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Data should be immediately available — no wait needed
	actual, err := os.ReadFile(mountPath)
	require.NoError(t, err)
	assert.Equal(t, content, actual)
}

// testFsyncThenRead verifies that after fsync, a freshly opened read
// returns the correct data without any delay.
func testFsyncThenRead(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_fsync_read.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	content := make([]byte, 64*1024) // 64KB
	_, err := rand.Read(content)
	require.NoError(t, err)

	// Write with explicit fsync
	f, err := os.OpenFile(mountPath, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Immediate read should succeed
	actual, err := os.ReadFile(mountPath)
	require.NoError(t, err)
	assert.Equal(t, content, actual)
}

// TestWritebackCacheConcurrentSmallFiles is the primary test for issue #8718:
// many small files written concurrently should all be eventually readable.
func TestWritebackCacheConcurrentSmallFiles(t *testing.T) {
	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	t.Run("ConcurrentSmallFiles", func(t *testing.T) {
		testWritebackConcurrentSmallFiles(t, framework)
	})

	t.Run("ConcurrentSmallFilesMultiDir", func(t *testing.T) {
		testWritebackConcurrentSmallFilesMultiDir(t, framework)
	})

	t.Run("RapidCreateCloseSequence", func(t *testing.T) {
		testWritebackRapidCreateClose(t, framework)
	})
}

// testWritebackConcurrentSmallFiles simulates the rsync workload from #8718:
// multiple workers creating many small files in parallel.
func testWritebackConcurrentSmallFiles(t *testing.T, framework *FuseTestFramework) {
	dir := "writeback_concurrent_small"
	framework.CreateTestDir(dir)

	numWorkers := 8
	filesPerWorker := 20
	totalFiles := numWorkers * filesPerWorker

	type fileRecord struct {
		path    string
		content []byte
	}

	var mu sync.Mutex
	var writeErrors []error
	records := make([]fileRecord, 0, totalFiles)

	// Phase 1: Write files concurrently (simulating rsync workers)
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for f := 0; f < filesPerWorker; f++ {
				filename := fmt.Sprintf("w%02d_f%03d.dat", workerID, f)
				path := filepath.Join(framework.GetMountPoint(), dir, filename)

				// Vary sizes: 100B to 100KB
				size := 100 + (workerID*filesPerWorker+f)*500
				if size > 100*1024 {
					size = 100*1024
				}
				content := make([]byte, size)
				if _, err := rand.Read(content); err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, fmt.Errorf("worker %d file %d rand: %v", workerID, f, err))
					mu.Unlock()
					return
				}

				if err := os.WriteFile(path, content, 0644); err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, fmt.Errorf("worker %d file %d: %v", workerID, f, err))
					mu.Unlock()
					return
				}

				mu.Lock()
				records = append(records, fileRecord{path: path, content: content})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	require.Empty(t, writeErrors, "write errors: %v", writeErrors)
	assert.Equal(t, totalFiles, len(records))

	// Phase 2: Wait for async flushes and verify all files
	for _, rec := range records {
		waitForFileContent(t, rec.path, rec.content, 60*time.Second)
	}

	// Phase 3: Verify directory listing has correct count
	entries, err := os.ReadDir(filepath.Join(framework.GetMountPoint(), dir))
	require.NoError(t, err)
	assert.Equal(t, totalFiles, len(entries))
}

// testWritebackConcurrentSmallFilesMultiDir tests concurrent writes across
// multiple directories — a common pattern for parallel copy tools.
func testWritebackConcurrentSmallFilesMultiDir(t *testing.T, framework *FuseTestFramework) {
	baseDir := "writeback_multidir"
	framework.CreateTestDir(baseDir)

	numDirs := 4
	filesPerDir := 25

	type fileRecord struct {
		path    string
		content []byte
	}
	var mu sync.Mutex
	var records []fileRecord
	var writeErrors []error

	var wg sync.WaitGroup
	for d := 0; d < numDirs; d++ {
		subDir := filepath.Join(baseDir, fmt.Sprintf("dir_%02d", d))
		framework.CreateTestDir(subDir)

		wg.Add(1)
		go func(dirID int, dirPath string) {
			defer wg.Done()

			for f := 0; f < filesPerDir; f++ {
				filename := fmt.Sprintf("file_%03d.txt", f)
				path := filepath.Join(framework.GetMountPoint(), dirPath, filename)
				content := []byte(fmt.Sprintf("dir=%d file=%d data=%s", dirID, f, time.Now().Format(time.RFC3339Nano)))

				if err := os.WriteFile(path, content, 0644); err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, fmt.Errorf("dir %d file %d: %v", dirID, f, err))
					mu.Unlock()
					return
				}

				mu.Lock()
				records = append(records, fileRecord{path: path, content: content})
				mu.Unlock()
			}
		}(d, subDir)
	}
	wg.Wait()

	require.Empty(t, writeErrors, "write errors: %v", writeErrors)

	// Verify all files
	for _, rec := range records {
		waitForFileContent(t, rec.path, rec.content, 60*time.Second)
	}
}

// testWritebackRapidCreateClose rapidly creates and closes files to stress
// the async flush goroutine pool.
func testWritebackRapidCreateClose(t *testing.T, framework *FuseTestFramework) {
	dir := "writeback_rapid"
	framework.CreateTestDir(dir)

	numFiles := 200
	type fileRecord struct {
		path    string
		content []byte
	}
	records := make([]fileRecord, numFiles)

	// Rapidly create files without pausing
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("rapid_%04d.bin", i))
		content := []byte(fmt.Sprintf("rapid-file-%d", i))
		require.NoError(t, os.WriteFile(path, content, 0644))
		records[i] = fileRecord{path: path, content: content}
	}

	// Verify all files eventually appear with correct content
	for _, rec := range records {
		waitForFileContent(t, rec.path, rec.content, 60*time.Second)
	}
}

// TestWritebackCacheDataIntegrity tests that data integrity is preserved
// across various write patterns with writebackCache enabled.
func TestWritebackCacheDataIntegrity(t *testing.T) {
	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	t.Run("AppendAfterClose", func(t *testing.T) {
		testWritebackAppendAfterClose(t, framework)
	})

	t.Run("PartialWrites", func(t *testing.T) {
		testWritebackPartialWrites(t, framework)
	})

	t.Run("FileSizeCorrectness", func(t *testing.T) {
		testWritebackFileSizeCorrectness(t, framework)
	})

	t.Run("BinaryData", func(t *testing.T) {
		testWritebackBinaryData(t, framework)
	})
}

// testWritebackAppendAfterClose writes a file, closes it (triggering async flush),
// waits for flush, then reopens and appends more data.
func testWritebackAppendAfterClose(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_append.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// First write
	part1 := []byte("First part of the data.\n")
	require.NoError(t, os.WriteFile(mountPath, part1, 0644))

	// Wait for first async flush
	waitForFileContent(t, mountPath, part1, 30*time.Second)

	// Append more data
	part2 := []byte("Second part appended.\n")
	f, err := os.OpenFile(mountPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(part2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Verify combined content
	expected := append(part1, part2...)
	waitForFileContent(t, mountPath, expected, 30*time.Second)
}

// testWritebackPartialWrites tests writing to specific offsets within a file
func testWritebackPartialWrites(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_partial.bin"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create file with initial content
	initial := bytes.Repeat([]byte("A"), 4096)
	require.NoError(t, os.WriteFile(mountPath, initial, 0644))
	waitForFileContent(t, mountPath, initial, 30*time.Second)

	// Open and write at specific offset
	f, err := os.OpenFile(mountPath, os.O_WRONLY, 0644)
	require.NoError(t, err)
	patch := []byte("PATCHED")
	_, err = f.WriteAt(patch, 100)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Build expected content
	expected := make([]byte, 4096)
	copy(expected, initial)
	copy(expected[100:], patch)

	waitForFileContent(t, mountPath, expected, 30*time.Second)
}

// testWritebackFileSizeCorrectness verifies that file sizes are correct
// after async flush completes.
func testWritebackFileSizeCorrectness(t *testing.T, framework *FuseTestFramework) {
	sizes := []int{0, 1, 100, 4096, 65536, 1024 * 1024}

	for _, size := range sizes {
		filename := fmt.Sprintf("writeback_size_%d.bin", size)
		mountPath := filepath.Join(framework.GetMountPoint(), filename)

		content := make([]byte, size)
		if size > 0 {
			_, err := rand.Read(content)
			require.NoError(t, err, "rand.Read failed for size %d", size)
		}

		require.NoError(t, os.WriteFile(mountPath, content, 0644), "failed to write file of size %d", size)

		if size > 0 {
			waitForFileSize(t, mountPath, int64(size), 30*time.Second)
			waitForFileContent(t, mountPath, content, 30*time.Second)
		}
	}
}

// testWritebackBinaryData verifies that arbitrary binary data (including null bytes)
// is preserved correctly through the async flush path.
func testWritebackBinaryData(t *testing.T, framework *FuseTestFramework) {
	filename := "writeback_binary.bin"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Generate data with all byte values including nulls
	content := make([]byte, 256*100)
	for i := range content {
		content[i] = byte(i % 256)
	}

	require.NoError(t, os.WriteFile(mountPath, content, 0644))
	waitForFileContent(t, mountPath, content, 30*time.Second)
}

// TestWritebackCachePerformance measures whether writebackCache actually
// improves throughput for small file workloads compared to synchronous flush.
func TestWritebackCachePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	numFiles := 200
	fileSize := 4096 // 4KB files

	// Generate test data upfront
	testData := make([][]byte, numFiles)
	for i := range testData {
		testData[i] = make([]byte, fileSize)
		rand.Read(testData[i])
	}

	// Benchmark with writebackCache enabled
	t.Run("WithWritebackCache", func(t *testing.T) {
		config := writebackConfig()
		framework := NewFuseTestFramework(t, config)
		defer framework.Cleanup()
		require.NoError(t, framework.Setup(config))

		dir := "perf_writeback"
		framework.CreateTestDir(dir)

		start := time.Now()
		for i := 0; i < numFiles; i++ {
			path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%04d.bin", i))
			require.NoError(t, os.WriteFile(path, testData[i], 0644))
		}
		writebackDuration := time.Since(start)

		// Wait for all files to be flushed
		for i := 0; i < numFiles; i++ {
			path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%04d.bin", i))
			waitForFileContent(t, path, testData[i], 60*time.Second)
		}

		t.Logf("writebackCache: wrote %d files in %v (%.0f files/sec)",
			numFiles, writebackDuration, float64(numFiles)/writebackDuration.Seconds())
	})

	// Benchmark without writebackCache (synchronous flush)
	t.Run("WithoutWritebackCache", func(t *testing.T) {
		config := DefaultTestConfig()
		framework := NewFuseTestFramework(t, config)
		defer framework.Cleanup()
		require.NoError(t, framework.Setup(config))

		dir := "perf_sync"
		framework.CreateTestDir(dir)

		start := time.Now()
		for i := 0; i < numFiles; i++ {
			path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%04d.bin", i))
			require.NoError(t, os.WriteFile(path, testData[i], 0644))
		}
		syncDuration := time.Since(start)

		t.Logf("synchronous: wrote %d files in %v (%.0f files/sec)",
			numFiles, syncDuration, float64(numFiles)/syncDuration.Seconds())
	})
}

// TestWritebackCacheConcurrentMixedOps tests a mix of operations happening
// concurrently with writebackCache: creates, reads, overwrites, and deletes.
func TestWritebackCacheConcurrentMixedOps(t *testing.T) {
	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	dir := "writeback_mixed"
	framework.CreateTestDir(dir)

	numFiles := 50
	var mu sync.Mutex
	var errors []error
	var completedWrites int64

	addError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errors = append(errors, err)
	}

	// Phase 1: Create initial files and wait for async flushes
	initialContents := make(map[int][]byte, numFiles)
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%03d.txt", i))
		content := []byte(fmt.Sprintf("initial content %d", i))
		require.NoError(t, os.WriteFile(path, content, 0644))
		initialContents[i] = content
	}

	// Poll until initial files are flushed (instead of fixed sleep)
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%03d.txt", i))
		waitForFileContent(t, path, initialContents[i], 30*time.Second)
	}

	// Phase 2: Concurrent mixed operations
	var wg sync.WaitGroup

	// Writers: overwrite existing files
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numFiles; j++ {
				if j%4 != workerID {
					continue // each worker handles a subset
				}
				path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%03d.txt", j))
				content := []byte(fmt.Sprintf("overwritten by worker %d at %s", workerID, time.Now().Format(time.RFC3339Nano)))
				if err := os.WriteFile(path, content, 0644); err != nil {
					addError(fmt.Errorf("writer %d file %d: %v", workerID, j, err))
					return
				}
				atomic.AddInt64(&completedWrites, 1)
			}
		}(i)
	}

	// Readers: read files (may see old or new content, but should not error)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < numFiles; j++ {
				path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("file_%03d.txt", j))
				_, err := os.ReadFile(path)
				if err != nil && !os.IsNotExist(err) {
					addError(fmt.Errorf("reader %d file %d: %v", readerID, j, err))
					return
				}
			}
		}(i)
	}

	// New file creators
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("new_file_%03d.txt", i))
			content := []byte(fmt.Sprintf("new file %d", i))
			if err := os.WriteFile(path, content, 0644); err != nil {
				addError(fmt.Errorf("creator file %d: %v", i, err))
				return
			}
		}
	}()

	wg.Wait()

	require.Empty(t, errors, "mixed operation errors: %v", errors)
	assert.True(t, atomic.LoadInt64(&completedWrites) > 0, "should have completed some writes")

	// Verify new files exist after async flushes complete (poll instead of fixed sleep)
	for i := 0; i < 20; i++ {
		path := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("new_file_%03d.txt", i))
		expected := []byte(fmt.Sprintf("new file %d", i))
		waitForFileContent(t, path, expected, 30*time.Second)
	}
}

// TestWritebackCacheStressSmallFiles is a focused stress test for the
// async flush path with many small files — the core scenario from #8718.
func TestWritebackCacheStressSmallFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	config := writebackConfig()
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	dir := "writeback_stress"
	framework.CreateTestDir(dir)

	numWorkers := 16
	filesPerWorker := 100
	totalFiles := numWorkers * filesPerWorker

	type fileRecord struct {
		path    string
		content []byte
	}

	var mu sync.Mutex
	var writeErrors []error
	records := make([]fileRecord, 0, totalFiles)

	start := time.Now()

	// Simulate rsync-like workload: many workers each writing small files
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for f := 0; f < filesPerWorker; f++ {
				filename := fmt.Sprintf("w%02d/f%04d.dat", workerID, f)
				path := filepath.Join(framework.GetMountPoint(), dir, filename)

				// Ensure subdirectory exists
				if f == 0 {
					subDir := filepath.Join(framework.GetMountPoint(), dir, fmt.Sprintf("w%02d", workerID))
					if err := os.MkdirAll(subDir, 0755); err != nil {
						mu.Lock()
						writeErrors = append(writeErrors, fmt.Errorf("worker %d mkdir: %v", workerID, err))
						mu.Unlock()
						return
					}
				}

				// Small file: 1KB-10KB (typical for rsync of config/source files)
				size := 1024 + (f%10)*1024
				content := make([]byte, size)
				if _, err := rand.Read(content); err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, fmt.Errorf("worker %d file %d rand: %v", workerID, f, err))
					mu.Unlock()
					return
				}

				if err := os.WriteFile(path, content, 0644); err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, fmt.Errorf("worker %d file %d: %v", workerID, f, err))
					mu.Unlock()
					return
				}

				mu.Lock()
				records = append(records, fileRecord{path: path, content: content})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	writeDuration := time.Since(start)
	t.Logf("wrote %d files in %v (%.0f files/sec)",
		totalFiles, writeDuration, float64(totalFiles)/writeDuration.Seconds())

	require.Empty(t, writeErrors, "write errors: %v", writeErrors)
	assert.Equal(t, totalFiles, len(records))

	// Verify all files are eventually readable with correct content
	var verifyErrors []error
	for _, rec := range records {
		deadline := time.Now().Add(120 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			actual, err := os.ReadFile(rec.path)
			if err == nil && bytes.Equal(rec.content, actual) {
				lastErr = nil
				break
			}
			if err != nil {
				lastErr = err
			} else {
				lastErr = fmt.Errorf("content mismatch for %s: got %d bytes, want %d", rec.path, len(actual), len(rec.content))
			}
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			verifyErrors = append(verifyErrors, lastErr)
		}
	}
	require.Empty(t, verifyErrors, "verification errors after stress test: %v", verifyErrors)

	t.Logf("all %d files verified successfully", totalFiles)
}
