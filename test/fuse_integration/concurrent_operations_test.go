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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentFileOperations tests concurrent file operations
func TestConcurrentFileOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("ConcurrentFileWrites", func(t *testing.T) {
		testConcurrentFileWrites(t, framework)
	})

	t.Run("ConcurrentFileReads", func(t *testing.T) {
		testConcurrentFileReads(t, framework)
	})

	t.Run("ConcurrentReadWrite", func(t *testing.T) {
		testConcurrentReadWrite(t, framework)
	})

	t.Run("ConcurrentDirectoryOperations", func(t *testing.T) {
		testConcurrentDirectoryOperations(t, framework)
	})

	t.Run("ConcurrentFileCreation", func(t *testing.T) {
		testConcurrentFileCreation(t, framework)
	})
}

// testConcurrentFileWrites tests multiple goroutines writing to different files
func testConcurrentFileWrites(t *testing.T, framework *FuseTestFramework) {
	numWorkers := 10
	filesPerWorker := 5
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)

	// Function to collect errors safely
	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	// Start concurrent workers
	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for file := 0; file < filesPerWorker; file++ {
				filename := fmt.Sprintf("worker_%d_file_%d.txt", workerID, file)
				content := []byte(fmt.Sprintf("Worker %d, File %d - %s", workerID, file, time.Now().String()))

				mountPath := filepath.Join(framework.GetMountPoint(), filename)
				if err := os.WriteFile(mountPath, content, 0644); err != nil {
					addError(fmt.Errorf("worker %d file %d: %v", workerID, file, err))
					return
				}

				// Verify file was written correctly
				readContent, err := os.ReadFile(mountPath)
				if err != nil {
					addError(fmt.Errorf("worker %d file %d read: %v", workerID, file, err))
					return
				}

				if !bytes.Equal(content, readContent) {
					addError(fmt.Errorf("worker %d file %d: content mismatch", workerID, file))
					return
				}
			}
		}(worker)
	}

	wg.Wait()

	// Check for errors
	require.Empty(t, errors, "Concurrent writes failed: %v", errors)

	// Verify all files exist and have correct content
	for worker := 0; worker < numWorkers; worker++ {
		for file := 0; file < filesPerWorker; file++ {
			filename := fmt.Sprintf("worker_%d_file_%d.txt", worker, file)
			framework.AssertFileExists(filename)
		}
	}
}

// testConcurrentFileReads tests multiple goroutines reading from the same file
func testConcurrentFileReads(t *testing.T, framework *FuseTestFramework) {
	// Create a test file
	filename := "concurrent_read_test.txt"
	testData := make([]byte, 1024*1024) // 1MB
	_, err := rand.Read(testData)
	require.NoError(t, err)

	framework.CreateTestFile(filename, testData)

	numReaders := 20
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)

	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	// Start concurrent readers
	for reader := 0; reader < numReaders; reader++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			mountPath := filepath.Join(framework.GetMountPoint(), filename)

			// Read multiple times
			for i := 0; i < 3; i++ {
				readData, err := os.ReadFile(mountPath)
				if err != nil {
					addError(fmt.Errorf("reader %d iteration %d: %v", readerID, i, err))
					return
				}

				if !bytes.Equal(testData, readData) {
					addError(fmt.Errorf("reader %d iteration %d: data mismatch", readerID, i))
					return
				}
			}
		}(reader)
	}

	wg.Wait()
	require.Empty(t, errors, "Concurrent reads failed: %v", errors)
}

// testConcurrentReadWrite tests simultaneous read and write operations
func testConcurrentReadWrite(t *testing.T, framework *FuseTestFramework) {
	filename := "concurrent_rw_test.txt"
	initialData := bytes.Repeat([]byte("INITIAL"), 1000)
	framework.CreateTestFile(filename, initialData)

	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)

	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Start readers
	numReaders := 5
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				_, err := os.ReadFile(mountPath)
				if err != nil {
					addError(fmt.Errorf("reader %d: %v", readerID, err))
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Start writers
	numWriters := 2
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				newData := bytes.Repeat([]byte(fmt.Sprintf("WRITER%d", writerID)), 1000)
				err := os.WriteFile(mountPath, newData, 0644)
				if err != nil {
					addError(fmt.Errorf("writer %d: %v", writerID, err))
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	require.Empty(t, errors, "Concurrent read/write failed: %v", errors)

	// Verify file still exists and is readable
	framework.AssertFileExists(filename)
}

// testConcurrentDirectoryOperations tests concurrent directory operations
func testConcurrentDirectoryOperations(t *testing.T, framework *FuseTestFramework) {
	numWorkers := 8
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)

	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	// Each worker creates a directory tree
	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create worker directory
			workerDir := fmt.Sprintf("worker_%d", workerID)
			mountPath := filepath.Join(framework.GetMountPoint(), workerDir)

			if err := os.Mkdir(mountPath, 0755); err != nil {
				addError(fmt.Errorf("worker %d mkdir: %v", workerID, err))
				return
			}

			// Create subdirectories and files
			for i := 0; i < 5; i++ {
				subDir := filepath.Join(mountPath, fmt.Sprintf("subdir_%d", i))
				if err := os.Mkdir(subDir, 0755); err != nil {
					addError(fmt.Errorf("worker %d subdir %d: %v", workerID, i, err))
					return
				}

				// Create file in subdirectory
				testFile := filepath.Join(subDir, "test.txt")
				content := []byte(fmt.Sprintf("Worker %d, Subdir %d", workerID, i))
				if err := os.WriteFile(testFile, content, 0644); err != nil {
					addError(fmt.Errorf("worker %d file %d: %v", workerID, i, err))
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	require.Empty(t, errors, "Concurrent directory operations failed: %v", errors)

	// Verify all structures were created
	for worker := 0; worker < numWorkers; worker++ {
		workerDir := fmt.Sprintf("worker_%d", worker)
		mountPath := filepath.Join(framework.GetMountPoint(), workerDir)

		info, err := os.Stat(mountPath)
		require.NoError(t, err)
		assert.True(t, info.IsDir())

		// Check subdirectories
		for i := 0; i < 5; i++ {
			subDir := filepath.Join(mountPath, fmt.Sprintf("subdir_%d", i))
			info, err := os.Stat(subDir)
			require.NoError(t, err)
			assert.True(t, info.IsDir())

			testFile := filepath.Join(subDir, "test.txt")
			expectedContent := []byte(fmt.Sprintf("Worker %d, Subdir %d", worker, i))
			actualContent, err := os.ReadFile(testFile)
			require.NoError(t, err)
			assert.Equal(t, expectedContent, actualContent)
		}
	}
}

// testConcurrentFileCreation tests concurrent creation of files in same directory
func testConcurrentFileCreation(t *testing.T, framework *FuseTestFramework) {
	// Create test directory
	testDir := "concurrent_creation"
	framework.CreateTestDir(testDir)

	numWorkers := 15
	filesPerWorker := 10
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)
	createdFiles := make(map[string]bool)

	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	addFile := func(filename string) {
		mutex.Lock()
		defer mutex.Unlock()
		createdFiles[filename] = true
	}

	// Create files concurrently
	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for file := 0; file < filesPerWorker; file++ {
				filename := fmt.Sprintf("file_%d_%d.txt", workerID, file)
				relativePath := filepath.Join(testDir, filename)
				mountPath := filepath.Join(framework.GetMountPoint(), relativePath)

				content := []byte(fmt.Sprintf("Worker %d, File %d, Time: %s",
					workerID, file, time.Now().Format(time.RFC3339Nano)))

				if err := os.WriteFile(mountPath, content, 0644); err != nil {
					addError(fmt.Errorf("worker %d file %d: %v", workerID, file, err))
					return
				}

				addFile(filename)
			}
		}(worker)
	}

	wg.Wait()
	require.Empty(t, errors, "Concurrent file creation failed: %v", errors)

	// Verify all files were created
	expectedCount := numWorkers * filesPerWorker
	assert.Equal(t, expectedCount, len(createdFiles))

	// Read directory and verify count
	mountPath := filepath.Join(framework.GetMountPoint(), testDir)
	entries, err := os.ReadDir(mountPath)
	require.NoError(t, err)
	assert.Equal(t, expectedCount, len(entries))

	// Verify each file exists and has content
	for filename := range createdFiles {
		relativePath := filepath.Join(testDir, filename)
		framework.AssertFileExists(relativePath)
	}
}

// TestStressOperations tests high-load scenarios
func TestStressOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("HighFrequencySmallWrites", func(t *testing.T) {
		testHighFrequencySmallWrites(t, framework)
	})

	t.Run("ManySmallFiles", func(t *testing.T) {
		testManySmallFiles(t, framework)
	})
}

// testHighFrequencySmallWrites tests many small writes to the same file
func testHighFrequencySmallWrites(t *testing.T, framework *FuseTestFramework) {
	filename := "high_freq_writes.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Open file for writing
	file, err := os.OpenFile(mountPath, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer file.Close()

	// Perform many small writes
	numWrites := 1000
	writeSize := 100

	for i := 0; i < numWrites; i++ {
		data := []byte(fmt.Sprintf("Write %04d: %s\n", i, bytes.Repeat([]byte("x"), writeSize-20)))
		_, err := file.Write(data)
		require.NoError(t, err)
	}
	file.Close()

	// Verify file size
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, totalSize, info.Size())
}

// testManySmallFiles tests creating many small files
func testManySmallFiles(t *testing.T, framework *FuseTestFramework) {
	testDir := "many_small_files"
	framework.CreateTestDir(testDir)

	numFiles := 500
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]error, 0)

	addError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		errors = append(errors, err)
	}

	// Create files in batches
	batchSize := 50
	for batch := 0; batch < numFiles/batchSize; batch++ {
		wg.Add(1)
		go func(batchID int) {
			defer wg.Done()

			for i := 0; i < batchSize; i++ {
				fileNum := batchID*batchSize + i
				filename := filepath.Join(testDir, fmt.Sprintf("small_file_%04d.txt", fileNum))
				content := []byte(fmt.Sprintf("File %d content", fileNum))

				mountPath := filepath.Join(framework.GetMountPoint(), filename)
				if err := os.WriteFile(mountPath, content, 0644); err != nil {
					addError(fmt.Errorf("file %d: %v", fileNum, err))
					return
				}
			}
		}(batch)
	}

	wg.Wait()
	require.Empty(t, errors, "Many small files creation failed: %v", errors)

	// Verify directory listing
	mountPath := filepath.Join(framework.GetMountPoint(), testDir)
	entries, err := os.ReadDir(mountPath)
	require.NoError(t, err)
	assert.Equal(t, numFiles, len(entries))
}
