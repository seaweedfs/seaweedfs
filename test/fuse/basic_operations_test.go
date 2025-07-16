package fuse_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicFileOperations tests fundamental FUSE file operations
func TestBasicFileOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("CreateAndReadFile", func(t *testing.T) {
		testBasicCreateAndRead(t, framework)
	})

	t.Run("WriteAndReadFile", func(t *testing.T) {
		testBasicWriteAndRead(t, framework)
	})

	t.Run("AppendToFile", func(t *testing.T) {
		testAppendToFile(t, framework)
	})

	t.Run("DeleteFile", func(t *testing.T) {
		testDeleteFile(t, framework)
	})

	t.Run("FileAttributes", func(t *testing.T) {
		testFileAttributes(t, framework)
	})

	t.Run("FilePermissions", func(t *testing.T) {
		testFilePermissions(t, framework)
	})
}

// testBasicCreateAndRead tests creating and reading a simple file
func testBasicCreateAndRead(t *testing.T, framework *FuseTestFramework) {
	content := []byte("Hello, SeaweedFS FUSE!")
	filename := "test_create_read.txt"

	// Create file
	framework.CreateTestFile(filename, content)

	// Verify file exists
	framework.AssertFileExists(filename)

	// Verify content
	framework.AssertFileContent(filename, content)
}

// testBasicWriteAndRead tests writing to and reading from a file
func testBasicWriteAndRead(t *testing.T, framework *FuseTestFramework) {
	filename := "test_write_read.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Test 1: Basic write and read
	content1 := []byte("First write operation")
	require.NoError(t, os.WriteFile(mountPath, content1, 0644))
	framework.AssertFileContent(filename, content1)

	// Test 2: Overwrite file
	content2 := []byte("Second write operation (overwrite)")
	require.NoError(t, os.WriteFile(mountPath, content2, 0644))
	framework.AssertFileContent(filename, content2)

	// Test 3: Multiple small writes
	file, err := os.OpenFile(mountPath, os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer file.Close()

	writes := [][]byte{
		[]byte("Part 1 "),
		[]byte("Part 2 "),
		[]byte("Part 3"),
	}

	for _, write := range writes {
		_, err := file.Write(write)
		require.NoError(t, err)
	}
	file.Close()

	expected := bytes.Join(writes, nil)
	framework.AssertFileContent(filename, expected)
}

// testAppendToFile tests appending data to existing files
func testAppendToFile(t *testing.T, framework *FuseTestFramework) {
	filename := "test_append.txt"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create initial file
	initialContent := []byte("Initial content\n")
	require.NoError(t, os.WriteFile(mountPath, initialContent, 0644))

	// Open file for appending
	file, err := os.OpenFile(mountPath, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer file.Close()

	// Append data
	appendData := []byte("Appended line 1\nAppended line 2\n")
	_, err = file.Write(appendData)
	require.NoError(t, err)
	file.Close()

	// Verify final content
	expected := append(initialContent, appendData...)
	framework.AssertFileContent(filename, expected)
}

// testDeleteFile tests file deletion operations
func testDeleteFile(t *testing.T, framework *FuseTestFramework) {
	filename := "test_delete.txt"
	content := []byte("This file will be deleted")

	// Create file
	framework.CreateTestFile(filename, content)
	framework.AssertFileExists(filename)

	// Delete file
	mountPath := filepath.Join(framework.GetMountPoint(), filename)
	require.NoError(t, os.Remove(mountPath))

	// Verify file is gone
	framework.AssertFileNotExists(filename)
}

// testFileAttributes tests file attribute operations
func testFileAttributes(t *testing.T, framework *FuseTestFramework) {
	filename := "test_attributes.txt"
	content := []byte("File with attributes")
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create file
	framework.CreateTestFile(filename, content)

	// Get initial attributes
	info1, err := os.Stat(mountPath)
	require.NoError(t, err)

	// Verify basic attributes
	assert.Equal(t, filename, info1.Name())
	assert.Equal(t, int64(len(content)), info1.Size())
	assert.False(t, info1.IsDir())

	// Test file timestamps
	now := time.Now()
	assert.True(t, info1.ModTime().Before(now.Add(time.Second)))
	assert.True(t, info1.ModTime().After(now.Add(-time.Minute)))

	// Modify file and check mtime update
	time.Sleep(100 * time.Millisecond) // Ensure different timestamp
	newContent := []byte("Modified content")
	require.NoError(t, os.WriteFile(mountPath, newContent, 0644))

	info2, err := os.Stat(mountPath)
	require.NoError(t, err)

	assert.Equal(t, int64(len(newContent)), info2.Size())
	assert.True(t, info2.ModTime().After(info1.ModTime()))
}

// testFilePermissions tests file permission operations
func testFilePermissions(t *testing.T, framework *FuseTestFramework) {
	filename := "test_permissions.txt"
	content := []byte("File with permissions")
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create file with specific permissions
	require.NoError(t, os.WriteFile(mountPath, content, 0600))

	// Check initial permissions
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

	// Change permissions
	require.NoError(t, os.Chmod(mountPath, 0644))

	// Verify permission change
	info, err = os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0644), info.Mode().Perm())
}

// TestLargeFileOperations tests operations on larger files
func TestLargeFileOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("LargeFileWrite", func(t *testing.T) {
		testLargeFileWrite(t, framework)
	})

	t.Run("LargeFileRead", func(t *testing.T) {
		testLargeFileRead(t, framework)
	})

	t.Run("SparseFileOperations", func(t *testing.T) {
		testSparseFileOperations(t, framework)
	})
}

// testLargeFileWrite tests writing large files (> chunk size)
func testLargeFileWrite(t *testing.T, framework *FuseTestFramework) {
	filename := "large_file.dat"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create 10MB file (larger than default chunk size)
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	// Write file in chunks
	file, err := os.Create(mountPath)
	require.NoError(t, err)
	defer file.Close()

	chunkSize := 1024 * 1024 // 1MB chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		n, err := file.Write(data[i:end])
		require.NoError(t, err)
		assert.Equal(t, end-i, n)
	}
	file.Close()

	// Verify file size
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, int64(size), info.Size())

	// Verify content
	readData, err := os.ReadFile(mountPath)
	require.NoError(t, err)
	assert.Equal(t, data, readData)
}

// testLargeFileRead tests reading large files with various patterns
func testLargeFileRead(t *testing.T, framework *FuseTestFramework) {
	filename := "large_read_test.dat"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	// Create test file with known pattern
	size := 5 * 1024 * 1024 // 5MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	require.NoError(t, os.WriteFile(mountPath, data, 0644))

	// Test 1: Sequential read
	file, err := os.Open(mountPath)
	require.NoError(t, err)
	defer file.Close()

	readData := make([]byte, size)
	n, err := io.ReadFull(file, readData)
	require.NoError(t, err)
	assert.Equal(t, size, n)
	assert.Equal(t, data, readData)

	// Test 2: Random access reads
	testOffsets := []int64{0, 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024}
	readSize := 4096

	for _, offset := range testOffsets {
		if offset+int64(readSize) > int64(size) {
			continue
		}

		_, err := file.Seek(offset, io.SeekStart)
		require.NoError(t, err)

		readChunk := make([]byte, readSize)
		n, err := file.Read(readChunk)
		require.NoError(t, err)
		assert.Equal(t, readSize, n)

		expected := data[offset : offset+int64(readSize)]
		assert.Equal(t, expected, readChunk)
	}
}

// testSparseFileOperations tests operations on sparse files
func testSparseFileOperations(t *testing.T, framework *FuseTestFramework) {
	filename := "sparse_file.dat"
	mountPath := filepath.Join(framework.GetMountPoint(), filename)

	file, err := os.Create(mountPath)
	require.NoError(t, err)
	defer file.Close()

	// Write data at various offsets to create sparse file
	writes := []struct {
		offset int64
		data   []byte
	}{
		{0, []byte("Beginning")},
		{1024 * 1024, []byte("Middle")},  // 1MB offset
		{5 * 1024 * 1024, []byte("End")}, // 5MB offset
	}

	for _, write := range writes {
		_, err := file.Seek(write.offset, io.SeekStart)
		require.NoError(t, err)

		n, err := file.Write(write.data)
		require.NoError(t, err)
		assert.Equal(t, len(write.data), n)
	}
	file.Close()

	// Verify file size
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	expectedSize := writes[len(writes)-1].offset + int64(len(writes[len(writes)-1].data))
	assert.Equal(t, expectedSize, info.Size())

	// Verify sparse reads
	file, err = os.Open(mountPath)
	require.NoError(t, err)
	defer file.Close()

	for _, write := range writes {
		_, err := file.Seek(write.offset, io.SeekStart)
		require.NoError(t, err)

		readData := make([]byte, len(write.data))
		n, err := file.Read(readData)
		require.NoError(t, err)
		assert.Equal(t, len(write.data), n)
		assert.Equal(t, write.data, readData)
	}

	// Verify zero-filled gaps
	_, err = file.Seek(512, io.SeekStart)
	require.NoError(t, err)

	gapData := make([]byte, 512)
	n, err := file.Read(gapData)
	require.NoError(t, err)
	assert.Equal(t, 512, n)

	// Should be all zeros
	expectedZeros := make([]byte, 512)
	assert.Equal(t, expectedZeros, gapData)
}
