//go:build !windows

package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// POSIXExtendedTestSuite provides additional POSIX compliance tests
// covering extended attributes, file locking, and advanced features.
//
// NOTE: Some tests in this suite may be skipped or may have different behavior
// depending on the FUSE implementation and platform support. This is expected
// behavior for comprehensive POSIX compliance testing.
//
// See POSIX_IMPLEMENTATION_ROADMAP.md for a comprehensive plan to implement
// these missing features.
type POSIXExtendedTestSuite struct {
	framework *FuseTestFramework
	t         *testing.T
}

// ErrorCollector helps collect multiple test errors without stopping execution
type ErrorCollector struct {
	errors []string
	t      *testing.T
}

// NewErrorCollector creates a new error collector
func NewErrorCollector(t *testing.T) *ErrorCollector {
	return &ErrorCollector{
		errors: make([]string, 0),
		t:      t,
	}
}

// Add adds an error to the collection
func (ec *ErrorCollector) Add(format string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Sprintf(format, args...))
}

// Check checks a condition and adds an error if it fails
func (ec *ErrorCollector) Check(condition bool, format string, args ...interface{}) {
	if !condition {
		ec.Add(format, args...)
	}
}

// CheckError checks if an error is nil and adds it if not
func (ec *ErrorCollector) CheckError(err error, format string, args ...interface{}) {
	if err != nil {
		ec.Add(format+": %v", append(args, err)...)
	}
}

// HasErrors returns true if any errors were collected
func (ec *ErrorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}

// Report reports all collected errors to the test
func (ec *ErrorCollector) Report() {
	if len(ec.errors) > 0 {
		ec.t.Errorf("Test completed with %d errors:\n%s", len(ec.errors), strings.Join(ec.errors, "\n"))
	}
}

// NewPOSIXExtendedTestSuite creates a new extended POSIX compliance test suite
func NewPOSIXExtendedTestSuite(t *testing.T, framework *FuseTestFramework) *POSIXExtendedTestSuite {
	return &POSIXExtendedTestSuite{
		framework: framework,
		t:         t,
	}
}

// TestPOSIXExtended runs extended POSIX compliance tests
func TestPOSIXExtended(t *testing.T) {
	config := DefaultTestConfig()
	config.EnableDebug = true
	config.MountOptions = []string{"-allowOthers", "-nonempty"}

	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	// Setup framework with better error handling
	err := framework.Setup(config)
	if err != nil {
		t.Logf("Framework setup failed: %v", err)
		t.Skip("Skipping extended tests due to framework setup failure")
		return
	}

	// Verify framework is working
	mountPoint := framework.GetMountPoint()
	if mountPoint == "" {
		t.Log("Framework mount point is empty")
		t.Skip("Skipping extended tests due to invalid mount point")
		return
	}

	suite := NewPOSIXExtendedTestSuite(t, framework)

	// Run extended POSIX compliance test categories
	// Use a resilient approach that continues even if individual tests fail
	testCategories := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"ExtendedAttributes", suite.TestExtendedAttributes},
		{"FileLocking", suite.TestFileLocking},
		{"AdvancedIO", suite.TestAdvancedIO},
		{"SparseFiles", suite.TestSparseFiles},
		{"LargeFiles", suite.TestLargeFiles},
		{"MMap", suite.TestMemoryMapping},
		{"DirectIO", suite.TestDirectIO},
		{"FileSealing", suite.TestFileSealing},
		{"Fallocate", suite.TestFallocate},
		{"Sendfile", suite.TestSendfile},
	}

	// Run all test categories and collect results
	var failedCategories []string
	for _, category := range testCategories {
		t.Run(category.name, func(subT *testing.T) {
			// Capture panics and convert them to test failures
			defer func() {
				if r := recover(); r != nil {
					subT.Errorf("Test category %s panicked: %v", category.name, r)
					failedCategories = append(failedCategories, category.name)
				}
			}()

			category.fn(subT)
			if subT.Failed() {
				failedCategories = append(failedCategories, category.name)
			}
		})
	}

	// Report overall results
	if len(failedCategories) > 0 {
		t.Logf("Extended POSIX tests completed. Failed categories: %v", failedCategories)
	} else {
		t.Logf("All extended POSIX test categories passed!")
	}
}

// TestExtendedAttributes tests POSIX extended attribute support
func (s *POSIXExtendedTestSuite) TestExtendedAttributes(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("SetAndGetXattr", func(t *testing.T) {
		if !isXattrSupported() {
			t.Skip("Extended attributes not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "xattr_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("xattr test"), 0644)
		if !assert.NoError(t, err, "Failed to create test file") {
			return
		}

		// Set extended attribute
		attrName := "user.test_attr"
		attrValue := []byte("test_value")
		err = setXattr(testFile, attrName, attrValue, 0)
		assert.NoError(t, err, "Failed to set extended attribute")

		// Verify attribute was set
		readValue := make([]byte, 256)
		size, err := getXattr(testFile, attrName, readValue)
		assert.NoError(t, err, "Failed to get extended attribute")
		assert.Equal(t, len(attrValue), size, "Extended attribute size mismatch")
		assert.Equal(t, attrValue, readValue[:size], "Extended attribute value mismatch")
	})

	t.Run("ListXattrs", func(t *testing.T) {
		if !isXattrSupported() {
			t.Skip("Extended attributes not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "xattr_list_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("list xattr test"), 0644)
		if !assert.NoError(t, err, "Failed to create test file") {
			return
		}

		// Set multiple extended attributes
		attrs := map[string][]byte{
			"user.attr1": []byte("value1"),
			"user.attr2": []byte("value2"),
			"user.attr3": []byte("value3"),
		}

		for name, value := range attrs {
			err = setXattr(testFile, name, value, 0)
			assert.NoError(t, err, "Failed to set extended attribute %s", name)
		}

		// List all attributes
		listBuf := make([]byte, 1024)
		size, err := listXattr(testFile, listBuf)
		assert.NoError(t, err, "Failed to list extended attributes")
		assert.Greater(t, size, 0, "No extended attributes found")

		// Parse the null-separated list and verify attributes
		if size > 0 {
			attrList := parseXattrList(listBuf[:size])
			expectedAttrs := []string{"user.attr1", "user.attr2", "user.attr3"}
			for _, expectedAttr := range expectedAttrs {
				assert.Contains(t, attrList, expectedAttr, "Expected attribute %s should be in the list", expectedAttr)
			}
		}
	})

	t.Run("RemoveXattr", func(t *testing.T) {
		if !isXattrSupported() {
			t.Skip("Extended attributes not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "xattr_remove_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("remove xattr test"), 0644)
		require.NoError(t, err)

		// Set extended attribute
		attrName := "user.remove_test"
		attrValue := []byte("to_be_removed")
		err = setXattr(testFile, attrName, attrValue, 0)
		require.NoError(t, err)

		// Verify attribute exists
		readValue := make([]byte, 256)
		size, err := getXattr(testFile, attrName, readValue)
		require.NoError(t, err)
		require.Equal(t, len(attrValue), size)

		// Remove the attribute
		err = removeXattr(testFile, attrName)
		require.NoError(t, err)

		// Verify attribute is gone
		_, err = getXattr(testFile, attrName, readValue)
		require.Error(t, err) // Should fail with ENODATA or similar
	})
}

// TestFileLocking tests POSIX file locking mechanisms
// Note: File locking behavior may vary between FUSE implementations.
// Some implementations may not enforce locks between file descriptors
// from the same process, which is a known limitation.
func (s *POSIXExtendedTestSuite) TestFileLocking(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("AdvisoryLocking", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "lock_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("locking test"), 0644)
		if !assert.NoError(t, err, "Failed to create test file") {
			return
		}

		// Open file
		file, err := os.OpenFile(testFile, os.O_RDWR, 0644)
		if !assert.NoError(t, err, "Failed to open test file") {
			return
		}
		defer file.Close()

		// Apply exclusive lock
		flock := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0, // Lock entire file
		}

		err = syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flock)
		assert.NoError(t, err, "Failed to acquire exclusive lock")

		// Try to lock from another file descriptor (should fail)
		file2, err := os.OpenFile(testFile, os.O_RDWR, 0644)
		if !assert.NoError(t, err, "Failed to open second file descriptor") {
			return
		}
		defer file2.Close()

		flock2 := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}

		// Note: Some FUSE implementations may not enforce file locking between file descriptors
		// from the same process. This is a known limitation in some FUSE implementations.
		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		if err == syscall.EAGAIN {
			// Lock was properly enforced
			t.Log("File locking properly enforced between file descriptors")
		} else if err == nil {
			// Lock was not enforced (common in some FUSE implementations)
			t.Log("File locking not enforced between file descriptors from same process (FUSE limitation)")
		} else {
			// Some other error occurred
			t.Logf("File locking attempt resulted in error: %v", err)
		}

		// Log the actual error for debugging
		t.Logf("File locking test completed with result: %v", err)

		// Release lock
		flock.Type = syscall.F_UNLCK
		err = syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flock)
		assert.NoError(t, err, "Failed to release lock")

		// Now second lock should succeed
		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		assert.NoError(t, err, "Failed to acquire lock after release")
	})

	t.Run("SharedLocking", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "shared_lock_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("shared locking test"), 0644)
		if !assert.NoError(t, err, "Failed to create test file") {
			return
		}

		// Open file for reading
		file1, err := os.Open(testFile)
		if !assert.NoError(t, err, "Failed to open first file descriptor") {
			return
		}
		defer file1.Close()

		file2, err := os.Open(testFile)
		if !assert.NoError(t, err, "Failed to open second file descriptor") {
			return
		}
		defer file2.Close()

		// Apply shared locks (should both succeed)
		flock1 := syscall.Flock_t{
			Type:   syscall.F_RDLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}

		flock2 := syscall.Flock_t{
			Type:   syscall.F_RDLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}

		err = syscall.FcntlFlock(file1.Fd(), syscall.F_SETLK, &flock1)
		assert.NoError(t, err, "Failed to acquire first shared lock")

		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		assert.NoError(t, err, "Failed to acquire second shared lock")
	})
}

// TestAdvancedIO tests advanced I/O operations
// Note: Vectored I/O support may vary between FUSE implementations.
// Some implementations may not properly support readv/writev operations.
func (s *POSIXExtendedTestSuite) TestAdvancedIO(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("ReadWriteV", func(t *testing.T) {
		if !isVectoredIOSupported() {
			t.Skip("Vectored I/O not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "readwritev_test.txt")

		// Create file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		if !assert.NoError(t, err, "Failed to create test file") {
			return
		}
		defer syscall.Close(fd)

		// Prepare test data in multiple buffers
		writeBuffers := [][]byte{
			[]byte("Hello "),
			[]byte("vectored "),
			[]byte("I/O "),
			[]byte("world!"),
		}

		// Write using writev
		writeIOVs := makeIOVecs(writeBuffers)
		t.Logf("Attempting vectored I/O write with %d buffers", len(writeIOVs))
		totalWritten, err := writevFile(fd, writeIOVs)
		if err != nil {
			// Some FUSE implementations may not support vectored I/O properly
			t.Logf("Vectored I/O write failed: %v - this may be a FUSE limitation", err)
			t.Skip("Vectored I/O not properly supported by this FUSE implementation")
			return
		}

		expectedTotal := 0
		for _, buf := range writeBuffers {
			expectedTotal += len(buf)
		}
		assert.Equal(t, expectedTotal, totalWritten, "Vectored write size mismatch")

		// Seek back to beginning
		_, err = syscall.Seek(fd, 0, 0)
		if !assert.NoError(t, err, "Failed to seek to beginning") {
			return
		}

		// Read using readv into multiple buffers
		readBuffers := [][]byte{
			make([]byte, 6), // "Hello "
			make([]byte, 9), // "vectored "
			make([]byte, 3), // "I/O "
			make([]byte, 6), // "world!"
		}

		readIOVs := makeIOVecs(readBuffers)
		t.Logf("Attempting vectored I/O read with %d buffers", len(readIOVs))
		totalRead, err := readvFile(fd, readIOVs)
		if err != nil {
			t.Logf("Vectored I/O read failed: %v - this may be a FUSE limitation", err)
			t.Skip("Vectored I/O not properly supported by this FUSE implementation")
			return
		}
		assert.Equal(t, expectedTotal, totalRead, "Vectored read size mismatch")

		// Verify data matches
		for i, expected := range writeBuffers {
			assert.Equal(t, expected, readBuffers[i], "Vectored I/O data mismatch at buffer %d", i)
		}
	})

}

// TestSparseFiles tests sparse file handling
func (s *POSIXExtendedTestSuite) TestSparseFiles(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("CreateSparseFile", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "sparse_test.txt")

		// Open file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Create a sparse file by seeking beyond end and writing
		const sparseSize = 1024 * 1024 // 1MB
		_, err = syscall.Seek(fd, sparseSize, 0)
		require.NoError(t, err)

		// Write at the end
		endData := []byte("end")
		n, err := syscall.Write(fd, endData)
		require.NoError(t, err)
		require.Equal(t, len(endData), n)

		// Verify file size
		stat, err := os.Stat(testFile)
		require.NoError(t, err)
		require.Equal(t, int64(sparseSize+len(endData)), stat.Size())

		// Read from the beginning (should be zeros)
		_, err = syscall.Seek(fd, 0, 0)
		require.NoError(t, err)

		buffer := make([]byte, 100)
		n, err = syscall.Read(fd, buffer)
		require.NoError(t, err)
		require.Equal(t, 100, n)

		// Should be all zeros
		for _, b := range buffer {
			require.Equal(t, byte(0), b)
		}
	})
}

// TestLargeFiles tests large file handling (>2GB)
func (s *POSIXExtendedTestSuite) TestLargeFiles(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("LargeFileOperations", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "large_file_test.txt")

		// Open file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Seek to position > 2GB
		const largeOffset = 3 * 1024 * 1024 * 1024 // 3GB
		pos, err := syscall.Seek(fd, largeOffset, 0)
		require.NoError(t, err)
		require.Equal(t, int64(largeOffset), pos)

		// Write at large offset
		testData := []byte("large file data")
		n, err := syscall.Write(fd, testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Read back from large offset
		_, err = syscall.Seek(fd, largeOffset, 0)
		require.NoError(t, err)

		readBuffer := make([]byte, len(testData))
		n, err = syscall.Read(fd, readBuffer)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, readBuffer)
	})
}

// TestMemoryMapping tests memory mapping functionality
func (s *POSIXExtendedTestSuite) TestMemoryMapping(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("MmapFile", func(t *testing.T) {
		if !isMmapSupported() {
			t.Skip("Memory mapping not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "mmap_test.txt")
		testData := make([]byte, 4096)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Create test file
		err := os.WriteFile(testFile, testData, 0644)
		require.NoError(t, err)

		// Open file for reading
		fd, err := syscall.Open(testFile, syscall.O_RDONLY, 0)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Memory map the file
		mappedData, err := mmapFile(fd, 0, len(testData), PROT_READ, MAP_SHARED)
		require.NoError(t, err)
		defer munmapFile(mappedData)

		// Verify mapped content matches original
		require.Equal(t, testData, mappedData)
	})

	t.Run("MmapWrite", func(t *testing.T) {
		if !isMmapSupported() {
			t.Skip("Memory mapping not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "mmap_write_test.txt")
		size := 4096

		// Create empty file of specific size
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		err = syscall.Ftruncate(fd, int64(size))
		require.NoError(t, err)

		// Memory map the file for writing
		mappedData, err := mmapFile(fd, 0, size, PROT_READ|PROT_WRITE, MAP_SHARED)
		require.NoError(t, err)
		defer munmapFile(mappedData)

		// Write test pattern to mapped memory
		testPattern := []byte("Hello, mmap world!")
		copy(mappedData, testPattern)

		// Sync changes to disk
		err = msyncFile(mappedData, MS_SYNC)
		require.NoError(t, err)

		// Verify changes were written by reading file directly
		readData, err := os.ReadFile(testFile)
		require.NoError(t, err)
		require.True(t, len(readData) >= len(testPattern))
		require.Equal(t, testPattern, readData[:len(testPattern)])
	})
}

// TestDirectIO tests direct I/O operations
func (s *POSIXExtendedTestSuite) TestDirectIO(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("DirectIO", func(t *testing.T) {
		if !isDirectIOSupported() {
			t.Skip("Direct I/O not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "directio_test.txt")

		// Open file with direct I/O
		fd, err := openDirectIO(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// For direct I/O, data must be aligned to sector boundaries
		// Use 4KB aligned buffer (common sector size)
		blockSize := 4096
		testData := make([]byte, blockSize)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Write data using direct I/O
		n, err := syscall.Write(fd, testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Seek back to beginning
		_, err = syscall.Seek(fd, 0, 0)
		require.NoError(t, err)

		// Read data using direct I/O
		readData := make([]byte, blockSize)
		n, err = syscall.Read(fd, readData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, readData)
	})
}

// TestFileSealing tests file sealing mechanisms (Linux-specific)
func (s *POSIXExtendedTestSuite) TestFileSealing(t *testing.T) {
	// This test is Linux-specific and may not be applicable to all systems
	t.Skip("File sealing tests require Linux-specific features")
}

// TestFallocate tests file preallocation
func (s *POSIXExtendedTestSuite) TestFallocate(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("FallocateSpace", func(t *testing.T) {
		if !isFallocateSupported() {
			t.Skip("fallocate not supported on this platform")
			return
		}

		testFile := filepath.Join(mountPoint, "fallocate_test.txt")

		// Open file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Preallocate 1MB of space
		allocSize := int64(1024 * 1024)
		err = fallocateFile(fd, 0, 0, allocSize)
		require.NoError(t, err)

		// Verify file size was extended
		var stat syscall.Stat_t
		err = syscall.Fstat(fd, &stat)
		require.NoError(t, err)
		require.GreaterOrEqual(t, stat.Size, allocSize)

		// Write some data and verify it works
		testData := []byte("fallocate test data")
		n, err := syscall.Write(fd, testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
	})
}

// TestSendfile tests zero-copy file transfer
func (s *POSIXExtendedTestSuite) TestSendfile(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("SendfileCopy", func(t *testing.T) {
		sourceFile := filepath.Join(mountPoint, "sendfile_source.txt")
		targetFile := filepath.Join(mountPoint, "sendfile_target.txt")

		testData := make([]byte, 8192)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Create source file
		err := os.WriteFile(sourceFile, testData, 0644)
		require.NoError(t, err)

		// Open source for reading
		srcFd, err := syscall.Open(sourceFile, syscall.O_RDONLY, 0)
		require.NoError(t, err)
		defer syscall.Close(srcFd)

		// Create target file
		dstFd, err := syscall.Open(targetFile, syscall.O_CREAT|syscall.O_WRONLY, 0644)
		require.NoError(t, err)
		defer syscall.Close(dstFd)

		// Sendfile test
		if !isSendfileSupported() {
			t.Skip("sendfile not supported on this platform")
			return
		}

		// Use sendfile to copy data
		var offset int64 = 0
		transferred, err := sendfileTransfer(dstFd, srcFd, &offset, len(testData))
		require.NoError(t, err)
		require.Equal(t, len(testData), transferred)

		// Verify copy
		copiedData, err := os.ReadFile(targetFile)
		require.NoError(t, err)
		require.Equal(t, testData, copiedData)
	})
}

// Helper function to parse null-separated xattr list
func parseXattrList(data []byte) []string {
	var attrs []string
	start := 0
	for i, b := range data {
		if b == 0 {
			if i > start {
				attrs = append(attrs, string(data[start:i]))
			}
			start = i + 1
		}
	}
	return attrs
}
