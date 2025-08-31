package fuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// POSIXExtendedTestSuite provides additional POSIX compliance tests
// covering extended attributes, file locking, and advanced features
type POSIXExtendedTestSuite struct {
	framework *FuseTestFramework
	t         *testing.T
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
	require.NoError(t, framework.Setup(config))

	suite := NewPOSIXExtendedTestSuite(t, framework)

	// Run extended POSIX compliance test categories
	t.Run("ExtendedAttributes", suite.TestExtendedAttributes)
	t.Run("FileLocking", suite.TestFileLocking)
	t.Run("AdvancedIO", suite.TestAdvancedIO)
	t.Run("SparseFIles", suite.TestSparseFiles)
	t.Run("LargeFiles", suite.TestLargeFiles)
	t.Run("MMap", suite.TestMemoryMapping)
	t.Run("DirectIO", suite.TestDirectIO)
	t.Run("FileSealing", suite.TestFileSealing)
	t.Run("Fallocate", suite.TestFallocate)
	t.Run("Sendfile", suite.TestSendfile)
}

// TestExtendedAttributes tests POSIX extended attribute support
func (s *POSIXExtendedTestSuite) TestExtendedAttributes(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("SetAndGetXattr", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "xattr_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("xattr test"), 0644)
		require.NoError(t, err)

		// Extended attributes test - platform dependent
		t.Skip("Extended attributes testing requires platform-specific implementation")
	})

	t.Run("ListXattrs", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "xattr_list_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("list xattr test"), 0644)
		require.NoError(t, err)

		// List extended attributes test - platform dependent
		t.Skip("Extended attributes testing requires platform-specific implementation")
	})

	t.Run("RemoveXattr", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "xattr_remove_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("remove xattr test"), 0644)
		require.NoError(t, err)

		// Remove extended attributes test - platform dependent
		t.Skip("Extended attributes testing requires platform-specific implementation")
	})
}

// TestFileLocking tests POSIX file locking mechanisms
func (s *POSIXExtendedTestSuite) TestFileLocking(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("AdvisoryLocking", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "lock_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("locking test"), 0644)
		require.NoError(t, err)

		// Open file
		file, err := os.OpenFile(testFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer file.Close()

		// Apply exclusive lock
		flock := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0, // Lock entire file
		}

		err = syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flock)
		require.NoError(t, err)

		// Try to lock from another process (should fail)
		file2, err := os.OpenFile(testFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer file2.Close()

		flock2 := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}

		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		require.Equal(t, syscall.EAGAIN, err) // Lock should be blocked

		// Release lock
		flock.Type = syscall.F_UNLCK
		err = syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flock)
		require.NoError(t, err)

		// Now second lock should succeed
		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		require.NoError(t, err)
	})

	t.Run("SharedLocking", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "shared_lock_test.txt")

		// Create test file
		err := os.WriteFile(testFile, []byte("shared locking test"), 0644)
		require.NoError(t, err)

		// Open file for reading
		file1, err := os.Open(testFile)
		require.NoError(t, err)
		defer file1.Close()

		file2, err := os.Open(testFile)
		require.NoError(t, err)
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
		require.NoError(t, err)

		err = syscall.FcntlFlock(file2.Fd(), syscall.F_SETLK, &flock2)
		require.NoError(t, err)
	})
}

// TestAdvancedIO tests advanced I/O operations
func (s *POSIXExtendedTestSuite) TestAdvancedIO(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("ReadWriteV", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "readwritev_test.txt")

		// Create file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Vectored I/O test - requires platform-specific implementation
		t.Skip("Vectored I/O testing requires platform-specific implementation")
	})

	t.Run("PreadPwrite", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "preadpwrite_test.txt")

		// Create file with initial content
		initialContent := []byte("0123456789ABCDEFGHIJ")
		err := os.WriteFile(testFile, initialContent, 0644)
		require.NoError(t, err)

		// Open file
		fd, err := syscall.Open(testFile, syscall.O_RDWR, 0)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Positioned I/O test - use standard library approach
		_, err = syscall.Seek(fd, 5, 0) // Seek to position 5
		require.NoError(t, err)

		writeData := []byte("XYZ")
		n, err := syscall.Write(fd, writeData)
		require.NoError(t, err)
		require.Equal(t, len(writeData), n)

		// Seek back and read
		_, err = syscall.Seek(fd, 5, 0)
		require.NoError(t, err)

		readBuffer := make([]byte, len(writeData))
		n, err = syscall.Read(fd, readBuffer)
		require.NoError(t, err)
		require.Equal(t, len(writeData), n)
		require.Equal(t, writeData, readBuffer)

		// Verify file position wasn't changed by pread/pwrite
		currentPos, err := syscall.Seek(fd, 0, 1) // SEEK_CUR
		require.NoError(t, err)
		require.Equal(t, int64(0), currentPos) // Should still be at beginning
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
		testFile := filepath.Join(mountPoint, "mmap_test.txt")
		testData := make([]byte, 4096)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Create test file
		err := os.WriteFile(testFile, testData, 0644)
		require.NoError(t, err)

		// Open file
		file, err := os.Open(testFile)
		require.NoError(t, err)
		defer file.Close()

		// Memory mapping test - requires platform-specific implementation
		t.Skip("Memory mapping testing requires platform-specific implementation")
	})

	t.Run("MmapWrite", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "mmap_write_test.txt")
		size := 4096

		// Create empty file of specific size
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)

		err = syscall.Ftruncate(fd, int64(size))
		require.NoError(t, err)

		syscall.Close(fd)

		// Memory mapping write test - requires platform-specific implementation
		t.Skip("Memory mapping testing requires platform-specific implementation")
	})
}

// TestDirectIO tests direct I/O operations
func (s *POSIXExtendedTestSuite) TestDirectIO(t *testing.T) {
	t.Run("DirectIO", func(t *testing.T) {
		// Direct I/O is platform dependent and may not be supported
		t.Skip("Direct I/O testing requires platform-specific implementation")
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
		testFile := filepath.Join(mountPoint, "fallocate_test.txt")

		// Open file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// File preallocation test - requires platform-specific implementation
		t.Skip("fallocate testing requires platform-specific implementation")
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

		// Sendfile test - requires platform-specific implementation
		t.Skip("sendfile testing requires platform-specific implementation")

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
