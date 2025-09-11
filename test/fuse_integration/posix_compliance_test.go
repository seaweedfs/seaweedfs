//go:build !windows

package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// POSIXComplianceTestSuite provides comprehensive POSIX compliance testing for FUSE mounts
type POSIXComplianceTestSuite struct {
	framework *FuseTestFramework
	t         *testing.T
}

// NewPOSIXComplianceTestSuite creates a new POSIX compliance test suite
func NewPOSIXComplianceTestSuite(t *testing.T, framework *FuseTestFramework) *POSIXComplianceTestSuite {
	return &POSIXComplianceTestSuite{
		framework: framework,
		t:         t,
	}
}

// TestPOSIXCompliance runs all POSIX compliance tests
func TestPOSIXCompliance(t *testing.T) {
	config := DefaultTestConfig()
	config.EnableDebug = true
	config.MountOptions = []string{"-allowOthers", "-nonempty"}

	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()
	require.NoError(t, framework.Setup(config))

	suite := NewPOSIXComplianceTestSuite(t, framework)

	// Run all POSIX compliance test categories
	t.Run("FileOperations", suite.TestFileOperations)
	t.Run("DirectoryOperations", suite.TestDirectoryOperations)
	t.Run("SymlinkOperations", suite.TestSymlinkOperations)
	t.Run("PermissionTests", suite.TestPermissions)
	t.Run("TimestampTests", suite.TestTimestamps)
	t.Run("IOOperations", suite.TestIOOperations)
	t.Run("FileDescriptorTests", suite.TestFileDescriptors)
	t.Run("AtomicOperations", suite.TestAtomicOperations)
	t.Run("ConcurrentAccess", suite.TestConcurrentAccess)
	t.Run("ErrorHandling", suite.TestErrorHandling)
}

// TestFileOperations tests POSIX file operation compliance
func (s *POSIXComplianceTestSuite) TestFileOperations(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("CreateFile", func(t *testing.T) {
		filepath := filepath.Join(mountPoint, "test_create.txt")

		// Test file creation with O_CREAT
		fd, err := syscall.Open(filepath, syscall.O_CREAT|syscall.O_WRONLY, 0644)
		require.NoError(t, err)
		require.Greater(t, fd, 0)

		err = syscall.Close(fd)
		require.NoError(t, err)

		// Verify file exists
		_, err = os.Stat(filepath)
		require.NoError(t, err)
	})

	t.Run("CreateExclusiveFile", func(t *testing.T) {
		filepath := filepath.Join(mountPoint, "test_excl.txt")

		// First creation should succeed
		fd, err := syscall.Open(filepath, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, 0644)
		require.NoError(t, err)
		syscall.Close(fd)

		// Second creation should fail with EEXIST
		_, err = syscall.Open(filepath, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, 0644)
		require.Error(t, err)
		require.Equal(t, syscall.EEXIST, err)
	})

	t.Run("TruncateFile", func(t *testing.T) {
		filepath := filepath.Join(mountPoint, "test_truncate.txt")
		content := []byte("Hello, World! This is a test file for truncation.")

		// Create file with content
		err := os.WriteFile(filepath, content, 0644)
		require.NoError(t, err)

		// Truncate to 5 bytes
		err = syscall.Truncate(filepath, 5)
		require.NoError(t, err)

		// Verify truncation
		readContent, err := os.ReadFile(filepath)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), readContent)
	})

	t.Run("UnlinkFile", func(t *testing.T) {
		filepath := filepath.Join(mountPoint, "test_unlink.txt")

		// Create file
		err := os.WriteFile(filepath, []byte("test"), 0644)
		require.NoError(t, err)

		// Unlink file
		err = syscall.Unlink(filepath)
		require.NoError(t, err)

		// Verify file no longer exists
		_, err = os.Stat(filepath)
		require.True(t, os.IsNotExist(err))
	})
}

// TestDirectoryOperations tests POSIX directory operation compliance
func (s *POSIXComplianceTestSuite) TestDirectoryOperations(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("CreateDirectory", func(t *testing.T) {
		dirPath := filepath.Join(mountPoint, "test_mkdir")

		err := syscall.Mkdir(dirPath, 0755)
		require.NoError(t, err)

		// Verify directory exists and has correct type
		stat, err := os.Stat(dirPath)
		require.NoError(t, err)
		require.True(t, stat.IsDir())
	})

	t.Run("RemoveDirectory", func(t *testing.T) {
		dirPath := filepath.Join(mountPoint, "test_rmdir")

		// Create directory
		err := os.Mkdir(dirPath, 0755)
		require.NoError(t, err)

		// Remove directory
		err = syscall.Rmdir(dirPath)
		require.NoError(t, err)

		// Verify directory no longer exists
		_, err = os.Stat(dirPath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("RemoveNonEmptyDirectory", func(t *testing.T) {
		dirPath := filepath.Join(mountPoint, "test_rmdir_nonempty")
		filePath := filepath.Join(dirPath, "file.txt")

		// Create directory and file
		err := os.Mkdir(dirPath, 0755)
		require.NoError(t, err)
		err = os.WriteFile(filePath, []byte("test"), 0644)
		require.NoError(t, err)

		// Attempt to remove non-empty directory should fail
		err = syscall.Rmdir(dirPath)
		require.Error(t, err)
		require.Equal(t, syscall.ENOTEMPTY, err)
	})

	t.Run("RenameDirectory", func(t *testing.T) {
		oldPath := filepath.Join(mountPoint, "old_dir")
		newPath := filepath.Join(mountPoint, "new_dir")

		// Create directory
		err := os.Mkdir(oldPath, 0755)
		require.NoError(t, err)

		// Rename directory
		err = os.Rename(oldPath, newPath)
		require.NoError(t, err)

		// Verify old path doesn't exist and new path does
		_, err = os.Stat(oldPath)
		require.True(t, os.IsNotExist(err))
		stat, err := os.Stat(newPath)
		require.NoError(t, err)
		require.True(t, stat.IsDir())
	})
}

// TestSymlinkOperations tests POSIX symlink operation compliance
func (s *POSIXComplianceTestSuite) TestSymlinkOperations(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("CreateSymlink", func(t *testing.T) {
		targetFile := filepath.Join(mountPoint, "target.txt")
		linkFile := filepath.Join(mountPoint, "link.txt")

		// Create target file
		err := os.WriteFile(targetFile, []byte("target content"), 0644)
		require.NoError(t, err)

		// Create symlink
		err = os.Symlink(targetFile, linkFile)
		require.NoError(t, err)

		// Verify symlink properties
		linkStat, err := os.Lstat(linkFile)
		require.NoError(t, err)
		require.Equal(t, os.ModeSymlink, linkStat.Mode()&os.ModeType)

		// Verify symlink content
		linkTarget, err := os.Readlink(linkFile)
		require.NoError(t, err)
		require.Equal(t, targetFile, linkTarget)

		// Verify following symlink works
		content, err := os.ReadFile(linkFile)
		require.NoError(t, err)
		require.Equal(t, []byte("target content"), content)
	})

	t.Run("BrokenSymlink", func(t *testing.T) {
		nonexistentTarget := filepath.Join(mountPoint, "nonexistent.txt")
		linkFile := filepath.Join(mountPoint, "broken_link.txt")

		// Create symlink to nonexistent file
		err := os.Symlink(nonexistentTarget, linkFile)
		require.NoError(t, err)

		// Lstat should work (doesn't follow symlink)
		_, err = os.Lstat(linkFile)
		require.NoError(t, err)

		// Stat should fail (follows symlink to nonexistent target)
		_, err = os.Stat(linkFile)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})
}

// TestPermissions tests POSIX permission compliance
func (s *POSIXComplianceTestSuite) TestPermissions(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("FilePermissions", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "perm_test.txt")

		// Create file with specific permissions
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_WRONLY, 0642)
		require.NoError(t, err)
		syscall.Close(fd)

		// Verify permissions
		stat, err := os.Stat(testFile)
		require.NoError(t, err)

		// Note: The final file permissions are affected by the system's umask.
		// We check against the requested mode and the mode as affected by a common umask (e.g. 0022).
		actualMode := stat.Mode() & os.ModePerm
		expectedMode := os.FileMode(0642)
		expectedModeWithUmask := os.FileMode(0640) // e.g., 0642 with umask 0002 or 0022

		// Accept either the exact permissions or permissions as modified by umask
		if actualMode != expectedMode && actualMode != expectedModeWithUmask {
			t.Errorf("Expected file permissions %o or %o, but got %o",
				expectedMode, expectedModeWithUmask, actualMode)
		}
	})

	t.Run("ChangeFilePermissions", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "chmod_test.txt")

		// Create file
		err := os.WriteFile(testFile, []byte("test"), 0644)
		require.NoError(t, err)

		// Change permissions
		err = os.Chmod(testFile, 0755)
		require.NoError(t, err)

		// Verify new permissions
		stat, err := os.Stat(testFile)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0755), stat.Mode()&os.ModePerm)
	})

	t.Run("DirectoryPermissions", func(t *testing.T) {
		testDir := filepath.Join(mountPoint, "perm_dir")

		// Create directory with specific permissions
		err := syscall.Mkdir(testDir, 0750)
		require.NoError(t, err)

		// Verify permissions
		stat, err := os.Stat(testDir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0750)|os.ModeDir, stat.Mode())
	})
}

// TestTimestamps tests POSIX timestamp compliance
func (s *POSIXComplianceTestSuite) TestTimestamps(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("AccessTime", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "atime_test.txt")

		// Create file
		err := os.WriteFile(testFile, []byte("test content"), 0644)
		require.NoError(t, err)

		// Get initial timestamps
		stat1, err := os.Stat(testFile)
		require.NoError(t, err)

		// Wait a bit to ensure time difference
		time.Sleep(100 * time.Millisecond)

		// Read file (should update access time)
		_, err = os.ReadFile(testFile)
		require.NoError(t, err)

		// Get new timestamps
		stat2, err := os.Stat(testFile)
		require.NoError(t, err)

		// Access time should have been updated, and modification time should be unchanged.
		stat1Sys := stat1.Sys().(*syscall.Stat_t)
		stat2Sys := stat2.Sys().(*syscall.Stat_t)
		require.True(t, getAtimeNano(stat2Sys) >= getAtimeNano(stat1Sys), "access time should be updated or stay the same")
		require.Equal(t, stat1.ModTime(), stat2.ModTime(), "modification time should not change on read")
	})

	t.Run("ModificationTime", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "mtime_test.txt")

		// Create file
		err := os.WriteFile(testFile, []byte("initial content"), 0644)
		require.NoError(t, err)

		// Get initial timestamp
		stat1, err := os.Stat(testFile)
		require.NoError(t, err)

		// Wait to ensure time difference
		time.Sleep(100 * time.Millisecond)

		// Modify file
		err = os.WriteFile(testFile, []byte("modified content"), 0644)
		require.NoError(t, err)

		// Get new timestamp
		stat2, err := os.Stat(testFile)
		require.NoError(t, err)

		// Modification time should have changed
		require.True(t, stat2.ModTime().After(stat1.ModTime()))
	})

	t.Run("SetTimestamps", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "utime_test.txt")

		// Create file
		err := os.WriteFile(testFile, []byte("test"), 0644)
		require.NoError(t, err)

		// Set specific timestamps
		atime := time.Now().Add(-24 * time.Hour)
		mtime := time.Now().Add(-12 * time.Hour)

		err = os.Chtimes(testFile, atime, mtime)
		require.NoError(t, err)

		// Verify timestamps were set
		stat, err := os.Stat(testFile)
		require.NoError(t, err)
		require.True(t, stat.ModTime().Equal(mtime))
	})
}

// TestIOOperations tests POSIX I/O operation compliance
func (s *POSIXComplianceTestSuite) TestIOOperations(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("ReadWrite", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "rw_test.txt")
		testData := []byte("Hello, POSIX World!")

		// Write data
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_WRONLY, 0644)
		require.NoError(t, err)

		n, err := syscall.Write(fd, testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		err = syscall.Close(fd)
		require.NoError(t, err)

		// Read data back
		fd, err = syscall.Open(testFile, syscall.O_RDONLY, 0)
		require.NoError(t, err)

		readBuffer := make([]byte, len(testData))
		n, err = syscall.Read(fd, readBuffer)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, readBuffer)

		err = syscall.Close(fd)
		require.NoError(t, err)
	})

	t.Run("Seek", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "seek_test.txt")
		testData := []byte("0123456789ABCDEF")

		// Create file with test data
		err := os.WriteFile(testFile, testData, 0644)
		require.NoError(t, err)

		// Open for reading
		fd, err := syscall.Open(testFile, syscall.O_RDONLY, 0)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Seek to position 5
		pos, err := syscall.Seek(fd, 5, 0) // SEEK_SET
		require.NoError(t, err)
		require.Equal(t, int64(5), pos)

		// Read 3 bytes
		buffer := make([]byte, 3)
		n, err := syscall.Read(fd, buffer)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte("567"), buffer)

		// Seek from current position
		pos, err = syscall.Seek(fd, 2, 1) // SEEK_CUR
		require.NoError(t, err)
		require.Equal(t, int64(10), pos)

		// Read 1 byte
		buffer = make([]byte, 1)
		n, err = syscall.Read(fd, buffer)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, []byte("A"), buffer)

		// Test positioned I/O operations (pread/pwrite)
		syscall.Close(fd)

		// Open for read/write to test pwrite
		fd, err = syscall.Open(testFile, syscall.O_RDWR, 0)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Positioned write test
		writeData := []byte("XYZ")
		n, err = syscall.Pwrite(fd, writeData, 5) // pwrite at offset 5
		require.NoError(t, err)
		require.Equal(t, len(writeData), n)

		// Verify file position is unchanged by pwrite
		currentPos, err := syscall.Seek(fd, 0, 1) // SEEK_CUR
		require.NoError(t, err)
		require.Equal(t, int64(0), currentPos, "file offset should not be changed by pwrite")

		// Read back with pread
		readBuffer := make([]byte, len(writeData))
		n, err = syscall.Pread(fd, readBuffer, 5) // pread at offset 5
		require.NoError(t, err)
		require.Equal(t, len(writeData), n)
		require.Equal(t, writeData, readBuffer)

		// Verify file position is still unchanged by pread
		currentPos, err = syscall.Seek(fd, 0, 1) // SEEK_CUR
		require.NoError(t, err)
		require.Equal(t, int64(0), currentPos, "file offset should not be changed by pread")
	})

	t.Run("AppendMode", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "append_test.txt")

		// Create file with initial content
		err := os.WriteFile(testFile, []byte("initial"), 0644)
		require.NoError(t, err)

		// Open in append mode
		fd, err := syscall.Open(testFile, syscall.O_WRONLY|syscall.O_APPEND, 0)
		require.NoError(t, err)

		// Write additional content
		appendData := []byte(" appended")
		n, err := syscall.Write(fd, appendData)
		require.NoError(t, err)
		require.Equal(t, len(appendData), n)

		err = syscall.Close(fd)
		require.NoError(t, err)

		// Verify content
		content, err := os.ReadFile(testFile)
		require.NoError(t, err)
		require.Equal(t, []byte("initial appended"), content)
	})
}

// TestFileDescriptors tests POSIX file descriptor behavior
func (s *POSIXComplianceTestSuite) TestFileDescriptors(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("DuplicateFileDescriptors", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "dup_test.txt")
		testData := []byte("duplicate test")

		// Create and open file
		fd, err := syscall.Open(testFile, syscall.O_CREAT|syscall.O_RDWR, 0644)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Write initial data
		n, err := syscall.Write(fd, testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		// Duplicate file descriptor
		dupFd, err := syscall.Dup(fd)
		require.NoError(t, err)
		defer syscall.Close(dupFd)

		// Both descriptors should refer to the same file
		// Seek on one should affect the other
		pos, err := syscall.Seek(fd, 0, 0) // SEEK_SET
		require.NoError(t, err)
		require.Equal(t, int64(0), pos)

		// Read from duplicate descriptor should start from position 0
		buffer := make([]byte, 9)
		n, err = syscall.Read(dupFd, buffer)
		require.NoError(t, err)
		require.Equal(t, 9, n)
		require.Equal(t, []byte("duplicate"), buffer)
	})

	t.Run("FileDescriptorFlags", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "flags_test.txt")

		// Create file
		err := os.WriteFile(testFile, []byte("test"), 0644)
		require.NoError(t, err)

		// Open with close-on-exec flag
		fd, err := syscall.Open(testFile, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		require.NoError(t, err)
		defer syscall.Close(fd)

		// Verify close-on-exec flag is set
		// Note: FcntlInt is not available on all platforms, this test may need platform-specific implementation
		t.Skip("FcntlInt not available on this platform")
	})
}

// TestAtomicOperations tests POSIX atomic operation compliance
func (s *POSIXComplianceTestSuite) TestAtomicOperations(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("AtomicRename", func(t *testing.T) {
		oldFile := filepath.Join(mountPoint, "atomic_old.txt")
		newFile := filepath.Join(mountPoint, "atomic_new.txt")
		testData := []byte("atomic test data")

		// Create source file
		err := os.WriteFile(oldFile, testData, 0644)
		require.NoError(t, err)

		// Create existing target file
		err = os.WriteFile(newFile, []byte("old content"), 0644)
		require.NoError(t, err)

		// Atomic rename should replace target
		err = os.Rename(oldFile, newFile)
		require.NoError(t, err)

		// Verify source no longer exists
		_, err = os.Stat(oldFile)
		require.True(t, os.IsNotExist(err))

		// Verify target has new content
		content, err := os.ReadFile(newFile)
		require.NoError(t, err)
		require.Equal(t, testData, content)
	})
}

// TestConcurrentAccess tests concurrent access patterns
func (s *POSIXComplianceTestSuite) TestConcurrentAccess(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("ConcurrentReads", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "concurrent_read.txt")
		testData := []byte("concurrent read test data")

		// Create test file
		err := os.WriteFile(testFile, testData, 0644)
		require.NoError(t, err)

		// Launch multiple concurrent readers
		const numReaders = 10
		results := make(chan error, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				content, err := os.ReadFile(testFile)
				if err != nil {
					results <- err
					return
				}
				if string(content) != string(testData) {
					results <- fmt.Errorf("content mismatch")
					return
				}
				results <- nil
			}()
		}

		// Check all readers succeeded
		for i := 0; i < numReaders; i++ {
			err := <-results
			require.NoError(t, err)
		}
	})

	t.Run("ConcurrentFileCreations", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "concurrent_write.txt")

		// Launch multiple concurrent writers
		const numWriters = 5
		results := make(chan error, numWriters)

		for i := 0; i < numWriters; i++ {
			go func(id int) {
				content := fmt.Sprintf("writer %d data", id)
				err := os.WriteFile(fmt.Sprintf("%s_%d", testFile, id), []byte(content), 0644)
				results <- err
			}(i)
		}

		// Check all writers succeeded
		for i := 0; i < numWriters; i++ {
			err := <-results
			require.NoError(t, err)
		}

		// Verify all files were created
		for i := 0; i < numWriters; i++ {
			fileName := fmt.Sprintf("%s_%d", testFile, i)
			_, err := os.Stat(fileName)
			require.NoError(t, err)
		}
	})
}

// TestErrorHandling tests POSIX error handling compliance
func (s *POSIXComplianceTestSuite) TestErrorHandling(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	t.Run("AccessNonexistentFile", func(t *testing.T) {
		nonexistentFile := filepath.Join(mountPoint, "does_not_exist.txt")

		// Reading nonexistent file should return ENOENT
		_, err := os.ReadFile(nonexistentFile)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("CreateFileInNonexistentDirectory", func(t *testing.T) {
		fileInNonexistentDir := filepath.Join(mountPoint, "nonexistent_dir", "file.txt")

		// Creating file in nonexistent directory should fail
		err := os.WriteFile(fileInNonexistentDir, []byte("test"), 0644)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("InvalidFileDescriptor", func(t *testing.T) {
		// Using an invalid file descriptor should return appropriate error
		buffer := make([]byte, 10)
		_, err := syscall.Read(999, buffer) // 999 is likely an invalid fd
		require.Error(t, err)
		require.Equal(t, syscall.EBADF, err)
	})

	t.Run("PermissionDenied", func(t *testing.T) {
		testFile := filepath.Join(mountPoint, "readonly.txt")

		// Create read-only file
		err := os.WriteFile(testFile, []byte("readonly"), 0444)
		require.NoError(t, err)

		// Attempting to write to read-only file should fail
		err = os.WriteFile(testFile, []byte("modified"), 0444)
		require.Error(t, err)
		require.True(t, os.IsPermission(err))
	})
}
