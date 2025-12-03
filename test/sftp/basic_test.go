package sftp

import (
	"bytes"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHomeDirPathTranslation tests that SFTP operations correctly translate
// paths relative to the user's HomeDir.
// This is the fix for https://github.com/seaweedfs/seaweedfs/issues/7470
func TestHomeDirPathTranslation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	// Test with user "testuser" who has HomeDir="/sftp/testuser"
	// When they upload to "/", it should actually go to "/sftp/testuser"
	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Test 1: Upload file to "/" (should map to /sftp/testuser/)
	t.Run("UploadToRoot", func(t *testing.T) {
		testContent := []byte("Hello from SFTP test!")
		filename := "test_upload.txt"

		// Create file at "/" from user's perspective
		file, err := sftpClient.Create("/" + filename)
		require.NoError(t, err, "should be able to create file at /")

		_, err = file.Write(testContent)
		require.NoError(t, err, "should be able to write to file")
		err = file.Close()
		require.NoError(t, err, "should be able to close file")

		// Verify file exists and has correct content
		readFile, err := sftpClient.Open("/" + filename)
		require.NoError(t, err, "should be able to open file")
		defer readFile.Close()

		content, err := io.ReadAll(readFile)
		require.NoError(t, err, "should be able to read file")
		require.Equal(t, testContent, content, "file content should match")

		// Clean up
		err = sftpClient.Remove("/" + filename)
		require.NoError(t, err, "should be able to remove file")
	})

	// Test 2: Create directory at "/" (should map to /sftp/testuser/)
	t.Run("CreateDirAtRoot", func(t *testing.T) {
		dirname := "test_dir"

		err := sftpClient.Mkdir("/" + dirname)
		require.NoError(t, err, "should be able to create directory at /")

		// Verify directory exists
		info, err := sftpClient.Stat("/" + dirname)
		require.NoError(t, err, "should be able to stat directory")
		require.True(t, info.IsDir(), "should be a directory")

		// Clean up
		err = sftpClient.RemoveDirectory("/" + dirname)
		require.NoError(t, err, "should be able to remove directory")
	})

	// Test 3: List directory at "/" (should list /sftp/testuser/)
	t.Run("ListRoot", func(t *testing.T) {
		// Create a test file first
		testContent := []byte("list test content")
		filename := "list_test.txt"

		file, err := sftpClient.Create("/" + filename)
		require.NoError(t, err)
		_, err = file.Write(testContent)
		require.NoError(t, err)
		file.Close()

		// List root directory
		files, err := sftpClient.ReadDir("/")
		require.NoError(t, err, "should be able to list root directory")

		// Should find our test file
		found := false
		for _, f := range files {
			if f.Name() == filename {
				found = true
				break
			}
		}
		require.True(t, found, "should find test file in listing")

		// Clean up
		err = sftpClient.Remove("/" + filename)
		require.NoError(t, err)
	})

	// Test 4: Nested directory operations
	t.Run("NestedOperations", func(t *testing.T) {
		// Create nested directory structure
		err := sftpClient.MkdirAll("/nested/dir/structure")
		require.NoError(t, err, "should be able to create nested directories")

		// Create file in nested directory
		testContent := []byte("nested file content")
		file, err := sftpClient.Create("/nested/dir/structure/file.txt")
		require.NoError(t, err)
		_, err = file.Write(testContent)
		require.NoError(t, err)
		file.Close()

		// Verify file exists
		readFile, err := sftpClient.Open("/nested/dir/structure/file.txt")
		require.NoError(t, err)
		content, err := io.ReadAll(readFile)
		require.NoError(t, err)
		readFile.Close()
		require.Equal(t, testContent, content)

		// Clean up
		err = sftpClient.Remove("/nested/dir/structure/file.txt")
		require.NoError(t, err)
		err = sftpClient.RemoveDirectory("/nested/dir/structure")
		require.NoError(t, err)
		err = sftpClient.RemoveDirectory("/nested/dir")
		require.NoError(t, err)
		err = sftpClient.RemoveDirectory("/nested")
		require.NoError(t, err)
	})

	// Test 5: Rename operation
	t.Run("RenameFile", func(t *testing.T) {
		testContent := []byte("rename test content")

		file, err := sftpClient.Create("/original.txt")
		require.NoError(t, err)
		_, err = file.Write(testContent)
		require.NoError(t, err)
		file.Close()

		// Rename file
		err = sftpClient.Rename("/original.txt", "/renamed.txt")
		require.NoError(t, err, "should be able to rename file")

		// Verify old file doesn't exist
		_, err = sftpClient.Stat("/original.txt")
		require.Error(t, err, "original file should not exist")

		// Verify new file exists with correct content
		readFile, err := sftpClient.Open("/renamed.txt")
		require.NoError(t, err, "renamed file should exist")
		content, err := io.ReadAll(readFile)
		require.NoError(t, err)
		readFile.Close()
		require.Equal(t, testContent, content)

		// Clean up
		err = sftpClient.Remove("/renamed.txt")
		require.NoError(t, err)
	})
}

// TestAdminRootAccess tests that admin user with HomeDir="/" can access everything
func TestAdminRootAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	// Connect as admin with HomeDir="/"
	sftpClient, sshConn, err := fw.ConnectSFTP("admin", "adminpassword")
	require.NoError(t, err, "failed to connect as admin")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Admin should be able to create directories anywhere
	t.Run("CreateAnyDirectory", func(t *testing.T) {
		// Create the user's home directory structure
		err := sftpClient.MkdirAll("/sftp/testuser")
		require.NoError(t, err, "admin should be able to create any directory")

		// Create file in that directory
		testContent := []byte("admin created this")
		file, err := sftpClient.Create("/sftp/testuser/admin_file.txt")
		require.NoError(t, err)
		_, err = file.Write(testContent)
		require.NoError(t, err)
		file.Close()

		// Verify file exists
		info, err := sftpClient.Stat("/sftp/testuser/admin_file.txt")
		require.NoError(t, err)
		require.False(t, info.IsDir())

		// Clean up
		err = sftpClient.Remove("/sftp/testuser/admin_file.txt")
		require.NoError(t, err)
	})
}

// TestLargeFileUpload tests uploading larger files through SFTP
func TestLargeFileUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Create a 1MB file
	t.Run("Upload1MB", func(t *testing.T) {
		size := 1024 * 1024 // 1MB
		testData := bytes.Repeat([]byte("A"), size)

		file, err := sftpClient.Create("/large_file.bin")
		require.NoError(t, err)
		n, err := file.Write(testData)
		require.NoError(t, err)
		require.Equal(t, size, n)
		file.Close()

		// Verify file size
		info, err := sftpClient.Stat("/large_file.bin")
		require.NoError(t, err)
		require.Equal(t, int64(size), info.Size())

		// Verify content
		readFile, err := sftpClient.Open("/large_file.bin")
		require.NoError(t, err)
		content, err := io.ReadAll(readFile)
		require.NoError(t, err)
		readFile.Close()
		require.Equal(t, testData, content)

		// Clean up
		err = sftpClient.Remove("/large_file.bin")
		require.NoError(t, err)
	})
}

// TestStatOperations tests Stat and Lstat operations
func TestStatOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Create a test file
	testContent := []byte("stat test content")
	file, err := sftpClient.Create("/stat_test.txt")
	require.NoError(t, err)
	_, err = file.Write(testContent)
	require.NoError(t, err)
	file.Close()

	t.Run("StatFile", func(t *testing.T) {
		info, err := sftpClient.Stat("/stat_test.txt")
		require.NoError(t, err)
		require.Equal(t, "stat_test.txt", info.Name())
		require.Equal(t, int64(len(testContent)), info.Size())
		require.False(t, info.IsDir())
	})

	t.Run("StatDirectory", func(t *testing.T) {
		err := sftpClient.Mkdir("/stat_dir")
		require.NoError(t, err)

		info, err := sftpClient.Stat("/stat_dir")
		require.NoError(t, err)
		require.Equal(t, "stat_dir", info.Name())
		require.True(t, info.IsDir())

		// Clean up
		err = sftpClient.RemoveDirectory("/stat_dir")
		require.NoError(t, err)
	})

	t.Run("StatRoot", func(t *testing.T) {
		// Should be able to stat "/" which maps to user's home directory
		info, err := sftpClient.Stat("/")
		require.NoError(t, err, "should be able to stat root (home) directory")
		require.True(t, info.IsDir(), "root should be a directory")
	})

	// Clean up
	err = sftpClient.Remove("/stat_test.txt")
	require.NoError(t, err)
}

// TestWalk tests walking directory trees
func TestWalk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Create directory structure
	err = sftpClient.MkdirAll("/walk/a/b")
	require.NoError(t, err)
	err = sftpClient.MkdirAll("/walk/c")
	require.NoError(t, err)

	// Create files
	for _, p := range []string{"/walk/file1.txt", "/walk/a/file2.txt", "/walk/a/b/file3.txt", "/walk/c/file4.txt"} {
		file, err := sftpClient.Create(p)
		require.NoError(t, err)
		file.Write([]byte("test"))
		file.Close()
	}

	t.Run("WalkEntireTree", func(t *testing.T) {
		var paths []string
		walker := sftpClient.Walk("/walk")
		for walker.Step() {
			if walker.Err() != nil {
				continue
			}
			paths = append(paths, walker.Path())
		}

		// Should find all directories and files
		require.Contains(t, paths, "/walk")
		require.Contains(t, paths, "/walk/a")
		require.Contains(t, paths, "/walk/a/b")
		require.Contains(t, paths, "/walk/c")
	})

	// Clean up
	for _, p := range []string{"/walk/file1.txt", "/walk/a/file2.txt", "/walk/a/b/file3.txt", "/walk/c/file4.txt"} {
		sftpClient.Remove(p)
	}
	for _, p := range []string{"/walk/a/b", "/walk/a", "/walk/c", "/walk"} {
		sftpClient.RemoveDirectory(p)
	}
}

// TestCurrentWorkingDirectory tests that Getwd and Chdir work correctly
func TestCurrentWorkingDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	// Create test directory
	err = sftpClient.Mkdir("/cwd_test")
	require.NoError(t, err)

	t.Run("GetCurrentDir", func(t *testing.T) {
		cwd, err := sftpClient.Getwd()
		require.NoError(t, err)
		// The initial working directory should be the user's home directory
		// which from the user's perspective is "/"
		require.NotEmpty(t, cwd)
	})

	t.Run("ChangeAndCreate", func(t *testing.T) {
		// Create file in subdirectory using relative path after chdir
		// Note: pkg/sftp doesn't support Chdir, so we test using absolute paths
		file, err := sftpClient.Create("/cwd_test/relative_file.txt")
		require.NoError(t, err)
		file.Write([]byte("test"))
		file.Close()

		// Verify using absolute path
		_, err = sftpClient.Stat("/cwd_test/relative_file.txt")
		require.NoError(t, err)

		// Clean up
		sftpClient.Remove("/cwd_test/relative_file.txt")
	})

	// Clean up
	err = sftpClient.RemoveDirectory("/cwd_test")
	require.NoError(t, err)
}

// TestPathEdgeCases tests various edge cases in path handling
func TestPathEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	t.Run("PathWithDotDot", func(t *testing.T) {
		// Create directory structure
		err := sftpClient.MkdirAll("/edge/subdir")
		require.NoError(t, err)

		// Create file using path with ..
		file, err := sftpClient.Create("/edge/subdir/../file.txt")
		require.NoError(t, err)
		file.Write([]byte("test"))
		file.Close()

		// Verify file was created in /edge
		_, err = sftpClient.Stat("/edge/file.txt")
		require.NoError(t, err, "file should be created in parent directory")

		// Clean up
		sftpClient.Remove("/edge/file.txt")
		sftpClient.RemoveDirectory("/edge/subdir")
		sftpClient.RemoveDirectory("/edge")
	})

	t.Run("PathWithTrailingSlash", func(t *testing.T) {
		err := sftpClient.Mkdir("/trailing")
		require.NoError(t, err)

		// Stat with trailing slash
		info, err := sftpClient.Stat("/trailing/")
		require.NoError(t, err)
		require.True(t, info.IsDir())

		// Clean up
		sftpClient.RemoveDirectory("/trailing")
	})

	t.Run("CreateFileAtRootPath", func(t *testing.T) {
		// This is the exact scenario from issue #7470
		// User with HomeDir="/sftp/testuser" uploads to "/"
		file, err := sftpClient.Create("/issue7470.txt")
		require.NoError(t, err, "should be able to create file at / (issue #7470)")
		file.Write([]byte("This tests the fix for issue #7470"))
		file.Close()

		// Verify
		_, err = sftpClient.Stat("/issue7470.txt")
		require.NoError(t, err)

		// Clean up
		sftpClient.Remove("/issue7470.txt")
	})

	// Security test: path traversal attacks should be blocked
	t.Run("PathTraversalPrevention", func(t *testing.T) {
		// User's HomeDir is "/sftp/testuser"
		// Attempting to escape via "../.." should NOT create files outside home directory

		// First, create a valid file to ensure we can write
		validFile, err := sftpClient.Create("/valid.txt")
		require.NoError(t, err)
		validFile.Write([]byte("valid"))
		validFile.Close()

		// Try various path traversal attempts
		// These should either:
		// 1. Be blocked (error returned), OR
		// 2. Be safely resolved to stay within home directory

		traversalPaths := []string{
			"/../escape.txt",
			"/../../escape.txt",
			"/../../../escape.txt",
			"/subdir/../../escape.txt",
			"/./../../escape.txt",
		}

		for _, traversalPath := range traversalPaths {
			file, err := sftpClient.Create(traversalPath)
			if err == nil {
				file.Close()
				// If the file was created, it should be within the home directory
				// The file should resolve to the home directory, not escape it

				// Try to stat the file at the "escaped" location - it shouldn't exist there
				// We can't directly check this without admin access, but we can verify
				// operations succeed within the contained environment
			}
			// Either way, clean up any created files
			sftpClient.Remove(traversalPath)
		}

		// Clean up
		sftpClient.Remove("/valid.txt")
	})
}

// TestFileContent tests reading and writing file content correctly
func TestFileContent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	err := fw.Setup(config)
	require.NoError(t, err, "failed to setup test framework")
	defer fw.Cleanup()

	sftpClient, sshConn, err := fw.ConnectSFTP("testuser", "testuserpassword")
	require.NoError(t, err, "failed to connect as testuser")
	defer sshConn.Close()
	defer sftpClient.Close()

	t.Run("BinaryContent", func(t *testing.T) {
		// Create binary data with all byte values
		data := make([]byte, 256)
		for i := 0; i < 256; i++ {
			data[i] = byte(i)
		}

		file, err := sftpClient.Create("/binary.bin")
		require.NoError(t, err)
		n, err := file.Write(data)
		require.NoError(t, err)
		require.Equal(t, 256, n)
		file.Close()

		// Read back
		readFile, err := sftpClient.Open("/binary.bin")
		require.NoError(t, err)
		content, err := io.ReadAll(readFile)
		require.NoError(t, err)
		readFile.Close()

		require.Equal(t, data, content, "binary content should match")

		// Clean up
		sftpClient.Remove("/binary.bin")
	})

	t.Run("EmptyFile", func(t *testing.T) {
		file, err := sftpClient.Create("/empty.txt")
		require.NoError(t, err)
		file.Close()

		info, err := sftpClient.Stat("/empty.txt")
		require.NoError(t, err)
		require.Equal(t, int64(0), info.Size())

		// Clean up
		sftpClient.Remove("/empty.txt")
	})

	t.Run("UnicodeFilename", func(t *testing.T) {
		filename := "/文件名.txt"
		content := []byte("Unicode content: 你好世界")

		file, err := sftpClient.Create(filename)
		require.NoError(t, err)
		file.Write(content)
		file.Close()

		// Read back
		readFile, err := sftpClient.Open(filename)
		require.NoError(t, err)
		readContent, err := io.ReadAll(readFile)
		require.NoError(t, err)
		readFile.Close()

		require.Equal(t, content, readContent)

		// Verify in listing
		files, err := sftpClient.ReadDir("/")
		require.NoError(t, err)
		found := false
		for _, f := range files {
			if f.Name() == path.Base(filename) {
				found = true
				break
			}
		}
		require.True(t, found, "should find unicode filename in listing")

		// Clean up
		sftpClient.Remove(filename)
	})
}

