package fuse_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDirectoryOperations tests fundamental FUSE directory operations
func TestDirectoryOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("CreateDirectory", func(t *testing.T) {
		testCreateDirectory(t, framework)
	})

	t.Run("RemoveDirectory", func(t *testing.T) {
		testRemoveDirectory(t, framework)
	})

	t.Run("ReadDirectory", func(t *testing.T) {
		testReadDirectory(t, framework)
	})

	t.Run("NestedDirectories", func(t *testing.T) {
		testNestedDirectories(t, framework)
	})

	t.Run("DirectoryPermissions", func(t *testing.T) {
		testDirectoryPermissions(t, framework)
	})

	t.Run("DirectoryRename", func(t *testing.T) {
		testDirectoryRename(t, framework)
	})
}

// testCreateDirectory tests creating directories
func testCreateDirectory(t *testing.T, framework *FuseTestFramework) {
	dirName := "test_directory"
	mountPath := filepath.Join(framework.GetMountPoint(), dirName)

	// Create directory
	require.NoError(t, os.Mkdir(mountPath, 0755))

	// Verify directory exists
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

// testRemoveDirectory tests removing directories
func testRemoveDirectory(t *testing.T, framework *FuseTestFramework) {
	dirName := "test_remove_dir"
	mountPath := filepath.Join(framework.GetMountPoint(), dirName)

	// Create directory
	require.NoError(t, os.Mkdir(mountPath, 0755))

	// Verify it exists
	_, err := os.Stat(mountPath)
	require.NoError(t, err)

	// Remove directory
	require.NoError(t, os.Remove(mountPath))

	// Verify it's gone
	_, err = os.Stat(mountPath)
	require.True(t, os.IsNotExist(err))
}

// testReadDirectory tests reading directory contents
func testReadDirectory(t *testing.T, framework *FuseTestFramework) {
	testDir := "test_read_dir"
	framework.CreateTestDir(testDir)

	// Create various types of entries
	entries := []string{
		"file1.txt",
		"file2.log",
		"subdir1",
		"subdir2",
		"script.sh",
	}

	// Create files and subdirectories
	for _, entry := range entries {
		entryPath := filepath.Join(testDir, entry)
		if entry == "subdir1" || entry == "subdir2" {
			framework.CreateTestDir(entryPath)
		} else {
			framework.CreateTestFile(entryPath, []byte("content of "+entry))
		}
	}

	// Read directory
	mountPath := filepath.Join(framework.GetMountPoint(), testDir)
	dirEntries, err := os.ReadDir(mountPath)
	require.NoError(t, err)

	// Verify all entries are present
	var actualNames []string
	for _, entry := range dirEntries {
		actualNames = append(actualNames, entry.Name())
	}

	sort.Strings(entries)
	sort.Strings(actualNames)
	assert.Equal(t, entries, actualNames)

	// Verify entry types
	for _, entry := range dirEntries {
		if entry.Name() == "subdir1" || entry.Name() == "subdir2" {
			assert.True(t, entry.IsDir())
		} else {
			assert.False(t, entry.IsDir())
		}
	}
}

// testNestedDirectories tests operations on nested directory structures
func testNestedDirectories(t *testing.T, framework *FuseTestFramework) {
	// Create nested structure: parent/child1/grandchild/child2
	structure := []string{
		"parent",
		"parent/child1",
		"parent/child1/grandchild",
		"parent/child2",
	}

	// Create directories
	for _, dir := range structure {
		framework.CreateTestDir(dir)
	}

	// Create files at various levels
	files := map[string][]byte{
		"parent/root_file.txt":                   []byte("root level"),
		"parent/child1/child_file.txt":           []byte("child level"),
		"parent/child1/grandchild/deep_file.txt": []byte("deep level"),
		"parent/child2/another_file.txt":         []byte("another child"),
	}

	for path, content := range files {
		framework.CreateTestFile(path, content)
	}

	// Verify structure by walking
	mountPath := filepath.Join(framework.GetMountPoint(), "parent")
	var foundPaths []string

	err := filepath.Walk(mountPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path from mount point
		relPath, _ := filepath.Rel(framework.GetMountPoint(), path)
		foundPaths = append(foundPaths, relPath)
		return nil
	})
	require.NoError(t, err)

	// Verify all expected paths were found
	expectedPaths := []string{
		"parent",
		"parent/child1",
		"parent/child1/grandchild",
		"parent/child1/grandchild/deep_file.txt",
		"parent/child1/child_file.txt",
		"parent/child2",
		"parent/child2/another_file.txt",
		"parent/root_file.txt",
	}

	sort.Strings(expectedPaths)
	sort.Strings(foundPaths)
	assert.Equal(t, expectedPaths, foundPaths)

	// Verify file contents
	for path, expectedContent := range files {
		framework.AssertFileContent(path, expectedContent)
	}
}

// testDirectoryPermissions tests directory permission operations
func testDirectoryPermissions(t *testing.T, framework *FuseTestFramework) {
	dirName := "test_permissions_dir"
	mountPath := filepath.Join(framework.GetMountPoint(), dirName)

	// Create directory with specific permissions
	require.NoError(t, os.Mkdir(mountPath, 0700))

	// Check initial permissions
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0700), info.Mode().Perm())

	// Change permissions
	require.NoError(t, os.Chmod(mountPath, 0755))

	// Verify permission change
	info, err = os.Stat(mountPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

// testDirectoryRename tests renaming directories
func testDirectoryRename(t *testing.T, framework *FuseTestFramework) {
	oldName := "old_directory"
	newName := "new_directory"

	// Create directory with content
	framework.CreateTestDir(oldName)
	framework.CreateTestFile(filepath.Join(oldName, "test_file.txt"), []byte("test content"))

	oldPath := filepath.Join(framework.GetMountPoint(), oldName)
	newPath := filepath.Join(framework.GetMountPoint(), newName)

	// Rename directory
	require.NoError(t, os.Rename(oldPath, newPath))

	// Verify old path doesn't exist
	_, err := os.Stat(oldPath)
	require.True(t, os.IsNotExist(err))

	// Verify new path exists and is a directory
	info, err := os.Stat(newPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Verify content still exists
	framework.AssertFileContent(filepath.Join(newName, "test_file.txt"), []byte("test content"))
}

// TestComplexDirectoryOperations tests more complex directory scenarios
func TestComplexDirectoryOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	t.Run("RemoveNonEmptyDirectory", func(t *testing.T) {
		testRemoveNonEmptyDirectory(t, framework)
	})

	t.Run("DirectoryWithManyFiles", func(t *testing.T) {
		testDirectoryWithManyFiles(t, framework)
	})

	t.Run("DeepDirectoryNesting", func(t *testing.T) {
		testDeepDirectoryNesting(t, framework)
	})
}

// testRemoveNonEmptyDirectory tests behavior when trying to remove non-empty directories
func testRemoveNonEmptyDirectory(t *testing.T, framework *FuseTestFramework) {
	dirName := "non_empty_dir"
	framework.CreateTestDir(dirName)

	// Add content to directory
	framework.CreateTestFile(filepath.Join(dirName, "file.txt"), []byte("content"))
	framework.CreateTestDir(filepath.Join(dirName, "subdir"))

	mountPath := filepath.Join(framework.GetMountPoint(), dirName)

	// Try to remove non-empty directory (should fail)
	err := os.Remove(mountPath)
	require.Error(t, err)

	// Directory should still exist
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Remove with RemoveAll should work
	require.NoError(t, os.RemoveAll(mountPath))

	// Verify it's gone
	_, err = os.Stat(mountPath)
	require.True(t, os.IsNotExist(err))
}

// testDirectoryWithManyFiles tests directories with large numbers of files
func testDirectoryWithManyFiles(t *testing.T, framework *FuseTestFramework) {
	dirName := "many_files_dir"
	framework.CreateTestDir(dirName)

	// Create many files
	numFiles := 100
	for i := 0; i < numFiles; i++ {
		filename := filepath.Join(dirName, fmt.Sprintf("file_%03d.txt", i))
		content := []byte(fmt.Sprintf("Content of file %d", i))
		framework.CreateTestFile(filename, content)
	}

	// Read directory
	mountPath := filepath.Join(framework.GetMountPoint(), dirName)
	entries, err := os.ReadDir(mountPath)
	require.NoError(t, err)

	// Verify count
	assert.Equal(t, numFiles, len(entries))

	// Verify some random files
	testIndices := []int{0, 10, 50, 99}
	for _, i := range testIndices {
		filename := filepath.Join(dirName, fmt.Sprintf("file_%03d.txt", i))
		expectedContent := []byte(fmt.Sprintf("Content of file %d", i))
		framework.AssertFileContent(filename, expectedContent)
	}
}

// testDeepDirectoryNesting tests very deep directory structures
func testDeepDirectoryNesting(t *testing.T, framework *FuseTestFramework) {
	// Create deep nesting (20 levels)
	depth := 20
	currentPath := ""

	for i := 0; i < depth; i++ {
		if i == 0 {
			currentPath = fmt.Sprintf("level_%02d", i)
		} else {
			currentPath = filepath.Join(currentPath, fmt.Sprintf("level_%02d", i))
		}
		framework.CreateTestDir(currentPath)
	}

	// Create a file at the deepest level
	deepFile := filepath.Join(currentPath, "deep_file.txt")
	deepContent := []byte("This is very deep!")
	framework.CreateTestFile(deepFile, deepContent)

	// Verify file exists and has correct content
	framework.AssertFileContent(deepFile, deepContent)

	// Verify we can navigate the full structure
	mountPath := filepath.Join(framework.GetMountPoint(), currentPath)
	info, err := os.Stat(mountPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}
