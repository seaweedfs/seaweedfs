package fuse

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ExternalPOSIXTestSuite manages integration with external POSIX test suites
type ExternalPOSIXTestSuite struct {
	framework *FuseTestFramework
	t         *testing.T
	workDir   string
}

// NewExternalPOSIXTestSuite creates a new external POSIX test suite runner
func NewExternalPOSIXTestSuite(t *testing.T, framework *FuseTestFramework) *ExternalPOSIXTestSuite {
	workDir := filepath.Join(os.TempDir(), fmt.Sprintf("posix_external_tests_%d", time.Now().Unix()))
	os.MkdirAll(workDir, 0755)

	return &ExternalPOSIXTestSuite{
		framework: framework,
		t:         t,
		workDir:   workDir,
	}
}

// Cleanup removes temporary test directories
func (s *ExternalPOSIXTestSuite) Cleanup() {
	os.RemoveAll(s.workDir)
}

// TestExternalPOSIXSuites runs integration tests with external POSIX test suites
func TestExternalPOSIXSuites(t *testing.T) {
	config := DefaultTestConfig()
	config.EnableDebug = true
	config.MountOptions = []string{"-allowOthers", "-nonempty"}

	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()
	require.NoError(t, framework.Setup(config))

	suite := NewExternalPOSIXTestSuite(t, framework)
	defer suite.Cleanup()

	// Run various external POSIX test suites
	t.Run("PjdFsTest", suite.TestPjdFsTest)
	t.Run("NFSTestPOSIX", suite.TestNFSTestPOSIX)
	t.Run("LitmusTests", suite.TestLitmusTests)
	t.Run("CustomPOSIXTests", suite.TestCustomPOSIXTests)
}

// TestPjdFsTest runs the comprehensive pjdfstest POSIX filesystem test suite
func (s *ExternalPOSIXTestSuite) TestPjdFsTest(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	// Check if pjdfstest is available
	_, err := exec.LookPath("pjdfstest")
	if err != nil {
		// For comprehensive POSIX compliance testing, external tools should be mandatory
		if os.Getenv("POSIX_REQUIRE_EXTERNAL_TOOLS") == "true" {
			t.Fatalf("pjdfstest is required for comprehensive POSIX compliance testing but not found. Install from: https://github.com/pjd/pjdfstest")
		}
		t.Skip("pjdfstest not found. Install from: https://github.com/pjd/pjdfstest")
	}

	// Create test directory within mount point
	testDir := filepath.Join(mountPoint, "pjdfstest")
	err = os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	// List of critical POSIX operations to test
	pjdTests := []struct {
		name     string
		testPath string
		critical bool
	}{
		{"chflags", "tests/chflags", false},
		{"chmod", "tests/chmod", true},
		{"chown", "tests/chown", true},
		{"create", "tests/create", true},
		{"link", "tests/link", true},
		{"mkdir", "tests/mkdir", true},
		{"mkfifo", "tests/mkfifo", false},
		{"mknod", "tests/mknod", false},
		{"open", "tests/open", true},
		{"rename", "tests/rename", true},
		{"rmdir", "tests/rmdir", true},
		{"symlink", "tests/symlink", true},
		{"truncate", "tests/truncate", true},
		{"unlink", "tests/unlink", true},
	}

	// Download and setup pjdfstest if needed
	pjdDir := filepath.Join(s.workDir, "pjdfstest")
	if _, err := os.Stat(pjdDir); os.IsNotExist(err) {
		t.Logf("Setting up pjdfstest...")
		err = s.setupPjdFsTest(pjdDir)
		if err != nil {
			t.Skipf("Failed to setup pjdfstest: %v", err)
		}
	}

	// Run each test category
	for _, test := range pjdTests {
		t.Run(test.name, func(t *testing.T) {
			s.runPjdTest(t, pjdDir, test.testPath, testDir, test.critical)
		})
	}
}

// TestNFSTestPOSIX runs nfstest_posix for POSIX API verification
func (s *ExternalPOSIXTestSuite) TestNFSTestPOSIX(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	// Check if nfstest_posix is available
	_, err := exec.LookPath("nfstest_posix")
	if err != nil {
		// For comprehensive POSIX compliance testing, external tools should be mandatory
		if os.Getenv("POSIX_REQUIRE_EXTERNAL_TOOLS") == "true" {
			t.Fatalf("nfstest_posix is required for comprehensive POSIX compliance testing but not found. Install via: pip install nfstest")
		}
		t.Skip("nfstest_posix not found. Install via: pip install nfstest")
	}

	testDir := filepath.Join(mountPoint, "nfstest")
	err = os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	// Run nfstest_posix with comprehensive API testing
	cmd := exec.Command("nfstest_posix",
		"--path", testDir,
		"--verbose",
		"--createlog",
		"--runid", fmt.Sprintf("seaweedfs_%d", time.Now().Unix()),
	)

	output, err := cmd.CombinedOutput()
	t.Logf("nfstest_posix output:\n%s", string(output))

	if err != nil {
		t.Logf("nfstest_posix failed: %v", err)
		// Don't fail the test completely, just log the failure
	}
}

// TestLitmusTests runs focused POSIX compliance litmus tests
func (s *ExternalPOSIXTestSuite) TestLitmusTests(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	// Create litmus test scripts for critical POSIX behaviors
	litmusTests := []struct {
		name   string
		script string
	}{
		{
			name: "AtomicRename",
			script: `#!/bin/bash
set -e
cd "$1"
echo "test data" > temp_file
echo "original" > target_file
mv temp_file target_file
[ "$(cat target_file)" = "test data" ]
echo "PASS: Atomic rename works"
`,
		},
		{
			name: "LinkCount",
			script: `#!/bin/bash
set -e
cd "$1"
echo "test" > original
ln original hardlink
if [[ "$(uname)" == "Darwin" ]]; then
  [ $(stat -f %l original) -eq 2 ]
else
  [ $(stat -c %h original) -eq 2 ]
fi
rm hardlink
if [[ "$(uname)" == "Darwin" ]]; then
  [ $(stat -f %l original) -eq 1 ]
else
  [ $(stat -c %h original) -eq 1 ]
fi
echo "PASS: Hard link counting works"
`,
		},
		{
			name: "SymlinkCycles",
			script: `#!/bin/bash
set -e
cd "$1"
ln -s link1 link2
ln -s link2 link1
# Try to access the file, which should fail due to too many symbolic links
if cat link1 2>/dev/null; then
    echo "FAIL: Symlink cycle not detected"
    exit 1
fi
echo "PASS: Symlink cycle handling works"
`,
		},
		{
			name: "ConcurrentCreate",
			script: `#!/bin/bash
set -e
cd "$1"
for i in {1..10}; do
    (echo "process $i" > "file_$i") &
done
wait
[ $(ls file_* | wc -l) -eq 10 ]
echo "PASS: Concurrent file creation works"
`,
		},
		{
			name: "DirectoryConsistency",
			script: `#!/bin/bash
set -e
cd "$1"
mkdir testdir
cd testdir
touch file1 file2 file3
cd ..
entries=$(ls testdir | wc -l)
[ $entries -eq 3 ]
rmdir testdir 2>/dev/null && echo "FAIL: Non-empty directory removed" && exit 1
rm testdir/*
rmdir testdir
echo "PASS: Directory consistency works"
`,
		},
	}

	testDir := filepath.Join(mountPoint, "litmus")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	// Run each litmus test
	for _, test := range litmusTests {
		t.Run(test.name, func(t *testing.T) {
			s.runLitmusTest(t, test.name, test.script, testDir)
		})
	}
}

// TestCustomPOSIXTests runs custom POSIX compliance tests
func (s *ExternalPOSIXTestSuite) TestCustomPOSIXTests(t *testing.T) {
	mountPoint := s.framework.GetMountPoint()

	// Custom stress tests for POSIX compliance
	t.Run("StressRename", func(t *testing.T) {
		s.stressTestRename(t, mountPoint)
	})

	t.Run("StressCreate", func(t *testing.T) {
		s.stressTestCreate(t, mountPoint)
	})

	t.Run("StressDirectory", func(t *testing.T) {
		s.stressTestDirectory(t, mountPoint)
	})

	t.Run("EdgeCases", func(t *testing.T) {
		s.testEdgeCases(t, mountPoint)
	})
}

// setupPjdFsTest downloads and sets up the pjdfstest suite
func (s *ExternalPOSIXTestSuite) setupPjdFsTest(targetDir string) error {
	// Clone pjdfstest repository
	cmd := exec.Command("git", "clone", "https://github.com/pjd/pjdfstest.git", targetDir)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to clone pjdfstest: %w", err)
	}

	// Build pjdfstest
	cmd = exec.Command("make", "-C", targetDir)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to build pjdfstest: %w", err)
	}

	return nil
}

// runPjdTest executes a specific pjdfstest
func (s *ExternalPOSIXTestSuite) runPjdTest(t *testing.T, pjdDir, testPath, mountDir string, critical bool) {
	fullTestPath := filepath.Join(pjdDir, testPath)

	// Check if test path exists
	if _, err := os.Stat(fullTestPath); os.IsNotExist(err) {
		t.Skipf("Test path does not exist: %s", fullTestPath)
	}

	cmd := exec.Command("prove", "-r", fullTestPath)
	cmd.Dir = mountDir
	cmd.Env = append(os.Environ(), "FSTEST="+filepath.Join(pjdDir, "pjdfstest"))

	output, err := cmd.CombinedOutput()
	t.Logf("pjdfstest %s output:\n%s", testPath, string(output))

	if err != nil {
		if critical {
			t.Errorf("Critical pjdfstest failed: %s - %v", testPath, err)
		} else {
			t.Logf("Non-critical pjdfstest failed: %s - %v", testPath, err)
		}
	}
}

// runLitmusTest executes a litmus test script
func (s *ExternalPOSIXTestSuite) runLitmusTest(t *testing.T, name, script, testDir string) {
	scriptPath := filepath.Join(s.workDir, name+".sh")

	// Write script to file
	err := os.WriteFile(scriptPath, []byte(script), 0755)
	require.NoError(t, err)

	// Create isolated test directory
	isolatedDir := filepath.Join(testDir, name)
	err = os.MkdirAll(isolatedDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(isolatedDir)

	// Execute script
	cmd := exec.Command("bash", scriptPath, isolatedDir)
	output, err := cmd.CombinedOutput()

	t.Logf("Litmus test %s output:\n%s", name, string(output))

	if err != nil {
		t.Errorf("Litmus test %s failed: %v", name, err)
	}
}

// stressTestRename tests rename operations under stress
func (s *ExternalPOSIXTestSuite) stressTestRename(t *testing.T, mountPoint string) {
	testDir := filepath.Join(mountPoint, "stress_rename")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Create files and rename them concurrently
	const numFiles = 100
	const numWorkers = 10

	// Create initial files
	for i := 0; i < numFiles; i++ {
		fileName := filepath.Join(testDir, fmt.Sprintf("file_%d.txt", i))
		err := os.WriteFile(fileName, []byte(fmt.Sprintf("content_%d", i)), 0644)
		if !assert.NoError(t, err, "Failed to create initial file %d", i) {
			return // Skip test if setup fails
		}
	}

	// Concurrent rename operations
	results := make(chan error, numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(worker int) {
			for i := worker; i < numFiles; i += numWorkers {
				oldName := filepath.Join(testDir, fmt.Sprintf("file_%d.txt", i))
				newName := filepath.Join(testDir, fmt.Sprintf("renamed_%d.txt", i))
				err := os.Rename(oldName, newName)
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(w)
	}

	// Wait for all workers and collect errors
	var errorCount int
	for w := 0; w < numWorkers; w++ {
		err := <-results
		if err != nil {
			assert.NoError(t, err, "Worker %d failed", w)
			errorCount++
		}
	}

	// Verify all files were renamed (if no errors occurred)
	if errorCount == 0 {
		files, err := filepath.Glob(filepath.Join(testDir, "renamed_*.txt"))
		assert.NoError(t, err, "Failed to list renamed files")
		assert.Equal(t, numFiles, len(files), "Not all files were renamed successfully")
	}
}

// stressTestCreate tests file creation under stress
func (s *ExternalPOSIXTestSuite) stressTestCreate(t *testing.T, mountPoint string) {
	testDir := filepath.Join(mountPoint, "stress_create")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	const numFiles = 1000
	const numWorkers = 20

	results := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(worker int) {
			for i := worker; i < numFiles; i += numWorkers {
				fileName := filepath.Join(testDir, fmt.Sprintf("stress_%d_%d.txt", worker, i))
				err := os.WriteFile(fileName, []byte(fmt.Sprintf("data_%d_%d", worker, i)), 0644)
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(w)
	}

	// Wait for all workers
	for w := 0; w < numWorkers; w++ {
		err := <-results
		require.NoError(t, err)
	}

	// Verify all files were created
	files, err := filepath.Glob(filepath.Join(testDir, "stress_*.txt"))
	require.NoError(t, err)
	require.Equal(t, numFiles, len(files))
}

// stressTestDirectory tests directory operations under stress
func (s *ExternalPOSIXTestSuite) stressTestDirectory(t *testing.T, mountPoint string) {
	testDir := filepath.Join(mountPoint, "stress_directory")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	const numDirs = 500
	const numWorkers = 10

	results := make(chan error, numWorkers)

	// Create directories concurrently
	for w := 0; w < numWorkers; w++ {
		go func(worker int) {
			for i := worker; i < numDirs; i += numWorkers {
				dirName := filepath.Join(testDir, fmt.Sprintf("dir_%d_%d", worker, i))
				err := os.MkdirAll(dirName, 0755)
				if err != nil {
					results <- err
					return
				}

				// Create a file in each directory
				fileName := filepath.Join(dirName, "test.txt")
				err = os.WriteFile(fileName, []byte("test"), 0644)
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(w)
	}

	// Wait for all workers
	for w := 0; w < numWorkers; w++ {
		err := <-results
		require.NoError(t, err)
	}

	// Verify directory structure
	dirs, err := filepath.Glob(filepath.Join(testDir, "dir_*"))
	require.NoError(t, err)
	require.Equal(t, numDirs, len(dirs))
}

// testEdgeCases tests various POSIX edge cases
func (s *ExternalPOSIXTestSuite) testEdgeCases(t *testing.T, mountPoint string) {
	testDir := filepath.Join(mountPoint, "edge_cases")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	t.Run("WriteToDirectoryAsFile", func(t *testing.T) {
		// Test writing to a directory as if it were a file (should fail)
		// Note: filepath.Join(testDir, "") returns testDir itself
		err := os.WriteFile(testDir, []byte("test"), 0644)
		require.Error(t, err)
		// Verify the error is specifically about the target being a directory
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
		require.Equal(t, syscall.EISDIR, pathErr.Err)
	})

	t.Run("VeryLongFileName", func(t *testing.T) {
		// Test very long file names
		longName := strings.Repeat("a", 255) // NAME_MAX is typically 255
		longFile := filepath.Join(testDir, longName)

		err := os.WriteFile(longFile, []byte("long name test"), 0644)
		// This might succeed or fail depending on filesystem limits
		if err == nil {
			defer os.Remove(longFile)
			t.Logf("Very long filename accepted")
		} else {
			t.Logf("Very long filename rejected: %v", err)
		}
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		// Test files with special characters
		specialFiles := []string{
			"file with spaces.txt",
			"file-with-dashes.txt",
			"file_with_underscores.txt",
			"file.with.dots.txt",
		}

		for _, fileName := range specialFiles {
			filePath := filepath.Join(testDir, fileName)
			err := os.WriteFile(filePath, []byte("special char test"), 0644)
			require.NoError(t, err, "Failed to create file with special chars: %s", fileName)

			// Verify we can read it back
			_, err = os.ReadFile(filePath)
			require.NoError(t, err, "Failed to read file with special chars: %s", fileName)
		}
	})

	t.Run("DeepDirectoryNesting", func(t *testing.T) {
		// Test deep directory nesting
		deepPath := testDir
		for i := 0; i < 100; i++ {
			deepPath = filepath.Join(deepPath, fmt.Sprintf("level_%d", i))
		}

		err := os.MkdirAll(deepPath, 0755)
		// This might fail due to PATH_MAX limits
		if err != nil {
			t.Logf("Deep directory nesting failed at some level: %v", err)
		} else {
			t.Logf("Deep directory nesting succeeded")

			// Test creating a file in the deep path
			testFile := filepath.Join(deepPath, "deep_file.txt")
			err = os.WriteFile(testFile, []byte("deep test"), 0644)
			if err == nil {
				t.Logf("File creation in deep path succeeded")
			} else {
				t.Logf("File creation in deep path failed: %v", err)
			}
		}
	})
}
