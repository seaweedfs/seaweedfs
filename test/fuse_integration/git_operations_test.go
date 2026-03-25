package fuse_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGitOperations exercises git clone, checkout, and pull on a FUSE mount.
//
// The test creates a bare repo on the mount (acting as a remote), clones it,
// makes commits, pushes, then clones from the mount into an on-mount working
// directory. It pushes additional commits, checks out an older revision in the
// on-mount clone, and runs git pull to fast-forward with real changes —
// verifying file content integrity at each step.
func TestGitOperations(t *testing.T) {
	framework := NewFuseTestFramework(t, DefaultTestConfig())
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(DefaultTestConfig()))

	mountPoint := framework.GetMountPoint()

	// We need a local scratch dir (not on the mount) for the "developer" clone.
	localDir, err := os.MkdirTemp("", "git_ops_local_")
	require.NoError(t, err)
	defer os.RemoveAll(localDir)

	t.Run("CloneAndPull", func(t *testing.T) {
		testGitCloneAndPull(t, mountPoint, localDir)
	})
}

func testGitCloneAndPull(t *testing.T, mountPoint, localDir string) {
	bareRepo := filepath.Join(mountPoint, "repo.git")
	localClone := filepath.Join(localDir, "clone")
	mountClone := filepath.Join(mountPoint, "working")

	// ---- Phase 1: Create bare repo on the mount ----
	t.Log("Phase 1: create bare repo on mount")
	gitRun(t, "", "init", "--bare", bareRepo)

	// ---- Phase 2: Clone locally, make initial commits, push ----
	t.Log("Phase 2: clone locally, commit, push")
	gitRun(t, "", "clone", bareRepo, localClone)
	gitRun(t, localClone, "config", "user.email", "test@seaweedfs.test")
	gitRun(t, localClone, "config", "user.name", "Test")

	// Commit 1
	writeFile(t, localClone, "README.md", "hello world\n")
	mkdirAll(t, localClone, "src")
	writeFile(t, localClone, "src/main.go", `package main; import "fmt"; func main() { fmt.Println("v1") }`)
	gitRun(t, localClone, "add", "-A")
	gitRun(t, localClone, "commit", "-m", "initial commit")
	commit1 := gitOutput(t, localClone, "rev-parse", "HEAD")

	// Commit 2: bulk files
	mkdirAll(t, localClone, "data")
	for i := 1; i <= 20; i++ {
		name := filepath.Join("data", "file-"+leftPad(i, 3)+".txt")
		writeFile(t, localClone, name, "content-"+strconv.Itoa(i)+"\n")
	}
	gitRun(t, localClone, "add", "-A")
	gitRun(t, localClone, "commit", "-m", "add data files")
	commit2 := gitOutput(t, localClone, "rev-parse", "HEAD")

	// Commit 3: modify + new dir
	writeFile(t, localClone, "src/main.go", `package main; import "fmt"; func main() { fmt.Println("v2") }`)
	writeFile(t, localClone, "README.md", "hello world\n# Updated\n")
	mkdirAll(t, localClone, "docs")
	writeFile(t, localClone, "docs/guide.md", "documentation\n")
	gitRun(t, localClone, "add", "-A")
	gitRun(t, localClone, "commit", "-m", "update src and add docs")
	commit3 := gitOutput(t, localClone, "rev-parse", "HEAD")

	branch := gitOutput(t, localClone, "rev-parse", "--abbrev-ref", "HEAD")
	gitRun(t, localClone, "push", "origin", branch)

	// ---- Phase 3: Clone from mount bare repo into on-mount working dir ----
	t.Log("Phase 3: clone from mount bare repo to on-mount working dir")
	gitRun(t, "", "clone", bareRepo, mountClone)

	assertFileContains(t, filepath.Join(mountClone, "README.md"), "# Updated")
	assertFileContains(t, filepath.Join(mountClone, "src/main.go"), "v2")
	assertFileExists(t, filepath.Join(mountClone, "docs/guide.md"))
	assertFileExists(t, filepath.Join(mountClone, "data/file-020.txt"))

	head := gitOutput(t, mountClone, "rev-parse", "HEAD")
	assert.Equal(t, commit3, head, "on-mount clone HEAD should be commit 3")

	dataFiles := countFiles(t, filepath.Join(mountClone, "data"))
	assert.Equal(t, 20, dataFiles, "data/ should have 20 files")

	// ---- Phase 4: Push more commits from the local clone ----
	t.Log("Phase 4: push more commits")

	for i := 21; i <= 50; i++ {
		name := filepath.Join("data", "file-"+leftPad(i, 3)+".txt")
		writeFile(t, localClone, name, "content-"+strconv.Itoa(i)+"\n")
	}
	writeFile(t, localClone, "src/main.go", `package main; import "fmt"; func main() { fmt.Println("v3") }`)
	gitRun(t, localClone, "add", "-A")
	gitRun(t, localClone, "commit", "-m", "expand data and update to v3")
	commit4 := gitOutput(t, localClone, "rev-parse", "HEAD")
	_ = commit4

	gitRun(t, localClone, "mv", "docs/guide.md", "docs/manual.md")
	gitRun(t, localClone, "rm", "data/file-001.txt")
	gitRun(t, localClone, "commit", "-m", "rename guide, remove file-001")
	commit5 := gitOutput(t, localClone, "rev-parse", "HEAD")

	gitRun(t, localClone, "push", "origin", branch)

	// ---- Phase 5: Checkout older revision in on-mount clone ----
	t.Log("Phase 5: checkout older revision on mount clone")
	gitRun(t, mountClone, "checkout", commit2)

	detachedHead := gitOutput(t, mountClone, "rev-parse", "HEAD")
	assert.Equal(t, commit2, detachedHead, "should be at commit 2")
	assertFileContains(t, filepath.Join(mountClone, "src/main.go"), "v1")
	assertFileNotExists(t, filepath.Join(mountClone, "docs/guide.md"))

	// ---- Phase 6: Return to branch and pull with real changes ----
	t.Log("Phase 6: checkout branch and pull")
	gitRun(t, mountClone, "checkout", branch)

	oldHead := gitOutput(t, mountClone, "rev-parse", "HEAD")
	assert.Equal(t, commit3, oldHead, "should be at commit 3 before pull")

	gitRun(t, mountClone, "pull")

	newHead := gitOutput(t, mountClone, "rev-parse", "HEAD")
	assert.Equal(t, commit5, newHead, "HEAD should be commit 5 after pull")

	assertFileContains(t, filepath.Join(mountClone, "src/main.go"), "v3")
	assertFileExists(t, filepath.Join(mountClone, "docs/manual.md"))
	assertFileNotExists(t, filepath.Join(mountClone, "docs/guide.md"))
	assertFileNotExists(t, filepath.Join(mountClone, "data/file-001.txt"))
	assertFileExists(t, filepath.Join(mountClone, "data/file-050.txt"))

	finalCount := countFiles(t, filepath.Join(mountClone, "data"))
	assert.Equal(t, 49, finalCount, "data/ should have 49 files after pull")

	// ---- Phase 7: Verify git log and status ----
	t.Log("Phase 7: verify log and status")
	logOutput := gitOutput(t, mountClone, "log", "--format=%s")
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	assert.Equal(t, 5, len(lines), "should have 5 commits in log")

	assert.Contains(t, logOutput, "initial commit")
	assert.Contains(t, logOutput, "expand data")
	assert.Contains(t, logOutput, "rename guide")

	status := gitOutput(t, mountClone, "status", "--porcelain")
	assert.Empty(t, status, "git status should be clean")

	_ = commit1 // used for documentation; not needed in assertions
}

// --- helpers ---

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s failed: %s", strings.Join(args, " "), string(out))
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s failed: %s", strings.Join(args, " "), string(out))
	return strings.TrimSpace(string(out))
}

func writeFile(t *testing.T, base, rel, content string) {
	t.Helper()
	p := filepath.Join(base, rel)
	require.NoError(t, os.WriteFile(p, []byte(content), 0644))
}

func mkdirAll(t *testing.T, base, rel string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(base, rel), 0755))
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err, "expected file to exist: %s", path)
}

func assertFileNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected file not to exist: %s", path)
}

func assertFileContains(t *testing.T, path, substr string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "failed to read %s", path)
	assert.Contains(t, string(data), substr, "file %s should contain %q", path, substr)
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "failed to read dir %s", dir)
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}

func leftPad(n, width int) string {
	s := strconv.Itoa(n)
	for len(s) < width {
		s = "0" + s
	}
	return s
}
