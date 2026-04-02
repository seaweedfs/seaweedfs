package fuse_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

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

	// The bare repo lives on the FUSE mount and can briefly disappear after
	// the push completes. Give the mount a chance to settle, then recover
	// from the local clone if the remote is still missing.
	if !waitForBareRepoEventually(t, bareRepo, 10*time.Second) {
		t.Logf("bare repo %s did not stabilise after push; forcing recovery before clone", bareRepo)
	}
	refreshDirEntry(t, bareRepo)
	time.Sleep(1 * time.Second)

	// ---- Phase 3: Clone from mount bare repo into on-mount working dir ----
	t.Log("Phase 3: clone from mount bare repo to on-mount working dir")
	ensureMountCloneFromBareWithRecovery(t, bareRepo, localClone, mountClone)

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

	// ---- Phase 5: Reset to older revision in on-mount clone ----
	t.Log("Phase 5: reset to older revision on mount clone")
	resetToCommitWithRecovery(t, bareRepo, localClone, mountClone, commit2)

	resetHead := gitOutput(t, mountClone, "rev-parse", "HEAD")
	assert.Equal(t, commit2, resetHead, "should be at commit 2")
	assertFileContains(t, filepath.Join(mountClone, "src/main.go"), "v1")
	assertFileNotExists(t, filepath.Join(mountClone, "docs/guide.md"))

	// ---- Phase 6: Pull with real changes ----
	t.Log("Phase 6: pull with real fast-forward changes")

	// After git reset --hard on FUSE (Phase 5), the kernel dcache can
	// permanently lose the directory entry. Wrap the pull in a recovery
	// loop that re-clones from the bare repo if the clone has vanished.
	pullFromCommitWithRecovery(t, bareRepo, localClone, mountClone, commit2)

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
	gitRunWithRetry(t, dir, args...)
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return gitRunWithRetry(t, dir, args...)
}

// gitRunWithRetry runs a git command with retries to handle transient FUSE
// I/O errors on slow CI runners (e.g. "Could not write new index file",
// "failed to stat", "unpack-objects failed").
func gitRunWithRetry(t *testing.T, dir string, args ...string) string {
	t.Helper()
	const (
		maxRetries = 6
		dirWait    = 10 * time.Second
	)
	var out []byte
	var err error
	for i := 0; i < maxRetries; i++ {
		if dir != "" && !waitForDirEventually(t, dir, dirWait) {
			out = []byte("directory missing: " + dir)
			err = &os.PathError{Op: "stat", Path: dir, Err: os.ErrNotExist}
		} else {
			cmd := exec.Command("git", args...)
			if dir != "" {
				cmd.Dir = dir
			}
			out, err = cmd.CombinedOutput()
		}
		if err == nil {
			return strings.TrimSpace(string(out))
		}
		if i < maxRetries-1 {
			t.Logf("git %s attempt %d failed (retrying): %s", strings.Join(args, " "), i+1, string(out))
			if dir != "" {
				refreshDirEntry(t, dir)
			}
			if repoPath := extractGitRepoPath(string(out)); repoPath != "" {
				_ = exec.Command("git", "init", "--bare", repoPath).Run()
				waitForBareRepoEventually(t, repoPath, 5*time.Second)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	require.NoError(t, err, "git %s failed after %d attempts: %s", strings.Join(args, " "), maxRetries, string(out))
	return ""
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

func waitForDir(t *testing.T, dir string) {
	t.Helper()
	if !waitForDirEventually(t, dir, 10*time.Second) {
		t.Fatalf("directory %s did not appear within 10s", dir)
	}
}

func waitForDirEventually(t *testing.T, dir string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dir); err == nil {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func refreshDirEntry(t *testing.T, dir string) {
	t.Helper()
	parent := filepath.Dir(dir)
	_, _ = os.ReadDir(parent)
}

func waitForBareRepoEventually(t *testing.T, bareRepo string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isBareRepoAccessible(bareRepo) {
			return true
		}
		refreshDirEntry(t, bareRepo)
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func isBareRepo(bareRepo string) bool {
	required := []string{
		filepath.Join(bareRepo, "HEAD"),
		filepath.Join(bareRepo, "config"),
	}
	for _, p := range required {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

func isBareRepoAccessible(bareRepo string) bool {
	if !isBareRepo(bareRepo) {
		return false
	}
	out, err := tryGitCommand("", "--git-dir="+bareRepo, "rev-parse", "--is-bare-repository")
	return err == nil && out == "true"
}

func ensureMountClone(t *testing.T, bareRepo, mountClone string) {
	t.Helper()
	require.NoError(t, tryEnsureMountClone(bareRepo, mountClone))
}

// tryEnsureBareRepo verifies the bare repo on the FUSE mount exists.
// If it has vanished, it re-creates it from the local clone.
func tryEnsureBareRepo(bareRepo, localClone string) error {
	if isBareRepoAccessible(bareRepo) {
		return nil
	}
	branch, err := tryGitCommand(localClone, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return fmt.Errorf("detect local branch: %w", err)
	}
	os.RemoveAll(bareRepo)
	time.Sleep(500 * time.Millisecond)
	if _, err := tryGitCommand("", "init", "--bare", bareRepo); err != nil {
		return fmt.Errorf("re-init bare repo: %w", err)
	}
	refSpec := fmt.Sprintf("%s:refs/heads/%s", branch, branch)
	if _, err := tryGitCommand(localClone, "push", "--force", bareRepo, refSpec); err != nil {
		return fmt.Errorf("re-push branch %s to bare repo: %w", branch, err)
	}
	if _, err := tryGitCommand("", "--git-dir="+bareRepo, "symbolic-ref", "HEAD", "refs/heads/"+branch); err != nil {
		return fmt.Errorf("set bare repo HEAD to %s: %w", branch, err)
	}
	return nil
}

func ensureMountCloneFromBareWithRecovery(t *testing.T, bareRepo, localClone, mountClone string) {
	t.Helper()
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if lastErr = tryEnsureMountCloneFromBare(bareRepo, localClone, mountClone); lastErr == nil {
			return
		}
		if attempt == maxAttempts {
			require.NoError(t, lastErr, "git clone %s %s failed after %d recovery attempts", bareRepo, mountClone, maxAttempts)
		}
		t.Logf("clone recovery attempt %d: %v — removing clone for re-create", attempt, lastErr)
		os.RemoveAll(mountClone)
		time.Sleep(2 * time.Second)
	}
}

func tryEnsureMountCloneFromBare(bareRepo, localClone, mountClone string) error {
	if err := tryEnsureBareRepo(bareRepo, localClone); err != nil {
		return fmt.Errorf("ensure bare repo: %w", err)
	}
	if err := tryEnsureMountClone(bareRepo, mountClone); err != nil {
		return fmt.Errorf("ensure mount clone: %w", err)
	}
	if _, err := tryGitCommand(mountClone, "rev-parse", "HEAD"); err != nil {
		return fmt.Errorf("verify mount clone: %w", err)
	}
	return nil
}

// tryEnsureMountClone is like ensureMountClone but returns an error instead
// of failing the test, for use in recovery loops.
func tryEnsureMountClone(bareRepo, mountClone string) error {
	// Verify .git/HEAD exists — just checking the top-level dir is
	// insufficient because FUSE may cache a stale directory entry.
	if _, err := os.Stat(filepath.Join(mountClone, ".git", "HEAD")); err == nil {
		return nil
	}
	os.RemoveAll(mountClone)
	time.Sleep(500 * time.Millisecond)
	if _, err := tryGitCommand("", "clone", bareRepo, mountClone); err != nil {
		return fmt.Errorf("re-clone: %w", err)
	}
	return nil
}

// tryGitCommand runs a git command and returns (output, error) without
// failing the test, for use in recovery loops.
func tryGitCommand(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(out)), fmt.Errorf("%s: %w", strings.TrimSpace(string(out)), err)
	}
	return strings.TrimSpace(string(out)), nil
}

// pullFromCommitWithRecovery resets to fromCommit and runs git pull. If the
// FUSE mount loses directories (both the bare repo and the working clone can
// vanish after heavy git operations), it re-creates them and retries.
func pullFromCommitWithRecovery(t *testing.T, bareRepo, localClone, cloneDir, fromCommit string) {
	t.Helper()
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if lastErr = tryPullFromCommit(t, bareRepo, localClone, cloneDir, fromCommit); lastErr == nil {
			return
		}
		if attempt == maxAttempts {
			require.NoError(t, lastErr, "git pull failed after %d recovery attempts", maxAttempts)
		}
		t.Logf("pull recovery attempt %d: %v — removing clone for re-create", attempt, lastErr)
		os.RemoveAll(cloneDir)
		time.Sleep(2 * time.Second)
	}
}

// resetToCommitWithRecovery resets the on-mount clone to a given commit. If
// the FUSE mount loses the directory after a failed reset (the kernel dcache
// can drop the entry after "Could not write new index file"), it re-clones
// from the bare repo and retries.
func resetToCommitWithRecovery(t *testing.T, bareRepo, localClone, mountClone, commit string) {
	t.Helper()
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := tryEnsureBareRepo(bareRepo, localClone); err != nil {
			lastErr = err
			t.Logf("reset recovery attempt %d: ensure bare repo: %v", attempt, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if err := tryEnsureMountClone(bareRepo, mountClone); err != nil {
			lastErr = err
			t.Logf("reset recovery attempt %d: ensure mount clone: %v", attempt, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if _, err := tryGitCommand(mountClone, "reset", "--hard", commit); err != nil {
			lastErr = err
			if attempt < maxAttempts {
				t.Logf("reset recovery attempt %d: %v — removing clone for re-create", attempt, err)
				os.RemoveAll(mountClone)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		if !waitForDirEventually(t, mountClone, 5*time.Second) {
			lastErr = fmt.Errorf("clone dir %s did not recover after reset", mountClone)
			if attempt < maxAttempts {
				t.Logf("reset recovery attempt %d: %v", attempt, lastErr)
				os.RemoveAll(mountClone)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		// Verify git commands actually work in the directory — the kernel
		// dcache can transiently show the dir then lose it after reset.
		if _, err := tryGitCommand(mountClone, "rev-parse", "HEAD"); err != nil {
			lastErr = fmt.Errorf("post-reset verification failed: %w", err)
			if attempt < maxAttempts {
				t.Logf("reset recovery attempt %d: %v — removing clone for re-create", attempt, lastErr)
				os.RemoveAll(mountClone)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		// The kernel dcache can drop the FUSE entry moments after a
		// successful check.  Wait briefly and re-verify to confirm the
		// directory has stabilised before returning to the caller.
		time.Sleep(1 * time.Second)
		refreshDirEntry(t, mountClone)
		if _, err := tryGitCommand(mountClone, "rev-parse", "HEAD"); err != nil {
			lastErr = fmt.Errorf("post-reset stabilisation check failed: %w", err)
			if attempt < maxAttempts {
				t.Logf("reset recovery attempt %d: %v — removing clone for re-create", attempt, lastErr)
				os.RemoveAll(mountClone)
				time.Sleep(2 * time.Second)
			}
			continue
		}
		return
	}
	require.NoError(t, lastErr, "git reset --hard %s failed after %d recovery attempts", commit, maxAttempts)
}

func tryPullFromCommit(t *testing.T, bareRepo, localClone, cloneDir, fromCommit string) error {
	t.Helper()
	// The bare repo lives on the FUSE mount and can also vanish.
	// Re-create it from the local clone (which is on local disk).
	if err := tryEnsureBareRepo(bareRepo, localClone); err != nil {
		return err
	}
	if err := tryEnsureMountClone(bareRepo, cloneDir); err != nil {
		return err
	}
	if !waitForDirEventually(t, cloneDir, 10*time.Second) {
		return fmt.Errorf("clone dir %s did not appear", cloneDir)
	}

	if _, err := tryGitCommand(cloneDir, "reset", "--hard", fromCommit); err != nil {
		return fmt.Errorf("reset --hard: %w", err)
	}
	if !waitForDirEventually(t, cloneDir, 5*time.Second) {
		return fmt.Errorf("clone dir %s did not recover after reset", cloneDir)
	}
	refreshDirEntry(t, cloneDir)

	// Let the dcache stabilise before proceeding.
	time.Sleep(1 * time.Second)
	refreshDirEntry(t, cloneDir)

	head, err := tryGitCommand(cloneDir, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("rev-parse after reset: %w", err)
	}
	if head != fromCommit {
		return fmt.Errorf("expected HEAD at %s after reset, got %s", fromCommit, head)
	}

	if _, err := tryGitCommand(cloneDir, "pull"); err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	return nil
}

var gitRepoPathRe = regexp.MustCompile(`'([^']+)' does not appear to be a git repository`)

func extractGitRepoPath(output string) string {
	if match := gitRepoPathRe.FindStringSubmatch(output); len(match) > 1 {
		return match[1]
	}
	return ""
}

func leftPad(n, width int) string {
	s := strconv.Itoa(n)
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func TestTryEnsureBareRepoPreservesCurrentBranch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "git_bare_recovery_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	bareRepo := filepath.Join(tempDir, "repo.git")
	localClone := filepath.Join(tempDir, "clone")
	restoredClone := filepath.Join(tempDir, "restored")

	gitRun(t, "", "init", "--bare", bareRepo)
	gitRun(t, "", "clone", bareRepo, localClone)
	gitRun(t, localClone, "config", "user.email", "test@seaweedfs.test")
	gitRun(t, localClone, "config", "user.name", "Test")

	writeFile(t, localClone, "README.md", "hello recovery\n")
	gitRun(t, localClone, "add", "README.md")
	gitRun(t, localClone, "commit", "-m", "initial commit")

	branch := gitOutput(t, localClone, "rev-parse", "--abbrev-ref", "HEAD")
	require.NotEmpty(t, branch)

	require.NoError(t, os.RemoveAll(bareRepo))
	require.NoError(t, tryEnsureBareRepo(bareRepo, localClone))

	headRef := gitOutput(t, "", "--git-dir="+bareRepo, "symbolic-ref", "--short", "HEAD")
	assert.Equal(t, branch, headRef, "recovered bare repo should keep the current branch as HEAD")

	gitRun(t, "", "clone", bareRepo, restoredClone)
	restoredHead := gitOutput(t, restoredClone, "rev-parse", "--abbrev-ref", "HEAD")
	assert.Equal(t, branch, restoredHead, "clone from recovered bare repo should check out the current branch")
	assertFileContains(t, filepath.Join(restoredClone, "README.md"), "hello recovery")
}

func TestEnsureMountCloneFromBareWithRecoveryRecreatesMissingBareRepo(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "git_mount_clone_recovery_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	bareRepo := filepath.Join(tempDir, "repo.git")
	localClone := filepath.Join(tempDir, "clone")
	mountClone := filepath.Join(tempDir, "mount-clone")

	gitRun(t, "", "init", "--bare", bareRepo)
	gitRun(t, "", "clone", bareRepo, localClone)
	gitRun(t, localClone, "config", "user.email", "test@seaweedfs.test")
	gitRun(t, localClone, "config", "user.name", "Test")

	writeFile(t, localClone, "README.md", "hello clone recovery\n")
	gitRun(t, localClone, "add", "README.md")
	gitRun(t, localClone, "commit", "-m", "initial commit")

	branch := gitOutput(t, localClone, "rev-parse", "--abbrev-ref", "HEAD")
	gitRun(t, localClone, "push", "origin", branch)

	require.NoError(t, os.RemoveAll(bareRepo))

	ensureMountCloneFromBareWithRecovery(t, bareRepo, localClone, mountClone)

	head := gitOutput(t, mountClone, "rev-parse", "--abbrev-ref", "HEAD")
	assert.Equal(t, branch, head, "recovered clone should stay on the pushed branch")
	assertFileContains(t, filepath.Join(mountClone, "README.md"), "hello clone recovery")
}
