package fuse_test

import (
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// FuseTestFramework provides utilities for FUSE integration testing.
// It starts a single "weed mini" process (master+volume+filer in one)
// and a separate "weed mount" process for the FUSE filesystem.
type FuseTestFramework struct {
	t            *testing.T
	tempDir      string
	mountPoint   string
	dataDir      string
	logDir       string
	miniProcess  *os.Process
	mountProcess *os.Process
	filerAddr    string
	filerPort    int
	weedBinary   string
	isSetup      bool
}

// TestConfig holds configuration for FUSE tests
type TestConfig struct {
	Collection   string
	Replication  string
	ChunkSizeMB  int
	CacheSizeMB  int
	NumVolumes   int
	EnableDebug  bool
	MountOptions []string
	SkipCleanup  bool // for debugging failed tests
}

// DefaultTestConfig returns a default configuration for FUSE tests
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		Collection:   "",
		Replication:  "000",
		ChunkSizeMB:  4,
		CacheSizeMB:  100,
		NumVolumes:   3,
		EnableDebug:  false,
		MountOptions: []string{},
		SkipCleanup:  false,
	}
}

// NewFuseTestFramework creates a new FUSE testing framework.
func NewFuseTestFramework(t *testing.T, config *TestConfig) *FuseTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_fuse_test_")
	require.NoError(t, err)

	filerPort := freePort(t)

	return &FuseTestFramework{
		t:          t,
		tempDir:    tempDir,
		mountPoint: filepath.Join(tempDir, "mount"),
		dataDir:    filepath.Join(tempDir, "data"),
		logDir:     filepath.Join(tempDir, "logs"),
		filerPort:  filerPort,
		filerAddr:  fmt.Sprintf("127.0.0.1:%d", filerPort),
		weedBinary: findWeedBinary(),
		isSetup:    false,
	}
}

// freePort asks the OS for a free TCP port in a range where the gRPC
// offset (port + 10000) won't collide with well-known ports.
// Stay below the Linux ephemeral floor (32768) so the kernel does not
// reuse the chosen port for an outbound connection between close() here
// and re-bind in the child "weed mini" process.
func freePort(t *testing.T) int {
	t.Helper()
	const (
		minServicePort = 20000
		maxServicePort = 32000
	)

	portCount := maxServicePort - minServicePort + 1
	start := minServicePort + int(time.Now().UnixNano()%int64(portCount))

	for attempt := 0; attempt < 512; attempt++ {
		port := minServicePort + (start-minServicePort+attempt)%portCount
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		l.Close()
		return port
	}

	t.Fatalf("failed to allocate port <= %d after repeated attempts", maxServicePort)
	return 0
}

// Setup starts "weed mini" and mounts the FUSE filesystem.
func (f *FuseTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}

	dirs := []string{f.mountPoint, f.logDir, f.dataDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	// Start weed mini (master + volume + filer in one process)
	if err := f.startMini(config); err != nil {
		return fmt.Errorf("failed to start weed mini: %v", err)
	}

	// Wait for filer to be ready (mini starts all services on filerPort)
	if err := f.waitForService(f.filerAddr, 30*time.Second); err != nil {
		f.dumpLog("mini")
		return fmt.Errorf("weed mini not ready: %v", err)
	}

	// Mount FUSE filesystem
	if err := f.mountFuse(config); err != nil {
		return fmt.Errorf("failed to mount FUSE: %v", err)
	}

	// Wait for mount to be ready
	if err := f.waitForMount(30 * time.Second); err != nil {
		f.dumpLog("mount")
		return fmt.Errorf("FUSE mount not ready: %v", err)
	}

	f.isSetup = true
	return nil
}

// Cleanup stops all processes and removes temporary files.
// If the test failed, it dumps logs automatically.
func (f *FuseTestFramework) Cleanup() {
	if f.t.Failed() {
		f.DumpLogs()
	}

	if f.mountProcess != nil {
		f.unmountFuse()
	}

	// Stop processes in reverse order
	for _, proc := range []*os.Process{f.mountProcess, f.miniProcess} {
		if proc != nil {
			proc.Signal(syscall.SIGTERM)
			proc.Wait()
		}
	}

	f.copyLogsForCI()

	if !DefaultTestConfig().SkipCleanup {
		os.RemoveAll(f.tempDir)
	}
}

// DumpLogs prints the tail of all SeaweedFS process logs to test output.
func (f *FuseTestFramework) DumpLogs() {
	for _, name := range []string{"mini", "mount"} {
		f.dumpLog(name)
	}
}

// GetMountPoint returns the FUSE mount point path
func (f *FuseTestFramework) GetMountPoint() string {
	return f.mountPoint
}

// GetFilerAddr returns the filer address
func (f *FuseTestFramework) GetFilerAddr() string {
	return f.filerAddr
}

// startProcess is a helper that starts a weed sub-command with output captured
// to a log file in f.logDir.
func (f *FuseTestFramework) startProcess(name string, args []string) (*os.Process, error) {
	logFile, err := os.Create(filepath.Join(f.logDir, name+".log"))
	if err != nil {
		return nil, fmt.Errorf("create log file: %v", err)
	}
	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	// Close the file handle — the child process inherited it.
	logFile.Close()
	return cmd.Process, nil
}

// dumpLog prints the last lines of a process log file to the test output
// for debugging when a service fails to start or a test fails.
func (f *FuseTestFramework) dumpLog(name string) {
	data, err := os.ReadFile(filepath.Join(f.logDir, name+".log"))
	if err != nil {
		f.t.Logf("[%s log] (not available: %v)", name, err)
		return
	}
	// Show last 16KB on failure for meaningful context.
	const maxTail = 16 * 1024
	if len(data) > maxTail {
		data = data[len(data)-maxTail:]
	}
	f.t.Logf("[%s log tail (%d bytes)]\n%s", name, len(data), string(data))
}

// copyLogsForCI copies SeaweedFS process logs to /tmp/seaweedfs-fuse-logs/
// so the CI workflow can upload them as artifacts.
func (f *FuseTestFramework) copyLogsForCI() {
	ciLogDir := "/tmp/seaweedfs-fuse-logs"
	os.MkdirAll(ciLogDir, 0755)
	for _, name := range []string{"mini", "mount"} {
		src := filepath.Join(f.logDir, name+".log")
		data, err := os.ReadFile(src)
		if err != nil {
			continue
		}
		os.WriteFile(filepath.Join(ciLogDir, name+".log"), data, 0644)
	}
}

// startMini starts "weed mini" which runs master+volume+filer in one process.
func (f *FuseTestFramework) startMini(config *TestConfig) error {
	args := []string{
		"mini",
		"-dir=" + f.dataDir,
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-filer.port=" + strconv.Itoa(f.filerPort),
		"-s3=false",
		"-webdav=false",
		"-admin.ui=false",
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	proc, err := f.startProcess("mini", args)
	if err != nil {
		return err
	}
	f.miniProcess = proc
	return nil
}

// mountFuse mounts the SeaweedFS FUSE filesystem
func (f *FuseTestFramework) mountFuse(config *TestConfig) error {
	args := []string{
		"mount",
		"-filer=127.0.0.1:" + strconv.Itoa(f.filerPort),
		"-dir=" + f.mountPoint,
		"-filer.path=/",
		"-dirAutoCreate",
		"-allowOthers=false",
	}

	if config.Collection != "" {
		args = append(args, "-collection="+config.Collection)
	}
	if config.Replication != "" {
		args = append(args, "-replication="+config.Replication)
	}
	if config.ChunkSizeMB > 0 {
		args = append(args, fmt.Sprintf("-chunkSizeLimitMB=%d", config.ChunkSizeMB))
	}
	if config.CacheSizeMB > 0 {
		args = append(args, fmt.Sprintf("-cacheCapacityMB=%d", config.CacheSizeMB))
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	args = append(args, config.MountOptions...)

	proc, err := f.startProcess("mount", args)
	if err != nil {
		return err
	}
	f.mountProcess = proc
	return nil
}

// unmountFuse unmounts the FUSE filesystem
func (f *FuseTestFramework) unmountFuse() error {
	if f.mountProcess != nil {
		f.mountProcess.Signal(syscall.SIGTERM)
		f.mountProcess.Wait()
		f.mountProcess = nil
	}

	// Also try system unmount as backup
	exec.Command("fusermount3", "-u", f.mountPoint).Run()
	exec.Command("fusermount", "-u", f.mountPoint).Run()
	return nil
}

// waitForService waits for a service to be available
func (f *FuseTestFramework) waitForService(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout", addr)
}

// waitForMount waits for the FUSE mount to be ready
func (f *FuseTestFramework) waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(f.mountPoint); err == nil {
			if _, err := os.ReadDir(f.mountPoint); err == nil {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount point not ready within timeout")
}

// findWeedBinary locates the weed binary.
func findWeedBinary() string {
	if p, err := exec.LookPath("weed"); err == nil {
		return p
	}

	candidates := []string{
		"../../weed/weed",
		"./weed",
		"../weed",
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			abs, _ := filepath.Abs(candidate)
			return abs
		}
	}

	return "weed"
}

// Helper functions for test assertions

// AssertFileExists checks if a file exists in the mount point. On failure,
// it gathers diagnostic state (retry stat, parent listing, direct open) so
// transient FUSE flakes leave enough information to identify the layer that
// dropped the entry. The diagnostic block runs only on the failure path.
func (f *FuseTestFramework) AssertFileExists(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	_, err := os.Stat(fullPath)
	if err == nil {
		return
	}
	f.dumpExistenceDiagnostics(relativePath, fullPath, err)
	require.NoError(f.t, err, "file should exist: %s (see diagnostic logs above)", relativePath)
}

// dumpExistenceDiagnostics is invoked when AssertFileExists fails. It probes
// whether the missing entry is transient (retries appear), missing from the
// parent directory listing, or missing via a direct open syscall — three
// signals that triangulate where the entry was lost (kernel dentry cache vs
// mount lookup vs filer).
func (f *FuseTestFramework) dumpExistenceDiagnostics(relativePath, fullPath string, initialErr error) {
	f.t.Logf("AssertFileExists diagnostic dump for %s", relativePath)
	f.t.Logf("  initial stat: err=%v isNotExist=%v", initialErr, os.IsNotExist(initialErr))

	// Retry stat to detect transient invisibility — if it becomes visible
	// the issue is a short-lived dentry/cache window; if it stays missing
	// the entry is genuinely gone from the filer or the local cache.
	for attempt := 1; attempt <= 5; attempt++ {
		time.Sleep(100 * time.Millisecond)
		_, err := os.Stat(fullPath)
		f.t.Logf("  retry #%d after %dms: err=%v", attempt, attempt*100, err)
		if err == nil {
			break
		}
	}

	// List the parent directory: if the file appears here but stat fails,
	// the failure is in the kernel's per-name dentry cache; if not, the
	// mount-side LOOKUP itself is dropping the entry.
	parentDir := filepath.Dir(fullPath)
	target := filepath.Base(fullPath)
	if entries, listErr := os.ReadDir(parentDir); listErr != nil {
		f.t.Logf("  ReadDir(%s) failed: %v", parentDir, listErr)
	} else {
		found := false
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
			if e.Name() == target {
				found = true
			}
		}
		f.t.Logf("  ReadDir(%s): %d entries, target %q present=%v", parentDir, len(entries), target, found)
		if len(names) <= 32 {
			f.t.Logf("  ReadDir entries: %v", names)
		}
	}

	// Direct O_RDONLY open — exercises the same FUSE Lookup+Open path
	// userspace would, but separately from stat, in case stat-only caching
	// is interfering.
	if fd, openErr := os.OpenFile(fullPath, os.O_RDONLY, 0); openErr != nil {
		f.t.Logf("  Open(%s, O_RDONLY): %v", fullPath, openErr)
	} else {
		st, statErr := fd.Stat()
		fd.Close()
		size := int64(-1)
		if statErr == nil && st != nil {
			size = st.Size()
		}
		f.t.Logf("  Open(%s, O_RDONLY): success size=%d statErr=%v", fullPath, size, statErr)
	}
}

// AssertFileNotExists checks if a file does not exist in the mount point
func (f *FuseTestFramework) AssertFileNotExists(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	_, err := os.Stat(fullPath)
	require.True(f.t, os.IsNotExist(err), "file should not exist: %s", relativePath)
}

// AssertFileContent checks if a file has expected content
func (f *FuseTestFramework) AssertFileContent(relativePath string, expectedContent []byte) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	actualContent, err := os.ReadFile(fullPath)
	require.NoError(f.t, err, "failed to read file: %s", relativePath)
	require.Equal(f.t, expectedContent, actualContent, "file content mismatch: %s", relativePath)
}

// AssertFileMode checks if a file has expected permissions
func (f *FuseTestFramework) AssertFileMode(relativePath string, expectedMode fs.FileMode) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	info, err := os.Stat(fullPath)
	require.NoError(f.t, err, "failed to stat file: %s", relativePath)
	require.Equal(f.t, expectedMode, info.Mode(), "file mode mismatch: %s", relativePath)
}

// CreateTestFile creates a test file with specified content
func (f *FuseTestFramework) CreateTestFile(relativePath string, content []byte) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	dir := filepath.Dir(fullPath)
	require.NoError(f.t, os.MkdirAll(dir, 0755), "failed to create directory: %s", dir)
	require.NoError(f.t, os.WriteFile(fullPath, content, 0644), "failed to create file: %s", relativePath)
}

// CreateTestDir creates a test directory
func (f *FuseTestFramework) CreateTestDir(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	require.NoError(f.t, os.MkdirAll(fullPath, 0755), "failed to create directory: %s", relativePath)
}
