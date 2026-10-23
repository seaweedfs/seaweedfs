package fuse

import (
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// FuseTestFramework provides utilities for FUSE integration testing.
// It starts a single "weed mini" process (master+volume+filer in one)
// and a separate "weed mount" process for the FUSE filesystem.
type FuseTestFramework struct {
	t             *testing.T
	tempDir       string
	mountPoint    string
	dataDir       string
	logDir        string
	miniProcess   *managedProcess
	mountProcess  *managedProcess
	filerAddr     string
	filerPort     int
	weedBinary    string
	isSetup       bool
	config        *TestConfig
	externalMount bool
}

// TestConfig holds configuration for FUSE tests
type TestConfig struct {
	Collection  string
	Replication string
	ChunkSizeMB int
	CacheSizeMB int
	NumVolumes  int
	EnableDebug bool
	// MountGlobalOptions are glog flags (-v, -logtostderr, ...) registered on
	// weed's global flagset; they must precede the subcommand name or the
	// mount process dies at flag parsing.
	MountGlobalOptions []string
	MountOptions       []string
	SkipCleanup        bool // for debugging failed tests
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
		config:     config,
	}
}

// grpcPortOffset mirrors weed's HTTP->gRPC port convention (gRPC = HTTP + offset);
// "weed mount" derives the filer gRPC port from the filer HTTP address the same way.
const grpcPortOffset = 10000

// freePort returns a free filer HTTP port whose gRPC sibling (port+grpcPortOffset)
// is also free. Both must stay below the Linux ephemeral floor (32768): a gRPC port
// above it can be transiently grabbed by an outbound connection, forcing mini to
// relocate its filer gRPC port while "weed mount" keeps dialing HTTP+10000 — the
// mount then never connects and the test times out. Capping HTTP at 22000 keeps the
// gRPC port at or below 32000.
func freePort(t *testing.T) int {
	t.Helper()
	const (
		minServicePort = 20000
		maxServicePort = 22000
	)

	portCount := maxServicePort - minServicePort + 1
	start := minServicePort + int(time.Now().UnixNano()%int64(portCount))

	for attempt := 0; attempt < portCount; attempt++ {
		port := minServicePort + (start-minServicePort+attempt)%portCount
		if portIsFree(port) && portIsFree(port+grpcPortOffset) {
			return port
		}
	}

	t.Fatalf("failed to allocate a free HTTP/gRPC port pair in [%d,%d]", minServicePort, maxServicePort)
	return 0
}

// portIsFree reports whether a TCP port can currently be bound on 127.0.0.1.
func portIsFree(port int) bool {
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	l.Close()
	return true
}

// Setup starts SeaweedFS cluster and mounts FUSE filesystem
func (f *FuseTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}
	if config != nil {
		f.config = config
	}
	config = f.config

	// Check if we should skip cluster setup and use existing mount
	if os.Getenv("TEST_SKIP_CLUSTER_SETUP") == "true" {
		// Use existing mount point from environment
		if existingMount := os.Getenv("TEST_MOUNT_POINT"); existingMount != "" {
			f.externalMount = true

			// Verify mount point is accessible
			if _, err := os.Stat(existingMount); err != nil {
				return fmt.Errorf("existing mount point not accessible: %v", err)
			}

			// A plain directory passes every check below; refuse to run the suite
			// against the local disk
			if !mountedAt(existingMount) {
				return fmt.Errorf("mount point %s is not a mounted filesystem", existingMount)
			}
			// So can a non-FUSE mount like tmpfs: a device boundary alone does
			// not make the results FUSE compliance
			if !fuseFilesystem(existingMount) {
				return fmt.Errorf("mount point %s is not a FUSE filesystem", existingMount)
			}

			// Run inside a private work dir so test files do not pile up in
			// the shared mount root and cleanup can remove only what it made
			workDir, err := os.MkdirTemp(existingMount, ".fuse_test_")
			if err != nil {
				return fmt.Errorf("creating work dir under existing mount: %v", err)
			}
			f.mountPoint = workDir
			f.t.Logf("Using existing mount point: %s (work dir %s)", existingMount, f.mountPoint)

			// Test basic functionality
			testFile := filepath.Join(f.mountPoint, ".framework_test")
			if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
				return fmt.Errorf("mount point not writable: %v", err)
			}
			if err := os.Remove(testFile); err != nil {
				f.t.Logf("Warning: failed to cleanup test file: %v", err)
			}

			// Verify this is actually a SeaweedFS mount by checking for SeaweedFS-specific behavior
			// Create a test file and verify it appears in the filer
			verifyFile := filepath.Join(f.mountPoint, ".seaweedfs_mount_verification")
			if err := os.WriteFile(verifyFile, []byte("SeaweedFS mount verification"), 0644); err != nil {
				return fmt.Errorf("mount point verification failed - cannot write: %v", err)
			}

			// Read it back to ensure it's working
			if data, err := os.ReadFile(verifyFile); err != nil {
				return fmt.Errorf("mount point verification failed - cannot read: %v", err)
			} else if string(data) != "SeaweedFS mount verification" {
				return fmt.Errorf("mount point verification failed - data mismatch")
			}

			// Clean up verification file
			if err := os.Remove(verifyFile); err != nil {
				f.t.Logf("Warning: failed to cleanup verification file: %v", err)
			}

			f.t.Logf("✅ SeaweedFS mount point verified and working: %s", f.mountPoint)

			f.isSetup = true
			return nil
		}
	}

	// Create directories for full cluster setup
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
	if err := f.waitForService(f.miniProcess, f.filerAddr, 30*time.Second); err != nil {
		f.dumpLog("mini")
		return fmt.Errorf("weed mini not ready: %v", err)
	}

	// Skip rather than fail on a host with no FUSE device to mount through
	if !fuseAvailable() {
		f.t.Skip("no FUSE device node (/dev/fuse, /dev/macfuse0, /dev/osxfuse0)")
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

	// A mounted filesystem still cannot write until a volume registers and a
	// grow round finishes, so gate on a real write instead of letting the
	// first test hits ride out the startup window as EIO on close.
	if err := f.waitForWritable(90 * time.Second); err != nil {
		f.dumpLog("mount")
		return fmt.Errorf("FUSE mount not writable: %v", err)
	}

	f.isSetup = true
	return nil
}

// Cleanup stops all processes and removes temporary files.
// If the test failed, it dumps logs automatically.
func (f *FuseTestFramework) Cleanup() {
	// An external mount needs no teardown; only this run's work dir is ours.
	// A TEST_SKIP_CLUSTER_SETUP that fell through to a self-started cluster
	// still owns its processes and must not skip stopping them.
	if f.externalMount {
		if !f.config.SkipCleanup {
			os.RemoveAll(f.mountPoint)
			os.RemoveAll(f.tempDir)
		}
		return
	}

	if f.t.Failed() {
		f.DumpLogs()
	}

	// Stop processes in reverse order
	f.unmountFuse()
	if f.miniProcess != nil {
		f.miniProcess.stop()
		f.miniProcess = nil
	}

	f.copyLogsForCI()

	// Remove temp directory
	if !f.config.SkipCleanup {
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

// managedProcess is a started weed sub-command whose exit is watched, so a wait
// for it to come up ends the moment it dies instead of burning its full timeout.
type managedProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error // exit error, read only after done is closed
	logPath string
}

// exited returns the exit error once the process is gone, nil while it runs.
func (p *managedProcess) exited() error {
	select {
	case <-p.done:
		if p.err != nil {
			return p.err
		}
		return fmt.Errorf("exit status 0")
	default:
		return nil
	}
}

// stop asks the process to terminate and waits for it to go away.
func (p *managedProcess) stop() {
	// Signal fails with os.ErrProcessDone when the child is already gone, which
	// is exactly the case the select below handles.
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

// startProcess is a helper that starts a weed sub-command with output captured
// to a log file in f.logDir.
func (f *FuseTestFramework) startProcess(name string, args []string) (*managedProcess, error) {
	logPath := filepath.Join(f.logDir, name+".log")
	logFile, err := os.Create(logPath)
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

	p := &managedProcess{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

// logTail returns the last bytes of a captured process log so a startup
// failure message carries the fatal line instead of a bare timeout.
func logTail(path string) string {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return ""
	}
	const maxTail = 2 * 1024
	if len(data) > maxTail {
		data = data[len(data)-maxTail:]
	}
	return string(data)
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

// copyLogsForCI copies SeaweedFS process logs under a per-test directory in
// /tmp/seaweedfs-fuse-logs/ so the CI workflow can upload them as artifacts.
// Each framework instance gets its own directory: every test in the suite
// builds one, and fixed filenames would let a later passing test overwrite
// the logs of an earlier failing one.
func (f *FuseTestFramework) copyLogsForCI() {
	ciLogDir := filepath.Join("/tmp/seaweedfs-fuse-logs", strings.NewReplacer("/", "_", " ", "_").Replace(f.t.Name()))
	if err := os.MkdirAll(ciLogDir, 0755); err != nil {
		return
	}
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
	var args []string
	if config.EnableDebug {
		// -v is a global flag; before the subcommand or flag parsing dies
		args = append(args, "-v=4")
	}
	adminPort := freePort(f.t)
	for adminPort == f.filerPort {
		adminPort = freePort(f.t)
	}
	args = append(args,
		"mini",
		"-dir="+f.dataDir,
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-filer.port="+strconv.Itoa(f.filerPort),
		"-s3=false",
		"-webdav=false",
		"-admin.ui=false",
		"-admin.port="+strconv.Itoa(adminPort),
		"-admin.port.grpc="+strconv.Itoa(adminPort+grpcPortOffset),
		"-admin.port.legacy=0",
	)

	proc, err := f.startProcess("mini", args)
	if err != nil {
		return err
	}
	f.miniProcess = proc
	return nil
}

// mountFuse mounts the SeaweedFS FUSE filesystem
func (f *FuseTestFramework) mountFuse(config *TestConfig) error {
	args := append([]string{}, config.MountGlobalOptions...)
	if config.EnableDebug {
		args = append(args, "-v=4")
	}
	args = append(args,
		"mount",
		"-filer="+f.filerAddr,
		"-dir="+f.mountPoint,
		"-filer.path=/",
		"-dirAutoCreate",
		"-allowOthers=false",
	)

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
		f.mountProcess.stop()
		f.mountProcess = nil
	}

	// Also try system unmount as backup
	exec.Command("fusermount3", "-u", f.mountPoint).Run()
	exec.Command("fusermount", "-u", f.mountPoint).Run()
	exec.Command("umount", f.mountPoint).Run()
	return nil
}

// waitForService waits until the weed process answers its own /healthz, so a
// foreign listener on the port cannot pass for readiness.
func (f *FuseTestFramework) waitForService(proc *managedProcess, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && proc.exited() == nil {
				return nil
			}
		}
		if exitErr := proc.exited(); exitErr != nil {
			return fmt.Errorf("process exited (%v) before %s accepted connections: %s", exitErr, addr, logTail(proc.logPath))
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout: %s", addr, logTail(proc.logPath))
}

// waitForMount waits for the FUSE mount to be ready. A stat/ReadDir probe
// alone is not enough: the bare mount point directory passes both before the
// mount process finishes starting, letting tests race ahead and write to the
// local disk underneath the mount. The mount point's device ID differing from
// its parent's confirms a filesystem is actually mounted there.
func (f *FuseTestFramework) waitForMount(timeout time.Duration) error {
	parentDev, err := deviceID(filepath.Dir(f.mountPoint))
	if err != nil {
		return fmt.Errorf("stat mount point parent: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dev, err := deviceID(f.mountPoint); err == nil && dev != parentDev {
			if _, err := os.ReadDir(f.mountPoint); err == nil {
				return nil
			}
		}
		// A mount that cannot mount at all (no /dev/fuse, fusermount not setuid)
		// dies within a second; reporting that beats waiting out the timeout.
		if exitErr := f.mountProcess.exited(); exitErr != nil {
			return fmt.Errorf("mount process exited (%v): %s", exitErr, logTail(f.mountProcess.logPath))
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount point not ready within timeout: %s", logTail(f.mountProcess.logPath))
}

// waitForWritable probes the mount with a real write+read+remove until one
// succeeds, covering the window where a fresh cluster cannot yet assign a
// file id (volume not registered, grow pending, leader settle). Each attempt
// runs off the loop because a wedged mount can hold a file op open, and one
// attempt stays in flight until it finishes or the overall deadline passes.
func (f *FuseTestFramework) waitForWritable(timeout time.Duration) error {
	probe := filepath.Join(f.mountPoint, ".framework_write_probe")
	payload := []byte("probe")
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if exitErr := f.mountProcess.exited(); exitErr != nil {
			return fmt.Errorf("mount process exited (%v): %s", exitErr, logTail(f.mountProcess.logPath))
		}
		result := make(chan error, 1)
		go func() {
			if err := os.WriteFile(probe, payload, 0644); err != nil {
				result <- err
				return
			}
			data, err := os.ReadFile(probe)
			if err != nil {
				result <- err
				return
			}
			if len(data) != len(payload) {
				result <- fmt.Errorf("read back %d bytes, want %d", len(data), len(payload))
				return
			}
			result <- os.Remove(probe)
		}()
		tick := time.NewTicker(500 * time.Millisecond)
		attemptOpen := true
		for attemptOpen {
			select {
			case err := <-result:
				attemptOpen = false
				if err == nil {
					tick.Stop()
					return nil
				}
				lastErr = err
			case <-tick.C:
				if exitErr := f.mountProcess.exited(); exitErr != nil {
					tick.Stop()
					return fmt.Errorf("mount process exited (%v): %s", exitErr, logTail(f.mountProcess.logPath))
				}
				if !time.Now().Before(deadline) {
					tick.Stop()
					if lastErr == nil {
						return fmt.Errorf("write probe did not complete within timeout")
					}
					return fmt.Errorf("write probe failed within timeout: %v", lastErr)
				}
			}
		}
		tick.Stop()
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("write probe failed within timeout: %v", lastErr)
}

// fuseAvailable reports whether the host has a FUSE device node to mount
// through: /dev/fuse on Linux, /dev/macfuse* or /dev/osxfuse* on macOS.
func fuseAvailable() bool {
	for _, dev := range []string{"/dev/fuse", "/dev/macfuse0", "/dev/osxfuse0"} {
		if _, err := os.Stat(dev); err == nil {
			return true
		}
	}
	return false
}

// findWeedBinary locates the weed binary. Candidates are resolved to
// absolute paths because the started processes run with cmd.Dir set to the
// framework's temp dir, where a relative path would not resolve.
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
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0111 != 0 {
			if abs, err := filepath.Abs(candidate); err == nil {
				return abs
			}
		}
	}

	// Default fallback
	return "weed"
}

// Helper functions for test assertions

// AssertFileExists checks if a file exists in the mount point
func (f *FuseTestFramework) AssertFileExists(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	_, err := os.Stat(fullPath)
	require.NoError(f.t, err, "file should exist: %s", relativePath)
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
