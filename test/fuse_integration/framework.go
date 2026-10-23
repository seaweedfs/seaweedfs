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
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// FuseTestFramework provides utilities for FUSE integration testing
type FuseTestFramework struct {
	t             *testing.T
	tempDir       string
	mountPoint    string
	dataDir       string
	masterProcess *managedProcess
	volumeProcess *managedProcess
	filerProcess  *managedProcess
	mountProcess  *managedProcess
	masterPort    int
	volumePort    int
	filerPort     int
	masterAddr    string
	volumeAddr    string
	filerAddr     string
	weedBinary    string
	isSetup       bool
	config        *TestConfig
	externalMount bool
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

// NewFuseTestFramework creates a new FUSE testing framework
func NewFuseTestFramework(t *testing.T, config *TestConfig) *FuseTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_fuse_test_")
	require.NoError(t, err)

	return &FuseTestFramework{
		t:          t,
		tempDir:    tempDir,
		mountPoint: filepath.Join(tempDir, "mount"),
		dataDir:    filepath.Join(tempDir, "data"),
		weedBinary: findWeedBinary(),
		isSetup:    false,
		config:     config,
	}
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

	// Ports are picked here rather than in the constructor so an external
	// mount never probes the service port band.
	allocated := make(map[int]bool)
	f.masterPort = freeServicePort(f.t, allocated)
	f.volumePort = freeServicePort(f.t, allocated)
	f.filerPort = freeServicePort(f.t, allocated)
	f.masterAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(f.masterPort))
	f.volumeAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(f.volumePort))
	f.filerAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(f.filerPort))

	// Create directories for full cluster setup; weed volume refuses a -dir
	// that does not already exist.
	dirs := []string{f.mountPoint, f.dataDir, filepath.Join(f.dataDir, "volume")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	// Start master
	if err := f.startMaster(config); err != nil {
		return fmt.Errorf("failed to start master: %v", err)
	}

	// Wait for master to be ready
	if err := f.waitForService(f.masterProcess, f.masterAddr, 30*time.Second); err != nil {
		return fmt.Errorf("master not ready: %v", err)
	}

	// Start volume servers
	if err := f.startVolumeServers(config); err != nil {
		return fmt.Errorf("failed to start volume servers: %v", err)
	}

	// Wait for volume server to be ready
	if err := f.waitForService(f.volumeProcess, f.volumeAddr, 30*time.Second); err != nil {
		return fmt.Errorf("volume server not ready: %v", err)
	}

	// Start filer
	if err := f.startFiler(config); err != nil {
		return fmt.Errorf("failed to start filer: %v", err)
	}

	// Wait for filer to be ready
	if err := f.waitForService(f.filerProcess, f.filerAddr, 30*time.Second); err != nil {
		return fmt.Errorf("filer not ready: %v", err)
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
	if err := f.waitForMount(f.mountProcess, 30*time.Second); err != nil {
		return fmt.Errorf("FUSE mount not ready: %v", err)
	}

	f.isSetup = true
	return nil
}

// Cleanup stops all processes and removes temporary files
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

	if f.mountProcess != nil {
		f.unmountFuse()
	}

	// Stop processes in reverse order
	processes := []*managedProcess{f.mountProcess, f.filerProcess, f.volumeProcess, f.masterProcess}
	for _, proc := range processes {
		if proc != nil {
			proc.stop()
		}
	}

	// Remove temp directory
	if !f.config.SkipCleanup {
		os.RemoveAll(f.tempDir)
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

// startMaster starts the SeaweedFS master server
func (f *FuseTestFramework) startMaster(config *TestConfig) error {
	var args []string
	if config.EnableDebug {
		// -v is a global flag; before the subcommand or flag parsing dies
		args = append(args, "-v=4")
	}
	args = append(args,
		"master",
		"-ip=127.0.0.1",
		"-port="+strconv.Itoa(f.masterPort),
		"-mdir="+filepath.Join(f.dataDir, "master"),
		"-raftBootstrap",
	)

	proc, err := f.startProcess("master", args)
	if err != nil {
		return err
	}
	f.masterProcess = proc
	return nil
}

// startVolumeServers starts SeaweedFS volume servers
func (f *FuseTestFramework) startVolumeServers(config *TestConfig) error {
	var args []string
	if config.EnableDebug {
		args = append(args, "-v=4")
	}
	args = append(args,
		"volume",
		"-mserver="+f.masterAddr,
		"-ip=127.0.0.1",
		"-port="+strconv.Itoa(f.volumePort),
		"-dir="+filepath.Join(f.dataDir, "volume"),
		fmt.Sprintf("-max=%d", config.NumVolumes),
	)

	proc, err := f.startProcess("volume", args)
	if err != nil {
		return err
	}
	f.volumeProcess = proc
	return nil
}

// startFiler starts the SeaweedFS filer server
func (f *FuseTestFramework) startFiler(config *TestConfig) error {
	var args []string
	if config.EnableDebug {
		args = append(args, "-v=4")
	}
	args = append(args,
		"filer",
		"-master="+f.masterAddr,
		"-ip=127.0.0.1",
		"-port="+strconv.Itoa(f.filerPort),
	)

	proc, err := f.startProcess("filer", args)
	if err != nil {
		return err
	}
	f.filerProcess = proc
	return nil
}

// mountFuse mounts the SeaweedFS FUSE filesystem
func (f *FuseTestFramework) mountFuse(config *TestConfig) error {
	var args []string
	if config.EnableDebug {
		args = append(args, "-v=4")
	}
	args = append(args,
		"mount",
		"-filer="+f.filerAddr,
		"-dir="+f.mountPoint,
		"-filer.path=/",
		"-dirAutoCreate",
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

// waitForMount waits for the FUSE mount to be ready
func (f *FuseTestFramework) waitForMount(proc *managedProcess, timeout time.Duration) error {
	// The mount point is created before weed mount starts, so Stat/ReadDir
	// alone succeed on the plain directory. The device id only differs from
	// its parent's once a filesystem is actually mounted on it.
	parentDev, err := deviceID(filepath.Dir(f.mountPoint))
	if err != nil {
		return fmt.Errorf("stat mount parent: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dev, err := deviceID(f.mountPoint); err == nil && dev != parentDev {
			if _, err := os.ReadDir(f.mountPoint); err == nil {
				return nil
			}
		}
		if exitErr := proc.exited(); exitErr != nil {
			return fmt.Errorf("mount process exited (%v): %s", exitErr, logTail(proc.logPath))
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount point not ready within timeout: %s", logTail(proc.logPath))
}

// managedProcess tracks a started weed sub-command so a wait can bail out
// the moment it exits instead of polling a port it will never bind.
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
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

// startProcess is a helper that starts a weed sub-command with output
// captured to a log file under the framework's temp dir.
func (f *FuseTestFramework) startProcess(name string, args []string) (*managedProcess, error) {
	logPath := filepath.Join(f.tempDir, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create %s log file: %v", name, err)
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

// grpcPortOffset mirrors weed's HTTP->gRPC port convention (gRPC = HTTP +
// offset); a service port is only usable when both are free.
const grpcPortOffset = 10000

// The fuse_test package shares this `go test` invocation on its own port
// band; this band is picked to keep a service's gRPC sibling below the Linux
// ephemeral floor as well.
const (
	minServicePort = 22001
	maxServicePort = 22767
)

// portIsFree reports whether a TCP port can currently be bound on 127.0.0.1.
func portIsFree(port int) bool {
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	l.Close()
	return true
}

// freeServicePort returns a free service HTTP port whose gRPC sibling
// (port+grpcPortOffset) is free too. A probed port is released again before
// the service binds it, so ports already handed out are tracked in
// allocated and skipped — otherwise two sequential checks can return the
// same still-unbound port.
func freeServicePort(t *testing.T, allocated map[int]bool) int {
	t.Helper()
	portCount := maxServicePort - minServicePort + 1
	start := minServicePort + int(time.Now().UnixNano()%int64(portCount))
	for attempt := 0; attempt < portCount; attempt++ {
		port := minServicePort + (start-minServicePort+attempt)%portCount
		if allocated[port] {
			continue
		}
		if portIsFree(port) && portIsFree(port+grpcPortOffset) {
			allocated[port] = true
			return port
		}
	}
	t.Fatalf("failed to allocate a free HTTP/gRPC port pair in [%d,%d]", minServicePort, maxServicePort)
	return 0
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
