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

// FuseTestFramework provides utilities for FUSE integration testing
type FuseTestFramework struct {
	t             *testing.T
	tempDir       string
	mountPoint    string
	dataDir       string
	logDir        string
	masterProcess *os.Process
	volumeProcess *os.Process
	filerProcess  *os.Process
	mountProcess  *os.Process
	masterAddr    string
	volumeAddr    string
	filerAddr     string
	masterPort    int
	volumePort    int
	filerPort     int
	weedBinary    string
	isSetup       bool
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
// Each instance allocates its own free ports so multiple tests can run
// sequentially without port conflicts from slow cleanup.
func NewFuseTestFramework(t *testing.T, config *TestConfig) *FuseTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_fuse_test_")
	require.NoError(t, err)

	masterPort := freePort(t)
	volumePort := freePort(t)
	filerPort := freePort(t)

	return &FuseTestFramework{
		t:          t,
		tempDir:    tempDir,
		mountPoint: filepath.Join(tempDir, "mount"),
		dataDir:    filepath.Join(tempDir, "data"),
		logDir:     filepath.Join(tempDir, "logs"),
		masterPort: masterPort,
		volumePort: volumePort,
		filerPort:  filerPort,
		masterAddr: fmt.Sprintf("127.0.0.1:%d", masterPort),
		volumeAddr: fmt.Sprintf("127.0.0.1:%d", volumePort),
		filerAddr:  fmt.Sprintf("127.0.0.1:%d", filerPort),
		weedBinary: findWeedBinary(),
		isSetup:    false,
	}
}

// freePort asks the OS for a free TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// Setup starts SeaweedFS cluster and mounts FUSE filesystem
func (f *FuseTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}

	// Create all required directories upfront
	dirs := []string{
		f.mountPoint,
		f.logDir,
		filepath.Join(f.dataDir, "master"),
		filepath.Join(f.dataDir, "volume"),
	}
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
	if err := f.waitForService(f.masterAddr, 30*time.Second); err != nil {
		f.dumpLog("master")
		return fmt.Errorf("master not ready: %v", err)
	}

	// Start volume servers
	if err := f.startVolumeServers(config); err != nil {
		return fmt.Errorf("failed to start volume servers: %v", err)
	}

	// Wait for volume server to be ready
	if err := f.waitForService(f.volumeAddr, 30*time.Second); err != nil {
		f.dumpLog("volume")
		return fmt.Errorf("volume server not ready: %v", err)
	}

	// Start filer
	if err := f.startFiler(config); err != nil {
		return fmt.Errorf("failed to start filer: %v", err)
	}

	// Wait for filer to be ready
	if err := f.waitForService(f.filerAddr, 30*time.Second); err != nil {
		f.dumpLog("filer")
		return fmt.Errorf("filer not ready: %v", err)
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

// Cleanup stops all processes and removes temporary files
func (f *FuseTestFramework) Cleanup() {
	if f.mountProcess != nil {
		f.unmountFuse()
	}

	// Stop processes in reverse order
	processes := []*os.Process{f.mountProcess, f.filerProcess, f.volumeProcess, f.masterProcess}
	for _, proc := range processes {
		if proc != nil {
			proc.Signal(syscall.SIGTERM)
			proc.Wait()
		}
	}

	// Remove temp directory
	if !DefaultTestConfig().SkipCleanup {
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
// for debugging when a service fails to start.
func (f *FuseTestFramework) dumpLog(name string) {
	data, err := os.ReadFile(filepath.Join(f.logDir, name+".log"))
	if err != nil {
		f.t.Logf("[%s log] (not available: %v)", name, err)
		return
	}
	// Truncate to last 2KB to keep output manageable
	if len(data) > 2048 {
		data = data[len(data)-2048:]
	}
	f.t.Logf("[%s log tail]\n%s", name, string(data))
}

// startMaster starts the SeaweedFS master server
func (f *FuseTestFramework) startMaster(config *TestConfig) error {
	masterGrpcPort := freePort(f.t)
	args := []string{
		"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(f.masterPort),
		"-port.grpc=" + strconv.Itoa(masterGrpcPort),
		"-mdir=" + filepath.Join(f.dataDir, "master"),
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	proc, err := f.startProcess("master", args)
	if err != nil {
		return err
	}
	f.masterProcess = proc
	return nil
}

// startVolumeServers starts SeaweedFS volume servers
func (f *FuseTestFramework) startVolumeServers(config *TestConfig) error {
	volumeGrpcPort := freePort(f.t)
	args := []string{
		"volume",
		"-master=127.0.0.1:" + strconv.Itoa(f.masterPort),
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(f.volumePort),
		"-port.grpc=" + strconv.Itoa(volumeGrpcPort),
		"-dir=" + filepath.Join(f.dataDir, "volume"),
		fmt.Sprintf("-max=%d", config.NumVolumes),
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	proc, err := f.startProcess("volume", args)
	if err != nil {
		return err
	}
	f.volumeProcess = proc
	return nil
}

// startFiler starts the SeaweedFS filer server
func (f *FuseTestFramework) startFiler(config *TestConfig) error {
	filerGrpcPort := freePort(f.t)
	args := []string{
		"filer",
		"-master=127.0.0.1:" + strconv.Itoa(f.masterPort),
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(f.filerPort),
		"-port.grpc=" + strconv.Itoa(filerGrpcPort),
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	proc, err := f.startProcess("filer", args)
	if err != nil {
		return err
	}
	f.filerProcess = proc
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
		// Check if mount point is accessible
		if _, err := os.Stat(f.mountPoint); err == nil {
			// Try to list directory
			if _, err := os.ReadDir(f.mountPoint); err == nil {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount point not ready within timeout")
}

// findWeedBinary locates the weed binary.
// Checks PATH first (most reliable in CI where the binary is installed to
// /usr/local/bin), then falls back to relative paths.  Each candidate is
// verified to be a regular file so that a source directory named "weed"
// is never mistaken for the binary.
func findWeedBinary() string {
	// PATH lookup first — works in CI and when weed is installed globally.
	if p, err := exec.LookPath("weed"); err == nil {
		return p
	}

	// Relative paths for local development (run from test/fuse_integration/).
	candidates := []string{
		"../../weed/weed", // built in-tree: weed/weed
		"./weed",
		"../weed",
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			abs, _ := filepath.Abs(candidate)
			return abs
		}
	}

	// Default fallback — will fail with a clear "not found" at exec time.
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
