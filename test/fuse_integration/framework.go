package fuse_test

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
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
	masterProcess *os.Process
	volumeProcess *os.Process
	filerProcess  *os.Process
	mountProcess  *os.Process
	masterAddr    string
	volumeAddr    string
	filerAddr     string
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
		masterAddr: "127.0.0.1:19333",
		volumeAddr: "127.0.0.1:18080",
		filerAddr:  "127.0.0.1:18888",
		weedBinary: findWeedBinary(),
		isSetup:    false,
	}
}

// Setup starts SeaweedFS cluster and mounts FUSE filesystem
func (f *FuseTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}

	// Create directories
	dirs := []string{f.mountPoint, f.dataDir}
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
		return fmt.Errorf("master not ready: %v", err)
	}

	// Start volume servers
	if err := f.startVolumeServers(config); err != nil {
		return fmt.Errorf("failed to start volume servers: %v", err)
	}

	// Wait for volume server to be ready
	if err := f.waitForService(f.volumeAddr, 30*time.Second); err != nil {
		return fmt.Errorf("volume server not ready: %v", err)
	}

	// Start filer
	if err := f.startFiler(config); err != nil {
		return fmt.Errorf("failed to start filer: %v", err)
	}

	// Wait for filer to be ready
	if err := f.waitForService(f.filerAddr, 30*time.Second); err != nil {
		return fmt.Errorf("filer not ready: %v", err)
	}

	// Mount FUSE filesystem
	if err := f.mountFuse(config); err != nil {
		return fmt.Errorf("failed to mount FUSE: %v", err)
	}

	// Wait for mount to be ready
	if err := f.waitForMount(30 * time.Second); err != nil {
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

// startMaster starts the SeaweedFS master server
func (f *FuseTestFramework) startMaster(config *TestConfig) error {
	args := []string{
		"master",
		"-ip=127.0.0.1",
		"-port=19333",
		"-mdir=" + filepath.Join(f.dataDir, "master"),
		"-raftBootstrap",
		"-peers=none", // Faster startup when no multiple masters needed
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if err := cmd.Start(); err != nil {
		return err
	}
	f.masterProcess = cmd.Process
	return nil
}

// startVolumeServers starts SeaweedFS volume servers
func (f *FuseTestFramework) startVolumeServers(config *TestConfig) error {
	args := []string{
		"volume",
		"-mserver=" + f.masterAddr,
		"-ip=127.0.0.1",
		"-port=18080",
		"-dir=" + filepath.Join(f.dataDir, "volume"),
		fmt.Sprintf("-max=%d", config.NumVolumes),
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if err := cmd.Start(); err != nil {
		return err
	}
	f.volumeProcess = cmd.Process
	return nil
}

// startFiler starts the SeaweedFS filer server
func (f *FuseTestFramework) startFiler(config *TestConfig) error {
	args := []string{
		"filer",
		"-master=" + f.masterAddr,
		"-ip=127.0.0.1",
		"-port=18888",
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if err := cmd.Start(); err != nil {
		return err
	}
	f.filerProcess = cmd.Process
	return nil
}

// mountFuse mounts the SeaweedFS FUSE filesystem
func (f *FuseTestFramework) mountFuse(config *TestConfig) error {
	args := []string{
		"mount",
		"-filer=" + f.filerAddr,
		"-dir=" + f.mountPoint,
		"-filer.path=/",
		"-dirAutoCreate",
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
		args = append(args, fmt.Sprintf("-cacheSizeMB=%d", config.CacheSizeMB))
	}
	if config.EnableDebug {
		args = append(args, "-v=4")
	}

	args = append(args, config.MountOptions...)

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if err := cmd.Start(); err != nil {
		return err
	}
	f.mountProcess = cmd.Process
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
	exec.Command("umount", f.mountPoint).Run()
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

// findWeedBinary locates the weed binary
func findWeedBinary() string {
	// Try different possible locations
	candidates := []string{
		"./weed",
		"../weed",
		"../../weed",
		"weed", // in PATH
	}

	for _, candidate := range candidates {
		if _, err := exec.LookPath(candidate); err == nil {
			return candidate
		}
		if _, err := os.Stat(candidate); err == nil {
			abs, _ := filepath.Abs(candidate)
			return abs
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
