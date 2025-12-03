package sftp

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// SftpTestFramework provides utilities for SFTP integration testing
type SftpTestFramework struct {
	t             *testing.T
	tempDir       string
	dataDir       string
	masterProcess *os.Process
	volumeProcess *os.Process
	filerProcess  *os.Process
	sftpProcess   *os.Process
	masterAddr    string
	volumeAddr    string
	filerAddr     string
	sftpAddr      string
	weedBinary    string
	userStoreFile string
	hostKeyFile   string
	isSetup       bool
}

// TestConfig holds configuration for SFTP tests
type TestConfig struct {
	NumVolumes    int
	EnableDebug   bool
	SkipCleanup   bool // for debugging failed tests
	UserStoreFile string
}

// DefaultTestConfig returns a default configuration for SFTP tests
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		NumVolumes:    3,
		EnableDebug:   false,
		SkipCleanup:   false,
		UserStoreFile: "",
	}
}

// NewSftpTestFramework creates a new SFTP testing framework
func NewSftpTestFramework(t *testing.T, config *TestConfig) *SftpTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_sftp_test_")
	require.NoError(t, err)

	// Generate SSH host key for SFTP server
	hostKeyFile := filepath.Join(tempDir, "ssh_host_key")
	cmd := exec.Command("ssh-keygen", "-t", "ed25519", "-f", hostKeyFile, "-N", "")
	err = cmd.Run()
	require.NoError(t, err, "failed to generate SSH host key")

	// Use provided userstore or copy the test one
	userStoreFile := config.UserStoreFile
	if userStoreFile == "" {
		// Copy test userstore to temp dir
		userStoreFile = filepath.Join(tempDir, "userstore.json")
		testDataPath := findTestDataPath()
		input, err := os.ReadFile(filepath.Join(testDataPath, "userstore.json"))
		require.NoError(t, err, "failed to read test userstore.json")
		err = os.WriteFile(userStoreFile, input, 0644)
		require.NoError(t, err, "failed to write userstore.json")
	}

	return &SftpTestFramework{
		t:             t,
		tempDir:       tempDir,
		dataDir:       filepath.Join(tempDir, "data"),
		masterAddr:    "127.0.0.1:19333",
		volumeAddr:    "127.0.0.1:18080",
		filerAddr:     "127.0.0.1:18888",
		sftpAddr:      "127.0.0.1:12022",
		weedBinary:    findWeedBinary(),
		userStoreFile: userStoreFile,
		hostKeyFile:   hostKeyFile,
		isSetup:       false,
	}
}

// Setup starts SeaweedFS cluster with SFTP server
func (f *SftpTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}

	// Create all data directories
	dirs := []string{
		f.dataDir,
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
		return fmt.Errorf("master not ready: %v", err)
	}

	// Start volume server
	if err := f.startVolumeServer(config); err != nil {
		return fmt.Errorf("failed to start volume server: %v", err)
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

	// Start SFTP server
	if err := f.startSftpServer(config); err != nil {
		return fmt.Errorf("failed to start SFTP server: %v", err)
	}

	// Wait for SFTP server to be ready
	if err := f.waitForService(f.sftpAddr, 30*time.Second); err != nil {
		return fmt.Errorf("SFTP server not ready: %v", err)
	}

	// Additional wait for all services to stabilize (gRPC endpoints)
	time.Sleep(500 * time.Millisecond)

	f.isSetup = true
	return nil
}

// Cleanup stops all processes and removes temporary files
func (f *SftpTestFramework) Cleanup() {
	// Stop processes in reverse order
	processes := []*os.Process{f.sftpProcess, f.filerProcess, f.volumeProcess, f.masterProcess}
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

// GetSftpAddr returns the SFTP server address
func (f *SftpTestFramework) GetSftpAddr() string {
	return f.sftpAddr
}

// GetFilerAddr returns the filer address
func (f *SftpTestFramework) GetFilerAddr() string {
	return f.filerAddr
}

// ConnectSFTP creates an SFTP client connection with the given credentials
func (f *SftpTestFramework) ConnectSFTP(username, password string) (*sftp.Client, *ssh.Client, error) {
	// Load the known host public key for verification
	hostKeyCallback, err := f.getHostKeyCallback()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get host key callback: %v", err)
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: hostKeyCallback,
		Timeout:         5 * time.Second,
	}

	sshConn, err := ssh.Dial("tcp", f.sftpAddr, config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect SSH: %v", err)
	}

	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		sshConn.Close()
		return nil, nil, fmt.Errorf("failed to create SFTP client: %v", err)
	}

	return sftpClient, sshConn, nil
}

// getHostKeyCallback returns a callback that verifies the server's host key
// matches the known test server key we generated
func (f *SftpTestFramework) getHostKeyCallback() (ssh.HostKeyCallback, error) {
	// Read the public key file generated alongside the private key
	pubKeyFile := f.hostKeyFile + ".pub"
	pubKeyBytes, err := os.ReadFile(pubKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read host public key: %v", err)
	}

	// Parse the public key
	pubKey, _, _, _, err := ssh.ParseAuthorizedKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host public key: %v", err)
	}

	// Return a callback that verifies the server key matches our known key
	return ssh.FixedHostKey(pubKey), nil
}

// startMaster starts the SeaweedFS master server
func (f *SftpTestFramework) startMaster(config *TestConfig) error {
	args := []string{
		"master",
		"-ip=127.0.0.1",
		"-port=19333",
		"-mdir=" + filepath.Join(f.dataDir, "master"),
		"-raftBootstrap",
		"-peers=none",
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if config.EnableDebug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	f.masterProcess = cmd.Process
	return nil
}

// startVolumeServer starts SeaweedFS volume server
func (f *SftpTestFramework) startVolumeServer(config *TestConfig) error {
	args := []string{
		"volume",
		"-mserver=" + f.masterAddr,
		"-ip=127.0.0.1",
		"-port=18080",
		"-dir=" + filepath.Join(f.dataDir, "volume"),
		fmt.Sprintf("-max=%d", config.NumVolumes),
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if config.EnableDebug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	f.volumeProcess = cmd.Process
	return nil
}

// startFiler starts the SeaweedFS filer server
func (f *SftpTestFramework) startFiler(config *TestConfig) error {
	args := []string{
		"filer",
		"-master=" + f.masterAddr,
		"-ip=127.0.0.1",
		"-port=18888",
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if config.EnableDebug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	f.filerProcess = cmd.Process
	return nil
}

// startSftpServer starts the SeaweedFS SFTP server
func (f *SftpTestFramework) startSftpServer(config *TestConfig) error {
	args := []string{
		"sftp",
		"-filer=" + f.filerAddr,
		"-ip.bind=127.0.0.1",
		"-port=12022",
		"-sshPrivateKey=" + f.hostKeyFile,
		"-userStoreFile=" + f.userStoreFile,
	}

	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if config.EnableDebug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	f.sftpProcess = cmd.Process
	return nil
}

// waitForService waits for a service to be available
func (f *SftpTestFramework) waitForService(addr string, timeout time.Duration) error {
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

// findWeedBinary locates the weed binary
// Prefers local build over system-installed weed to ensure we test the latest code
func findWeedBinary() string {
	// Get the directory where this source file is located
	// This ensures we find the locally built weed binary first
	_, thisFile, _, ok := runtime.Caller(0)
	if ok {
		thisDir := filepath.Dir(thisFile)
		// From test/sftp/, the weed binary should be at ../../weed/weed
		candidates := []string{
			filepath.Join(thisDir, "../../weed/weed"),
			filepath.Join(thisDir, "../weed/weed"),
		}
		for _, candidate := range candidates {
			if _, err := os.Stat(candidate); err == nil {
				abs, _ := filepath.Abs(candidate)
				return abs
			}
		}
	}

	// Try relative paths from current working directory
	cwd, _ := os.Getwd()
	candidates := []string{
		filepath.Join(cwd, "../../weed/weed"),
		filepath.Join(cwd, "../weed/weed"),
		filepath.Join(cwd, "./weed"),
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			abs, _ := filepath.Abs(candidate)
			return abs
		}
	}

	// Fallback to PATH only if local build not found
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}

	// Default fallback
	return "weed"
}

// findTestDataPath locates the testdata directory
func findTestDataPath() string {
	// Get the directory where this source file is located
	_, thisFile, _, ok := runtime.Caller(0)
	if ok {
		thisDir := filepath.Dir(thisFile)
		testDataPath := filepath.Join(thisDir, "testdata")
		if _, err := os.Stat(testDataPath); err == nil {
			return testDataPath
		}
	}

	// Try relative paths from current working directory
	cwd, _ := os.Getwd()
	candidates := []string{
		filepath.Join(cwd, "testdata"),
		filepath.Join(cwd, "../sftp/testdata"),
		filepath.Join(cwd, "test/sftp/testdata"),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	return "./testdata"
}

