package nfs

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nfsclient "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

// NfsTestFramework boots a minimal SeaweedFS cluster (master + volume + filer)
// plus the experimental `weed nfs` frontend and hands out NFSv3 RPC clients
// that talk to it. Everything is driven via subprocesses so the tests exercise
// the same binary an operator would deploy, and no kernel mount is required.
type NfsTestFramework struct {
	t             *testing.T
	tempDir       string
	dataDir       string
	masterProcess *os.Process
	volumeProcess *os.Process
	filerProcess  *os.Process
	nfsProcess    *os.Process
	masterAddr    string
	masterGrpc    int
	volumeAddr    string
	volumeGrpc    int
	filerAddr     string
	filerGrpc     int
	nfsAddr       string
	exportRoot    string
	weedBinary    string
	isSetup       bool
	skipCleanup   bool
}

// TestConfig controls how the framework boots the cluster.
type TestConfig struct {
	NumVolumes  int
	EnableDebug bool
	SkipCleanup bool // keep temp dir on failure for inspection
	// ExportRoot is the filer path the NFS server exports. Defaults to "/"
	// so tests can use any path, with a single warning logged by the server.
	ExportRoot string
}

// DefaultTestConfig returns the defaults used by most tests. A dedicated
// /nfs_export subtree is used as the NFS export root because the NFS server
// requires the export directory to exist in the filer's namespace and carry
// a non-zero inode — passing "/" would succeed only for filer setups that
// have already backfilled the root inode.
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		NumVolumes:  3,
		EnableDebug: false,
		SkipCleanup: false,
		ExportRoot:  "/nfs_export",
	}
}

// NewNfsTestFramework allocates a framework bound to the current test. Call
// Setup next to actually start the cluster.
func NewNfsTestFramework(t *testing.T, config *TestConfig) *NfsTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_nfs_test_")
	require.NoError(t, err)

	// Allocate distinct ports per run. Every weed component needs both an
	// HTTP port and a gRPC port; we reserve them explicitly so we never hit
	// the "port + 10000 > 65535" trap that the implicit default has.
	masterPort := mustPickFreePort(t)
	masterGrpc := mustPickFreePort(t)
	volumePort := mustPickFreePort(t)
	volumeGrpc := mustPickFreePort(t)
	filerPort := mustPickFreePort(t)
	filerGrpc := mustPickFreePort(t)
	nfsPort := mustPickFreePort(t)

	exportRoot := config.ExportRoot
	if exportRoot == "" {
		exportRoot = "/"
	}

	return &NfsTestFramework{
		t:           t,
		tempDir:     tempDir,
		dataDir:     filepath.Join(tempDir, "data"),
		masterAddr:  fmt.Sprintf("127.0.0.1:%d", masterPort),
		masterGrpc:  masterGrpc,
		volumeAddr:  fmt.Sprintf("127.0.0.1:%d", volumePort),
		volumeGrpc:  volumeGrpc,
		filerAddr:   fmt.Sprintf("127.0.0.1:%d", filerPort),
		filerGrpc:   filerGrpc,
		nfsAddr:     fmt.Sprintf("127.0.0.1:%d", nfsPort),
		exportRoot:  exportRoot,
		weedBinary:  findWeedBinary(),
		isSetup:     false,
		skipCleanup: config.SkipCleanup,
	}
}

// Setup starts the SeaweedFS cluster and the NFS frontend, waiting for each
// component to accept connections before moving on.
func (f *NfsTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}

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

	if err := f.startMaster(config); err != nil {
		return fmt.Errorf("failed to start master: %v", err)
	}
	if err := f.waitForService(f.masterAddr, 30*time.Second); err != nil {
		return fmt.Errorf("master not ready: %v", err)
	}

	if err := f.startVolumeServer(config); err != nil {
		return fmt.Errorf("failed to start volume server: %v", err)
	}
	if err := f.waitForService(f.volumeAddr, 30*time.Second); err != nil {
		return fmt.Errorf("volume server not ready: %v", err)
	}

	if err := f.startFiler(config); err != nil {
		return fmt.Errorf("failed to start filer: %v", err)
	}
	if err := f.waitForService(f.filerAddr, 30*time.Second); err != nil {
		return fmt.Errorf("filer not ready: %v", err)
	}

	// Pre-create the export root in the filer's namespace. The NFS server
	// expects its export directory to exist with a real inode; uploading a
	// placeholder file creates the parent directory implicitly and then
	// removing the file leaves the empty directory in place.
	if f.exportRoot != "/" {
		if err := f.ensureExportRootExists(); err != nil {
			return fmt.Errorf("failed to pre-create export root %s: %v", f.exportRoot, err)
		}
	}

	if err := f.startNfsServer(config); err != nil {
		return fmt.Errorf("failed to start NFS server: %v", err)
	}
	if err := f.waitForService(f.nfsAddr, 30*time.Second); err != nil {
		return fmt.Errorf("NFS server not ready: %v", err)
	}

	// Let the NFS server finish wiring up its gRPC subscription to the filer
	// before the first client call hits MOUNT/LOOKUP.
	time.Sleep(500 * time.Millisecond)

	f.isSetup = true
	return nil
}

// Cleanup stops all processes. Temp state is preserved if SkipCleanup is set.
func (f *NfsTestFramework) Cleanup() {
	processes := []*os.Process{f.nfsProcess, f.filerProcess, f.volumeProcess, f.masterProcess}
	for _, proc := range processes {
		if proc != nil {
			_ = proc.Signal(syscall.SIGTERM)
			_, _ = proc.Wait()
		}
	}
	if !f.skipCleanup {
		_ = os.RemoveAll(f.tempDir)
	}
}

// NfsAddr returns the TCP address the NFS server is listening on.
func (f *NfsTestFramework) NfsAddr() string { return f.nfsAddr }

// FilerAddr returns the TCP address of the filer.
func (f *NfsTestFramework) FilerAddr() string { return f.filerAddr }

// ExportRoot returns the path the NFS server exports.
func (f *NfsTestFramework) ExportRoot() string { return f.exportRoot }

// Mount opens an NFSv3 MOUNT+NFS connection against the running NFS server
// and returns a Target that tests can drive like a mini-VFS. Caller is
// responsible for calling the returned cleanup func to Unmount and close the
// TCP connection.
func (f *NfsTestFramework) Mount() (*nfsclient.Target, func(), error) {
	var (
		client *rpc.Client
		err    error
	)
	// The NFS server's TCP listener may already be accepting connections when
	// waitForService returns, but the RPC program registration can trail it
	// by a few milliseconds. Retry the dial to absorb that small window.
	for attempt := 0; attempt < 20; attempt++ {
		client, err = rpc.DialTCP("tcp", f.nfsAddr, false)
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("dial NFS: %w", err)
	}

	// Note: do not set Mount.Addr here. When Addr is non-empty, the go-nfs
	// client re-dials via portmapper and concatenates `:111` onto the
	// address, which produces "too many colons" for a raw `host:port`
	// string. Reusing the existing RPC client avoids that path entirely.
	mounter := &nfsclient.Mount{Client: client}
	target, err := mounter.Mount(f.exportRoot, rpc.AuthNull)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("mount %s: %w", f.exportRoot, err)
	}

	cleanup := func() {
		_ = mounter.Unmount()
		client.Close()
	}
	return target, cleanup, nil
}

func (f *NfsTestFramework) startMaster(config *TestConfig) error {
	_, masterPort := splitHostPort(f.masterAddr)
	args := []string{
		"master",
		"-ip=127.0.0.1",
		fmt.Sprintf("-port=%d", masterPort),
		fmt.Sprintf("-port.grpc=%d", f.masterGrpc),
		"-mdir=" + filepath.Join(f.dataDir, "master"),
		"-raftBootstrap",
		"-peers=none",
	}
	return f.startProcess(&f.masterProcess, config, args)
}

func (f *NfsTestFramework) startVolumeServer(config *TestConfig) error {
	_, volumePort := splitHostPort(f.volumeAddr)
	// pb.ServerAddress encodes a non-default gRPC port as `host:port.grpc`.
	// See weed/pb/server_address.go — the dot, not a colon, is the separator
	// between the HTTP port and the gRPC port.
	masterWithGrpc := fmt.Sprintf("%s.%d", f.masterAddr, f.masterGrpc)
	args := []string{
		"volume",
		"-master=" + masterWithGrpc,
		"-ip=127.0.0.1",
		fmt.Sprintf("-port=%d", volumePort),
		fmt.Sprintf("-port.grpc=%d", f.volumeGrpc),
		"-dir=" + filepath.Join(f.dataDir, "volume"),
		fmt.Sprintf("-max=%d", config.NumVolumes),
	}
	return f.startProcess(&f.volumeProcess, config, args)
}

func (f *NfsTestFramework) startFiler(config *TestConfig) error {
	_, filerPort := splitHostPort(f.filerAddr)
	masterWithGrpc := fmt.Sprintf("%s.%d", f.masterAddr, f.masterGrpc)
	args := []string{
		"filer",
		"-master=" + masterWithGrpc,
		"-ip=127.0.0.1",
		fmt.Sprintf("-port=%d", filerPort),
		fmt.Sprintf("-port.grpc=%d", f.filerGrpc),
	}
	return f.startProcess(&f.filerProcess, config, args)
}

func (f *NfsTestFramework) startNfsServer(config *TestConfig) error {
	_, nfsPort := splitHostPort(f.nfsAddr)
	// `host:port.grpc` encoding — see pb/server_address.go.
	filerWithGrpc := fmt.Sprintf("%s.%d", f.filerAddr, f.filerGrpc)
	args := []string{
		"nfs",
		"-filer=" + filerWithGrpc,
		"-ip.bind=127.0.0.1",
		fmt.Sprintf("-port=%d", nfsPort),
		"-filer.path=" + f.exportRoot,
	}
	return f.startProcess(&f.nfsProcess, config, args)
}

func (f *NfsTestFramework) startProcess(target **os.Process, config *TestConfig, args []string) error {
	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	if config.EnableDebug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	*target = cmd.Process
	return nil
}

func (f *NfsTestFramework) waitForService(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout", addr)
}

// ensureExportRootExists posts a placeholder file to f.exportRoot via the
// filer's HTTP API, then deletes it. That roundtrip implicitly creates the
// target directory so the NFS server has something to mount. We bypass
// weed/pb here because the HTTP client is simpler and needs no gRPC stubs.
func (f *NfsTestFramework) ensureExportRootExists() error {
	exportRoot := strings.TrimRight(f.exportRoot, "/")
	if exportRoot == "" {
		return nil
	}
	placeholder := exportRoot + "/.nfs_test_init"
	filerURL := "http://" + f.filerAddr + placeholder

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", ".nfs_test_init")
	if err != nil {
		return err
	}
	if _, err := io.WriteString(part, ""); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodPost, filerURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("filer POST %s returned status %d", filerURL, resp.StatusCode)
	}

	// Delete the placeholder; the directory stays behind.
	deleteReq, err := http.NewRequest(http.MethodDelete, filerURL, nil)
	if err != nil {
		return err
	}
	deleteResp, err := httpClient.Do(deleteReq)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, deleteResp.Body)
	deleteResp.Body.Close()
	if deleteResp.StatusCode/100 != 2 && deleteResp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("filer DELETE %s returned status %d", filerURL, deleteResp.StatusCode)
	}
	return nil
}

// mustPickFreePort asks the kernel for an ephemeral port, closes the listener,
// and returns the port number. There is a race between closing and the
// subprocess reopening the port, but for local test runs it is effectively
// collision-free and avoids the hardcoded-port collisions that bite
// parallel test runs.
func mustPickFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func splitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0
	}
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

// findWeedBinary locates the weed binary, preferring the local build in the
// checkout so tests run against the code under review rather than whatever is
// on $PATH.
func findWeedBinary() string {
	if _, thisFile, _, ok := runtime.Caller(0); ok {
		thisDir := filepath.Dir(thisFile)
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
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}
	return "weed"
}
