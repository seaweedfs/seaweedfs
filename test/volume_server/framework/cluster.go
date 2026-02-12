package framework

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

const (
	defaultWaitTimeout    = 30 * time.Second
	defaultWaitTick       = 200 * time.Millisecond
	testVolumeSizeLimitMB = 32
)

// Cluster is a lightweight SeaweedFS master + one volume server test harness.
type Cluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary string
	baseDir    string
	configDir  string
	logsDir    string
	keepLogs   bool

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	volumePubPort  int

	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd

	cleanupOnce sync.Once
}

// StartSingleVolumeCluster boots one master and one volume server.
func StartSingleVolumeCluster(t testing.TB, profile matrix.Profile) *Cluster {
	t.Helper()

	weedBinary, err := FindOrBuildWeedBinary()
	if err != nil {
		t.Fatalf("resolve weed binary: %v", err)
	}

	baseDir, keepLogs, err := newWorkDir()
	if err != nil {
		t.Fatalf("create temp test directory: %v", err)
	}

	configDir := filepath.Join(baseDir, "config")
	logsDir := filepath.Join(baseDir, "logs")
	masterDataDir := filepath.Join(baseDir, "master")
	volumeDataDir := filepath.Join(baseDir, "volume")
	for _, dir := range []string{configDir, logsDir, masterDataDir, volumeDataDir} {
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			t.Fatalf("create %s: %v", dir, mkErr)
		}
	}

	if err = writeSecurityConfig(configDir, profile); err != nil {
		t.Fatalf("write security config: %v", err)
	}

	masterPort, masterGrpcPort, err := allocateMasterPortPair()
	if err != nil {
		t.Fatalf("allocate master port pair: %v", err)
	}

	ports, err := allocatePorts(3)
	if err != nil {
		t.Fatalf("allocate ports: %v", err)
	}

	c := &Cluster{
		testingTB:      t,
		profile:        profile,
		weedBinary:     weedBinary,
		baseDir:        baseDir,
		configDir:      configDir,
		logsDir:        logsDir,
		keepLogs:       keepLogs,
		masterPort:     masterPort,
		masterGrpcPort: masterGrpcPort,
		volumePort:     ports[0],
		volumeGrpcPort: ports[1],
		volumePubPort:  ports[0],
	}
	if profile.SplitPublicPort {
		c.volumePubPort = ports[2]
	}

	if err = c.startMaster(masterDataDir); err != nil {
		c.Stop()
		t.Fatalf("start master: %v", err)
	}
	if err = c.waitForHTTP(c.MasterURL() + "/dir/status"); err != nil {
		masterLog := c.tailLog("master.log")
		c.Stop()
		t.Fatalf("wait for master readiness: %v\nmaster log tail:\n%s", err, masterLog)
	}

	if err = c.startVolume(volumeDataDir); err != nil {
		masterLog := c.tailLog("master.log")
		c.Stop()
		t.Fatalf("start volume: %v\nmaster log tail:\n%s", err, masterLog)
	}
	if err = c.waitForHTTP(c.VolumeAdminURL() + "/status"); err != nil {
		volumeLog := c.tailLog("volume.log")
		c.Stop()
		t.Fatalf("wait for volume readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForTCP(c.VolumeGRPCAddress()); err != nil {
		volumeLog := c.tailLog("volume.log")
		c.Stop()
		t.Fatalf("wait for volume grpc readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}

	t.Cleanup(func() {
		c.Stop()
	})

	return c
}

// Stop terminates all processes and cleans temporary files.
func (c *Cluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		stopProcess(c.volumeCmd)
		stopProcess(c.masterCmd)
		if !c.keepLogs && !c.testingTB.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.testingTB.Logf("volume server integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *Cluster) startMaster(dataDir string) error {
	logFile, err := os.Create(filepath.Join(c.logsDir, "master.log"))
	if err != nil {
		return err
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(c.masterPort),
		"-port.grpc=" + strconv.Itoa(c.masterGrpcPort),
		"-mdir=" + dataDir,
		"-peers=none",
		"-volumeSizeLimitMB=" + strconv.Itoa(testVolumeSizeLimitMB),
		"-defaultReplication=000",
	}

	c.masterCmd = exec.Command(c.weedBinary, args...)
	c.masterCmd.Dir = c.baseDir
	c.masterCmd.Stdout = logFile
	c.masterCmd.Stderr = logFile
	return c.masterCmd.Start()
}

func (c *Cluster) startVolume(dataDir string) error {
	logFile, err := os.Create(filepath.Join(c.logsDir, "volume.log"))
	if err != nil {
		return err
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"volume",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(c.volumePort),
		"-port.grpc=" + strconv.Itoa(c.volumeGrpcPort),
		"-port.public=" + strconv.Itoa(c.volumePubPort),
		"-dir=" + dataDir,
		"-max=16",
		"-master=127.0.0.1:" + strconv.Itoa(c.masterPort),
		"-readMode=" + c.profile.ReadMode,
		"-concurrentUploadLimitMB=" + strconv.Itoa(c.profile.ConcurrentUploadLimitMB),
		"-concurrentDownloadLimitMB=" + strconv.Itoa(c.profile.ConcurrentDownloadLimitMB),
	}

	c.volumeCmd = exec.Command(c.weedBinary, args...)
	c.volumeCmd.Dir = c.baseDir
	c.volumeCmd.Stdout = logFile
	c.volumeCmd.Stderr = logFile
	return c.volumeCmd.Start()
}

func (c *Cluster) waitForHTTP(url string) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(defaultWaitTimeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(defaultWaitTick)
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

func (c *Cluster) waitForTCP(addr string) error {
	deadline := time.Now().Add(defaultWaitTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(defaultWaitTick)
	}
	return fmt.Errorf("timed out waiting for tcp %s", addr)
}

func stopProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	case <-done:
	}
}

func allocatePorts(count int) ([]int, error) {
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, ll := range listeners {
				_ = ll.Close()
			}
			return nil, err
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports, nil
}

func allocateMasterPortPair() (int, int, error) {
	for masterPort := 10000; masterPort <= 55535; masterPort++ {
		masterGrpcPort := masterPort + 10000
		l1, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(masterPort)))
		if err != nil {
			continue
		}
		l2, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(masterGrpcPort)))
		if err != nil {
			_ = l1.Close()
			continue
		}
		_ = l2.Close()
		_ = l1.Close()
		return masterPort, masterGrpcPort, nil
	}
	return 0, 0, errors.New("unable to find available master port pair")
}

func newWorkDir() (dir string, keepLogs bool, err error) {
	keepLogs = os.Getenv("VOLUME_SERVER_IT_KEEP_LOGS") == "1"
	dir, err = os.MkdirTemp("", "seaweedfs_volume_server_it_")
	return dir, keepLogs, err
}

func writeSecurityConfig(configDir string, profile matrix.Profile) error {
	var b strings.Builder
	if profile.EnableJWT {
		if profile.JWTSigningKey == "" || profile.JWTReadKey == "" {
			return errors.New("jwt profile requires both write and read keys")
		}
		b.WriteString("[jwt.signing]\n")
		b.WriteString("key = \"")
		b.WriteString(profile.JWTSigningKey)
		b.WriteString("\"\n")
		b.WriteString("expires_after_seconds = 60\n\n")

		b.WriteString("[jwt.signing.read]\n")
		b.WriteString("key = \"")
		b.WriteString(profile.JWTReadKey)
		b.WriteString("\"\n")
		b.WriteString("expires_after_seconds = 60\n")
	}
	if b.Len() == 0 {
		b.WriteString("# optional security config generated for integration tests\n")
	}
	return os.WriteFile(filepath.Join(configDir, "security.toml"), []byte(b.String()), 0o644)
}

// FindOrBuildWeedBinary returns an executable weed binary, building one when needed.
func FindOrBuildWeedBinary() (string, error) {
	if fromEnv := os.Getenv("WEED_BINARY"); fromEnv != "" {
		if isExecutableFile(fromEnv) {
			return fromEnv, nil
		}
		return "", fmt.Errorf("WEED_BINARY is set but not executable: %s", fromEnv)
	}

	repoRoot := ""
	if _, file, _, ok := runtime.Caller(0); ok {
		repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
		candidate := filepath.Join(repoRoot, "weed", "weed")
		if isExecutableFile(candidate) {
			return candidate, nil
		}
	}

	if repoRoot == "" {
		return "", errors.New("unable to detect repository root")
	}

	binDir := filepath.Join(os.TempDir(), "seaweedfs_volume_server_it_bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", fmt.Errorf("create binary directory %s: %w", binDir, err)
	}
	binPath := filepath.Join(binDir, "weed")
	if isExecutableFile(binPath) {
		return binPath, nil
	}

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = filepath.Join(repoRoot, "weed")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build weed binary: %w\n%s", err, out.String())
	}
	if !isExecutableFile(binPath) {
		return "", fmt.Errorf("built weed binary is not executable: %s", binPath)
	}
	return binPath, nil
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	mode := info.Mode().Perm()
	return mode&0o111 != 0
}

func (c *Cluster) tailLog(logName string) string {
	f, err := os.Open(filepath.Join(c.logsDir, logName))
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lines := make([]string, 0, 40)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 40 {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}

func (c *Cluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *Cluster) VolumeAdminAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePort))
}

func (c *Cluster) VolumePublicAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPort))
}

func (c *Cluster) VolumeGRPCAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPort))
}

func (c *Cluster) MasterURL() string {
	return "http://" + c.MasterAddress()
}

func (c *Cluster) VolumeAdminURL() string {
	return "http://" + c.VolumeAdminAddress()
}

func (c *Cluster) VolumePublicURL() string {
	return "http://" + c.VolumePublicAddress()
}

func (c *Cluster) BaseDir() string {
	return c.baseDir
}
