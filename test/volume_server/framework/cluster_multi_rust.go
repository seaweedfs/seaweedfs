package framework

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// RustMultiVolumeCluster wraps a Go master + multiple Rust volume servers
// for integration testing. It mirrors MultiVolumeCluster but uses the Rust
// volume binary instead of the Go weed binary for volume servers.
type RustMultiVolumeCluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary       string // Go weed binary (for the master)
	rustVolumeBinary string // Rust volume binary

	baseDir           string
	configDir         string
	logsDir           string
	keepLogs          bool
	volumeServerCount int

	masterPort     int
	masterGrpcPort int

	volumePorts     []int
	volumeGrpcPorts []int
	volumePubPorts  []int

	masterCmd  *exec.Cmd
	volumeCmds []*exec.Cmd

	cleanupOnce sync.Once
}

// StartRustMultiVolumeCluster starts a cluster with a Go master and the
// specified number of Rust volume servers.
func StartRustMultiVolumeCluster(t testing.TB, profile matrix.Profile, serverCount int) *RustMultiVolumeCluster {
	t.Helper()

	if serverCount < 1 {
		t.Fatalf("serverCount must be at least 1, got %d", serverCount)
	}

	weedBinary, err := FindOrBuildWeedBinary()
	if err != nil {
		t.Fatalf("resolve weed binary: %v", err)
	}

	rustBinary, err := FindOrBuildRustBinary()
	if err != nil {
		t.Fatalf("resolve rust volume binary: %v", err)
	}

	baseDir, keepLogs, err := newWorkDir()
	if err != nil {
		t.Fatalf("create temp test directory: %v", err)
	}

	configDir := filepath.Join(baseDir, "config")
	logsDir := filepath.Join(baseDir, "logs")
	masterDataDir := filepath.Join(baseDir, "master")

	// Create directories for master and all volume servers
	dirs := []string{configDir, logsDir, masterDataDir}
	for i := 0; i < serverCount; i++ {
		dirs = append(dirs, filepath.Join(baseDir, fmt.Sprintf("volume%d", i)))
	}
	for _, dir := range dirs {
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

	// Allocate ports for all volume servers (3 ports per server: admin, grpc, public)
	// If SplitPublicPort is true, we need an additional port per server
	portsPerServer := 3
	if profile.SplitPublicPort {
		portsPerServer = 4
	}
	totalPorts := serverCount * portsPerServer
	ports, err := allocatePorts(totalPorts)
	if err != nil {
		t.Fatalf("allocate volume ports: %v", err)
	}

	c := &RustMultiVolumeCluster{
		testingTB:         t,
		profile:           profile,
		weedBinary:        weedBinary,
		rustVolumeBinary:  rustBinary,
		baseDir:           baseDir,
		configDir:         configDir,
		logsDir:           logsDir,
		keepLogs:          keepLogs,
		volumeServerCount: serverCount,
		masterPort:        masterPort,
		masterGrpcPort:    masterGrpcPort,
		volumePorts:       make([]int, serverCount),
		volumeGrpcPorts:   make([]int, serverCount),
		volumePubPorts:    make([]int, serverCount),
		volumeCmds:        make([]*exec.Cmd, serverCount),
	}

	// Assign ports to each volume server
	for i := 0; i < serverCount; i++ {
		baseIdx := i * portsPerServer
		c.volumePorts[i] = ports[baseIdx]
		c.volumeGrpcPorts[i] = ports[baseIdx+1]

		// Assign public port, using baseIdx+3 if SplitPublicPort, else baseIdx+2
		pubPortIdx := baseIdx + 2
		if profile.SplitPublicPort {
			pubPortIdx = baseIdx + 3
		}
		c.volumePubPorts[i] = ports[pubPortIdx]
	}

	// Start master (Go)
	if err = c.startMaster(masterDataDir); err != nil {
		c.Stop()
		t.Fatalf("start master: %v", err)
	}
	helper := &Cluster{logsDir: logsDir}
	if err = helper.waitForHTTP(c.MasterURL() + "/dir/status"); err != nil {
		masterLog := helper.tailLog("master.log")
		c.Stop()
		t.Fatalf("wait for master readiness: %v\nmaster log tail:\n%s", err, masterLog)
	}

	// Start all Rust volume servers
	for i := 0; i < serverCount; i++ {
		volumeDataDir := filepath.Join(baseDir, fmt.Sprintf("volume%d", i))
		if err = c.startRustVolume(i, volumeDataDir); err != nil {
			volumeLog := fmt.Sprintf("volume%d.log", i)
			c.Stop()
			t.Fatalf("start rust volume server %d: %v\nvolume log tail:\n%s", i, err, helper.tailLog(volumeLog))
		}
		if err = helper.waitForHTTP(c.VolumeAdminURL(i) + "/healthz"); err != nil {
			volumeLog := fmt.Sprintf("volume%d.log", i)
			c.Stop()
			t.Fatalf("wait for rust volume server %d readiness: %v\nvolume log tail:\n%s", i, err, helper.tailLog(volumeLog))
		}
		if err = helper.waitForTCP(c.VolumeGRPCAddress(i)); err != nil {
			volumeLog := fmt.Sprintf("volume%d.log", i)
			c.Stop()
			t.Fatalf("wait for rust volume server %d grpc readiness: %v\nvolume log tail:\n%s", i, err, helper.tailLog(volumeLog))
		}
	}

	t.Cleanup(func() {
		c.Stop()
	})

	return c
}

func (c *RustMultiVolumeCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		// Stop volume servers in reverse order
		for i := len(c.volumeCmds) - 1; i >= 0; i-- {
			stopProcess(c.volumeCmds[i])
		}
		stopProcess(c.masterCmd)
		if !c.keepLogs && !c.testingTB.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.testingTB.Logf("rust multi volume server integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *RustMultiVolumeCluster) startMaster(dataDir string) error {
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

func (c *RustMultiVolumeCluster) startRustVolume(index int, dataDir string) error {
	logName := fmt.Sprintf("volume%d.log", index)
	logFile, err := os.Create(filepath.Join(c.logsDir, logName))
	if err != nil {
		return err
	}

	args := []string{
		"--port", strconv.Itoa(c.volumePorts[index]),
		"--port.grpc", strconv.Itoa(c.volumeGrpcPorts[index]),
		"--port.public", strconv.Itoa(c.volumePubPorts[index]),
		"--ip", "127.0.0.1",
		"--ip.bind", "127.0.0.1",
		"--dir", dataDir,
		"--max", "16",
		"--master", "127.0.0.1:" + strconv.Itoa(c.masterPort),
		"--securityFile", filepath.Join(c.configDir, "security.toml"),
		"--concurrentUploadLimitMB", strconv.Itoa(c.profile.ConcurrentUploadLimitMB),
		"--concurrentDownloadLimitMB", strconv.Itoa(c.profile.ConcurrentDownloadLimitMB),
		"--preStopSeconds", "0",
	}
	if c.profile.InflightUploadTimeout > 0 {
		args = append(args, "--inflightUploadDataTimeout", c.profile.InflightUploadTimeout.String())
	}
	if c.profile.InflightDownloadTimeout > 0 {
		args = append(args, "--inflightDownloadDataTimeout", c.profile.InflightDownloadTimeout.String())
	}

	cmd := exec.Command(c.rustVolumeBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err = cmd.Start(); err != nil {
		return err
	}
	c.volumeCmds[index] = cmd
	return nil
}

// --- accessor methods (mirror MultiVolumeCluster) ---

func (c *RustMultiVolumeCluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *RustMultiVolumeCluster) MasterURL() string {
	return "http://" + c.MasterAddress()
}

func (c *RustMultiVolumeCluster) VolumeAdminAddress(index int) string {
	if index < 0 || index >= len(c.volumePorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePorts[index]))
}

func (c *RustMultiVolumeCluster) VolumePublicAddress(index int) string {
	if index < 0 || index >= len(c.volumePubPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPorts[index]))
}

func (c *RustMultiVolumeCluster) VolumeGRPCAddress(index int) string {
	if index < 0 || index >= len(c.volumeGrpcPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPorts[index]))
}

func (c *RustMultiVolumeCluster) VolumeAdminURL(index int) string {
	return "http://" + c.VolumeAdminAddress(index)
}

func (c *RustMultiVolumeCluster) VolumePublicURL(index int) string {
	return "http://" + c.VolumePublicAddress(index)
}

func (c *RustMultiVolumeCluster) BaseDir() string {
	return c.baseDir
}
