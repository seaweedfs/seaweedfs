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

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// MixedVolumeCluster wraps a Go master + a mix of Go and Rust volume servers
// for integration testing. Go servers occupy indices [0, goCount) and Rust
// servers occupy indices [goCount, goCount+rustCount).
type MixedVolumeCluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary       string // Go weed binary (master + Go volume servers)
	rustVolumeBinary string // Rust volume binary

	baseDir   string
	configDir string
	logsDir   string
	keepLogs  bool

	masterPort     int
	masterGrpcPort int

	volumePorts     []int
	volumeGrpcPorts []int
	volumePubPorts  []int
	isRust          []bool // which servers are Rust

	masterCmd  *exec.Cmd
	volumeCmds []*exec.Cmd

	cleanupOnce sync.Once
}

// StartMixedVolumeCluster starts a cluster with 1 Go master, goCount Go volume
// servers, and rustCount Rust volume servers. Go servers come first in the index.
func StartMixedVolumeCluster(t testing.TB, profile matrix.Profile, goCount, rustCount int) *MixedVolumeCluster {
	t.Helper()

	if goCount < 0 || rustCount < 0 {
		t.Fatalf("goCount and rustCount must be non-negative, got go=%d rust=%d", goCount, rustCount)
	}
	total := goCount + rustCount
	if total < 2 {
		t.Fatalf("need at least 2 volume servers, got %d", total)
	}

	weedBinary, err := FindOrBuildWeedBinary()
	if err != nil {
		t.Fatalf("resolve weed binary: %v", err)
	}

	// Only build the Rust binary when Rust servers are requested.
	var rustBinary string
	if rustCount > 0 {
		rustBinary, err = FindOrBuildRustBinary()
		if err != nil {
			t.Skipf("skipping mixed cluster test: rust binary unavailable: %v", err)
		}
	}

	baseDir, keepLogs, err := newWorkDir()
	if err != nil {
		t.Fatalf("create temp test directory: %v", err)
	}

	configDir := filepath.Join(baseDir, "config")
	logsDir := filepath.Join(baseDir, "logs")
	masterDataDir := filepath.Join(baseDir, "master")

	dirs := []string{configDir, logsDir, masterDataDir}
	for i := 0; i < total; i++ {
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

	// 2 ports per server (admin, grpc); add 1 more when public port is split out.
	portsPerServer := 2
	if profile.SplitPublicPort {
		portsPerServer = 3
	}
	miniPorts, ports, err := testutil.AllocatePortSet(1, total*portsPerServer)
	if err != nil {
		t.Fatalf("allocate ports: %v", err)
	}
	masterPort := miniPorts[0]
	masterGrpcPort := masterPort + testutil.GrpcPortOffset

	isRust := make([]bool, total)
	for i := goCount; i < total; i++ {
		isRust[i] = true
	}

	c := &MixedVolumeCluster{
		testingTB:        t,
		profile:          profile,
		weedBinary:       weedBinary,
		rustVolumeBinary: rustBinary,
		baseDir:          baseDir,
		configDir:        configDir,
		logsDir:          logsDir,
		keepLogs:         keepLogs,
		masterPort:       masterPort,
		masterGrpcPort:   masterGrpcPort,
		volumePorts:      make([]int, total),
		volumeGrpcPorts:  make([]int, total),
		volumePubPorts:   make([]int, total),
		isRust:           isRust,
		volumeCmds:       make([]*exec.Cmd, total),
	}

	for i := 0; i < total; i++ {
		baseIdx := i * portsPerServer
		c.volumePorts[i] = ports[baseIdx]
		c.volumeGrpcPorts[i] = ports[baseIdx+1]
		if profile.SplitPublicPort {
			c.volumePubPorts[i] = ports[baseIdx+2]
		} else {
			c.volumePubPorts[i] = c.volumePorts[i] // reuse admin port
		}
	}

	// Start master
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

	// Start volume servers
	for i := 0; i < total; i++ {
		volumeDataDir := filepath.Join(baseDir, fmt.Sprintf("volume%d", i))
		if isRust[i] {
			err = c.startRustVolume(i, volumeDataDir)
		} else {
			err = c.startGoVolume(i, volumeDataDir)
		}
		if err != nil {
			logTail := helper.tailLog(fmt.Sprintf("volume%d.log", i))
			c.Stop()
			t.Fatalf("start volume server %d (rust=%v): %v\nlog tail:\n%s", i, isRust[i], err, logTail)
		}

		// Rust uses /healthz, Go uses /status
		healthURL := c.VolumeAdminURL(i) + "/status"
		if isRust[i] {
			healthURL = c.VolumeAdminURL(i) + "/healthz"
		}
		if err = helper.waitForHTTP(healthURL); err != nil {
			logTail := helper.tailLog(fmt.Sprintf("volume%d.log", i))
			c.Stop()
			t.Fatalf("wait for volume server %d readiness: %v\nlog tail:\n%s", i, err, logTail)
		}
		if err = helper.waitForTCP(c.VolumeGRPCAddress(i)); err != nil {
			logTail := helper.tailLog(fmt.Sprintf("volume%d.log", i))
			c.Stop()
			t.Fatalf("wait for volume server %d grpc readiness: %v\nlog tail:\n%s", i, err, logTail)
		}
	}

	t.Cleanup(func() {
		c.Stop()
	})

	return c
}

func (c *MixedVolumeCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		for i := len(c.volumeCmds) - 1; i >= 0; i-- {
			stopProcess(c.volumeCmds[i])
		}
		stopProcess(c.masterCmd)
		if !c.keepLogs && !c.testingTB.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.testingTB.Logf("mixed volume server integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *MixedVolumeCluster) startMaster(dataDir string) error {
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
	if err = c.masterCmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	logFile.Close() // child inherited the fd
	return nil
}

func (c *MixedVolumeCluster) startGoVolume(index int, dataDir string) error {
	logName := fmt.Sprintf("volume%d.log", index)
	logFile, err := os.Create(filepath.Join(c.logsDir, logName))
	if err != nil {
		return err
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"volume",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(c.volumePorts[index]),
		"-port.grpc=" + strconv.Itoa(c.volumeGrpcPorts[index]),
		"-port.public=" + strconv.Itoa(c.volumePubPorts[index]),
		"-dir=" + dataDir,
		"-max=16",
		"-master=127.0.0.1:" + strconv.Itoa(c.masterPort),
		"-readMode=" + c.profile.ReadMode,
		"-concurrentUploadLimitMB=" + strconv.Itoa(c.profile.ConcurrentUploadLimitMB),
		"-concurrentDownloadLimitMB=" + strconv.Itoa(c.profile.ConcurrentDownloadLimitMB),
	}
	if c.profile.InflightUploadTimeout > 0 {
		args = append(args, "-inflightUploadDataTimeout="+c.profile.InflightUploadTimeout.String())
	}
	if c.profile.InflightDownloadTimeout > 0 {
		args = append(args, "-inflightDownloadDataTimeout="+c.profile.InflightDownloadTimeout.String())
	}

	cmd := exec.Command(c.weedBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	logFile.Close() // child inherited the fd
	c.volumeCmds[index] = cmd
	return nil
}

func (c *MixedVolumeCluster) startRustVolume(index int, dataDir string) error {
	logName := fmt.Sprintf("volume%d.log", index)
	logFile, err := os.Create(filepath.Join(c.logsDir, logName))
	if err != nil {
		return err
	}

	args := rustVolumeArgs(
		c.profile,
		c.configDir,
		c.masterPort,
		c.volumePorts[index],
		c.volumeGrpcPorts[index],
		c.volumePubPorts[index],
		dataDir,
	)

	cmd := exec.Command(c.rustVolumeBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	logFile.Close() // child inherited the fd
	c.volumeCmds[index] = cmd
	return nil
}

// --- accessor methods (mirror MultiVolumeCluster) ---

func (c *MixedVolumeCluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *MixedVolumeCluster) MasterURL() string {
	return "http://" + c.MasterAddress()
}

func (c *MixedVolumeCluster) VolumeAdminAddress(index int) string {
	if index < 0 || index >= len(c.volumePorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePorts[index]))
}

func (c *MixedVolumeCluster) VolumePublicAddress(index int) string {
	if index < 0 || index >= len(c.volumePubPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPorts[index]))
}

func (c *MixedVolumeCluster) VolumeGRPCAddress(index int) string {
	if index < 0 || index >= len(c.volumeGrpcPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPorts[index]))
}

func (c *MixedVolumeCluster) VolumeAdminURL(index int) string {
	return "http://" + c.VolumeAdminAddress(index)
}

func (c *MixedVolumeCluster) VolumePublicURL(index int) string {
	return "http://" + c.VolumePublicAddress(index)
}

func (c *MixedVolumeCluster) BaseDir() string {
	return c.baseDir
}

// VolumeServerAddress returns SeaweedFS server address format: ip:httpPort.grpcPort
func (c *MixedVolumeCluster) VolumeServerAddress(index int) string {
	if index < 0 || index >= len(c.volumePorts) {
		return ""
	}
	return fmt.Sprintf("%s.%d", c.VolumeAdminAddress(index), c.volumeGrpcPorts[index])
}

func (c *MixedVolumeCluster) IsRust(index int) bool {
	if index < 0 || index >= len(c.isRust) {
		return false
	}
	return c.isRust[index]
}
