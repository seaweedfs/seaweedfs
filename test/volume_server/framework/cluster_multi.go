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

type MultiVolumeCluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary        string
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

// StartMultiVolumeCluster starts a cluster with a specified number of volume servers
func StartMultiVolumeCluster(t testing.TB, profile matrix.Profile, serverCount int) *MultiVolumeCluster {
	t.Helper()

	if serverCount < 1 {
		t.Fatalf("serverCount must be at least 1, got %d", serverCount)
	}

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

	c := &MultiVolumeCluster{
		testingTB:         t,
		profile:           profile,
		weedBinary:        weedBinary,
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
		c.volumePubPorts[i] = ports[baseIdx+2]

		if profile.SplitPublicPort {
			c.volumePubPorts[i] = ports[baseIdx+3]
		}
	}

	// Start master
	if err = c.startMaster(masterDataDir); err != nil {
		c.Stop()
		t.Fatalf("start master: %v", err)
	}
	if err = c.waitForHTTP(c.MasterURL() + "/dir/status"); err != nil {
		masterLog := c.tailLog("master.log")
		c.Stop()
		t.Fatalf("wait for master readiness: %v\nmaster log tail:\n%s", err, masterLog)
	}

	// Start all volume servers
	for i := 0; i < serverCount; i++ {
		volumeDataDir := filepath.Join(baseDir, fmt.Sprintf("volume%d", i))
		if err = c.startVolume(i, volumeDataDir); err != nil {
			prevIdx := i - 1
			prevLog := fmt.Sprintf("volume%d.log", prevIdx)
			if prevIdx < 0 {
				prevLog = "master.log"
			}
			c.Stop()
			t.Fatalf("start volume server %d: %v\nprevious log tail:\n%s", i, err, c.tailLog(prevLog))
		}
		if err = c.waitForHTTP(c.VolumeAdminURL(i) + "/status"); err != nil {
			volumeLog := fmt.Sprintf("volume%d.log", i)
			c.Stop()
			t.Fatalf("wait for volume server %d readiness: %v\nvolume log tail:\n%s", i, err, c.tailLog(volumeLog))
		}
		if err = c.waitForTCP(c.VolumeGRPCAddress(i)); err != nil {
			volumeLog := fmt.Sprintf("volume%d.log", i)
			c.Stop()
			t.Fatalf("wait for volume server %d grpc readiness: %v\nvolume log tail:\n%s", i, err, c.tailLog(volumeLog))
		}
	}

	t.Cleanup(func() {
		c.Stop()
	})

	return c
}

// StartTripleVolumeCluster is a convenience wrapper that starts a cluster with 3 volume servers
func StartTripleVolumeCluster(t testing.TB, profile matrix.Profile) *MultiVolumeCluster {
	return StartMultiVolumeCluster(t, profile, 3)
}

func (c *MultiVolumeCluster) Stop() {
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
			c.testingTB.Logf("volume server integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *MultiVolumeCluster) startMaster(dataDir string) error {
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

func (c *MultiVolumeCluster) startVolume(index int, dataDir string) error {
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
		return err
	}
	c.volumeCmds[index] = cmd
	return nil
}

func (c *MultiVolumeCluster) waitForHTTP(url string) error {
	return (&Cluster{}).waitForHTTP(url)
}

func (c *MultiVolumeCluster) waitForTCP(addr string) error {
	return (&Cluster{}).waitForTCP(addr)
}

func (c *MultiVolumeCluster) tailLog(logName string) string {
	return (&Cluster{logsDir: c.logsDir}).tailLog(logName)
}

func (c *MultiVolumeCluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *MultiVolumeCluster) MasterURL() string {
	return "http://" + c.MasterAddress()
}

func (c *MultiVolumeCluster) VolumeAdminAddress(index int) string {
	if index < 0 || index >= len(c.volumePorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePorts[index]))
}

func (c *MultiVolumeCluster) VolumePublicAddress(index int) string {
	if index < 0 || index >= len(c.volumePubPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPorts[index]))
}

func (c *MultiVolumeCluster) VolumeGRPCAddress(index int) string {
	if index < 0 || index >= len(c.volumeGrpcPorts) {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPorts[index]))
}

func (c *MultiVolumeCluster) VolumeAdminURL(index int) string {
	return "http://" + c.VolumeAdminAddress(index)
}

func (c *MultiVolumeCluster) VolumePublicURL(index int) string {
	return "http://" + c.VolumePublicAddress(index)
}

func (c *MultiVolumeCluster) BaseDir() string {
	return c.baseDir
}
