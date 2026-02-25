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

type TripleVolumeCluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary string
	baseDir    string
	configDir  string
	logsDir    string
	keepLogs   bool

	masterPort     int
	masterGrpcPort int

	volumePort0     int
	volumeGrpcPort0 int
	volumePubPort0  int
	volumePort1     int
	volumeGrpcPort1 int
	volumePubPort1  int
	volumePort2     int
	volumeGrpcPort2 int
	volumePubPort2  int

	masterCmd  *exec.Cmd
	volumeCmd0 *exec.Cmd
	volumeCmd1 *exec.Cmd
	volumeCmd2 *exec.Cmd

	cleanupOnce sync.Once
}

func StartTripleVolumeCluster(t testing.TB, profile matrix.Profile) *TripleVolumeCluster {
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
	volumeDataDir0 := filepath.Join(baseDir, "volume0")
	volumeDataDir1 := filepath.Join(baseDir, "volume1")
	volumeDataDir2 := filepath.Join(baseDir, "volume2")
	for _, dir := range []string{configDir, logsDir, masterDataDir, volumeDataDir0, volumeDataDir1, volumeDataDir2} {
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

	ports, err := allocatePorts(9)
	if err != nil {
		t.Fatalf("allocate volume ports: %v", err)
	}

	c := &TripleVolumeCluster{
		testingTB:       t,
		profile:         profile,
		weedBinary:      weedBinary,
		baseDir:         baseDir,
		configDir:       configDir,
		logsDir:         logsDir,
		keepLogs:        keepLogs,
		masterPort:      masterPort,
		masterGrpcPort:  masterGrpcPort,
		volumePort0:     ports[0],
		volumeGrpcPort0: ports[1],
		volumePubPort0:  ports[0],
		volumePort1:     ports[2],
		volumeGrpcPort1: ports[3],
		volumePubPort1:  ports[2],
		volumePort2:     ports[4],
		volumeGrpcPort2: ports[5],
		volumePubPort2:  ports[4],
	}
	if profile.SplitPublicPort {
		c.volumePubPort0 = ports[6]
		c.volumePubPort1 = ports[7]
		c.volumePubPort2 = ports[8]
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

	if err = c.startVolume(0, volumeDataDir0); err != nil {
		masterLog := c.tailLog("master.log")
		c.Stop()
		t.Fatalf("start first volume server: %v\nmaster log tail:\n%s", err, masterLog)
	}
	if err = c.waitForHTTP(c.VolumeAdminURL(0) + "/status"); err != nil {
		volumeLog := c.tailLog("volume0.log")
		c.Stop()
		t.Fatalf("wait for first volume readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForTCP(c.VolumeGRPCAddress(0)); err != nil {
		volumeLog := c.tailLog("volume0.log")
		c.Stop()
		t.Fatalf("wait for first volume grpc readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}

	if err = c.startVolume(1, volumeDataDir1); err != nil {
		volumeLog := c.tailLog("volume0.log")
		c.Stop()
		t.Fatalf("start second volume server: %v\nfirst volume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForHTTP(c.VolumeAdminURL(1) + "/status"); err != nil {
		volumeLog := c.tailLog("volume1.log")
		c.Stop()
		t.Fatalf("wait for second volume readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForTCP(c.VolumeGRPCAddress(1)); err != nil {
		volumeLog := c.tailLog("volume1.log")
		c.Stop()
		t.Fatalf("wait for second volume grpc readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}

	if err = c.startVolume(2, volumeDataDir2); err != nil {
		volumeLog := c.tailLog("volume1.log")
		c.Stop()
		t.Fatalf("start third volume server: %v\nsecond volume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForHTTP(c.VolumeAdminURL(2) + "/status"); err != nil {
		volumeLog := c.tailLog("volume2.log")
		c.Stop()
		t.Fatalf("wait for third volume readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}
	if err = c.waitForTCP(c.VolumeGRPCAddress(2)); err != nil {
		volumeLog := c.tailLog("volume2.log")
		c.Stop()
		t.Fatalf("wait for third volume grpc readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}

	t.Cleanup(func() {
		c.Stop()
	})

	return c
}

func (c *TripleVolumeCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		stopProcess(c.volumeCmd2)
		stopProcess(c.volumeCmd1)
		stopProcess(c.volumeCmd0)
		stopProcess(c.masterCmd)
		if !c.keepLogs && !c.testingTB.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.testingTB.Logf("volume server integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *TripleVolumeCluster) startMaster(dataDir string) error {
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

func (c *TripleVolumeCluster) startVolume(index int, dataDir string) error {
	logName := fmt.Sprintf("volume%d.log", index)
	logFile, err := os.Create(filepath.Join(c.logsDir, logName))
	if err != nil {
		return err
	}

	volumePort := c.volumePort0
	volumeGrpcPort := c.volumeGrpcPort0
	volumePubPort := c.volumePubPort0
	if index == 1 {
		volumePort = c.volumePort1
		volumeGrpcPort = c.volumeGrpcPort1
		volumePubPort = c.volumePubPort1
	} else if index == 2 {
		volumePort = c.volumePort2
		volumeGrpcPort = c.volumeGrpcPort2
		volumePubPort = c.volumePubPort2
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"volume",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(volumePort),
		"-port.grpc=" + strconv.Itoa(volumeGrpcPort),
		"-port.public=" + strconv.Itoa(volumePubPort),
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
	if index == 1 {
		c.volumeCmd1 = cmd
	} else if index == 2 {
		c.volumeCmd2 = cmd
	} else {
		c.volumeCmd0 = cmd
	}
	return nil
}

func (c *TripleVolumeCluster) waitForHTTP(url string) error {
	return (&Cluster{}).waitForHTTP(url)
}

func (c *TripleVolumeCluster) waitForTCP(addr string) error {
	return (&Cluster{}).waitForTCP(addr)
}

func (c *TripleVolumeCluster) tailLog(logName string) string {
	return (&Cluster{logsDir: c.logsDir}).tailLog(logName)
}

func (c *TripleVolumeCluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *TripleVolumeCluster) MasterURL() string {
	return "http://" + c.MasterAddress()
}

func (c *TripleVolumeCluster) VolumeAdminAddress(index int) string {
	switch index {
	case 1:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePort1))
	case 2:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePort2))
	default:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePort0))
	}
}

func (c *TripleVolumeCluster) VolumePublicAddress(index int) string {
	switch index {
	case 1:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPort1))
	case 2:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPort2))
	default:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePubPort0))
	}
}

func (c *TripleVolumeCluster) VolumeGRPCAddress(index int) string {
	switch index {
	case 1:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPort1))
	case 2:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPort2))
	default:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPort0))
	}
}

func (c *TripleVolumeCluster) VolumeAdminURL(index int) string {
	return "http://" + c.VolumeAdminAddress(index)
}

func (c *TripleVolumeCluster) VolumePublicURL(index int) string {
	return "http://" + c.VolumePublicAddress(index)
}

func (c *TripleVolumeCluster) BaseDir() string {
	return c.baseDir
}
