package framework

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// RustCluster wraps a Go master + Rust volume server for integration testing.
type RustCluster struct {
	testingTB testing.TB
	profile   matrix.Profile

	weedBinary       string // Go weed binary (for the master)
	rustVolumeBinary string // Rust volume binary

	baseDir   string
	configDir string
	logsDir   string
	keepLogs  bool

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	volumePubPort  int

	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd

	cleanupOnce sync.Once
}

var (
	rustBinaryOnce sync.Once
	rustBinaryPath string
	rustBinaryErr  error
)

// StartRustVolumeCluster starts a Go master + Rust volume server.
func StartRustVolumeCluster(t testing.TB, profile matrix.Profile) *RustCluster {
	t.Helper()

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
	volumeDataDir := filepath.Join(baseDir, "volume")
	for _, dir := range []string{configDir, logsDir, masterDataDir, volumeDataDir} {
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			t.Fatalf("create %s: %v", dir, mkErr)
		}
	}

	if err = writeSecurityConfig(configDir, profile); err != nil {
		t.Fatalf("write security config: %v", err)
	}

	miniPorts, err := testutil.AllocateMiniPorts(1)
	if err != nil {
		t.Fatalf("allocate master port pair: %v", err)
	}
	masterPort := miniPorts[0]
	masterGrpcPort := masterPort + testutil.GrpcPortOffset

	ports, err := testutil.AllocatePorts(3)
	if err != nil {
		t.Fatalf("allocate ports: %v", err)
	}

	rc := &RustCluster{
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
		volumePort:       ports[0],
		volumeGrpcPort:   ports[1],
		volumePubPort:    ports[0],
	}
	if profile.SplitPublicPort {
		rc.volumePubPort = ports[2]
	}

	if err = rc.startMaster(masterDataDir); err != nil {
		rc.Stop()
		t.Fatalf("start master: %v", err)
	}
	// Reuse the same HTTP readiness helper via an unexported Cluster shim.
	helper := &Cluster{logsDir: logsDir}
	if err = helper.waitForHTTP(rc.MasterURL() + "/dir/status"); err != nil {
		masterLog := helper.tailLog("master.log")
		rc.Stop()
		t.Fatalf("wait for master readiness: %v\nmaster log tail:\n%s", err, masterLog)
	}

	if err = rc.startRustVolume(volumeDataDir); err != nil {
		masterLog := helper.tailLog("master.log")
		rc.Stop()
		t.Fatalf("start rust volume: %v\nmaster log tail:\n%s", err, masterLog)
	}
	if err = helper.waitForHTTP(rc.VolumeAdminURL() + "/healthz"); err != nil {
		volumeLog := helper.tailLog("volume.log")
		rc.Stop()
		t.Fatalf("wait for rust volume readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}
	if err = helper.waitForTCP(rc.VolumeGRPCAddress()); err != nil {
		volumeLog := helper.tailLog("volume.log")
		rc.Stop()
		t.Fatalf("wait for rust volume grpc readiness: %v\nvolume log tail:\n%s", err, volumeLog)
	}

	t.Cleanup(func() {
		rc.Stop()
	})

	return rc
}

// Stop terminates all processes and cleans temporary files.
func (rc *RustCluster) Stop() {
	if rc == nil {
		return
	}
	rc.cleanupOnce.Do(func() {
		stopProcess(rc.volumeCmd)
		stopProcess(rc.masterCmd)
		if !rc.keepLogs && !rc.testingTB.Failed() {
			_ = os.RemoveAll(rc.baseDir)
		} else if rc.baseDir != "" {
			rc.testingTB.Logf("rust volume server integration logs kept at %s", rc.baseDir)
		}
	})
}

func (rc *RustCluster) startMaster(dataDir string) error {
	logFile, err := os.Create(filepath.Join(rc.logsDir, "master.log"))
	if err != nil {
		return err
	}

	args := []string{
		"-config_dir=" + rc.configDir,
		"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(rc.masterPort),
		"-port.grpc=" + strconv.Itoa(rc.masterGrpcPort),
		"-mdir=" + dataDir,
		"-peers=none",
		"-volumeSizeLimitMB=" + strconv.Itoa(testVolumeSizeLimitMB),
		"-defaultReplication=000",
	}

	rc.masterCmd = exec.Command(rc.weedBinary, args...)
	rc.masterCmd.Dir = rc.baseDir
	rc.masterCmd.Stdout = logFile
	rc.masterCmd.Stderr = logFile
	return rc.masterCmd.Start()
}

func rustVolumeArgs(
	profile matrix.Profile,
	configDir string,
	masterPort int,
	volumePort int,
	volumeGrpcPort int,
	volumePubPort int,
	dataDir string,
) []string {
	args := []string{
		"--port", strconv.Itoa(volumePort),
		"--port.grpc", strconv.Itoa(volumeGrpcPort),
		"--port.public", strconv.Itoa(volumePubPort),
		"--ip", "127.0.0.1",
		"--ip.bind", "127.0.0.1",
		"--dir", dataDir,
		"--max", "16",
		"--master", "127.0.0.1:" + strconv.Itoa(masterPort),
		"--securityFile", filepath.Join(configDir, "security.toml"),
		"--readMode", profile.ReadMode,
		"--concurrentUploadLimitMB", strconv.Itoa(profile.ConcurrentUploadLimitMB),
		"--concurrentDownloadLimitMB", strconv.Itoa(profile.ConcurrentDownloadLimitMB),
		"--preStopSeconds", "0",
	}
	if profile.InflightUploadTimeout > 0 {
		args = append(args, "--inflightUploadDataTimeout", profile.InflightUploadTimeout.String())
	}
	if profile.InflightDownloadTimeout > 0 {
		args = append(args, "--inflightDownloadDataTimeout", profile.InflightDownloadTimeout.String())
	}
	return args
}

func (rc *RustCluster) startRustVolume(dataDir string) error {
	logFile, err := os.Create(filepath.Join(rc.logsDir, "volume.log"))
	if err != nil {
		return err
	}

	args := rustVolumeArgs(
		rc.profile,
		rc.configDir,
		rc.masterPort,
		rc.volumePort,
		rc.volumeGrpcPort,
		rc.volumePubPort,
		dataDir,
	)

	rc.volumeCmd = exec.Command(rc.rustVolumeBinary, args...)
	rc.volumeCmd.Dir = rc.baseDir
	rc.volumeCmd.Stdout = logFile
	rc.volumeCmd.Stderr = logFile
	return rc.volumeCmd.Start()
}

// FindOrBuildRustBinary returns an executable Rust volume binary, building one when needed.
func FindOrBuildRustBinary() (string, error) {
	if fromEnv := os.Getenv("RUST_VOLUME_BINARY"); fromEnv != "" {
		if isExecutableFile(fromEnv) {
			return fromEnv, nil
		}
		return "", fmt.Errorf("RUST_VOLUME_BINARY is set but not executable: %s", fromEnv)
	}

	rustBinaryOnce.Do(func() {
		// Derive the Rust volume crate directory from this source file's location.
		rustCrateDir := ""
		if _, file, _, ok := runtime.Caller(0); ok {
			repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
			for _, candidate := range []string{"seaweed-volume", "weed-volume"} {
				dir := filepath.Join(repoRoot, candidate)
				if isDir(dir) && isFile(filepath.Join(dir, "Cargo.toml")) {
					rustCrateDir = dir
					break
				}
			}
		}
		if rustCrateDir == "" {
			rustBinaryErr = fmt.Errorf("unable to detect Rust volume crate directory")
			return
		}

		releaseBin := filepath.Join(rustCrateDir, "target", "release", "weed-volume")

		// Always rebuild once per test process so the harness uses current source and features.
		cmd := exec.Command("cargo", "build", "--release")
		cmd.Dir = rustCrateDir
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			rustBinaryErr = fmt.Errorf("build rust volume binary: %w\n%s", err, out.String())
			return
		}
		if !isExecutableFile(releaseBin) {
			rustBinaryErr = fmt.Errorf("built rust volume binary is not executable: %s", releaseBin)
			return
		}
		rustBinaryPath = releaseBin
	})

	if rustBinaryErr != nil {
		return "", rustBinaryErr
	}
	return rustBinaryPath, nil
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// --- accessor methods (mirror Cluster) ---

func (rc *RustCluster) MasterAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(rc.masterPort))
}

func (rc *RustCluster) VolumeAdminAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(rc.volumePort))
}

func (rc *RustCluster) VolumePublicAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(rc.volumePubPort))
}

func (rc *RustCluster) VolumeGRPCAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(rc.volumeGrpcPort))
}

// VolumeServerAddress returns SeaweedFS server address format: ip:httpPort.grpcPort
func (rc *RustCluster) VolumeServerAddress() string {
	return fmt.Sprintf("%s.%d", rc.VolumeAdminAddress(), rc.volumeGrpcPort)
}

func (rc *RustCluster) MasterURL() string {
	return "http://" + rc.MasterAddress()
}

func (rc *RustCluster) VolumeAdminURL() string {
	return "http://" + rc.VolumeAdminAddress()
}

func (rc *RustCluster) VolumePublicURL() string {
	return "http://" + rc.VolumePublicAddress()
}

func (rc *RustCluster) BaseDir() string {
	return rc.baseDir
}
