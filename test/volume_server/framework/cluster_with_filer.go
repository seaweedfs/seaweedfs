package framework

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

type ClusterWithFiler struct {
	*Cluster

	filerCmd      *exec.Cmd
	filerPort     int
	filerGrpcPort int
}

func StartSingleVolumeClusterWithFiler(t testing.TB, profile matrix.Profile) *ClusterWithFiler {
	t.Helper()

	baseCluster := StartSingleVolumeCluster(t, profile)

	ports, err := allocatePorts(2)
	if err != nil {
		t.Fatalf("allocate filer ports: %v", err)
	}

	filerDataDir := filepath.Join(baseCluster.baseDir, "filer")
	if mkErr := os.MkdirAll(filerDataDir, 0o755); mkErr != nil {
		t.Fatalf("create filer data dir: %v", mkErr)
	}

	logFile, err := os.Create(filepath.Join(baseCluster.logsDir, "filer.log"))
	if err != nil {
		t.Fatalf("create filer log file: %v", err)
	}

	filerPort := ports[0]
	filerGrpcPort := ports[1]
	args := []string{
		"-config_dir=" + baseCluster.configDir,
		"filer",
		"-master=127.0.0.1:" + strconv.Itoa(baseCluster.masterPort),
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(filerPort),
		"-port.grpc=" + strconv.Itoa(filerGrpcPort),
		"-defaultStoreDir=" + filerDataDir,
	}

	filerCmd := exec.Command(baseCluster.weedBinary, args...)
	filerCmd.Dir = baseCluster.baseDir
	filerCmd.Stdout = logFile
	filerCmd.Stderr = logFile
	if err = filerCmd.Start(); err != nil {
		t.Fatalf("start filer: %v", err)
	}

	if err = baseCluster.waitForTCP(net.JoinHostPort("127.0.0.1", strconv.Itoa(filerGrpcPort))); err != nil {
		filerLogTail := baseCluster.tailLog("filer.log")
		stopProcess(filerCmd)
		t.Fatalf("wait for filer grpc readiness: %v\nfiler log tail:\n%s", err, filerLogTail)
	}

	t.Cleanup(func() {
		stopProcess(filerCmd)
	})

	return &ClusterWithFiler{
		Cluster:       baseCluster,
		filerCmd:      filerCmd,
		filerPort:     filerPort,
		filerGrpcPort: filerGrpcPort,
	}
}

func (c *ClusterWithFiler) FilerAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.filerPort))
}

func (c *ClusterWithFiler) FilerGRPCAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.filerGrpcPort))
}

func (c *ClusterWithFiler) FilerServerAddress() string {
	return fmt.Sprintf("%s.%d", c.FilerAddress(), c.filerGrpcPort)
}
