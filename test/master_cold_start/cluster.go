// Package master_cold_start boots a fresh master plus empty volume servers
// and exercises the very first write against the cluster. The first assign
// triggers volume growth server-side; it must succeed within a single
// request, without client retries.
package master_cold_start

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	waitTimeout = 30 * time.Second
	waitTick    = 200 * time.Millisecond
)

type process struct {
	name    string
	cmd     *exec.Cmd
	logFile string
}

// Cluster is one fresh master plus n empty volume servers.
type Cluster struct {
	t          testing.TB
	weedBinary string
	baseDir    string

	masterPort     int
	masterGrpcPort int
	procs          []*process
}

// StartCluster boots a master and volumeServerCount empty volume servers,
// then waits for all of them to register with the master.
func StartCluster(t testing.TB, volumeServerCount int) *Cluster {
	t.Helper()

	weedBinary, err := findOrBuildWeedBinary()
	if err != nil {
		t.Fatalf("resolve weed binary: %v", err)
	}

	baseDir, err := os.MkdirTemp("", "seaweedfs_master_cold_start_it_")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	logsDir := filepath.Join(baseDir, "logs")
	os.MkdirAll(logsDir, 0o755)

	ports, err := testutil.AllocateMiniPorts(1 + volumeServerCount)
	if err != nil {
		t.Fatalf("allocate ports: %v", err)
	}

	c := &Cluster{
		t:              t,
		weedBinary:     weedBinary,
		baseDir:        baseDir,
		masterPort:     ports[0],
		masterGrpcPort: ports[0] + testutil.GrpcPortOffset,
	}
	t.Cleanup(func() {
		c.StopAll()
		if t.Failed() {
			c.DumpLogs()
			t.Logf("cold-start logs kept at %s", baseDir)
		} else {
			os.RemoveAll(baseDir)
		}
	})

	masterDir := filepath.Join(baseDir, "master")
	os.MkdirAll(masterDir, 0o755)
	c.startProcess("master", filepath.Join(logsDir, "master.log"),
		"master",
		"-ip=127.0.0.1",
		"-port="+strconv.Itoa(c.masterPort),
		"-port.grpc="+strconv.Itoa(c.masterGrpcPort),
		"-mdir="+masterDir,
		"-volumeSizeLimitMB=32",
		"-defaultReplication=000",
	)
	if err := c.waitForMaster(waitTimeout); err != nil {
		t.Fatalf("master not ready: %v", err)
	}

	for i := 0; i < volumeServerCount; i++ {
		volDir := filepath.Join(baseDir, fmt.Sprintf("vol%d", i))
		os.MkdirAll(volDir, 0o755)
		name := fmt.Sprintf("volume%d", i)
		c.startProcess(name, filepath.Join(logsDir, name+".log"),
			"volume",
			"-ip=127.0.0.1",
			"-port="+strconv.Itoa(ports[1+i]),
			"-port.grpc="+strconv.Itoa(ports[1+i]+testutil.GrpcPortOffset),
			"-dir="+volDir,
			"-max=10",
			"-mserver=127.0.0.1:"+strconv.Itoa(c.masterPort),
		)
	}
	if err := c.waitForVolumeServers(volumeServerCount, waitTimeout); err != nil {
		t.Fatalf("volume servers not registered: %v", err)
	}
	return c
}

func (c *Cluster) startProcess(name, logFile string, args ...string) {
	c.t.Helper()
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Fatalf("create log for %s: %v", name, err)
	}
	defer f.Close() // the child owns its own descriptor after Start
	cmd := exec.Command(c.weedBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", name, err)
	}
	c.procs = append(c.procs, &process{name: name, cmd: cmd, logFile: logFile})
}

// StopAll interrupts every process, volume servers first so the master does
// not log reconnect noise.
func (c *Cluster) StopAll() {
	for i := len(c.procs) - 1; i >= 0; i-- {
		p := c.procs[i]
		if p.cmd.Process == nil {
			continue
		}
		_ = p.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- p.cmd.Wait() }()
		select {
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
			<-done
		case <-done:
		}
	}
	c.procs = nil
}

// MasterURL returns the master HTTP endpoint.
func (c *Cluster) MasterURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.masterPort)
}

// MasterAddress returns "127.0.0.1:port" for the master HTTP port.
func (c *Cluster) MasterAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", c.masterPort)
}

func (c *Cluster) waitForMaster(timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(c.MasterURL() + "/cluster/status")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var cs struct {
				IsLeader bool `json:"IsLeader"`
			}
			if json.Unmarshal(body, &cs) == nil && cs.IsLeader {
				return nil
			}
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("master did not become leader within %v", timeout)
}

// waitForVolumeServers polls /dir/status until want data nodes are registered
// with free volume slots.
func (c *Cluster) waitForVolumeServers(want int, timeout time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get(c.MasterURL() + "/dir/status")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var status struct {
				Topology struct {
					Max         int64 `json:"Max"`
					DataCenters []struct {
						Racks []struct {
							DataNodes []struct {
								Url string `json:"Url"`
							} `json:"DataNodes"`
						} `json:"Racks"`
					} `json:"DataCenters"`
				} `json:"Topology"`
			}
			if json.Unmarshal(body, &status) == nil {
				nodes := 0
				for _, dc := range status.Topology.DataCenters {
					for _, rack := range dc.Racks {
						nodes += len(rack.DataNodes)
					}
				}
				if nodes >= want && status.Topology.Max > 0 {
					return nil
				}
				last = fmt.Sprintf("%d/%d nodes, %d slots", nodes, want, status.Topology.Max)
			}
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("volume servers not registered within %v (%s)", timeout, last)
}

// DumpLogs prints the tail of every process log.
func (c *Cluster) DumpLogs() {
	logsDir := filepath.Join(c.baseDir, "logs")
	entries, _ := os.ReadDir(logsDir)
	for _, e := range entries {
		c.t.Logf("=== %s tail ===\n%s", e.Name(), tailFile(filepath.Join(logsDir, e.Name())))
	}
}

func tailFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "(no log)"
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	lines := make([]string, 0, 50)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 50 {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}

func findOrBuildWeedBinary() (string, error) {
	if fromEnv := os.Getenv("WEED_BINARY"); fromEnv != "" {
		if isExecutableFile(fromEnv) {
			return fromEnv, nil
		}
		return "", fmt.Errorf("WEED_BINARY not executable: %s", fromEnv)
	}

	repoRoot := ""
	if _, file, _, ok := runtime.Caller(0); ok {
		repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	}
	if repoRoot == "" {
		return "", fmt.Errorf("unable to detect repository root")
	}

	binDir := filepath.Join(os.TempDir(), "seaweedfs_master_cold_start_it_bin")
	os.MkdirAll(binDir, 0o755)
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
	return binPath, nil
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode().Perm()&0o111 != 0
}
