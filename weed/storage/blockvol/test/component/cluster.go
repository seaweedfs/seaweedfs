//go:build integration

// Package component provides component-level integration tests for the block
// storage control plane. Tests start real weed master + volume server processes
// on localhost, exercise the HTTP API via blockapi.Client, and verify registry
// state. No SSH, no kernel iSCSI, no special hardware.
//
// Run: go test -tags integration -v -timeout 10m ./weed/storage/blockvol/test/component/
// Or:  WEED_BINARY=/path/to/weed go test -tags integration ...
package component

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// cluster manages a weed master + N volume servers for component testing.
type cluster struct {
	t          *testing.T
	weedBin    string
	masterPort int
	ip         string
	masterDir  string
	masterCmd  *exec.Cmd
	masterLog  *os.File
	volumes    []*volumeProc
}

type volumeProc struct {
	idx       int
	port      int
	blockPort int
	dir       string
	extraArgs []string
	cmd       *exec.Cmd
	logFd     *os.File
	stopped   bool
}

// newCluster creates a cluster helper. Cleanup is registered via t.Cleanup.
func newCluster(t *testing.T, weedBin string, masterPort int) *cluster {
	t.Helper()
	dir, err := os.MkdirTemp("", "sw-comp-master-")
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{
		t:          t,
		weedBin:    weedBin,
		masterPort: masterPort,
		ip:         "127.0.0.1",
		masterDir:  dir,
	}
	t.Cleanup(func() {
		c.stop()
		if t.Failed() {
			c.dumpLogs()
		}
	})
	return c
}

// addVolume registers a volume server to start. Returns its index.
// Optional extraArgs are appended to the weed volume command line.
func (c *cluster) addVolume(port, blockPort int, extraArgs ...string) int {
	c.t.Helper()
	dir, err := os.MkdirTemp("", fmt.Sprintf("sw-comp-vs%d-", len(c.volumes)))
	if err != nil {
		c.t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "blocks"), 0755); err != nil {
		c.t.Fatal(err)
	}
	idx := len(c.volumes)
	c.volumes = append(c.volumes, &volumeProc{
		idx: idx, port: port, blockPort: blockPort, dir: dir, extraArgs: extraArgs,
	})
	return idx
}

// start launches master + all volume servers and waits for readiness.
func (c *cluster) start(ctx context.Context) {
	c.t.Helper()

	// Start master.
	c.masterCmd = exec.Command(c.weedBin, "master",
		fmt.Sprintf("-port=%d", c.masterPort),
		fmt.Sprintf("-mdir=%s", c.masterDir),
	)
	logPath := filepath.Join(c.masterDir, "master.log")
	f, err := os.Create(logPath)
	if err != nil {
		c.t.Fatal(err)
	}
	c.masterLog = f
	c.masterCmd.Stdout = f
	c.masterCmd.Stderr = f
	if err := c.masterCmd.Start(); err != nil {
		f.Close()
		c.t.Fatalf("start master: %v", err)
	}

	// Wait for master to become leader.
	c.waitClusterReady(ctx, 30*time.Second)

	// Start volume servers.
	for _, vs := range c.volumes {
		c.startVolumeAt(ctx, vs)
	}
}

func (c *cluster) startVolumeAt(ctx context.Context, vs *volumeProc) {
	args := []string{"volume",
		fmt.Sprintf("-port=%d", vs.port),
		fmt.Sprintf("-mserver=%s:%d", c.ip, c.masterPort),
		fmt.Sprintf("-dir=%s", vs.dir),
		fmt.Sprintf("-block.dir=%s", filepath.Join(vs.dir, "blocks")),
		fmt.Sprintf("-block.listen=:%d", vs.blockPort),
		fmt.Sprintf("-ip=%s", c.ip),
	}
	args = append(args, vs.extraArgs...)
	vs.cmd = exec.Command(c.weedBin, args...)
	logPath := filepath.Join(vs.dir, "volume.log")
	f, err := os.Create(logPath)
	if err != nil {
		c.t.Fatal(err)
	}
	vs.logFd = f
	vs.cmd.Stdout = f
	vs.cmd.Stderr = f
	if err := vs.cmd.Start(); err != nil {
		f.Close()
		c.t.Fatalf("start volume server %d: %v", vs.idx, err)
	}
	vs.stopped = false
}

// client returns a blockapi.Client pointing at the master.
func (c *cluster) client() *blockapi.Client {
	return blockapi.NewClient(fmt.Sprintf("http://%s:%d", c.ip, c.masterPort))
}

// waitClusterReady polls /cluster/status until IsLeader is true.
func (c *cluster) waitClusterReady(ctx context.Context, timeout time.Duration) {
	c.t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	url := fmt.Sprintf("http://%s:%d/cluster/status", c.ip, c.masterPort)

	for {
		select {
		case <-deadline:
			c.t.Fatalf("master not ready after %s", timeout)
		case <-ctx.Done():
			c.t.Fatal("context cancelled waiting for master")
		case <-ticker.C:
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(body), `"IsLeader":true`) ||
				strings.Contains(string(body), `"isLeader":true`) {
				return
			}
		}
	}
}

// waitBlockServers polls until count block-capable servers are registered.
func (c *cluster) waitBlockServers(ctx context.Context, count int, timeout time.Duration) {
	c.t.Helper()
	cl := c.client()
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			c.t.Fatalf("wanted %d block servers, timed out after %s", count, timeout)
		case <-ctx.Done():
			c.t.Fatal("context cancelled waiting for block servers")
		case <-ticker.C:
			servers, err := cl.ListServers(ctx)
			if err != nil {
				continue
			}
			capable := 0
			for _, s := range servers {
				if s.BlockCapable {
					capable++
				}
			}
			if capable >= count {
				return
			}
		}
	}
}

// waitPrimaryChange polls until the volume's primary differs from notServer.
func (c *cluster) waitPrimaryChange(ctx context.Context, name, notServer string, timeout time.Duration) *blockapi.VolumeInfo {
	c.t.Helper()
	cl := c.client()
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			c.t.Fatalf("primary for %s didn't change from %s after %s", name, notServer, timeout)
		case <-ctx.Done():
			c.t.Fatalf("context cancelled waiting for primary change on %s", name)
		case <-ticker.C:
			info, err := cl.LookupVolume(ctx, name)
			if err != nil {
				continue
			}
			if info.VolumeServer != notServer && info.VolumeServer != "" {
				return info
			}
		}
	}
}

// stopVolume kills a volume server by index.
func (c *cluster) stopVolume(idx int) {
	vs := c.volumes[idx]
	if vs.stopped || vs.cmd == nil || vs.cmd.Process == nil {
		return
	}
	vs.cmd.Process.Kill()
	vs.cmd.Wait()
	if vs.logFd != nil {
		vs.logFd.Close()
		vs.logFd = nil
	}
	vs.stopped = true
}

// restartVolume starts a previously stopped volume server with the same params.
func (c *cluster) restartVolume(ctx context.Context, idx int) {
	c.t.Helper()
	vs := c.volumes[idx]
	if !vs.stopped {
		c.t.Fatalf("volume %d not stopped", idx)
	}
	c.startVolumeAt(ctx, vs)
}

// stop kills all processes and removes temp dirs.
func (c *cluster) stop() {
	for _, vs := range c.volumes {
		if !vs.stopped && vs.cmd != nil && vs.cmd.Process != nil {
			vs.cmd.Process.Kill()
			vs.cmd.Wait()
		}
		if vs.logFd != nil {
			vs.logFd.Close()
		}
		os.RemoveAll(vs.dir)
	}
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}
	if c.masterLog != nil {
		c.masterLog.Close()
	}
	os.RemoveAll(c.masterDir)
}

// dumpLogs prints process logs (called on test failure).
func (c *cluster) dumpLogs() {
	logPath := filepath.Join(c.masterDir, "master.log")
	if data, err := os.ReadFile(logPath); err == nil && len(data) > 0 {
		// Truncate to last 200 lines.
		lines := strings.Split(string(data), "\n")
		if len(lines) > 200 {
			lines = lines[len(lines)-200:]
		}
		c.t.Logf("=== Master log (last %d lines) ===\n%s", len(lines), strings.Join(lines, "\n"))
	}
	for i, vs := range c.volumes {
		logPath := filepath.Join(vs.dir, "volume.log")
		if data, err := os.ReadFile(logPath); err == nil && len(data) > 0 {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			c.t.Logf("=== Volume %d log (last %d lines) ===\n%s", i, len(lines), strings.Join(lines, "\n"))
		}
	}
}
