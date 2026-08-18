//go:build linux || darwin

package fuse_failover

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// failoverCluster runs 1 master, N volume servers, 1 filer and M FUSE mounts,
// with the master defaulting to 001 replication so every chunk lands on two
// distinct volume servers. Individual volume servers can be stopped and started
// while IO is in flight, which is what the Docker Swarm reports in discussion
// 10206 exercise: a volume server disappears mid-append and the mounts must
// keep reading from the surviving replica and keep writing on another volume.
type failoverCluster struct {
	t          testing.TB
	baseDir    string
	weedBinary string

	masterPort     int
	masterGrpcPort int
	filerPort      int
	filerGrpcPort  int
	volumePorts    []int
	volumeGrpcPort []int
	volumeDirs     []string

	masterCmd   *exec.Cmd
	filerCmd    *exec.Cmd
	volumeCmds  []*exec.Cmd
	mountCmds   []*exec.Cmd
	mountPoints []string
	logFiles    []*os.File

	mu          sync.Mutex
	waits       map[*exec.Cmd]chan error
	cleanupOnce sync.Once
}

func startFailoverCluster(t testing.TB, numVolumes, numMounts int) *failoverCluster {
	require.GreaterOrEqual(t, numVolumes, 2, "001 replication needs at least 2 volume servers")
	require.GreaterOrEqual(t, numMounts, 1)

	binary := findWeedBinary()
	if binary == "" {
		t.Skip("weed binary not found; set WEED_BINARY or ensure it is on PATH")
	}
	baseDir, err := os.MkdirTemp("", "seaweedfs_fuse_failover_")
	require.NoError(t, err)

	c := &failoverCluster{
		t:              t,
		baseDir:        baseDir,
		weedBinary:     binary,
		volumePorts:    make([]int, numVolumes),
		volumeGrpcPort: make([]int, numVolumes),
		volumeDirs:     make([]string, numVolumes),
		volumeCmds:     make([]*exec.Cmd, numVolumes),
		mountCmds:      make([]*exec.Cmd, numMounts),
		mountPoints:    make([]string, numMounts),
	}
	t.Cleanup(c.Stop)

	ports, err := testutil.AllocatePorts(4 + 2*numVolumes)
	require.NoError(t, err)
	c.masterPort, c.masterGrpcPort = ports[0], ports[1]
	c.filerPort, c.filerGrpcPort = ports[2], ports[3]
	for i := 0; i < numVolumes; i++ {
		c.volumePorts[i] = ports[4+2*i]
		c.volumeGrpcPort[i] = ports[5+2*i]
		c.volumeDirs[i] = filepath.Join(baseDir, fmt.Sprintf("volume%d", i))
		require.NoError(t, os.MkdirAll(c.volumeDirs[i], 0755))
	}

	require.NoError(t, c.startMaster())
	require.NoError(t, c.waitForTCP(c.masterCmd, "master",
		fmt.Sprintf("127.0.0.1:%d", c.masterPort), 30*time.Second))

	for i := 0; i < numVolumes; i++ {
		require.NoError(t, c.StartVolume(i))
	}

	require.NoError(t, c.startFiler())
	require.NoError(t, c.waitForTCP(c.filerCmd, "filer",
		fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPort), 30*time.Second))

	for i := 0; i < numMounts; i++ {
		mp := filepath.Join(baseDir, fmt.Sprintf("mount%d", i))
		require.NoError(t, os.MkdirAll(mp, 0755))
		c.mountPoints[i] = mp
		require.NoError(t, c.startMount(i))
		require.NoError(t, c.waitForMount(mp, 30*time.Second),
			"mount %d not ready\n%s", i, c.tailLog(fmt.Sprintf("mount%d", i)))
	}
	return c
}

func (c *failoverCluster) MountDir(i int) string { return c.mountPoints[i] }

func (c *failoverCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		for i := len(c.mountCmds) - 1; i >= 0; i-- {
			c.stopCmd(c.mountCmds[i], syscall.SIGTERM)
			_ = exec.Command("fusermount3", "-u", c.mountPoints[i]).Run()
			_ = exec.Command("fusermount", "-u", c.mountPoints[i]).Run()
		}
		c.stopCmd(c.filerCmd, syscall.SIGTERM)
		for i := len(c.volumeCmds) - 1; i >= 0; i-- {
			c.stopCmd(c.volumeCmds[i], syscall.SIGTERM)
		}
		c.stopCmd(c.masterCmd, syscall.SIGTERM)

		c.mu.Lock()
		for _, f := range c.logFiles {
			_ = f.Close()
		}
		c.mu.Unlock()
		c.copyLogsForCI()
		if !c.t.Failed() {
			os.RemoveAll(c.baseDir)
		}
	})
}

// KillVolume drops a volume server without letting it deregister, the closest
// local equivalent of a Swarm task vanishing from the overlay network.
func (c *failoverCluster) KillVolume(i int) {
	c.stopCmd(c.volumeCmds[i], syscall.SIGKILL)
	c.volumeCmds[i] = nil
}

// StartVolume (re)starts volume server i on its original ports and data dir.
func (c *failoverCluster) StartVolume(i int) error {
	cmd := exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"volume",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.volumePorts[i]),
		"-port.grpc="+strconv.Itoa(c.volumeGrpcPort[i]),
		"-master="+c.masterAddress(),
		"-dir="+c.volumeDirs[i],
		"-dataCenter=dc1",
		"-rack=rack1",
		"-max=10",
	)
	c.volumeCmds[i] = cmd
	if err := c.startCmd(cmd, fmt.Sprintf("volume%d", i)); err != nil {
		return err
	}
	return c.waitForTCP(cmd, fmt.Sprintf("volume%d", i),
		fmt.Sprintf("127.0.0.1:%d", c.volumePorts[i]), 30*time.Second)
}

func (c *failoverCluster) startMaster() error {
	c.masterCmd = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"master",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.masterPort),
		"-port.grpc="+strconv.Itoa(c.masterGrpcPort),
		"-mdir="+filepath.Join(c.baseDir, "master"),
		"-defaultReplication=001",
		"-volumeSizeLimitMB=64",
	)
	return c.startCmd(c.masterCmd, "master")
}

func (c *failoverCluster) startFiler() error {
	filerDir := filepath.Join(c.baseDir, "filer")
	if err := os.MkdirAll(filerDir, 0755); err != nil {
		return fmt.Errorf("create filer dir: %w", err)
	}
	c.filerCmd = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"filer",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.filerPort),
		"-port.grpc="+strconv.Itoa(c.filerGrpcPort),
		"-master="+c.masterAddress(),
		"-defaultReplicaPlacement=001",
		"-defaultStoreDir="+filerDir,
	)
	return c.startCmd(c.filerCmd, "filer")
}

func (c *failoverCluster) startMount(idx int) error {
	cacheDir := filepath.Join(c.baseDir, fmt.Sprintf("cache%d", idx))
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	// Chunk-level detail needs -v=4; keep CI at -v=2 so the logs stay small.
	verbosity := os.Getenv("FUSE_FAILOVER_MOUNT_V")
	if verbosity == "" {
		verbosity = "2"
	}
	c.mountCmds[idx] = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"-v="+verbosity,
		"mount",
		"-filer="+c.filerAddress(),
		"-dir="+c.mountPoints[idx],
		"-filer.path=/",
		"-dirAutoCreate",
		"-allowOthers=false",
		"-replication=001",
		"-cacheDir="+cacheDir,
	)
	return c.startCmd(c.mountCmds[idx], fmt.Sprintf("mount%d", idx))
}

// MasterGet fetches a master HTTP endpoint, e.g. "/dir/status?pretty=y" or
// "/dir/lookup?volumeId=6", so a failing test can show where the replicas are.
func (c *failoverCluster) MasterGet(path string) string {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", c.masterPort, path))
	if err != nil {
		return fmt.Sprintf("(master %s failed: %v)", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("(master %s read failed: %v)", path, err)
	}
	return string(body)
}

// FilerGet reads a file back through the filer's own HTTP handler: a view of
// the chunk list that neither mount's cache can colour.
func (c *failoverCluster) FilerGet(path string) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", c.filerPort, path))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("filer %s: %s", path, resp.Status)
	}
	return body, nil
}

// VolumeServerAddress is the address volume server i registers with the master.
func (c *failoverCluster) VolumeServerAddress(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", c.volumePorts[i])
}

// FileVolumeIds returns the volume ids backing a file, read from the filer's
// own entry rather than inferred, so a test can tell which servers a given
// file actually depends on. Manifests are resolved first: a manifest chunk's
// own fid names the volume holding the manifest, not the data.
func (c *failoverCluster) FileVolumeIds(path string) ([]uint32, error) {
	body, err := c.FilerGet(path + "?metadata=true&resolveManifest=true")
	if err != nil {
		return nil, err
	}
	var entry struct {
		Chunks []struct {
			FileId string `json:"file_id"`
			Fid    struct {
				VolumeId uint32 `json:"volume_id"`
			} `json:"fid"`
		} `json:"chunks"`
	}
	if err = json.Unmarshal(body, &entry); err != nil {
		return nil, fmt.Errorf("decode entry %s: %w", path, err)
	}
	seen := make(map[uint32]bool)
	var vids []uint32
	for _, chunk := range entry.Chunks {
		vid := chunk.Fid.VolumeId
		if vid == 0 && chunk.FileId != "" {
			parsed, parseErr := strconv.ParseUint(strings.SplitN(chunk.FileId, ",", 2)[0], 10, 32)
			if parseErr != nil {
				return nil, fmt.Errorf("parse file id %s: %w", chunk.FileId, parseErr)
			}
			vid = uint32(parsed)
		}
		if !seen[vid] {
			seen[vid] = true
			vids = append(vids, vid)
		}
	}
	return vids, nil
}

// VolumeHolders returns the volume server addresses the master currently lists
// for a volume id.
func (c *failoverCluster) VolumeHolders(vid uint32) ([]string, error) {
	var lookup struct {
		Locations []struct {
			Url string `json:"url"`
		} `json:"locations"`
	}
	body := c.MasterGet(fmt.Sprintf("/dir/lookup?volumeId=%d", vid))
	if err := json.Unmarshal([]byte(body), &lookup); err != nil {
		return nil, fmt.Errorf("decode lookup for volume %d: %w (%s)", vid, err, body)
	}
	holders := make([]string, 0, len(lookup.Locations))
	for _, loc := range lookup.Locations {
		holders = append(holders, loc.Url)
	}
	return holders, nil
}

// WaitForHolders polls the master until it lists exactly count servers for a
// volume. The master only drops a dead node after three missed heartbeats, so a
// test that depends on the cluster's view having caught up has to wait for it.
func (c *failoverCluster) WaitForHolders(vid uint32, count int, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	for {
		holders, err := c.VolumeHolders(vid)
		if err == nil && len(holders) == count {
			return holders, nil
		}
		if time.Now().After(deadline) {
			return holders, fmt.Errorf("volume %d still has %d holders (%v), want %d", vid, len(holders), holders, count)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// volumeIndexOf maps a server address back to its index, or -1.
func (c *failoverCluster) volumeIndexOf(address string) int {
	for i := range c.volumePorts {
		if c.VolumeServerAddress(i) == address {
			return i
		}
	}
	return -1
}

// FileIsOn reports whether any of path's chunks live on the given volume
// server, i.e. whether taking that server down actually costs this file a
// replica.
func (c *failoverCluster) FileIsOn(path, serverAddress string) (bool, error) {
	vids, err := c.FileVolumeIds(path)
	if err != nil {
		return false, err
	}
	for _, vid := range vids {
		holders, holdersErr := c.VolumeHolders(vid)
		if holdersErr != nil {
			return false, holdersErr
		}
		for _, holder := range holders {
			if holder == serverAddress {
				return true, nil
			}
		}
	}
	return false, nil
}

func (c *failoverCluster) masterAddress() string {
	return string(pb.NewServerAddress("127.0.0.1", c.masterPort, c.masterGrpcPort))
}

func (c *failoverCluster) filerAddress() string {
	return string(pb.NewServerAddress("127.0.0.1", c.filerPort, c.filerGrpcPort))
}

func (c *failoverCluster) startCmd(cmd *exec.Cmd, name string) error {
	logPath := filepath.Join(c.baseDir, "logs")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	// Append so a restarted volume server keeps the log of its earlier run.
	logFile, err := os.OpenFile(filepath.Join(logPath, name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.logFiles = append(c.logFiles, logFile)
	c.mu.Unlock()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap in the background and publish the result: Signal(0) succeeds for a
	// zombie, so an unreaped child that died at startup would otherwise look
	// alive until the readiness timeout expired.
	ch := make(chan error, 1)
	c.mu.Lock()
	if c.waits == nil {
		c.waits = make(map[*exec.Cmd]chan error)
	}
	c.waits[cmd] = ch
	c.mu.Unlock()
	go func() {
		ch <- cmd.Wait()
		close(ch)
	}()
	return nil
}

// waitChan returns the channel carrying cmd's exit, or nil if it was never
// started through startCmd. It stays readable after the exit is consumed.
func (c *failoverCluster) waitChan(cmd *exec.Cmd) chan error {
	if cmd == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waits[cmd]
}

func (c *failoverCluster) tailLog(name string) string {
	data, err := os.ReadFile(filepath.Join(c.baseDir, "logs", name+".log"))
	if err != nil {
		return fmt.Sprintf("(log %s not available: %v)", name, err)
	}
	const maxTail = 8192
	if len(data) > maxTail {
		data = data[len(data)-maxTail:]
	}
	return string(data)
}

func (c *failoverCluster) copyLogsForCI() {
	// One directory per test: subtests share a log dir name otherwise, and the
	// last one to finish would overwrite the logs of the one that failed.
	ciLogDir := filepath.Join("/tmp/seaweedfs-fuse-failover-logs",
		strings.ReplaceAll(c.t.Name(), "/", "_"))
	os.MkdirAll(ciLogDir, 0755)
	entries, err := os.ReadDir(filepath.Join(c.baseDir, "logs"))
	if err != nil {
		return
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(c.baseDir, "logs", e.Name()))
		if err != nil {
			continue
		}
		os.WriteFile(filepath.Join(ciLogDir, e.Name()), data, 0644)
	}
}

func (c *failoverCluster) waitForTCP(cmd *exec.Cmd, name, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		if ch := c.waitChan(cmd); ch != nil {
			select {
			case waitErr := <-ch:
				return fmt.Errorf("%s exited before listening on %s: %v\n%s",
					name, addr, waitErr, c.tailLog(name))
			default:
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout\n%s", addr, c.tailLog(name))
}

func (c *failoverCluster) waitForMount(mountPoint string, timeout time.Duration) error {
	parentDir := filepath.Dir(mountPoint)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		parentStat, err := os.Stat(parentDir)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		mountStat, err := os.Stat(mountPoint)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if parentStat.Sys().(*syscall.Stat_t).Dev != mountStat.Sys().(*syscall.Stat_t).Dev {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("mount point %s not ready within timeout (FUSE not detected)", mountPoint)
}

func findWeedBinary() string {
	if env := os.Getenv("WEED_BINARY"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	if p, err := exec.LookPath("weed"); err == nil {
		return p
	}
	return ""
}

// stopCmd signals cmd and waits for the reaper goroutine started by startCmd to
// report its exit, escalating to SIGKILL if it does not go quietly.
func (c *failoverCluster) stopCmd(cmd *exec.Cmd, sig syscall.Signal) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(sig)
	done := c.waitChan(cmd)
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGKILL)
		<-done
	}
}
