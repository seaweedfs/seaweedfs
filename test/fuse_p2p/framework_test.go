//go:build linux || darwin

package fuse_p2p

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// p2pTestCluster manages a minimal SeaweedFS cluster exercising the peer
// chunk-sharing (p2p) read path: 1 master, 1 volume, 1 filer, and N FUSE
// mounts that all have -peer.enable set. Three mounts is the sweet spot
// for integration testing — with 2 mounts the HRW owner of a chunk is
// ~50% the reader itself (which short-circuits the peer path); with 3+
// mounts it's ≤ 1/3, so a multi-chunk file almost certainly exercises
// the remote-owner fan-out.
type p2pTestCluster struct {
	t          testing.TB
	baseDir    string
	weedBinary string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPort      int
	filerGrpcPort  int
	mountPeerPorts []int
	mountPoints    []string

	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd
	filerCmd  *exec.Cmd
	mountCmds []*exec.Cmd
	logFiles  []*os.File

	cleanupOnce sync.Once
}

// startP2PTestCluster brings up a cluster with numMounts FUSE mounts,
// every one advertising on its own -peer.listen port. All mounts register
// with the same filer. Verbose logging (-v=4) is enabled on each mount so
// peer-read success/failure messages land in the log file and the test
// can grep them to verify the p2p path fired.
func startP2PTestCluster(t testing.TB, numMounts int) *p2pTestCluster {
	require.GreaterOrEqual(t, numMounts, 2, "need at least 2 mounts to exercise p2p")
	binary := findWeedBinary()
	if binary == "" {
		t.Skip("weed binary not found; set WEED_BINARY or ensure it is on PATH")
	}
	baseDir, err := os.MkdirTemp("", "seaweedfs_fuse_p2p_test_")
	require.NoError(t, err)

	c := &p2pTestCluster{
		t:              t,
		baseDir:        baseDir,
		weedBinary:     binary,
		mountPeerPorts: make([]int, numMounts),
		mountPoints:    make([]string, numMounts),
		mountCmds:      make([]*exec.Cmd, numMounts),
	}
	t.Cleanup(c.Stop)

	// master(2) + volume(2) + filer(2) + one peer port per mount.
	// testutil.AllocatePorts holds all listeners open until every port
	// is reserved, avoiding the brief close→bind race that would
	// happen with a per-listener close loop.
	ports, err := testutil.AllocatePorts(6 + numMounts)
	require.NoError(t, err)
	c.masterPort = ports[0]
	c.masterGrpcPort = ports[1]
	c.volumePort = ports[2]
	c.volumeGrpcPort = ports[3]
	c.filerPort = ports[4]
	c.filerGrpcPort = ports[5]
	for i := 0; i < numMounts; i++ {
		c.mountPeerPorts[i] = ports[6+i]
	}

	configDir := filepath.Join(baseDir, "config")
	require.NoError(t, os.MkdirAll(configDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "security.toml"), []byte(""), 0644))

	require.NoError(t, c.startMaster(configDir))
	require.NoError(t, c.waitForTCP(c.masterCmd, "master",
		fmt.Sprintf("127.0.0.1:%d", c.masterPort), 30*time.Second))

	require.NoError(t, c.startVolume(configDir))
	require.NoError(t, c.waitForTCP(c.volumeCmd, "volume",
		fmt.Sprintf("127.0.0.1:%d", c.volumePort), 30*time.Second))

	require.NoError(t, c.startFiler(configDir))
	require.NoError(t, c.waitForTCP(c.filerCmd, "filer",
		fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPort), 30*time.Second))

	for i := 0; i < numMounts; i++ {
		mp := filepath.Join(baseDir, fmt.Sprintf("mount%d", i))
		require.NoError(t, os.MkdirAll(mp, 0755))
		c.mountPoints[i] = mp
		require.NoError(t, c.startMount(i, configDir))
		require.NoError(t, c.waitForMount(mp, 30*time.Second),
			"mount %d not ready\n%s", i, c.tailLog(fmt.Sprintf("mount%d", i)))
	}
	return c
}

func (c *p2pTestCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		for i := len(c.mountCmds) - 1; i >= 0; i-- {
			stopCmd(c.mountCmds[i])
			// Backup unmount in case the FUSE teardown didn't clean up.
			exec.Command("fusermount3", "-u", c.mountPoints[i]).Run()
			exec.Command("fusermount", "-u", c.mountPoints[i]).Run()
		}
		stopCmd(c.filerCmd)
		stopCmd(c.volumeCmd)
		stopCmd(c.masterCmd)

		for _, f := range c.logFiles {
			f.Close()
		}
		c.copyLogsForCI()
		if !c.t.Failed() {
			os.RemoveAll(c.baseDir)
		}
		time.Sleep(2 * time.Second) // let ports drain
	})
}

// MountDir returns the filesystem path of the i-th mount.
func (c *p2pTestCluster) MountDir(i int) string { return c.mountPoints[i] }

// masterAddress / filerAddress return SeaweedFS-style addresses encoding
// both ports as "host:httpPort.grpcPort". Without this, downstream
// components fall back to the grpcPort = httpPort + 10000 default,
// which doesn't match the random port we allocate.
func (c *p2pTestCluster) masterAddress() string {
	return string(pb.NewServerAddress("127.0.0.1", c.masterPort, c.masterGrpcPort))
}

func (c *p2pTestCluster) filerAddress() string {
	return string(pb.NewServerAddress("127.0.0.1", c.filerPort, c.filerGrpcPort))
}

// MountLog returns the contents of mount i's log file.
func (c *p2pTestCluster) MountLog(i int) string {
	return c.tailLogFull(fmt.Sprintf("mount%d", i))
}

func (c *p2pTestCluster) startMaster(configDir string) error {
	c.masterCmd = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"master",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.masterPort),
		"-port.grpc="+strconv.Itoa(c.masterGrpcPort),
		"-mdir="+filepath.Join(c.baseDir, "master"),
	)
	return c.startCmd(c.masterCmd, "master")
}

func (c *p2pTestCluster) startVolume(configDir string) error {
	volDir := filepath.Join(c.baseDir, "volume")
	if err := os.MkdirAll(volDir, 0755); err != nil {
		return fmt.Errorf("create volume dir: %w", err)
	}
	c.volumeCmd = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"volume",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.volumePort),
		"-port.grpc="+strconv.Itoa(c.volumeGrpcPort),
		"-master="+c.masterAddress(),
		"-dir="+volDir,
		"-max=10",
	)
	return c.startCmd(c.volumeCmd, "volume")
}

func (c *p2pTestCluster) startFiler(configDir string) error {
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
		"-defaultStoreDir="+filerDir,
	)
	return c.startCmd(c.filerCmd, "filer")
}

func (c *p2pTestCluster) startMount(idx int, configDir string) error {
	cacheDir := filepath.Join(c.baseDir, fmt.Sprintf("cache%d", idx))
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	c.mountCmds[idx] = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"-v=4",
		"mount",
		"-filer="+c.filerAddress(),
		"-dir="+c.mountPoints[idx],
		"-filer.path=/",
		"-dirAutoCreate",
		"-allowOthers=false",
		"-cacheDir="+cacheDir,
		"-peer.enable=true",
		fmt.Sprintf("-peer.listen=127.0.0.1:%d", c.mountPeerPorts[idx]),
		fmt.Sprintf("-peer.advertise=127.0.0.1:%d", c.mountPeerPorts[idx]),
		"-peer.dataCenter=dc1",
		fmt.Sprintf("-peer.rack=rack%d", idx),
	)
	return c.startCmd(c.mountCmds[idx], fmt.Sprintf("mount%d", idx))
}

func (c *p2pTestCluster) startCmd(cmd *exec.Cmd, name string) error {
	logPath := filepath.Join(c.baseDir, "logs")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	logFile, err := os.Create(filepath.Join(logPath, name+".log"))
	if err != nil {
		return err
	}
	c.logFiles = append(c.logFiles, logFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	return cmd.Start()
}

func (c *p2pTestCluster) tailLog(name string) string {
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

func (c *p2pTestCluster) tailLogFull(name string) string {
	data, err := os.ReadFile(filepath.Join(c.baseDir, "logs", name+".log"))
	if err != nil {
		return ""
	}
	return string(data)
}

func (c *p2pTestCluster) copyLogsForCI() {
	ciLogDir := "/tmp/seaweedfs-fuse-p2p-logs"
	os.MkdirAll(ciLogDir, 0755)
	logsDir := filepath.Join(c.baseDir, "logs")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(logsDir, e.Name()))
		if err != nil {
			continue
		}
		os.WriteFile(filepath.Join(ciLogDir, e.Name()), data, 0644)
	}
}

// waitForTCP polls addr until it accepts a connection, OR the supplied
// subprocess exits — whichever comes first. Short-circuiting on child
// exit turns a 30 s-spin-on-dead-process into an immediate failure with
// the tail of its log. cmd may be nil for callers that don't track a
// process; in that case we fall back to pure polling.
//
// Liveness is checked with signal 0 (POSIX "is this process alive").
// We deliberately do NOT call cmd.Wait() in a goroutine here because
// stopCmd() later calls Wait() at shutdown, and only one Wait per
// process is allowed.
func (c *p2pTestCluster) waitForTCP(cmd *exec.Cmd, name, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		if cmd != nil && cmd.Process != nil {
			// signal 0 doesn't actually send anything; it just asks
			// the kernel whether the process exists. An error here
			// (typically "process already finished" or ESRCH) means
			// the child died before coming up.
			if sigErr := cmd.Process.Signal(syscall.Signal(0)); sigErr != nil {
				return fmt.Errorf("%s exited before listening on %s: %v\n%s",
					name, addr, sigErr, c.tailLog(name))
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout\n%s", addr, c.tailLog(name))
}

// waitForMount waits for a FUSE filesystem to actually be mounted by
// watching the device id flip (FUSE mounts have a different Dev than
// their parent dir).
func (c *p2pTestCluster) waitForMount(mountPoint string, timeout time.Duration) error {
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
		parentSys := parentStat.Sys().(*syscall.Stat_t)
		mountSys := mountStat.Sys().(*syscall.Stat_t)
		if parentSys.Dev != mountSys.Dev {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("mount point %s not ready within timeout (FUSE not detected)", mountPoint)
}

// --- utilities (binary discovery, process shutdown) ---
//
// Port allocation is delegated to testutil.MustAllocatePorts, which
// holds all listeners open until every port is reserved before
// returning them in a batch — safer than the per-listener
// close-then-reserve pattern fuse_dlm originally used.

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

// stopCmd kills a child process with SIGTERM and waits up to 5 s, then
// escalates to SIGKILL. Tolerates nil and already-exited processes.
func stopCmd(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cmd.Process.Signal(syscall.SIGKILL)
		<-done
	}
}
