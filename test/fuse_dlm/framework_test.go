package fuse_dlm

import (
	"context"
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

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const filerGroup = "fuse-dlm-test"

// dlmTestCluster manages a full SeaweedFS cluster with 2 filers and 2 FUSE
// mounts for testing DLM-based cross-mount write coordination.
type dlmTestCluster struct {
	t          testing.TB
	baseDir    string
	weedBinary string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPorts     [2]int
	filerGrpcPorts [2]int
	mountPoints    [2]string

	masterCmd  *exec.Cmd
	volumeCmd  *exec.Cmd
	filerCmds  [2]*exec.Cmd
	mountCmds  [2]*exec.Cmd
	logFiles   []*os.File

	cleanupOnce sync.Once
}

func startDLMTestCluster(t testing.TB) *dlmTestCluster {
	binary := findWeedBinary()
	if binary == "" {
		t.Skip("weed binary not found; set WEED_BINARY or ensure it is on PATH")
	}

	baseDir, err := os.MkdirTemp("", "seaweedfs_fuse_dlm_test_")
	require.NoError(t, err)

	c := &dlmTestCluster{
		t:          t,
		baseDir:    baseDir,
		weedBinary: binary,
	}
	// Register cleanup early so processes are stopped even if a require fails below.
	t.Cleanup(c.Stop)

	// Allocate ports: master(2) + volume(2) + filer0(2) + filer1(2) = 8
	ports := allocatePorts(t, 8)
	c.masterPort = ports[0]
	c.masterGrpcPort = ports[1]
	c.volumePort = ports[2]
	c.volumeGrpcPort = ports[3]
	c.filerPorts[0] = ports[4]
	c.filerGrpcPorts[0] = ports[5]
	c.filerPorts[1] = ports[6]
	c.filerGrpcPorts[1] = ports[7]

	// Write empty security.toml
	configDir := filepath.Join(baseDir, "config")
	require.NoError(t, os.MkdirAll(configDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "security.toml"), []byte(""), 0644))

	// Start master
	require.NoError(t, c.startMaster(configDir))
	require.NoError(t, c.waitForTCP(fmt.Sprintf("127.0.0.1:%d", c.masterPort), 30*time.Second),
		"master not ready\n%s", c.tailLog("master"))

	// Start volume
	require.NoError(t, c.startVolume(configDir))
	require.NoError(t, c.waitForTCP(fmt.Sprintf("127.0.0.1:%d", c.volumePort), 30*time.Second),
		"volume not ready\n%s", c.tailLog("volume"))

	// Start 2 filers
	for i := 0; i < 2; i++ {
		require.NoError(t, c.startFiler(i, configDir))
		require.NoError(t, c.waitForTCP(fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPorts[i]), 30*time.Second),
			"filer %d not ready\n%s", i, c.tailLog(fmt.Sprintf("filer%d", i)))
	}
	require.NoError(t, c.waitForFilerCount(2, 30*time.Second), "filer group registration")
	require.NoError(t, c.waitForLockRingConverged(30*time.Second), "lock ring convergence")

	// Start 2 mounts, both pointing at filer0 for metadata consistency.
	// (filer1 exists for the DLM lock ring but both mounts share filer0's
	// metadata store since leveldb is per-filer.)
	for i := 0; i < 2; i++ {
		mp := filepath.Join(baseDir, fmt.Sprintf("mount%d", i))
		require.NoError(t, os.MkdirAll(mp, 0755))
		c.mountPoints[i] = mp
		require.NoError(t, c.startMount(i, configDir))
		require.NoError(t, c.waitForMount(mp, 30*time.Second),
			"mount %d not ready\n%s", i, c.tailLog(fmt.Sprintf("mount%d", i)))
	}

	return c
}

func (c *dlmTestCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		// Stop mounts first (triggers flush + DLM unlock).
		// Use stopCmd for bounded wait to avoid hanging on wedged FUSE processes.
		for i := 1; i >= 0; i-- {
			stopCmd(c.mountCmds[i])
			// Backup unmount in case FUSE teardown didn't clean up
			exec.Command("fusermount3", "-u", c.mountPoints[i]).Run()
			exec.Command("fusermount", "-u", c.mountPoints[i]).Run()
		}
		// Stop filers, volume, master
		for i := 1; i >= 0; i-- {
			stopCmd(c.filerCmds[i])
		}
		stopCmd(c.volumeCmd)
		stopCmd(c.masterCmd)

		for _, f := range c.logFiles {
			f.Close()
		}

		// Copy logs for CI
		c.copyLogsForCI()

		if !c.t.Failed() {
			os.RemoveAll(c.baseDir)
		}

		// Wait for ports to be fully released before the next test
		// allocates new ports (avoids TIME_WAIT collisions).
		time.Sleep(2 * time.Second)
	})
}

// masterAddress returns the master address in the format that encodes both
// HTTP and gRPC ports: "host:httpPort.grpcPort". This is the format that
// SeaweedFS uses to communicate non-default gRPC ports between components.
func (c *dlmTestCluster) masterAddress() string {
	return string(pb.NewServerAddress("127.0.0.1", c.masterPort, c.masterGrpcPort))
}

func (c *dlmTestCluster) startMaster(configDir string) error {
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

func (c *dlmTestCluster) startVolume(configDir string) error {
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

func (c *dlmTestCluster) startFiler(idx int, configDir string) error {
	filerDir := filepath.Join(c.baseDir, fmt.Sprintf("filer%d", idx))
	if err := os.MkdirAll(filerDir, 0755); err != nil {
		return fmt.Errorf("create filer dir: %w", err)
	}
	c.filerCmds[idx] = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"filer",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port="+strconv.Itoa(c.filerPorts[idx]),
		"-port.grpc="+strconv.Itoa(c.filerGrpcPorts[idx]),
		"-master="+c.masterAddress(),
		"-filerGroup="+filerGroup,
		"-defaultStoreDir="+filerDir,
	)
	return c.startCmd(c.filerCmds[idx], fmt.Sprintf("filer%d", idx))
}

func (c *dlmTestCluster) filerAddress(idx int) string {
	return string(pb.NewServerAddress("127.0.0.1", c.filerPorts[idx], c.filerGrpcPorts[idx]))
}

func (c *dlmTestCluster) startMount(idx int, configDir string) error {
	cacheDir := filepath.Join(c.baseDir, fmt.Sprintf("cache%d", idx))
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	c.mountCmds[idx] = exec.Command(c.weedBinary,
		"-logdir="+filepath.Join(c.baseDir, "logs"),
		"mount",
		"-filer="+c.filerAddress(0), // both mounts use filer0 for shared metadata
		"-dir="+c.mountPoints[idx],
		"-filer.path=/",
		"-dirAutoCreate",
		"-allowOthers=false",
		"-cacheDir="+cacheDir,
		"-dlm",
	)
	return c.startCmd(c.mountCmds[idx], fmt.Sprintf("mount%d", idx))
}

func (c *dlmTestCluster) startCmd(cmd *exec.Cmd, name string) error {
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

func (c *dlmTestCluster) tailLog(name string) string {
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

func (c *dlmTestCluster) copyLogsForCI() {
	ciLogDir := "/tmp/seaweedfs-fuse-dlm-logs"
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

func (c *dlmTestCluster) waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout", addr)
}

// waitForMount waits for a FUSE filesystem to actually be mounted at
// mountPoint by comparing the device ID of the mount point against its parent.
// A plain directory (pre-created before mount) has the same device as its
// parent; a mounted FUSE filesystem has a different device.
func (c *dlmTestCluster) waitForMount(mountPoint string, timeout time.Duration) error {
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
			// Different device = FUSE mounted
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("mount point %s not ready within timeout (FUSE not detected)", mountPoint)
}

func (c *dlmTestCluster) filerGRPCAddress(idx int) string {
	return fmt.Sprintf("127.0.0.1:%d", c.filerGrpcPorts[idx])
}

func (c *dlmTestCluster) waitForFilerCount(expected int, timeout time.Duration) error {
	addr := fmt.Sprintf("127.0.0.1:%d", c.masterGrpcPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: "filer",
			FilerGroup: filerGroup,
		})
		cancel()
		if err == nil && len(resp.ClusterNodes) >= expected {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d filers in group %q", expected, filerGroup)
}

// waitForLockRingConverged verifies that both filers have a consistent view of
// the lock ring by acquiring the same lock through each filer and checking
// mutual exclusion. Adapted from test/s3/distributed_lock/.
func (c *dlmTestCluster) waitForLockRingConverged(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	owners := []pb.ServerAddress{
		pb.ServerAddress(c.filerGRPCAddress(0)),
		pb.ServerAddress(c.filerGRPCAddress(1)),
	}
	ring := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	ring.SetServers(owners)

	attempt := 0
	for time.Now().Before(deadline) {
		testKeys := convergenceKeysPerPrimary(ring, owners, attempt)
		attempt++

		allConverged := true
		for _, key := range testKeys {
			converged, _ := c.checkLockMutualExclusion(key)
			if !converged {
				allConverged = false
				break
			}
		}
		if allConverged {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("lock ring did not converge")
}

// convergenceKeysPerPrimary generates one test key per primary filer.
func convergenceKeysPerPrimary(ring *lock_manager.HashRing, owners []pb.ServerAddress, attempt int) []string {
	found := make(map[pb.ServerAddress]bool)
	var keys []string
	for i := 0; len(found) < len(owners) && i < 10000; i++ {
		key := fmt.Sprintf("convergence-test-%d-%d", attempt, i)
		primary, _ := ring.GetPrimaryAndBackup(key)
		if !found[primary] {
			found[primary] = true
			keys = append(keys, key)
		}
	}
	return keys
}

func (c *dlmTestCluster) checkLockMutualExclusion(key string) (bool, error) {
	// Try to lock via filer0
	conn0, err := grpc.NewClient(c.filerGRPCAddress(0), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn0.Close()

	client0 := filer_pb.NewSeaweedFilerClient(conn0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp0, err := client0.DistributedLock(ctx, &filer_pb.LockRequest{
		Name:          key,
		SecondsToLock: 5,
		Owner:         "convergence-test-0",
	})
	if err != nil {
		return false, err
	}
	if resp0.Error != "" {
		return false, fmt.Errorf("lock0: %s", resp0.Error)
	}

	// Try to lock via filer1 — should fail (already locked)
	conn1, err := grpc.NewClient(c.filerGRPCAddress(1), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn1.Close()

	client1 := filer_pb.NewSeaweedFilerClient(conn1)
	resp1, err := client1.DistributedLock(ctx, &filer_pb.LockRequest{
		Name:          key,
		SecondsToLock: 5,
		Owner:         "convergence-test-1",
	})

	// Unlock via filer0
	client0.DistributedUnlock(ctx, &filer_pb.UnlockRequest{
		Name:       key,
		RenewToken: resp0.RenewToken,
	})

	if err != nil {
		return false, err
	}
	// If filer1 also got the lock, the ring hasn't converged
	if resp1.Error == "" {
		// Unlock the second one too
		client1.DistributedUnlock(ctx, &filer_pb.UnlockRequest{
			Name:       key,
			RenewToken: resp1.RenewToken,
		})
		return false, nil
	}

	return true, nil
}

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
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		<-done
	}
}

func allocatePorts(t testing.TB, n int) []int {
	t.Helper()
	// Hold all listeners open until all ports are collected, then close
	// them together. This prevents the OS from reassigning a just-freed
	// port to the next Listen call within the same allocation batch.
	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports
}

func findWeedBinary() string {
	if p := os.Getenv("WEED_BINARY"); p != "" {
		return p
	}
	if p, err := exec.LookPath("weed"); err == nil {
		return p
	}
	candidates := []string{
		"../../weed/weed",
		"./weed",
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}
	return ""
}
