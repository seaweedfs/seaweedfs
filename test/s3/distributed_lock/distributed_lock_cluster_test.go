package distributed_lock

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	distributedLockTestRegion    = "us-east-1"
	distributedLockTestAccessKey = "some_access_key1"
	distributedLockTestSecretKey = "some_secret_key1"
	distributedLockTestGroup     = "distributed-lock-it"
)

type distributedLockCluster struct {
	t         testing.TB
	baseDir   string
	configDir string
	logsDir   string
	keepLogs  bool

	weedBinary string
	filerGroup string
	s3Config   string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPorts     []int
	filerGrpcPorts []int
	s3Ports        []int
	s3GrpcPorts    []int

	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd
	filerCmds []*exec.Cmd
	s3Cmds    []*exec.Cmd
	logFiles  []*os.File

	cleanupOnce sync.Once
}

type s3IdentityConfig struct {
	Identities []s3Identity `json:"identities"`
}

type s3Identity struct {
	Name        string         `json:"name"`
	Credentials []s3Credential `json:"credentials,omitempty"`
	Actions     []string       `json:"actions"`
}

type s3Credential struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

func startDistributedLockCluster(t *testing.T) *distributedLockCluster {
	t.Helper()

	weedBinary, err := framework.FindOrBuildWeedBinary()
	require.NoError(t, err, "resolve weed binary")

	baseDir, err := os.MkdirTemp("", "seaweedfs_s3_distributed_lock_")
	require.NoError(t, err, "create temp directory")

	cluster := &distributedLockCluster{
		t:          t,
		baseDir:    baseDir,
		configDir:  filepath.Join(baseDir, "config"),
		logsDir:    filepath.Join(baseDir, "logs"),
		keepLogs:   os.Getenv("S3_DISTRIBUTED_LOCK_KEEP_LOGS") == "1",
		weedBinary: weedBinary,
		filerGroup: distributedLockTestGroup,
		filerCmds:  make([]*exec.Cmd, 0, 2),
		s3Cmds:     make([]*exec.Cmd, 0, 2),
	}
	t.Cleanup(cluster.Stop)

	dirs := []string{
		cluster.configDir,
		cluster.logsDir,
		filepath.Join(baseDir, "master"),
		filepath.Join(baseDir, "volume"),
		filepath.Join(baseDir, "filer0"),
		filepath.Join(baseDir, "filer1"),
	}
	for _, dir := range dirs {
		require.NoError(t, os.MkdirAll(dir, 0o755), "create %s", dir)
	}

	ports, err := allocatePorts(12)
	require.NoError(t, err, "allocate ports")
	cluster.masterPort = ports[0]
	cluster.masterGrpcPort = ports[1]
	cluster.volumePort = ports[2]
	cluster.volumeGrpcPort = ports[3]
	cluster.filerPorts = []int{ports[4], ports[6]}
	cluster.filerGrpcPorts = []int{ports[5], ports[7]}
	cluster.s3Ports = []int{ports[8], ports[10]}
	cluster.s3GrpcPorts = []int{ports[9], ports[11]}

	require.NoError(t, cluster.writeSecurityConfig(), "write security config")
	require.NoError(t, cluster.writeS3Config(), "write s3 config")

	require.NoError(t, cluster.startMaster(), "start master")
	require.NoError(t, cluster.waitForHTTP("http://"+cluster.masterHTTPAddress()+"/dir/status", 30*time.Second), "wait for master\n%s", cluster.tailLog("master.log"))

	require.NoError(t, cluster.startVolume(), "start volume")
	require.NoError(t, cluster.waitForHTTP("http://"+cluster.volumeHTTPAddress()+"/status", 30*time.Second), "wait for volume\n%s", cluster.tailLog("volume.log"))
	require.NoError(t, cluster.waitForTCP(cluster.volumeGRPCAddress(), 30*time.Second), "wait for volume grpc\n%s", cluster.tailLog("volume.log"))

	for i := 0; i < 2; i++ {
		require.NoError(t, cluster.startFiler(i), "start filer %d", i)
		require.NoError(t, cluster.waitForTCP(cluster.filerGRPCAddress(i), 30*time.Second), "wait for filer %d grpc\n%s", i, cluster.tailLog(fmt.Sprintf("filer%d.log", i)))
	}
	require.NoError(t, cluster.waitForFilerCount(2, 30*time.Second), "wait for filer group registration")
	require.NoError(t, cluster.waitForLockRingConverged(30*time.Second), "wait for lock ring convergence")

	for i := 0; i < 2; i++ {
		require.NoError(t, cluster.startS3(i), "start s3 %d", i)
		client := cluster.newS3Client(t, cluster.s3Endpoint(i))
		require.NoError(t, cluster.waitForS3Ready(client, 30*time.Second), "wait for s3 %d\n%s", i, cluster.tailLog(fmt.Sprintf("s3-%d.log", i)))
	}

	return cluster
}

func (c *distributedLockCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		for i := len(c.s3Cmds) - 1; i >= 0; i-- {
			stopProcess(c.s3Cmds[i])
		}
		for i := len(c.filerCmds) - 1; i >= 0; i-- {
			stopProcess(c.filerCmds[i])
		}
		stopProcess(c.volumeCmd)
		stopProcess(c.masterCmd)

		for _, f := range c.logFiles {
			_ = f.Close()
		}

		if !c.keepLogs && !c.t.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.t.Logf("distributed lock integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *distributedLockCluster) masterHTTPAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterPort))
}

func (c *distributedLockCluster) masterGRPCAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.masterGrpcPort))
}

func (c *distributedLockCluster) volumeHTTPAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumePort))
}

func (c *distributedLockCluster) volumeGRPCAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.volumeGrpcPort))
}

func (c *distributedLockCluster) filerServerAddress(index int) pb.ServerAddress {
	return pb.NewServerAddress("127.0.0.1", c.filerPorts[index], c.filerGrpcPorts[index])
}

func (c *distributedLockCluster) filerGRPCAddress(index int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(c.filerGrpcPorts[index]))
}

func (c *distributedLockCluster) s3Endpoint(index int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.s3Ports[index])
}

func (c *distributedLockCluster) startMaster() error {
	logFile, err := c.openLog("master.log")
	if err != nil {
		return err
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"master",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(c.masterPort),
		"-port.grpc=" + strconv.Itoa(c.masterGrpcPort),
		"-mdir=" + filepath.Join(c.baseDir, "master"),
		"-peers=none",
		"-volumeSizeLimitMB=32",
		"-defaultReplication=000",
	}

	c.masterCmd = exec.Command(c.weedBinary, args...)
	c.masterCmd.Dir = c.baseDir
	c.masterCmd.Stdout = logFile
	c.masterCmd.Stderr = logFile
	return c.masterCmd.Start()
}

func (c *distributedLockCluster) startVolume() error {
	logFile, err := c.openLog("volume.log")
	if err != nil {
		return err
	}

	masterAddress := string(pb.NewServerAddress("127.0.0.1", c.masterPort, c.masterGrpcPort))

	args := []string{
		"-config_dir=" + c.configDir,
		"volume",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(c.volumePort),
		"-port.grpc=" + strconv.Itoa(c.volumeGrpcPort),
		"-dir=" + filepath.Join(c.baseDir, "volume"),
		"-max=16",
		"-master=" + masterAddress,
		"-readMode=proxy",
	}

	c.volumeCmd = exec.Command(c.weedBinary, args...)
	c.volumeCmd.Dir = c.baseDir
	c.volumeCmd.Stdout = logFile
	c.volumeCmd.Stderr = logFile
	return c.volumeCmd.Start()
}

func (c *distributedLockCluster) startFiler(index int) error {
	logFile, err := c.openLog(fmt.Sprintf("filer%d.log", index))
	if err != nil {
		return err
	}

	masterAddress := string(pb.NewServerAddress("127.0.0.1", c.masterPort, c.masterGrpcPort))

	args := []string{
		"-config_dir=" + c.configDir,
		"filer",
		"-master=" + masterAddress,
		"-filerGroup=" + c.filerGroup,
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(c.filerPorts[index]),
		"-port.grpc=" + strconv.Itoa(c.filerGrpcPorts[index]),
		"-defaultStoreDir=" + filepath.Join(c.baseDir, fmt.Sprintf("filer%d", index)),
	}

	cmd := exec.Command(c.weedBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	c.filerCmds = append(c.filerCmds, cmd)
	return nil
}

func (c *distributedLockCluster) startS3(index int) error {
	logFile, err := c.openLog(fmt.Sprintf("s3-%d.log", index))
	if err != nil {
		return err
	}

	filers := []string{string(c.filerServerAddress(0)), string(c.filerServerAddress(1))}
	if index%2 == 1 {
		filers[0], filers[1] = filers[1], filers[0]
	}

	args := []string{
		"-config_dir=" + c.configDir,
		"s3",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(c.s3Ports[index]),
		"-port.grpc=" + strconv.Itoa(c.s3GrpcPorts[index]),
		"-port.iceberg=0",
		"-filer=" + strings.Join(filers, ","),
		"-config=" + c.s3Config,
		"-iam.readOnly=false",
	}

	cmd := exec.Command(c.weedBinary, args...)
	cmd.Dir = c.baseDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	c.s3Cmds = append(c.s3Cmds, cmd)
	return nil
}

func (c *distributedLockCluster) writeSecurityConfig() error {
	return os.WriteFile(filepath.Join(c.configDir, "security.toml"), []byte("# generated for distributed lock integration tests\n"), 0o644)
}

func (c *distributedLockCluster) writeS3Config() error {
	configPath := filepath.Join(c.configDir, "s3.json")
	payload := s3IdentityConfig{
		Identities: []s3Identity{
			{
				Name: "distributed-lock-admin",
				Credentials: []s3Credential{
					{
						AccessKey: distributedLockTestAccessKey,
						SecretKey: distributedLockTestSecretKey,
					},
				},
				Actions: []string{"Admin", "Read", "List", "Tagging", "Write"},
			},
		},
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		return err
	}
	c.s3Config = configPath
	return nil
}

func (c *distributedLockCluster) newS3Client(t testing.TB, endpoint string) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(distributedLockTestRegion),
		config.WithRetryMaxAttempts(1),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			distributedLockTestAccessKey,
			distributedLockTestSecretKey,
			"",
		)),
	)
	require.NoError(t, err, "load aws config")

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func (c *distributedLockCluster) waitForS3Ready(client *s3.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for s3 readiness")
}

func (c *distributedLockCluster) waitForFilerCount(expected int, timeout time.Duration) error {
	conn, err := grpc.NewClient(c.masterGRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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
			FilerGroup: c.filerGroup,
		})
		cancel()
		if err == nil && len(resp.ClusterNodes) >= expected {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d filers in group %q", expected, c.filerGroup)
}

// waitForLockRingConverged verifies that both filers have a consistent view of the
// lock ring by acquiring the same lock through each filer and checking mutual exclusion.
// This guards against the window between master seeing both filers and the filers
// actually receiving the LockRingUpdate broadcast (delayed by the stabilization timer).
func (c *distributedLockCluster) waitForLockRingConverged(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	testKey := fmt.Sprintf("lock-ring-convergence-%d", time.Now().UnixNano())

	for time.Now().Before(deadline) {
		converged, err := c.checkLockMutualExclusion(testKey)
		if err == nil && converged {
			return nil
		}
		// Use a fresh key each attempt to avoid stale lock state
		testKey = fmt.Sprintf("lock-ring-convergence-%d", time.Now().UnixNano())
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("lock ring did not converge: both filers independently grant the same lock")
}

// checkLockMutualExclusion acquires a lock via filer0, then tries the same lock via filer1.
// Returns true if mutual exclusion holds (second attempt is denied).
func (c *distributedLockCluster) checkLockMutualExclusion(testKey string) (bool, error) {
	conn0, err := grpc.NewClient(c.filerGRPCAddress(0), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn0.Close()

	conn1, err := grpc.NewClient(c.filerGRPCAddress(1), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn1.Close()

	client0 := filer_pb.NewSeaweedFilerClient(conn0)
	client1 := filer_pb.NewSeaweedFilerClient(conn1)

	// Acquire lock via filer0
	ctx0, cancel0 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel0()
	resp0, err := client0.DistributedLock(ctx0, &filer_pb.LockRequest{
		Name:          testKey,
		SecondsToLock: 5,
		Owner:         "convergence-filer0",
	})
	if err != nil || resp0.RenewToken == "" {
		return false, fmt.Errorf("filer0 lock failed: err=%v resp=%v", err, resp0)
	}
	defer func() {
		// Always release the lock we acquired
		client0.DistributedUnlock(context.Background(), &filer_pb.UnlockRequest{
			Name:       testKey,
			RenewToken: resp0.RenewToken,
		})
	}()

	// Try the same lock via filer1 - should be denied
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	resp1, err := client1.DistributedLock(ctx1, &filer_pb.LockRequest{
		Name:          testKey,
		SecondsToLock: 5,
		Owner:         "convergence-filer1",
	})
	if err != nil {
		return false, err
	}
	if resp1.RenewToken != "" {
		// Both filers granted the lock - ring not converged. Release the second lock.
		client1.DistributedUnlock(context.Background(), &filer_pb.UnlockRequest{
			Name:       testKey,
			RenewToken: resp1.RenewToken,
		})
		return false, nil
	}
	// Verify the denial is specifically because the lock is already held,
	// not due to a transient error that might give a false positive.
	if !strings.Contains(resp1.Error, "lock already owned") {
		return false, nil
	}
	return true, nil
}

func (c *distributedLockCluster) waitForHTTP(url string, timeout time.Duration) error {
	client := &net.Dialer{Timeout: time.Second}
	httpClient := &httpClientWithDialer{dialer: client}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := httpClient.Get(url); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for http %s", url)
}

func (c *distributedLockCluster) waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for tcp %s", addr)
}

func (c *distributedLockCluster) openLog(name string) (*os.File, error) {
	f, err := os.Create(filepath.Join(c.logsDir, name))
	if err != nil {
		return nil, err
	}
	c.logFiles = append(c.logFiles, f)
	return f, nil
}

func (c *distributedLockCluster) tailLog(name string) string {
	f, err := os.Open(filepath.Join(c.logsDir, name))
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lines := make([]string, 0, 40)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 40 {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}

func allocatePorts(count int) ([]int, error) {
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, openListener := range listeners {
				_ = openListener.Close()
			}
			return nil, err
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports, nil
}

func stopProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

type httpClientWithDialer struct {
	dialer *net.Dialer
}

func (h *httpClientWithDialer) Get(url string) error {
	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DialContext: h.dialer.DialContext,
		},
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}
