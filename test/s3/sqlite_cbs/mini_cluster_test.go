package sqlite_cbs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/stretchr/testify/require"
)

const (
	cbsTestRegion    = "us-east-1"
	cbsTestAccessKey = "cbs_access_key1"
	cbsTestSecretKey = "cbs_secret_key1"
)

// cbsMiniCluster runs a single "weed mini" (master + volume + filer + s3) as a
// subprocess. The S3 gateway learns the master addresses via the filer's
// GetFilerConfiguration, so it builds the object-write lock client and
// conditional writes take the routed path — evaluated as WriteCondition
// clauses under the object's write lock on the owner filer, exactly the
// atomicity SQLite CBS relies on for conditional PUT.
type cbsMiniCluster struct {
	t         testing.TB
	baseDir   string
	configDir string
	logsDir   string
	keepLogs  bool

	weedBinary string
	s3Config   string
	s3Endpoint string

	miniCmd     *exec.Cmd
	logFile     *os.File
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

func startCBSMiniCluster(t *testing.T) *cbsMiniCluster {
	t.Helper()

	weedBinary, err := framework.FindOrBuildWeedBinary()
	require.NoError(t, err, "resolve weed binary")

	baseDir, err := os.MkdirTemp("", "seaweedfs_sqlite_cbs_")
	require.NoError(t, err, "create temp directory")

	cluster := &cbsMiniCluster{
		t:          t,
		baseDir:    baseDir,
		configDir:  filepath.Join(baseDir, "config"),
		logsDir:    filepath.Join(baseDir, "logs"),
		keepLogs:   os.Getenv("S3_SQLITE_CBS_KEEP_LOGS") == "1",
		weedBinary: weedBinary,
	}
	t.Cleanup(cluster.Stop)

	require.NoError(t, os.MkdirAll(cluster.configDir, 0o755), "create config dir")
	require.NoError(t, os.MkdirAll(cluster.logsDir, 0o755), "create logs dir")
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "data"), 0o755), "create data dir")

	// Ports must come from MustFreeMiniPorts: each service's gRPC listener is
	// derived as httpPort+10000, and the admin/worker ports must be pinned or
	// concurrent minis collide on the fixed defaults.
	ports := testutil.MustFreeMiniPorts(t, []string{"master", "volume", "filer", "s3", "admin"})
	masterPort, volumePort, filerPort, s3Port, adminPort := ports[0], ports[1], ports[2], ports[3], ports[4]

	cluster.s3Endpoint = fmt.Sprintf("http://127.0.0.1:%d", s3Port)

	require.NoError(t, cluster.writeSecurityConfig(), "write security config")
	require.NoError(t, cluster.writeS3Config(), "write s3 config")

	args := []string{
		"-config_dir=" + cluster.configDir,
		"mini",
		"-dir=" + filepath.Join(baseDir, "data"),
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-master.port=" + strconv.Itoa(masterPort),
		"-master.peers=none",
		"-master.volumeSizeLimitMB=32",
		"-master.telemetry=false",
		"-volume.port=" + strconv.Itoa(volumePort),
		"-volume.max=4",
		"-filer.port=" + strconv.Itoa(filerPort),
		"-s3.port=" + strconv.Itoa(s3Port),
		"-s3.config=" + cluster.s3Config,
		"-s3.iam.readOnly=false",
		"-s3.port.iceberg=0",
		"-s3.port.lance=0",
		"-webdav=false",
		"-admin.ui=false",
	}
	args = append(args, testutil.MiniAdminArgs(adminPort)...)

	logFile, err := cluster.openLog("weed-mini.log")
	require.NoError(t, err, "open mini log")

	cluster.miniCmd = exec.Command(weedBinary, args...)
	cluster.miniCmd.Dir = baseDir
	cluster.miniCmd.Stdout = logFile
	cluster.miniCmd.Stderr = logFile
	require.NoError(t, cluster.miniCmd.Start(), "start weed mini")

	client := cluster.newS3Client(t)
	require.NoError(t, cluster.waitForS3Ready(client, testutil.SeaweedMiniStartupTimeout), "wait for s3\n%s", cluster.tailLog("weed-mini.log"))

	return cluster
}

func (c *cbsMiniCluster) Stop() {
	if c == nil {
		return
	}
	c.cleanupOnce.Do(func() {
		stopProcess(c.miniCmd)

		if c.logFile != nil {
			_ = c.logFile.Close()
		}

		if !c.keepLogs && !c.t.Failed() {
			_ = os.RemoveAll(c.baseDir)
		} else if c.baseDir != "" {
			c.t.Logf("sqlite cbs integration logs kept at %s", c.baseDir)
		}
	})
}

func (c *cbsMiniCluster) writeSecurityConfig() error {
	return os.WriteFile(filepath.Join(c.configDir, "security.toml"), []byte("# generated for sqlite cbs integration tests\n"), 0o644)
}

func (c *cbsMiniCluster) writeS3Config() error {
	configPath := filepath.Join(c.configDir, "s3.json")
	payload := s3IdentityConfig{
		Identities: []s3Identity{
			{
				Name: "cbs-admin",
				Credentials: []s3Credential{
					{
						AccessKey: cbsTestAccessKey,
						SecretKey: cbsTestSecretKey,
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

func (c *cbsMiniCluster) newS3Client(t testing.TB) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cbsTestRegion),
		config.WithRetryMaxAttempts(1),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cbsTestAccessKey,
			cbsTestSecretKey,
			"",
		)),
	)
	require.NoError(t, err, "load aws config")

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.s3Endpoint)
		o.UsePathStyle = true
	})
}

func (c *cbsMiniCluster) waitForS3Ready(client *s3.Client, timeout time.Duration) error {
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
	return fmt.Errorf("timed out waiting for s3 readiness at %s", c.s3Endpoint)
}

func (c *cbsMiniCluster) openLog(name string) (*os.File, error) {
	f, err := os.Create(filepath.Join(c.logsDir, name))
	if err != nil {
		return nil, err
	}
	c.logFile = f
	return f, nil
}

func (c *cbsMiniCluster) tailLog(name string) string {
	f, err := os.Open(filepath.Join(c.logsDir, name))
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	lines := make([]string, 0, 40)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 40 {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
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
