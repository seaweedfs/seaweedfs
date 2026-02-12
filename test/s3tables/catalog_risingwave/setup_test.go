package catalog_risingwave

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
)

var (
	miniProcessMu   sync.Mutex
	lastMiniProcess *exec.Cmd
)

func stopPreviousMini() {
	miniProcessMu.Lock()
	defer miniProcessMu.Unlock()

	if lastMiniProcess != nil && lastMiniProcess.Process != nil {
		_ = lastMiniProcess.Process.Kill()
		_ = lastMiniProcess.Wait()
	}
	lastMiniProcess = nil
}

func registerMiniProcess(cmd *exec.Cmd) {
	miniProcessMu.Lock()
	lastMiniProcess = cmd
	miniProcessMu.Unlock()
}

func clearMiniProcess(cmd *exec.Cmd) {
	miniProcessMu.Lock()
	if lastMiniProcess == cmd {
		lastMiniProcess = nil
	}
	miniProcessMu.Unlock()
}

type TestEnvironment struct {
	t                   *testing.T
	dockerAvailable     bool
	seaweedfsDataDir    string
	masterPort          int
	filerPort           int
	s3Port              int
	icebergRestPort     int
	risingwavePort      int
	bindIP              string
	accessKey           string
	secretKey           string
	risingwaveContainer string
	masterProcess       *exec.Cmd
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	env := &TestEnvironment{
		t:         t,
		accessKey: "AKIAIOSFODNN7EXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	// Check if Docker is available
	cmd := exec.Command("docker", "version")
	env.dockerAvailable = cmd.Run() == nil

	return env
}

func (env *TestEnvironment) hostMasterAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", env.masterPort)
}

func (env *TestEnvironment) hostS3Endpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.s3Port)
}

func (env *TestEnvironment) hostIcebergEndpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.icebergRestPort)
}

func (env *TestEnvironment) dockerS3Endpoint() string {
	return fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
}

func (env *TestEnvironment) dockerIcebergEndpoint() string {
	return fmt.Sprintf("http://host.docker.internal:%d", env.icebergRestPort)
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	stopPreviousMini()

	var err error
	env.seaweedfsDataDir, err = os.MkdirTemp("", "seaweed-risingwave-test-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}

	env.masterPort = mustFreePort(t, "Master")
	env.filerPort = mustFreePort(t, "Filer")
	env.s3Port = mustFreePort(t, "S3")
	env.icebergRestPort = mustFreePort(t, "Iceberg")
	env.risingwavePort = mustFreePort(t, "RisingWave")

	env.bindIP = testutil.FindBindIP()

	iamConfigPath, err := testutil.WriteIAMConfig(env.seaweedfsDataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("failed to create IAM config: %v", err)
	}

	// Create log file for SeaweedFS
	logFile, err := os.Create(filepath.Join(env.seaweedfsDataDir, "seaweedfs.log"))
	if err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	// Start SeaweedFS using weed mini (all-in-one including Iceberg REST)
	env.masterProcess = exec.Command(
		"weed", "mini",
		"-ip", env.bindIP,
		"-ip.bind", "0.0.0.0",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergRestPort),
		"-s3.config", iamConfigPath,
		"-dir", env.seaweedfsDataDir,
	)
	env.masterProcess.Stdout = logFile
	env.masterProcess.Stderr = logFile
	env.masterProcess.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+env.accessKey,
		"AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"ICEBERG_WAREHOUSE=s3://iceberg-tables",
		"S3TABLES_DEFAULT_BUCKET=iceberg-tables",
	)
	if err := env.masterProcess.Start(); err != nil {
		t.Fatalf("failed to start weed mini: %v", err)
	}
	registerMiniProcess(env.masterProcess)

	// Wait for all services to be ready
	if !waitForPort(env.masterPort, 15*time.Second) {
		t.Fatalf("weed mini failed to start - master port %d not listening", env.masterPort)
	}
	if !waitForPort(env.filerPort, 15*time.Second) {
		t.Fatalf("weed mini failed to start - filer port %d not listening", env.filerPort)
	}
	if !waitForPort(env.s3Port, 15*time.Second) {
		t.Fatalf("weed mini failed to start - s3 port %d not listening", env.s3Port)
	}
	if !waitForPort(env.icebergRestPort, 15*time.Second) {
		t.Fatalf("weed mini failed to start - iceberg rest port %d not listening", env.icebergRestPort)
	}
}

func mustFreePort(t *testing.T, name string) int {
	t.Helper()
	// Listen on port 0 to let the OS choose an available ephemeral port.
	// We try multiple times to find a port < 50000 because weed mini adds 10000 for gRPC port,
	// and if the port is > 55535, the gRPC port (port+10000) will be > 65535, which is invalid.
	for i := 0; i < 100; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to find a free port for %s: %v", name, err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		if port < 50000 {
			return port
		}
		// Try again
	}
	t.Fatalf("failed to find a free port < 50000 for %s after 100 attempts", name)
	return 0
}

func waitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (env *TestEnvironment) StartRisingWave(t *testing.T) {
	t.Helper()

	containerName := "seaweed-risingwave-" + randomString(8)
	env.risingwaveContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"-p", fmt.Sprintf("%d:4566", env.risingwavePort),
		"--add-host", "host.docker.internal:host-gateway",
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-east-1",
		"-e", "AWS_S3_PATH_STYLE_ACCESS=true",
		"-e", "AWS_S3_FORCE_PATH_STYLE=true",
		"risingwavelabs/risingwave:v2.5.0",
		"playground",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to start RisingWave container: %v\n%s", err, string(output))
	}

	// Wait for RisingWave port to be open on host
	if !waitForPort(env.risingwavePort, 120*time.Second) {
		t.Fatalf("timed out waiting for RisingWave port %d to be open", env.risingwavePort)
	}

	// Wait for RisingWave to be truly ready via psql in a dedicated postgres client container.
	if !env.waitForRisingWave(120 * time.Second) {
		t.Fatalf("timed out waiting for RisingWave to be ready via psql")
	}
}

func (env *TestEnvironment) waitForRisingWave(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	env.t.Logf(">>> Waiting for RisingWave to be ready (timeout %v)...\n", timeout)
	for time.Now().Before(deadline) {
		if output, err := runPostgresClientSQL(env.risingwaveContainer, "SELECT 1;"); err == nil {
			env.t.Logf(">>> RisingWave is ready.\n")
			return true
		} else {
			env.t.Logf(">>> RisingWave not ready yet: %v (Output: %s)\n", err, string(output))
		}
		time.Sleep(5 * time.Second)
	}
	return false
}

func runPostgresClientSQL(containerName, sql string) ([]byte, error) {
	cmd := exec.Command("docker", "run", "--rm",
		"--network", fmt.Sprintf("container:%s", containerName),
		"postgres:16-alpine",
		"psql",
		"-h", "127.0.0.1",
		"-p", "4566",
		"-U", "root",
		"-d", "dev",
		"-v", "ON_ERROR_STOP=1",
		"-c", sql,
	)
	return cmd.CombinedOutput()
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.risingwaveContainer != "" {
		if t.Failed() {
			logs, err := exec.Command("docker", "logs", env.risingwaveContainer).CombinedOutput()
			if err == nil {
				env.t.Logf(">>> RisingWave Logs:\n%s\n", string(logs))
			} else {
				env.t.Logf(">>> Failed to get RisingWave logs: %v\n", err)
			}
		}
		_ = exec.Command("docker", "rm", "-f", env.risingwaveContainer).Run()
	}

	if env.masterProcess != nil && env.masterProcess.Process != nil {
		_ = env.masterProcess.Process.Kill()
		_ = env.masterProcess.Wait()
	}
	clearMiniProcess(env.masterProcess)

	if env.seaweedfsDataDir != "" {
		if t.Failed() {
			logPath := filepath.Join(env.seaweedfsDataDir, "seaweedfs.log")
			if content, err := os.ReadFile(logPath); err == nil {
				env.t.Logf(">>> SeaweedFS Logs:\n%s\n", string(content))
			}
			env.t.Logf(">>> Filer Contents:\n")
			listFilerContents(t, env, "/")
		}
		_ = os.RemoveAll(env.seaweedfsDataDir)
	}
}

func runRisingWaveSQL(t *testing.T, containerName, sql string) string {
	t.Helper()

	output, err := runPostgresClientSQL(containerName, sql)
	if err != nil {
		t.Fatalf("RisingWave command failed: %v\nSQL: %s\nOutput:\n%s", err, sql, string(output))
	}
	return string(output)
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "weed", "shell",
		fmt.Sprintf("-master=%s", env.hostMasterAddress()),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("s3tables.bucket -create -name %s -account 000000000000\nexit\n", bucketName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to create table bucket %s via weed shell: %v\nOutput: %s", bucketName, err, string(output))
	}
}

func doIcebergSignedJSONRequest(env *TestEnvironment, method, path string, payload any) (int, string, error) {
	url := env.hostIcebergEndpoint() + path

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, "", err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create request: %w", err)
	}

	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Sign the request
	credsProvider := credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")
	creds, err := credsProvider.Retrieve(context.Background())
	if err != nil {
		return 0, "", fmt.Errorf("failed to retrieve credentials: %w", err)
	}
	signer := v4.NewSigner()

	// Calculate payload hash
	var payloadHash string
	if payload != nil {
		data, _ := json.Marshal(payload)
		hash := sha256.Sum256(data)
		payloadHash = hex.EncodeToString(hash[:])
	} else {
		hash := sha256.Sum256([]byte(""))
		payloadHash = hex.EncodeToString(hash[:])
	}

	if err := signer.SignHTTP(context.Background(), creds, req, payloadHash, "s3", "us-east-1", time.Now()); err != nil {
		return 0, "", fmt.Errorf("failed to sign request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read response body: %w", err)
	}

	return resp.StatusCode, string(respBody), nil
}

func createIcebergNamespace(t *testing.T, env *TestEnvironment, bucketName, namespace string) {
	t.Helper()

	status, raw, err := doIcebergSignedJSONRequest(env, "POST", "/v1/namespaces", map[string]any{
		"namespace": []string{namespace},
	})
	if err != nil {
		t.Fatalf("failed to create Iceberg namespace %s in bucket %s: %v", namespace, bucketName, err)
	}
	if status != 200 && status != 409 {
		t.Fatalf("failed to create Iceberg namespace %s in bucket %s: status %d body: %s", namespace, bucketName, status, raw)
	}
}

func createIcebergTable(t *testing.T, env *TestEnvironment, bucketName, namespace, tableName string) {
	t.Helper()

	createPath := fmt.Sprintf("/v1/namespaces/%s/tables", namespace)
	status, raw, err := doIcebergSignedJSONRequest(env, "POST", createPath, map[string]any{
		"name":     tableName,
		"location": fmt.Sprintf("s3://%s/%s/%s", bucketName, namespace, tableName),
		"schema": map[string]any{
			"type": "struct",
			"fields": []map[string]any{
				{"id": 1, "name": "id", "required": false, "type": "int"},
				{"id": 2, "name": "name", "required": false, "type": "string"},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create Iceberg table %s.%s in bucket %s: %v", namespace, tableName, bucketName, err)
	}
	if status != 200 && status != 409 {
		t.Fatalf("failed to create Iceberg table %s.%s in bucket %s: status %d body: %s", namespace, tableName, bucketName, status, raw)
	}
}

func listFilerContents(t *testing.T, env *TestEnvironment, path string) {
	t.Helper()

	cmd := exec.Command("weed", "shell",
		fmt.Sprintf("-master=%s", env.hostMasterAddress()),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("fs.ls -R %s\nexit\n", path))
	output, err := cmd.CombinedOutput()
	if err != nil {
		env.t.Logf(">>> Warning: failed to list filer contents: %v\nOutput: %s\n", err, string(output))
	} else {
		env.t.Logf("%s\n", string(output))
	}
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
