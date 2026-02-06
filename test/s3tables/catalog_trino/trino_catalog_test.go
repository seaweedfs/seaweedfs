package catalog_trino

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type TestEnvironment struct {
	seaweedDir      string
	weedBinary      string
	dataDir         string
	bindIP          string
	s3Port          int
	s3GrpcPort      int
	icebergPort     int
	masterPort      int
	masterGrpcPort  int
	filerPort       int
	filerGrpcPort   int
	volumePort      int
	volumeGrpcPort  int
	weedProcess     *exec.Cmd
	weedCancel      context.CancelFunc
	trinoContainer  string
	dockerAvailable bool
	accessKey       string
	secretKey       string
}

func TestTrinoIcebergCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	env.StartSeaweedFS(t)

	catalogBucket := "default"
	createTableBucket(t, env, catalogBucket)
	createObjectBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	schemaName := "trino_" + randomString(6)

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, schemaName) {
		t.Fatalf("Expected schema %s in output:\n%s", schemaName, output)
	}
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SHOW TABLES FROM iceberg.%s", schemaName))
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	seaweedDir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if _, err := os.Stat(weedBinary); os.IsNotExist(err) {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-trino-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := findBindIP()

	masterPort, masterGrpcPort := mustFreePortPair(t, "Master")
	volumePort, volumeGrpcPort := mustFreePortPair(t, "Volume")
	filerPort, filerGrpcPort := mustFreePortPair(t, "Filer")
	s3Port, s3GrpcPort := mustFreePortPair(t, "S3")
	icebergPort := mustFreePort(t, "Iceberg")

	return &TestEnvironment{
		seaweedDir:      seaweedDir,
		weedBinary:      weedBinary,
		dataDir:         dataDir,
		bindIP:          bindIP,
		s3Port:          s3Port,
		s3GrpcPort:      s3GrpcPort,
		icebergPort:     icebergPort,
		masterPort:      masterPort,
		masterGrpcPort:  masterGrpcPort,
		filerPort:       filerPort,
		filerGrpcPort:   filerGrpcPort,
		volumePort:      volumePort,
		volumeGrpcPort:  volumeGrpcPort,
		dockerAvailable: hasDocker(),
		accessKey:       "AKIAIOSFODNN7EXAMPLE",
		secretKey:       "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	securityToml := filepath.Join(env.dataDir, "security.toml")
	if err := os.WriteFile(securityToml, []byte("# Empty security config for testing\n"), 0644); err != nil {
		t.Fatalf("Failed to create security.toml: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	cmd := exec.CommandContext(ctx, env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergPort),
		"-s3.iam.readOnly=false",
		"-ip", env.bindIP,
		"-ip.bind", env.bindIP,
		"-dir", env.dataDir,
	)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	// Set AWS credentials in environment
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+env.accessKey,
		"AWS_SECRET_ACCESS_KEY="+env.secretKey,
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start SeaweedFS: %v", err)
	}
	env.weedProcess = cmd

	// Try to check if Iceberg API is ready
	// First try checking the /v1/config endpoint (requires auth, so will return 401 if server is up)
	icebergURL := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	if !env.waitForService(icebergURL, 30*time.Second) {
		// Try to get more info about why it failed
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(icebergURL)
		if err != nil {
			t.Logf("WARNING: Could not connect to Iceberg service at %s: %v", icebergURL, err)
		} else {
			t.Logf("WARNING: Iceberg service returned status %d at %s", resp.StatusCode, icebergURL)
			resp.Body.Close()
		}
		t.Fatalf("Iceberg REST API did not become ready")
	}
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.trinoContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.trinoContainer).Run()
	}

	if env.weedCancel != nil {
		env.weedCancel()
	}

	if env.weedProcess != nil {
		time.Sleep(2 * time.Second)
		_ = env.weedProcess.Wait()
	}

	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

func (env *TestEnvironment) waitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			// Service not responding yet
			time.Sleep(500 * time.Millisecond)
			continue
		}
		statusCode := resp.StatusCode
		resp.Body.Close()
		// Accept 2xx status codes (successful responses)
		if statusCode >= 200 && statusCode < 300 {
			return true
		}
		// Also accept 401/403 (auth errors mean service is up, just needs credentials)
		if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
			return true
		}
		// For other status codes, keep trying
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func (env *TestEnvironment) writeTrinoConfig(t *testing.T, warehouseBucket string) string {
	t.Helper()

	configDir := filepath.Join(env.dataDir, "trino")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create Trino config dir: %v", err)
	}

	config := fmt.Sprintf(`connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://%s:%d
iceberg.rest-catalog.warehouse=s3://%s/
iceberg.file-format=PARQUET
fs.native-s3.enabled=true
s3.endpoint=http://%s:%d
s3.path-style-access=true
s3.aws-access-key=%s
s3.aws-secret-key=%s
s3.region=us-west-2
`, env.bindIP, env.icebergPort, warehouseBucket, env.bindIP, env.s3Port, env.accessKey, env.secretKey)

	if err := os.WriteFile(filepath.Join(configDir, "iceberg.properties"), []byte(config), 0644); err != nil {
		t.Fatalf("Failed to write Trino config: %v", err)
	}

	return configDir
}

func (env *TestEnvironment) startTrinoContainer(t *testing.T, configDir string) {
	t.Helper()

	containerName := "seaweed-trino-" + randomString(8)
	env.trinoContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/etc/trino/catalog", configDir),
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-west-2",
		"trinodb/trino",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Trino container: %v\n%s", err, string(output))
	}
}

func waitForTrino(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastOutput []byte
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", containerName,
			"trino", "--catalog", "system", "--schema", "runtime",
			"--execute", "SELECT 1",
		)
		if output, err := cmd.CombinedOutput(); err == nil {
			return
		} else {
			lastOutput = output
			outputStr := string(output)
			if strings.Contains(outputStr, "No such container") ||
				strings.Contains(outputStr, "is not running") {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
	t.Fatalf("Timed out waiting for Trino to be ready\nLast output:\n%s\nTrino logs:\n%s", string(lastOutput), string(logs))
}

func runTrinoSQL(t *testing.T, containerName, sql string) string {
	t.Helper()

	cmd := exec.Command("docker", "exec", containerName,
		"trino", "--catalog", "system", "--schema", "runtime",
		"--output-format", "CSV",
		"--execute", sql,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Trino command failed: %v\nSQL: %s\nOutput:\n%s", err, sql, string(output))
	}
	return string(output)
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	endpoint := fmt.Sprintf("http://%s:%d/buckets", env.bindIP, env.s3Port)
	reqBody := fmt.Sprintf(`{"name":"%s"}`, bucketName)
	req, err := http.NewRequest(http.MethodPut, endpoint, strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create table bucket %s: %v", bucketName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to create table bucket %s, status %d: %s", bucketName, resp.StatusCode, body)
	}
}

func createObjectBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	endpoint := fmt.Sprintf("http://%s:%d/%s", env.bindIP, env.s3Port, bucketName)
	req, err := http.NewRequest(http.MethodPut, endpoint, nil)
	if err != nil {
		t.Fatalf("Failed to create S3 bucket request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create S3 bucket %s: %v", bucketName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to create S3 bucket %s, status %d: %s", bucketName, resp.StatusCode, body)
	}
}

func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

func mustFreePort(t *testing.T, name string) int {
	t.Helper()

	port, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for %s: %v", name, err)
	}
	return port
}

func mustFreePortPair(t *testing.T, name string) (int, int) {
	t.Helper()

	httpPort, grpcPort, err := findAvailablePortPair()
	if err != nil {
		t.Fatalf("Failed to get free port pair for %s: %v", name, err)
	}
	return httpPort, grpcPort
}

func findAvailablePortPair() (int, int, error) {
	httpPort, err := getFreePort()
	if err != nil {
		return 0, 0, err
	}
	grpcPort, err := getFreePort()
	if err != nil {
		return 0, 0, err
	}
	return httpPort, grpcPort, nil
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func findBindIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}
		ip := ipNet.IP.To4()
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		return ip.String()
	}
	return "127.0.0.1"
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate random string: " + err.Error())
	}
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b)
}
