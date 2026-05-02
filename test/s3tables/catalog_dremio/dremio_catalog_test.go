package catalog_dremio

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

	"github.com/seaweedfs/seaweedfs/test/testutil"
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
	dremioContainer string
	dockerAvailable bool
	accessKey       string
	secretKey       string
}

// TestDremioIcebergCatalog tests basic Dremio catalog connectivity and schema operations.
func TestDremioIcebergCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Dremio integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	tableBucket := "iceberg-tables"
	catalogBucket := tableBucket
	fmt.Printf(">>> Creating table bucket: %s\n", tableBucket)
	createTableBucket(t, env, tableBucket)
	fmt.Printf(">>> All buckets created.\n")

	testIcebergRestAPI(t, env)

	configDir := env.writeDremioConfig(t, catalogBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 120*time.Second)

	schemaName := "dremio_" + randomString(6)

	runDremioSQL(t, env.dremioContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	output := runDremioSQL(t, env.dremioContainer, "SHOW SCHEMAS")
	if !strings.Contains(output, schemaName) {
		t.Fatalf("Expected schema %s in output:\n%s", schemaName, output)
	}
	runDremioSQL(t, env.dremioContainer, fmt.Sprintf("SHOW TABLES IN %s", schemaName))
}

// TestDremioTableOperations tests table creation, insertion, and querying with Dremio.
func TestDremioTableOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Dremio integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)

	configDir := env.writeDremioConfig(t, tableBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 120*time.Second)

	schemaName := "test_schema_" + randomString(4)
	tableName := "test_table_" + randomString(4)

	t.Logf(">>> Creating schema: %s", schemaName)
	runDremioSQL(t, env.dremioContainer, fmt.Sprintf("CREATE SCHEMA %s", schemaName))

	t.Logf(">>> Creating table: %s.%s", schemaName, tableName)
	createSQL := fmt.Sprintf(`CREATE TABLE %s.%s (
		id INTEGER,
		name VARCHAR,
		timestamp TIMESTAMP
	) AS SELECT 1, 'test', CURRENT_TIMESTAMP WHERE FALSE`, schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, createSQL)

	t.Logf(">>> Inserting data into table")
	runDremioSQL(t, env.dremioContainer, fmt.Sprintf(`INSERT INTO %s.%s VALUES
		(1, 'alice', CURRENT_TIMESTAMP),
		(2, 'bob', CURRENT_TIMESTAMP)
	`, schemaName, tableName))

	t.Logf(">>> Querying data from table")
	output := runDremioSQL(t, env.dremioContainer, fmt.Sprintf(
		"SELECT COUNT(*) as count FROM %s.%s", schemaName, tableName))
	if !strings.Contains(output, "2") {
		t.Fatalf("Expected row count 2 in output:\n%s", output)
	}

	t.Logf(">>> TestDremioTableOperations PASSED")
}

// NewTestEnvironment creates a new test environment with allocated ports and configuration.
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
	info, err := os.Stat(weedBinary)
	if err != nil || info.IsDir() {
		weedBinary = filepath.Join(seaweedDir, "weed", "weed", "weed")
		info, err = os.Stat(weedBinary)
		if err != nil || info.IsDir() {
			weedBinary = "weed"
			if _, err := exec.LookPath(weedBinary); err != nil {
				t.Skip("weed binary not found, skipping integration test")
			}
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-dremio-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()

	ports := testutil.MustAllocatePorts(t, 9)

	env := &TestEnvironment{
		seaweedDir:     seaweedDir,
		weedBinary:     weedBinary,
		dataDir:        dataDir,
		bindIP:         bindIP,
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		s3Port:         ports[6],
		s3GrpcPort:     ports[7],
		icebergPort:    ports[8],
	}

	env.dockerAvailable = hasDocker()
	env.accessKey = "AKIAIOSFODNN7EXAMPLE"
	env.secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	return env
}

// StartSeaweedFS starts a SeaweedFS mini instance with all necessary services.
func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	iamConfigPath, err := testutil.WriteIAMConfig(env.dataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("Failed to create IAM config: %v", err)
	}

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
		"-s3.config", iamConfigPath,
		"-ip", env.bindIP,
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+env.accessKey,
		"AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"ICEBERG_WAREHOUSE=s3://iceberg-tables",
		"S3TABLES_DEFAULT_BUCKET=iceberg-tables",
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start SeaweedFS: %v", err)
	}
	env.weedProcess = cmd

	icebergURL := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	if !env.waitForService(icebergURL, 30*time.Second) {
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

// Cleanup stops all processes and removes temporary resources.
func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.dremioContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.dremioContainer).Run()
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

// waitForService polls a URL until it responds with a success status or timeout is reached.
func (env *TestEnvironment) waitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		statusCode := resp.StatusCode
		resp.Body.Close()
		if statusCode >= 200 && statusCode < 300 {
			return true
		}
		if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// testIcebergRestAPI verifies that the Iceberg REST API endpoint is responding.
func testIcebergRestAPI(t *testing.T, env *TestEnvironment) {
	t.Helper()
	fmt.Printf(">>> Testing Iceberg REST API directly...\n")

	addr := net.JoinHostPort(env.bindIP, fmt.Sprintf("%d", env.icebergPort))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Cannot connect to Iceberg service at %s: %v", addr, err)
	}
	conn.Close()
	t.Logf("Successfully connected to Iceberg service at %s", addr)

	url := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	t.Logf("Testing Iceberg REST API at %s", url)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to connect to Iceberg REST API at %s: %v", url, err)
	}
	defer resp.Body.Close()

	t.Logf("Iceberg REST API response status: %d", resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	t.Logf("Iceberg REST API response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK from /v1/config, got %d", resp.StatusCode)
	}
}

// writeDremioConfig creates a Dremio configuration file with Iceberg catalog settings.
func (env *TestEnvironment) writeDremioConfig(t *testing.T, warehouseBucket string) string {
	t.Helper()

	configDir := filepath.Join(env.dataDir, "dremio")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create Dremio config dir: %v", err)
	}

	config := fmt.Sprintf(`
{
  "catalog": {
    "iceberg": {
      "type": "rest",
      "uri": "http://host.docker.internal:%d",
      "warehouse": "s3://%s",
      "s3": {
        "endpoint": "http://host.docker.internal:%d",
        "path-style-access": true,
        "access-key": "%s",
        "secret-key": "%s",
        "region": "us-west-2"
      }
    }
  }
}
`, env.icebergPort, warehouseBucket, env.s3Port, env.accessKey, env.secretKey)

	if err := os.WriteFile(filepath.Join(configDir, "dremio.conf"), []byte(config), 0644); err != nil {
		t.Fatalf("Failed to write Dremio config: %v", err)
	}

	return configDir
}

// startDremioContainer starts a Dremio Docker container with the given configuration.
func (env *TestEnvironment) startDremioContainer(t *testing.T, configDir string) {
	t.Helper()

	containerName := "seaweed-dremio-" + randomString(8)
	env.dremioContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/opt/dremio/conf", configDir),
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-west-2",
		"-p", "9047:9047",
		"dremio/dremio:latest",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Dremio container: %v\n%s", err, string(output))
	}
}

// waitForDremio waits for Dremio container to be ready by polling its health endpoint.
func waitForDremio(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastOutput []byte
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", containerName,
			"curl", "-s", "http://localhost:9047/api/v2/ping",
		)
		if output, err := cmd.CombinedOutput(); err == nil {
			if strings.Contains(string(output), "pong") || strings.Contains(string(output), "\"ok\"") {
				return
			}
		} else {
			lastOutput = output
			outputStr := string(output)
			if strings.Contains(outputStr, "No such container") ||
				strings.Contains(outputStr, "is not running") {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	cmd := exec.Command("docker", "exec", containerName, "curl", "-I", "http://localhost:9047")
	if err := cmd.Run(); err == nil {
		time.Sleep(5 * time.Second)
		return
	}

	t.Fatalf("Timed out waiting for Dremio to be ready\nLast output:\n%s", string(lastOutput))
}

// runDremioSQL executes a SQL statement in Dremio and returns the output.
func runDremioSQL(t *testing.T, containerName, sql string) string {
	t.Helper()

	cmd := exec.Command("docker", "exec", containerName,
		"/bin/sh", "-c",
		fmt.Sprintf(`curl -s -X POST http://localhost:9047/api/v3/sql \
			-H "Content-Type: application/json" \
			-d '{"sql": %q}' | jq -r '.rows[][]? // empty'`, sql),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Dremio command failed: %v\nSQL: %s\nOutput:\n%s", err, sql, string(output))
		return string(output)
	}
	return strings.TrimSpace(string(output))
}

// createTableBucket creates an S3 table bucket using weed shell command.
func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	cmd := exec.Command(env.weedBinary, "shell",
		fmt.Sprintf("-master=%s:%d.%d", env.bindIP, env.masterPort, env.masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("s3tables.bucket -create -name %s -account 000000000000\nexit\n", bucketName))
	fmt.Printf(">>> EXECUTING: %v\n", cmd.Args)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf(">>> ERROR Output: %s\n", string(output))
		t.Fatalf("Failed to create table bucket %s via weed shell: %v\nOutput: %s", bucketName, err, string(output))
	}
	fmt.Printf(">>> SUCCESS: Created table bucket %s\n", bucketName)

	t.Logf("Created table bucket: %s", bucketName)
}

// hasDocker checks if Docker is available in the system.
func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// randomString generates a random string of the specified length.
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
