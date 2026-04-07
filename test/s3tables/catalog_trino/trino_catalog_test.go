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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
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
	closers         []io.Closer
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

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	tableBucket := "iceberg-tables"
	catalogBucket := tableBucket
	fmt.Printf(">>> Creating table bucket: %s\n", tableBucket)
	createTableBucket(t, env, tableBucket)
	fmt.Printf(">>> All buckets created.\n")

	// Test Iceberg REST API directly
	testIcebergRestAPI(t, env)

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

// TestTrinoMultiLevelNamespace tests that multi-level namespaces (dot-separated)
// produce correct S3 paths so Trino can read back data it writes.
// Regression test for https://github.com/seaweedfs/seaweedfs/issues/8959
func TestTrinoMultiLevelNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)

	configDir := env.writeTrinoConfig(t, tableBucket, withNestedNamespace())
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	// Use a two-level namespace: "analytics.daily"
	nsLevel1 := "analytics_" + randomString(4)
	nsLevel2 := "daily_" + randomString(4)
	flatNs := fmt.Sprintf("%s.%s", nsLevel1, nsLevel2)
	// Trino uses double-quoted schema names for multi-level namespaces
	multiNs := fmt.Sprintf(`"%s"`, flatNs)
	tableName := "events_" + randomString(4)

	// Create multi-level namespace (schema)
	t.Logf(">>> Creating multi-level schema: %s", flatNs)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", multiNs))

	// Verify the schema shows up
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, flatNs) {
		t.Fatalf("Expected schema %s in output:\n%s", flatNs, output)
	}

	// Create table with explicit location to avoid non-empty location conflict.
	// The location uses the dot-separated namespace — if #8959 regresses
	// (unit separator instead of dot), data would be written to the wrong path.
	tableLocation := fmt.Sprintf("s3://%s/%s/%s_%s", tableBucket, flatNs, tableName, randomString(6))
	t.Logf(">>> Creating table at location: %s", tableLocation)
	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS iceberg.%s.%s (
		id INTEGER,
		event VARCHAR,
		ts TIMESTAMP(6)
	) WITH (
		format = 'PARQUET',
		location = '%s'
	)`, multiNs, tableName, tableLocation)
	runTrinoSQLAllowExists(t, env.trinoContainer, createSQL)

	// Insert data
	t.Logf(">>> Inserting data into multi-level namespace table")
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(`
		INSERT INTO iceberg.%s.%s VALUES
			(1, 'click', TIMESTAMP '2025-01-01 00:00:00'),
			(2, 'view',  TIMESTAMP '2025-01-01 01:00:00'),
			(3, 'click', TIMESTAMP '2025-01-02 00:00:00')
	`, multiNs, tableName))

	// Query data back — if the namespace path separator were wrong (\x1F
	// instead of "."), the metadata location would point to a non-existent
	// S3 path and this query would fail.
	t.Logf(">>> Querying data from multi-level namespace table")
	countOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"SELECT count(*) FROM iceberg.%s.%s", multiNs, tableName))
	rowCount := mustParseCSVInt64(t, countOutput)
	if rowCount != 3 {
		t.Fatalf("expected row count 3, got %d", rowCount)
	}

	// Verify the S3 file path contains the dot-separated namespace, not \x1F.
	filesOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		`SELECT file_path FROM iceberg.%s."%s$files" LIMIT 1`, multiNs, tableName))
	filePath := strings.TrimSpace(filesOutput)
	if filePath == "" {
		t.Fatalf("expected at least one data file, got empty output")
	}
	if !strings.Contains(filePath, flatNs+"/") {
		t.Errorf("expected file path to contain dot-separated namespace %q, got: %s", flatNs, filePath)
	}
	if strings.Contains(filePath, "\x1F") {
		t.Errorf("file path contains unit separator (\\x1F), expected dot separator: %s", filePath)
	}

	t.Logf(">>> Trino multi-level namespace test passed")
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
	info, err := os.Stat(weedBinary)
	if err != nil || info.IsDir() {
		// Try looking for weed/weed/weed
		weedBinary = filepath.Join(seaweedDir, "weed", "weed", "weed")
		info, err = os.Stat(weedBinary)
		if err != nil || info.IsDir() {
			weedBinary = "weed"
			if _, err := exec.LookPath(weedBinary); err != nil {
				t.Skip("weed binary not found, skipping integration test")
			}
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-trino-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()

	env := &TestEnvironment{
		seaweedDir: seaweedDir,
		weedBinary: weedBinary,
		dataDir:    dataDir,
		bindIP:     bindIP,
		closers:    []io.Closer{},
	}

	env.masterPort, env.masterGrpcPort = env.mustFreePortPair("Master")
	env.volumePort, env.volumeGrpcPort = env.mustFreePortPair("Volume")
	env.filerPort, env.filerGrpcPort = env.mustFreePortPair("Filer")
	env.s3Port, env.s3GrpcPort = env.mustFreePortPair("S3")
	env.icebergPort = env.mustFreePort("Iceberg")

	env.dockerAvailable = hasDocker()
	env.accessKey = "AKIAIOSFODNN7EXAMPLE"
	env.secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	return env
}

func (env *TestEnvironment) mustFreePort(name string) int {
	port, closer, err := getFreePort()
	if err != nil {
		panic(fmt.Sprintf("Failed to get free port for %s: %v", name, err))
	}
	env.closers = append(env.closers, closer)
	return port
}

func (env *TestEnvironment) mustFreePortPair(name string) (int, int) {
	httpPort, httpCloser, grpcPort, grpcCloser, err := findAvailablePortPair()
	if err != nil {
		panic(fmt.Sprintf("Failed to get free port pair for %s: %v", name, err))
	}
	env.closers = append(env.closers, httpCloser, grpcCloser)
	return httpPort, grpcPort
}

func mustFreePort(t *testing.T, name string) int {
	t.Helper()

	port, closer, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for %s: %v", name, err)
	}
	closer.Close()
	return port
}

func mustFreePortPair(t *testing.T, name string) (int, int) {
	t.Helper()

	httpPort, httpCloser, grpcPort, grpcCloser, err := findAvailablePortPair()
	if err != nil {
		t.Fatalf("Failed to get free port pair for %s: %v", name, err)
	}
	httpCloser.Close()
	grpcCloser.Close()
	return httpPort, grpcPort
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	// Create IAM config file
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

	// Close all port listeners right before starting the weed process
	for _, closer := range env.closers {
		closer.Close()
	}
	env.closers = nil

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

	// Set AWS credentials in environment (for compatibility)
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

func testIcebergRestAPI(t *testing.T, env *TestEnvironment) {
	t.Helper()
	fmt.Printf(">>> Testing Iceberg REST API directly...\n")

	// First, verify the service is listening
	addr := net.JoinHostPort(env.bindIP, fmt.Sprintf("%d", env.icebergPort))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Cannot connect to Iceberg service at %s: %v", addr, err)
	}
	conn.Close()
	t.Logf("Successfully connected to Iceberg service at %s", addr)

	// Test /v1/config endpoint
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

func (env *TestEnvironment) writeTrinoConfig(t *testing.T, warehouseBucket string, opts ...func(*trinoConfigOptions)) string {
	t.Helper()

	o := trinoConfigOptions{}
	for _, fn := range opts {
		fn(&o)
	}

	dirName := "trino"
	if o.nestedNamespace {
		dirName = "trino-nested"
	}
	configDir := filepath.Join(env.dataDir, dirName)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create Trino config dir: %v", err)
	}

	nestedLine := ""
	if o.nestedNamespace {
		nestedLine = "\niceberg.rest-catalog.nested-namespace-enabled=true"
	}

	config := fmt.Sprintf(`connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.docker.internal:%d
iceberg.rest-catalog.warehouse=s3://%s%s
iceberg.file-format=PARQUET
iceberg.unique-table-location=true

# S3 storage config
fs.native-s3.enabled=true
s3.endpoint=http://host.docker.internal:%d
s3.path-style-access=true
s3.signer-type=AwsS3V4Signer
s3.aws-access-key=%s
s3.aws-secret-key=%s
s3.region=us-west-2

# REST catalog authentication
iceberg.rest-catalog.security=SIGV4
`, env.icebergPort, warehouseBucket, nestedLine, env.s3Port, env.accessKey, env.secretKey)

	if err := os.WriteFile(filepath.Join(configDir, "iceberg.properties"), []byte(config), 0644); err != nil {
		t.Fatalf("Failed to write Trino config: %v", err)
	}

	return configDir
}

type trinoConfigOptions struct {
	nestedNamespace bool
}

func withNestedNamespace() func(*trinoConfigOptions) {
	return func(o *trinoConfigOptions) { o.nestedNamespace = true }
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
		"trinodb/trino:479",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Trino container: %v\n%s", err, string(output))
	}
}

func waitForTrino(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastOutput []byte
	retryCount := 0
	for time.Now().Before(deadline) {
		// Try system catalog query as a readiness check
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
			retryCount++
		}
		time.Sleep(1 * time.Second)
	}

	// If we can't connect to system catalog, try to at least connect to Trino server
	cmd := exec.Command("docker", "exec", containerName, "trino", "--version")
	if err := cmd.Run(); err == nil {
		// Trino process is running, even if catalog isn't ready yet
		// Give it a bit more time
		time.Sleep(5 * time.Second)
		return
	}

	t.Fatalf("Timed out waiting for Trino to be ready\nLast output:\n%s", string(lastOutput))
}

func runTrinoSQL(t *testing.T, containerName, sql string) string {
	t.Helper()

	cmd := exec.Command("docker", "exec", containerName,
		"trino", "--catalog", "iceberg",
		"--output-format", "CSV",
		"--execute", sql,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Trino command failed: %v\nSQL: %s\nOutput:\n%s", err, sql, string(output))
	}
	return sanitizeTrinoOutput(string(output))
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	// Use weed shell to create the table bucket
	// Create with "000000000000" account ID (matches AccountAdmin.Id from auth_credentials.go)
	// This ensures bucket owner matches authenticated identity's Account.Id
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

func sanitizeTrinoOutput(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.Contains(line, "org.jline.utils.Log") {
			continue
		}
		if strings.Contains(line, "Unable to create a system terminal") {
			continue
		}
		if strings.HasPrefix(line, "WARNING:") {
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		filtered = append(filtered, line)
	}
	if len(filtered) == 0 {
		return ""
	}
	return strings.Join(filtered, "\n") + "\n"
}

func createObjectBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	// Create an AWS S3 client with the test credentials pointing to our local server
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://%s:%d", env.bindIP, env.s3Port)),
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Create the bucket using standard S3 API
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create object bucket %s: %v", bucketName, err)
	}
}

func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

func findAvailablePortPair() (int, io.Closer, int, io.Closer, error) {
	httpPort, httpCloser, err := getFreePort()
	if err != nil {
		return 0, nil, 0, nil, err
	}
	grpcPort, grpcCloser, err := getFreePort()
	if err != nil {
		httpCloser.Close()
		return 0, nil, 0, nil, err
	}
	return httpPort, httpCloser, grpcPort, grpcCloser, nil
}

func getFreePort() (int, io.Closer, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, nil, err
	}

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, listener, nil
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
