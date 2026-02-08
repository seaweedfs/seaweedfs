package catalog_spark

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type TestEnvironment struct {
	t                *testing.T
	dockerAvailable  bool
	seaweedfsDataDir string
	sparkConfigDir   string
	masterPort       int
	filerPort        int
	s3Port           int
	icebergRestPort  int
	sparkContainer   testcontainers.Container
	masterProcess    *exec.Cmd
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	env := &TestEnvironment{
		t: t,
	}

	// Check if Docker is available
	cmd := exec.Command("docker", "version")
	env.dockerAvailable = cmd.Run() == nil

	return env
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	var err error
	env.seaweedfsDataDir, err = os.MkdirTemp("", "seaweed-spark-test-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}

	// Allocate port range to avoid collisions
	basePort := 19000 + rand.Intn(1000)
	env.masterPort = basePort
	env.filerPort = basePort + 1
	env.s3Port = basePort + 2
	env.icebergRestPort = basePort + 3

	// Start SeaweedFS using weed mini (all-in-one including Iceberg REST)
	env.masterProcess = exec.Command(
		"weed", "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergRestPort),
		"-dir", env.seaweedfsDataDir,
	)
	if err := env.masterProcess.Start(); err != nil {
		t.Fatalf("failed to start weed mini: %v", err)
	}

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

func (env *TestEnvironment) writeSparkConfig(t *testing.T, catalogBucket string) string {
	t.Helper()

	configDir, err := os.MkdirTemp("", "spark-config-")
	if err != nil {
		t.Fatalf("failed to create config directory: %v", err)
	}

	// Store for cleanup
	env.sparkConfigDir = configDir

	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
	catalogEndpoint := fmt.Sprintf("http://host.docker.internal:%d", env.icebergRestPort)

	sparkConfig := fmt.Sprintf(`
[spark]
master = "local"
app.name = "SeaweedFS Iceberg Test"

[storage]
s3.endpoint = "%s"
s3.access-key = "test"
s3.secret-key = "test"
s3.path-style-access = "true"
s3.bucket = "%s"

[iceberg]
catalog.type = "rest"
catalog.uri = "%s"
catalog.s3.endpoint = "%s"
catalog.s3.access-key = "test"
catalog.s3.secret-key = "test"
catalog.s3.path-style-access = "true"
`, s3Endpoint, catalogBucket, catalogEndpoint, s3Endpoint)

	configPath := filepath.Join(configDir, "spark-config.ini")
	if err := os.WriteFile(configPath, []byte(sparkConfig), 0644); err != nil {
		t.Fatalf("failed to write spark config: %v", err)
	}

	return configDir
}

func (env *TestEnvironment) startSparkContainer(t *testing.T, configDir string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "apache/spark:3.5.1",
		ExposedPorts: []string{"4040/tcp"},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(configDir, "/config"),
		),
		Env: map[string]string{
			"SPARK_LOCAL_IP": "localhost",
		},
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		Cmd:        []string{"/bin/sh", "-c", "sleep 3600"},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start spark container: %v", err)
	}

	env.sparkContainer = container
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	// Kill weed mini process
	if env.masterProcess != nil && env.masterProcess.Process != nil {
		env.masterProcess.Process.Kill()
		env.masterProcess.Wait()
	}

	// Stop Spark container
	if env.sparkContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		env.sparkContainer.Terminate(ctx)
	}

	// Remove temporary directories after processes are stopped
	if env.seaweedfsDataDir != "" {
		os.RemoveAll(env.seaweedfsDataDir)
	}
	if env.sparkConfigDir != "" {
		os.RemoveAll(env.sparkConfigDir)
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

func runSparkPySQL(t *testing.T, container testcontainers.Container, sql string, icebergPort int, s3Port int) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pythonScript := fmt.Sprintf(`
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("SeaweedFS Iceberg Test") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri", "http://host.docker.internal:%d") \\
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://host.docker.internal:%d") \\
    .getOrCreate()

result = spark.sql("""
%s
""")

result.show()
`, icebergPort, s3Port, sql)

	code, out, err := container.Exec(ctx, []string{"python3", "-c", pythonScript})
	if code != 0 {
		t.Logf("Spark Python execution failed with code %d: %v", code, err)
		return ""
	}

	// Convert io.Reader to string
	outputBytes, err := io.ReadAll(out)
	if err != nil {
		t.Logf("failed to read output: %v", err)
		return ""
	}

	return string(outputBytes)
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	cmd := exec.Command("aws", "s3api", "create-bucket",
		"--bucket", bucketName,
		"--endpoint-url", fmt.Sprintf("http://localhost:%d", env.s3Port),
	)

	// Set AWS credentials via environment
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
	)

	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to create bucket %s: %v", bucketName, err)
	}
}
