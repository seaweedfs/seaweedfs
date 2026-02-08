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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/s3tables/testutil"
	"github.com/testcontainers/testcontainers-go"
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
	t                *testing.T
	dockerAvailable  bool
	seaweedfsDataDir string
	sparkConfigDir   string
	masterPort       int
	filerPort        int
	s3Port           int
	icebergRestPort  int
	accessKey        string
	secretKey        string
	sparkContainer   testcontainers.Container
	masterProcess    *exec.Cmd
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	env := &TestEnvironment{
		t:         t,
		accessKey: "test",
		secretKey: "test",
	}

	// Check if Docker is available
	cmd := exec.Command("docker", "version")
	env.dockerAvailable = cmd.Run() == nil

	return env
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	stopPreviousMini()

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

	bindIP := testutil.FindBindIP()

	iamConfigPath, err := testutil.WriteIAMConfig(env.seaweedfsDataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("failed to create IAM config: %v", err)
	}

	// Start SeaweedFS using weed mini (all-in-one including Iceberg REST)
	env.masterProcess = exec.Command(
		"weed", "mini",
		"-ip", bindIP,
		"-ip.bind", "0.0.0.0",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergRestPort),
		"-s3.config", iamConfigPath,
		"-dir", env.seaweedfsDataDir,
	)
	env.masterProcess.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+env.accessKey,
		"AWS_SECRET_ACCESS_KEY="+env.secretKey,
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	clearMiniProcess(env.masterProcess)

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
import glob
import os
import sys

spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
python_path = os.path.join(spark_home, "python")
py4j_glob = glob.glob(os.path.join(python_path, "lib", "py4j-*.zip"))
ivy_dir = "/tmp/ivy"
os.makedirs(ivy_dir, exist_ok=True)
os.environ["AWS_REGION"] = "us-west-2"
os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
if python_path not in sys.path:
    sys.path.insert(0, python_path)
if py4j_glob and py4j_glob[0] not in sys.path:
    sys.path.insert(0, py4j_glob[0])

from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("SeaweedFS Iceberg Test")
    .config("spark.jars.ivy", ivy_dir)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://host.docker.internal:%d")
    .config("spark.sql.catalog.iceberg.rest.auth.type", "sigv4")
    .config("spark.sql.catalog.iceberg.rest.auth.sigv4.delegate-auth-type", "none")
    .config("spark.sql.catalog.iceberg.rest.sigv4-enabled", "true")
    .config("spark.sql.catalog.iceberg.rest.signing-region", "us-west-2")
    .config("spark.sql.catalog.iceberg.rest.signing-name", "s3")
    .config("spark.sql.catalog.iceberg.rest.access-key-id", "test")
    .config("spark.sql.catalog.iceberg.rest.secret-access-key", "test")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://host.docker.internal:%d")
    .config("spark.sql.catalog.iceberg.s3.region", "us-west-2")
    .config("spark.sql.catalog.iceberg.s3.access-key", "test")
    .config("spark.sql.catalog.iceberg.s3.secret-key", "test")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate())

%s
`, icebergPort, s3Port, sql)

	code, out, err := container.Exec(ctx, []string{"python3", "-c", pythonScript})
	var output string
	if out != nil {
		outputBytes, readErr := io.ReadAll(out)
		if readErr != nil {
			t.Logf("failed to read output: %v", readErr)
		} else {
			output = string(outputBytes)
		}
	}
	if code != 0 {
		t.Logf("Spark Python execution failed with code %d: %v, output: %s", code, err, output)
		return output
	}

	return output
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	masterGrpcPort := env.masterPort + 10000
	cmd := exec.Command("weed", "shell",
		fmt.Sprintf("-master=localhost:%d.%d", env.masterPort, masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("s3tables.bucket -create -name %s -account 000000000000\nexit\n", bucketName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to create table bucket %s via weed shell: %v\nOutput: %s", bucketName, err, string(output))
	}
}
