package catalog_spark

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type TestEnvironment struct {
	t                    *testing.T
	dockerAvailable      bool
	seaweedfsDataDir     string
	masterPort           int
	filerPort            int
	s3Port               int
	icebergRestPort      int
	sparkContainer       testcontainers.Container
	masterProcess        *exec.Cmd
	filerProcess         *exec.Cmd
	volumeProcess        *exec.Cmd
	icebergRestProcess   *exec.Cmd
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

	// Start Master
	env.masterPort = 19000 + rand.Intn(100)
	env.masterProcess = exec.Command(
		"weed", "master",
		"-port", fmt.Sprintf("%d", env.masterPort),
		"-mdir", env.seaweedfsDataDir,
	)
	if err := env.masterProcess.Start(); err != nil {
		t.Fatalf("failed to start master: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start Volume
	volumePort := 19001 + rand.Intn(100)
	env.volumeProcess = exec.Command(
		"weed", "volume",
		"-port", fmt.Sprintf("%d", volumePort),
		"-master", fmt.Sprintf("localhost:%d", env.masterPort),
		"-dir", env.seaweedfsDataDir,
	)
	if err := env.volumeProcess.Start(); err != nil {
		t.Fatalf("failed to start volume: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start Filer
	env.filerPort = 19002 + rand.Intn(100)
	env.filerProcess = exec.Command(
		"weed", "filer",
		"-port", fmt.Sprintf("%d", env.filerPort),
		"-master", fmt.Sprintf("localhost:%d", env.masterPort),
	)
	if err := env.filerProcess.Start(); err != nil {
		t.Fatalf("failed to start filer: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start S3
	env.s3Port = 19003 + rand.Intn(100)
	s3Process := exec.Command(
		"weed", "s3",
		"-port", fmt.Sprintf("%d", env.s3Port),
		"-filer", fmt.Sprintf("localhost:%d", env.filerPort),
		"-cert", "",
		"-key", "",
	)
	if err := s3Process.Start(); err != nil {
		t.Fatalf("failed to start s3: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start Iceberg REST Catalog
	env.icebergRestPort = 19004 + rand.Intn(100)
	env.icebergRestProcess = exec.Command(
		"weed", "server",
		"-ip=localhost",
		"-port", fmt.Sprintf("%d", env.icebergRestPort),
		"-filer", fmt.Sprintf("localhost:%d", env.filerPort),
	)
	env.icebergRestProcess.Env = append(
		os.Environ(),
		"SEAWEEDFS_S3_PORT="+fmt.Sprintf("%d", env.s3Port),
	)
	if err := env.icebergRestProcess.Start(); err != nil {
		t.Fatalf("failed to start iceberg rest: %v", err)
	}

	time.Sleep(1 * time.Second)
}

func (env *TestEnvironment) writeSparkConfig(t *testing.T, catalogBucket string) string {
	t.Helper()

	configDir, err := os.MkdirTemp("", "spark-config-")
	if err != nil {
		t.Fatalf("failed to create config directory: %v", err)
	}

	s3Endpoint := fmt.Sprintf("http://localhost:%d", env.s3Port)
	catalogEndpoint := fmt.Sprintf("http://localhost:%d", env.icebergRestPort)

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
		Image:        "apache/spark:latest",
		ExposedPorts: []string{"4040/tcp"},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(configDir, "/config"),
		),
		Env: map[string]string{
			"SPARK_LOCAL_IP": "localhost",
		},
		WaitingFor: testcontainers.NewLogStrategy("Ready to accept connections").
			WithStartupTimeout(30 * time.Second),
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

	if env.sparkContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		env.sparkContainer.Terminate(ctx)
	}

	if env.icebergRestProcess != nil {
		env.icebergRestProcess.Process.Kill()
	}
	if env.masterProcess != nil {
		env.masterProcess.Process.Kill()
	}
	if env.filerProcess != nil {
		env.filerProcess.Process.Kill()
	}
	if env.volumeProcess != nil {
		env.volumeProcess.Process.Kill()
	}

	if env.seaweedfsDataDir != "" {
		os.RemoveAll(env.seaweedfsDataDir)
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

func runSparkPySQL(t *testing.T, container testcontainers.Container, sql string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pythonScript := fmt.Sprintf(`
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("SeaweedFS Iceberg Test") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181") \\
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:8080") \\
    .getOrCreate()

result = spark.sql("""
%s
""")

result.show()
`, sql)

	code, out, err := container.Exec(ctx, []string{"python", "-c", pythonScript})
	if code != 0 {
		t.Logf("Spark Python execution failed with code %d: %s\n%v", code, out, err)
		return ""
	}

	return out
}

func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	cmd := exec.Command("aws", "s3api", "create-bucket",
		"--bucket", bucketName,
		"--endpoint-url", fmt.Sprintf("http://localhost:%d", env.s3Port),
		"--access-key", "test",
		"--secret-key", "test",
	)
	if err := cmd.Run(); err != nil {
		t.Logf("Warning: failed to create bucket %s: %v", bucketName, err)
	}
}
