package spark

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	dockerAvailable  bool
	weedBinary       string
	seaweedfsDataDir string
	masterPort       int
	filerPort        int
	s3Port           int
	accessKey        string
	secretKey        string
	sparkContainer   testcontainers.Container
	masterProcess    *exec.Cmd
}

func NewTestEnvironment() *TestEnvironment {
	env := &TestEnvironment{
		accessKey: "test",
		secretKey: "test",
	}

	cmd := exec.Command("docker", "version")
	env.dockerAvailable = cmd.Run() == nil

	if weedPath, err := exec.LookPath("weed"); err == nil {
		env.weedBinary = weedPath
	}

	return env
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	if env.weedBinary == "" {
		t.Skip("weed binary not found in PATH, skipping Spark S3 integration test")
	}

	stopPreviousMini()

	var err error
	env.seaweedfsDataDir, err = os.MkdirTemp("", "seaweed-s3-spark-test-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}

	env.masterPort = mustFreePort(t, "Master")
	env.filerPort = mustFreePort(t, "Filer")
	env.s3Port = mustFreePort(t, "S3")

	bindIP := testutil.FindBindIP()
	iamConfigPath, err := testutil.WriteIAMConfig(env.seaweedfsDataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("failed to create IAM config: %v", err)
	}

	env.masterProcess = exec.Command(
		env.weedBinary, "mini",
		"-ip", bindIP,
		"-ip.bind", "0.0.0.0",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
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

	if !waitForPort(env.masterPort, 15*time.Second) {
		t.Fatalf("weed mini failed to start - master port %d not listening", env.masterPort)
	}
	if !waitForPort(env.filerPort, 15*time.Second) {
		t.Fatalf("weed mini failed to start - filer port %d not listening", env.filerPort)
	}
	if !waitForPort(env.s3Port, 15*time.Second) {
		t.Fatalf("weed mini failed to start - s3 port %d not listening", env.s3Port)
	}
}

func (env *TestEnvironment) startSparkContainer(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "apache/spark:3.5.8",
		ExposedPorts: []string{"4040/tcp"},
		Env: map[string]string{
			"SPARK_LOCAL_IP": "localhost",
		},
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		Cmd:        []string{"/bin/sh", "-c", "sleep 7200"},
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

func (env *TestEnvironment) Cleanup() {
	if env.masterProcess != nil && env.masterProcess.Process != nil {
		_ = env.masterProcess.Process.Kill()
		_ = env.masterProcess.Wait()
	}
	clearMiniProcess(env.masterProcess)

	if env.sparkContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = env.sparkContainer.Terminate(ctx)
	}

	if env.seaweedfsDataDir != "" {
		_ = os.RemoveAll(env.seaweedfsDataDir)
	}
}

func mustFreePort(t *testing.T, name string) int {
	t.Helper()

	for i := 0; i < 200; i++ {
		port := 20000 + rand.Intn(30000)
		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			continue
		}
		_ = listener.Close()

		grpcPort := port + 10000
		if grpcPort > 65535 {
			continue
		}
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", grpcPort))
		if err != nil {
			continue
		}
		_ = grpcListener.Close()
		return port
	}

	t.Fatalf("failed to get free port for %s", name)
	return 0
}

func waitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func runSparkPyScript(t *testing.T, container testcontainers.Container, script string, s3Port int) (int, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
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
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

if python_path not in sys.path:
    sys.path.insert(0, python_path)
if py4j_glob and py4j_glob[0] not in sys.path:
    sys.path.insert(0, py4j_glob[0])

from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .master("local[2]")
    .appName("SeaweedFS S3 Spark Issue 8234 Repro")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.executor.extraJavaOptions", "-Djdk.tls.client.protocols=TLSv1")
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.mergeSchema", "false")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("spark.sql.broadcastTimeout", "-1")
    .config("spark.network.timeout", "600")
    .config("spark.hadoop.hive.metastore.schema.verification", "false")
    .config("spark.hadoop.hive.metastore.schema.verification.record.version", "false")
    .config("spark.jars.ivy", ivy_dir)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.endpoint", "host.docker.internal:%d")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
    .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
    .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "warn")
    .config("spark.local.dir", "/tmp/spark-temp")
    .getOrCreate())

%s
`, s3Port, script)

	code, out, err := container.Exec(ctx, []string{"python3", "-c", pythonScript})
	var output string
	if out != nil {
		outputBytes, readErr := io.ReadAll(out)
		if readErr != nil {
			output = fmt.Sprintf("failed to read container output: %v", readErr)
		} else {
			output = string(outputBytes)
		}
	}
	if err != nil {
		output = output + fmt.Sprintf("\ncontainer exec error: %v\n", err)
	}
	return code, output
}

func createObjectBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://localhost:%d", env.s3Port)),
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("failed to create object bucket %s: %v", bucketName, err)
	}
}
