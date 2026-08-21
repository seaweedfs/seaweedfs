// Package sparklance drives the SeaweedFS Lance Namespace with Spark, through
// the Lance Spark connector's DSV2 catalog. It is the Lance counterpart of the
// catalog_spark suite, which does the same for the Iceberg REST catalog.
//
// Spark is the engine most likely to be pointed at a lakehouse, and it reaches
// the catalog over the same routes as every other client - so what this proves
// is the protocol, not our idea of it.
package sparklance

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	sparkImage = "apache/spark:3.5.1"
	// Pinned: the connector is as much the thing under test as the server, and
	// an unrelated release should not change what an old commit reproduces.
	lancePackages  = "org.lance:lance-spark-bundle-3.5_2.12:0.7.1"
	startupTimeout = 60 * time.Second
	clientTimeout  = 25 * time.Minute
)

// TestSparkLanceNamespace runs Spark SQL against the namespace end to end:
// create a namespace and a table through the catalog, write, read back, filter,
// and append a second time.
func TestSparkLanceNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping Spark Lance integration test")
	}

	env := newEnvironment(t)
	defer env.cleanup()

	env.start(t)

	bucket := "sparklance-" + randomSuffix()
	env.createTableBucket(t, bucket)

	env.runSpark(t, bucket)
}

type environment struct {
	weedBinary string
	dataDir    string
	bindIP     string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPort      int
	filerGrpcPort  int
	s3Port         int
	s3GrpcPort     int
	lancePort      int

	accessKey string
	secretKey string

	weedCancel context.CancelFunc
	weedCmd    *exec.Cmd
}

func newEnvironment(t *testing.T) *environment {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	seaweedDir := wd
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if info, statErr := os.Stat(weedBinary); statErr == nil && !info.IsDir() {
		// `make test` builds first; a plain `go test` will otherwise drive a
		// binary from days ago and report a pass for code it never ran.
		t.Logf("using %s, built %s", weedBinary, info.ModTime().Format(time.RFC3339))
	} else {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-spark-lance-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	ports := testutil.MustAllocatePorts(t, 9)
	return &environment{
		weedBinary:     weedBinary,
		dataDir:        dataDir,
		bindIP:         testutil.FindBindIP(),
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		s3Port:         ports[6],
		s3GrpcPort:     ports[7],
		lancePort:      ports[8],
		accessKey:      "AKIAIOSFODNN7EXAMPLE",
		secretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
}

func (env *environment) start(t *testing.T) {
	t.Helper()

	iamConfigPath, err := testutil.WriteIAMConfig(env.dataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("write IAM config: %v", err)
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
		"-s3.port.lance", fmt.Sprintf("%d", env.lancePort),
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
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start SeaweedFS: %v", err)
	}
	env.weedCmd = cmd

	// The namespace answers /v1/table once it is serving, which is a cheaper
	// readiness check than waiting on a bucket that does not exist yet.
	url := fmt.Sprintf("http://%s:%d/v1/table", env.bindIP, env.lancePort)
	if !waitForHTTP(url, startupTimeout) {
		t.Fatalf("the Lance namespace did not become ready at %s", url)
	}
}

// waitForHTTP polls until the URL answers at all. An auth refusal counts: it
// means the server is up, which is the only thing being waited on.
func waitForHTTP(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		status := resp.StatusCode
		resp.Body.Close()
		if status < 500 {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func (env *environment) cleanup() {
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedCmd != nil {
		_ = env.weedCmd.Wait()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

// createTableBucket makes the bucket LanceDB will read through, declared LANCE
// so the catalog refuses anything of another format in it.
func (env *environment) createTableBucket(t *testing.T, bucket string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, env.weedBinary, "shell",
		fmt.Sprintf("-master=%s:%d.%d", env.bindIP, env.masterPort, env.masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf(
		"s3tables.bucket -create -name %s -format LANCE -account 000000000000\nexit\n", bucket))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("create table bucket %s: %v\n%s", bucket, err, out)
	}
	// weed shell reports a command's own failure on stdout and still exits 0, so
	// the exit code alone would let a missing bucket through and turn a setup
	// failure into a confusing engine failure later.
	if !env.tableBucketExists(t, bucket) {
		t.Fatalf("table bucket %s was not created:\n%s", bucket, out)
	}
	t.Logf("created LANCE table bucket %s", bucket)
}

// tableBucketExists asks the namespace, which lists table buckets at its root.
func (env *environment) tableBucketExists(t *testing.T, bucket string) bool {
	t.Helper()

	url := fmt.Sprintf("http://%s:%d/v1/namespace/%s/exists", env.bindIP, env.lancePort, bucket)
	resp, err := http.Post(url, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("ask the namespace whether %s exists: %v", bucket, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// runSpark runs the SQL driver inside the stock Spark image. The connector is
// pulled from Maven at submit time, the way the Iceberg Spark suite pulls its
// runtime, so nothing has to be built here.
func (env *environment) runSpark(t *testing.T, bucket string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	script, absErr := filepath.Abs("spark_lance_ops.py")
	if absErr != nil {
		t.Fatalf("resolve the driver script: %v", absErr)
	}

	// Ivy needs somewhere writable, and the Spark image runs as a user without a
	// home directory it can write to. Kept outside the run's data directory so
	// the 287MB connector bundle is downloaded once rather than on every run,
	// and under the user's own cache rather than a shared temp path: this is
	// mounted into a container running as root, so another local user must not
	// be able to pre-create it and choose what Spark loads.
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = env.dataDir
	}
	ivyDir := filepath.Join(cacheRoot, "seaweedfs-lance-spark-ivy")
	if err := os.MkdirAll(ivyDir, 0o755); err != nil {
		t.Fatalf("create the ivy directory: %v", err)
	}

	// The container reaches the gateway through the host gateway address, which
	// is not the address the namespace advertises to its own clients.
	namespaceURL := fmt.Sprintf("http://host.docker.internal:%d", env.lancePort)
	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-v", script+":/opt/spark/work-dir/spark_lance_ops.py:ro",
		"-v", ivyDir+":/tmp/ivy",
		"-e", "HOME=/tmp",
		"-e", "SPARK_LOCAL_IP=127.0.0.1",
		"-u", "root",
		sparkImage,
		"/opt/spark/bin/spark-submit",
		"--packages", lancePackages,
		"--conf", "spark.jars.ivy=/tmp/ivy",
		"/opt/spark/work-dir/spark_lance_ops.py",
		"--namespace-url", namespaceURL,
		"--s3-endpoint", s3Endpoint,
		"--bucket", bucket,
		"--access-key", env.accessKey,
		"--secret-key", env.secretKey,
		"--packages", lancePackages,
		"--ivy-dir", "/tmp/ivy",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("Spark output:\n%s", tailLines(string(out), 60))
	if err != nil {
		t.Fatalf("the Spark driver failed: %v", err)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Fatalf("the Spark driver did not report PASS")
	}
}

// tailLines keeps the end of Spark's very chatty output, which is where the
// driver's own lines and any failure are.
func tailLines(out string, n int) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) <= n {
		return out
	}
	return "... " + fmt.Sprintf("%d earlier lines omitted", len(lines)-n) + "\n" +
		strings.Join(lines[len(lines)-n:], "\n")
}

// hasDocker reports whether a Docker daemon answers. Bounded, because an
// unhealthy daemon makes `docker version` hang, and this runs before the test
// has a timeout of its own: better to skip than to eat the whole budget.
func hasDocker() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "version").Run() == nil
}

func randomSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		suffix[i] = charset[rand.Intn(len(charset))]
	}
	return string(suffix)
}
