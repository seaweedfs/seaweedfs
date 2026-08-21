// Package lancedb drives the SeaweedFS Lance Namespace with LanceDB, the way
// the catalog_spark, catalog_trino and catalog_clickhouse suites drive the
// Iceberg REST catalog with their engines.
//
// A catalog is only as good as what a real client can do with it. Every serious
// bug in this surface so far - a deregister that deleted the dataset, an S3 door
// that refused every Lance file, a namespace that listed tables it would then
// deny - looked correct to a request written by hand.
package lancedb

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
	clientImage    = "seaweedfs-lancedb-test"
	startupTimeout = 60 * time.Second
	clientTimeout  = 15 * time.Minute
)

// TestLanceDBNamespace runs LanceDB against the namespace end to end: it lists
// what the catalog holds, opens a table through it, searches the vectors, and
// reads the same dataset straight off its URI to show the catalog stays
// optional.
func TestLanceDBNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping LanceDB integration test")
	}

	env := newEnvironment(t)
	defer env.cleanup()

	env.start(t)

	bucket := "lancedb-" + randomSuffix()
	env.createTableBucket(t, bucket)

	buildClientImage(t)
	env.runClient(t, bucket)
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

	dataDir, err := os.MkdirTemp("", "seaweed-lancedb-test-*")
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
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create table bucket %s: %v\n%s", bucket, err, out)
	}
	t.Logf("created LANCE table bucket %s", bucket)
}

func buildClientImage(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "build",
		"-t", clientImage, "-f", "Dockerfile.client", ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the LanceDB client image: %v\n%s", err, out)
	}
}

func (env *environment) runClient(t *testing.T, bucket string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	// The container reaches the gateway through the host gateway address, which
	// is not the address the namespace advertises to its own clients.
	namespaceURL := fmt.Sprintf("http://host.docker.internal:%d", env.lancePort)
	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		// Also in the environment, not only in storage_options: LanceDB takes
		// some paths through the options the namespace vends, and a gateway
		// without STS vends none, leaving lance's provider chain to find them.
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-east-1",
		"-e", "AWS_ENDPOINT_URL="+s3Endpoint,
		"-e", "AWS_ALLOW_HTTP=true",
		clientImage,
		"python3", "/app/lancedb_ops.py",
		"--namespace-url", namespaceURL,
		"--s3-endpoint", s3Endpoint,
		"--bucket", bucket,
		"--access-key", env.accessKey,
		"--secret-key", env.secretKey,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("LanceDB client output:\n%s", out)
	if err != nil {
		t.Fatalf("the LanceDB client failed: %v", err)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Fatalf("the LanceDB client did not report PASS")
	}
}

func hasDocker() bool {
	return exec.Command("docker", "version").Run() == nil
}

func randomSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		suffix[i] = charset[rand.Intn(len(charset))]
	}
	return string(suffix)
}
