// Package duckdblance reads SeaweedFS Lance tables from DuckDB's lance
// extension, the counterpart of the DuckDB Iceberg tests next door.
//
// DuckDB reaches the data over S3 rather than through the namespace, so what
// this proves is the other half of the design: a table bucket's layout is a
// valid Lance dataset directory, and a table stays readable with no catalog in
// the path at all. It also pins the one place that costs us - DuckDB's
// replacement scan recognises a dataset by its .lance suffix, which tables
// created through this catalog deliberately do not have.
package duckdblance

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
	// Tracks whatever DuckDB users are actually running; the lance extension is
	// a core extension, so there is nothing to pin beyond the image.
	duckDBImage = "duckdb/duckdb:latest"
	// Seeds a dataset for DuckDB to read.
	seedImage      = "seaweedfs-lance-seed"
	startupTimeout = 60 * time.Second
	clientTimeout  = 15 * time.Minute
	seededRows     = 128
)

// TestDuckDBLance reads a table this catalog created, from DuckDB.
func TestDuckDBLance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping DuckDB Lance integration test")
	}

	env := newEnvironment(t)
	defer env.cleanup()

	env.start(t)

	bucket := "duckdb-" + randomSuffix()
	env.createTableBucket(t, bucket)

	requireDuckDBLance(t)
	buildSeedImage(t)
	env.seedTable(t, bucket)
	env.runDuckDB(t, bucket)
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

	dataDir, err := os.MkdirTemp("", "seaweed-duckdb-lance-test-*")
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
// requireDuckDBLance skips rather than fails when the image cannot load the
// extension, so a DuckDB build without it reads as "not applicable" rather than
// as a broken catalog.
func requireDuckDBLance(t *testing.T) {
	t.Helper()

	const ready = "lance extension ready"
	cmd := exec.Command("docker", "run", "--rm", "--entrypoint", "duckdb", duckDBImage,
		"-c", fmt.Sprintf("INSTALL lance; LOAD lance; SELECT '%s' AS marker;", ready))
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), ready) {
		t.Skipf("DuckDB image cannot load the lance extension: %v\n%s", err, out)
	}
}

func buildSeedImage(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "build", "-t", seedImage, "-f", "Dockerfile.seed", ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the seed image: %v\n%s", err, out)
	}
}

// seedTable declares a table through the namespace and writes a dataset into
// it, which is the split the catalog serves: it records where a table lives and
// does not carry its data.
func (env *environment) seedTable(t *testing.T, bucket string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-east-1",
		seedImage,
		"python3", "/app/seed_table.py",
		"--namespace-url", fmt.Sprintf("http://host.docker.internal:%d", env.lancePort),
		"--s3-endpoint", fmt.Sprintf("http://host.docker.internal:%d", env.s3Port),
		"--bucket", bucket,
		"--rows", fmt.Sprintf("%d", seededRows),
		"--access-key", env.accessKey,
		"--secret-key", env.secretKey,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("seeder:\n%s", out)
	if err != nil {
		t.Fatalf("seeding the table failed: %v", err)
	}
}

// runDuckDB substitutes the endpoint and paths into the SQL and runs it.
func (env *environment) runDuckDB(t *testing.T, bucket string) {
	t.Helper()

	sqlBytes, err := os.ReadFile("duckdb_lance_ops.sql")
	if err != nil {
		t.Fatalf("read the SQL: %v", err)
	}
	table := fmt.Sprintf("s3://%s/ml/embeddings", bucket)
	sql := strings.NewReplacer(
		"__ENDPOINT__", fmt.Sprintf("http://host.docker.internal:%d", env.s3Port),
		"__KEY__", env.accessKey,
		"__SECRET__", env.secretKey,
		"__TABLE__", table,
		"__SUFFIXED__", table+"-direct.lance",
	).Replace(string(sqlBytes))

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i",
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb", duckDBImage, "-c", sql)
	out, err := cmd.CombinedOutput()
	t.Logf("DuckDB output:\n%s", out)
	if err != nil {
		t.Fatalf("the DuckDB query failed: %v", err)
	}

	output := string(out)
	for _, want := range []string{
		fmt.Sprintf("scan_rows=%d", seededRows),
		"scan_columns=id,title,vector",
		"filtered_rows=5",
		"nearest=1,0,2",
		fmt.Sprintf("suffixed_rows=%d", seededRows),
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("DuckDB did not report %q", want)
		}
	}

	// The other half of the suffix rule: a table this catalog created has no
	// .lance suffix, so DuckDB's replacement scan does not see it and
	// __lance_scan is the way in. If this ever starts working, the docs saying
	// otherwise are wrong.
	bare := exec.CommandContext(ctx, "docker", "run", "--rm", "-i",
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb", duckDBImage,
		"-c", fmt.Sprintf("INSTALL lance; LOAD lance; SELECT count(*) FROM '%s';", table))
	bareOut, bareErr := bare.CombinedOutput()
	// DuckDB exits nonzero for any error, so the exit status alone does not tell
	// "the replacement scan refused the path" from "the query never ran": require
	// the catalog error either way.
	if !strings.Contains(string(bareOut), "does not exist") {
		t.Fatalf("a suffix-less path did not fail the way the docs say it does (%v); "+
			"if the replacement scan now reads it, update them:\n%s", bareErr, bareOut)
	}
	t.Logf("a suffix-less path is not seen by the replacement scan, as expected")
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
