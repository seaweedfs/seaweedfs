package s3vectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

// TestEnvironment mirrors test/s3tables/sts_integration: a weed server on
// testutil-allocated ports against a throwaway data dir.
type TestEnvironment struct {
	seaweedDir string
	weedBinary string
	dataDir    string
	bindIP     string
	accessKey  string
	secretKey  string
	masterPort int
	volumePort int
	filerPort  int
	s3Port     int
	lancePort  int
	vectorPort int
	process    *exec.Cmd
	cancel     context.CancelFunc
	endpoint   string

	vectorProcess *exec.Cmd
}

func newTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	seaweedDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}
	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if info, err := os.Stat(weedBinary); err != nil || info.IsDir() {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-vectors-test-*")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}

	// weed server derives each service's gRPC port as HTTP+10000, so the
	// pairs must be allocated together.
	ports := testutil.MustFreeMiniPorts(t, []string{"master", "volume", "filer", "s3", "lance", "vector"})
	env := &TestEnvironment{
		seaweedDir: seaweedDir,
		weedBinary: weedBinary,
		dataDir:    dataDir,
		bindIP:     testutil.FindBindIP(),
		accessKey:  "admin",
		secretKey:  "adminadmin",
		masterPort: ports[0],
		volumePort: ports[1],
		filerPort:  ports[2],
		s3Port:     ports[3],
		lancePort:  ports[4],
		vectorPort: ports[5],
	}
	env.endpoint = fmt.Sprintf("http://%s:%d", env.bindIP, env.s3Port)
	return env
}

func (e *TestEnvironment) start(t *testing.T) {
	t.Helper()

	iamConfigPath, err := testutil.WriteIAMConfig(e.dataDir, e.accessKey, e.secretKey)
	if err != nil {
		t.Fatalf("iam config: %v", err)
	}
	// An empty security.toml in the working dir shadows any host-level one so
	// the spawned server is hermetic.
	if err := os.WriteFile(filepath.Join(e.dataDir, "security.toml"), nil, 0644); err != nil {
		t.Fatalf("security.toml: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.process = exec.CommandContext(ctx, e.weedBinary, "server",
		"-ip="+e.bindIP,
		"-ip.bind=0.0.0.0",
		"-dir="+e.dataDir,
		fmt.Sprintf("-master.port=%d", e.masterPort),
		fmt.Sprintf("-volume.port=%d", e.volumePort),
		"-filer=true",
		fmt.Sprintf("-filer.port=%d", e.filerPort),
		"-s3",
		fmt.Sprintf("-s3.port=%d", e.s3Port),
		"-s3.config="+iamConfigPath,
		"-s3.port.iceberg=0",
		fmt.Sprintf("-s3.port.lance=%d", e.lancePort),
	)
	e.process.Dir = e.dataDir
	e.process.Stdout = os.Stdout
	e.process.Stderr = os.Stderr
	if err := e.process.Start(); err != nil {
		t.Fatalf("start weed: %v", err)
	}

	// Wait for the S3 listener — the vectors router shares it. The client is
	// bounded so a hung server cannot stall past the deadline.
	if !testutil.WaitForService(e.endpoint+"/status", testutil.SeaweedMiniStartupTimeout) {
		t.Fatalf("weed server did not come up on %s", e.endpoint)
	}
}

func (e *TestEnvironment) cleanup(t *testing.T) {
	t.Helper()
	if e.vectorProcess != nil {
		_ = e.vectorProcess.Process.Kill()
		_ = e.vectorProcess.Wait()
	}
	if e.cancel != nil {
		e.cancel()
	}
	if e.process != nil {
		_ = e.process.Wait()
	}
	_ = os.RemoveAll(e.dataDir)
}

// vectorEngineBinary resolves the engine binary the way the pytest suite
// does: VECTOR_BINARY env, a Makefile-built ./weed-vector, then PATH. The
// engine runs with its working directory set to the data dir, so the path is
// made absolute here.
func vectorEngineBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("VECTOR_BINARY")
	if bin == "" {
		if _, err := os.Stat("./weed-vector"); err == nil {
			bin = "./weed-vector"
		} else if b, err := exec.LookPath("weed-vector"); err == nil {
			bin = b
		} else {
			t.Skip("weed-vector binary not found, skipping engine-dependent test")
		}
	}
	if abs, err := filepath.Abs(bin); err == nil {
		return abs
	}
	return bin
}

// startVectorEngine spawns weed-vector against the environment's lance
// namespace endpoint and waits for its listener to come up — mirroring the
// cluster the pytest suite builds in conftest.py.
func (e *TestEnvironment) startVectorEngine(t *testing.T, bin string) {
	t.Helper()
	masters := fmt.Sprintf("%s:%d.%d", e.bindIP, e.masterPort, e.masterPort+testutil.GrpcPortOffset)
	cmd := exec.Command(bin,
		"--namespace", fmt.Sprintf("http://%s:%d", e.bindIP, e.lancePort),
		"--masters", masters,
		"--listen", fmt.Sprintf("%s:%d", e.bindIP, e.vectorPort),
		"--storage-option", "aws_access_key_id="+e.accessKey,
		"--storage-option", "aws_secret_access_key="+e.secretKey,
	)
	cmd.Dir = e.dataDir
	log, err := os.Create(filepath.Join(e.dataDir, "weed-vector.log"))
	if err != nil {
		t.Fatalf("vector log: %v", err)
	}
	defer log.Close()
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start weed-vector: %v", err)
	}
	// Assign only after Start succeeded: cleanup kills Process unconditionally.
	e.vectorProcess = cmd
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", e.bindIP, e.vectorPort), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		select {
		case err := <-exited:
			t.Fatalf("weed-vector exited before listening on %d: %v", e.vectorPort, err)
		default:
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("weed-vector did not listen on %d within 30s", e.vectorPort)
}

// waitForEngineDiscovery polls a data-plane op until the gateway reports the
// engine instead of 503 — master registration and discovery lag the process
// itself coming up.
func (e *TestEnvironment) waitForEngineDiscovery(t *testing.T, c *vectorsClient) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		code, body := c.call(t, "CreateIndex", map[string]interface{}{
			"vectorBucketName": "vec-bucket", "indexName": "idx",
			"dataType": "float32", "dimension": 3, "distanceMetric": "euclidean",
		})
		if code == http.StatusOK {
			return
		}
		if code != http.StatusServiceUnavailable {
			t.Fatalf("CreateIndex = %d %v, want 200 or a retryable 503", code, body)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("engine never registered with the gateway within 30s")
}

// vectorsClient posts a JSON operation with an X-Amz-Target header, the way
// the AWS SDK for S3 Vectors calls the API. Requests are SigV4-signed with
// the s3vectors service name — the gateway picks the vectors router off the
// credential scope, so an s3-signed or unsigned request never reaches it.
type vectorsClient struct {
	endpoint string
	client   *http.Client
	signer   *v4.Signer
	creds    aws.Credentials
	region   string
}

func (e *TestEnvironment) client() *vectorsClient {
	return &vectorsClient{
		endpoint: e.endpoint,
		client:   &http.Client{Timeout: 10 * time.Second},
		signer:   v4.NewSigner(),
		creds:    aws.Credentials{AccessKeyID: e.accessKey, SecretAccessKey: e.secretKey},
		region:   "us-east-1",
	}
}

func (c *vectorsClient) call(t *testing.T, op string, payload interface{}) (int, map[string]interface{}) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", op, err)
	}
	req, err := http.NewRequest(http.MethodPost, c.endpoint+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build %s: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonS3Vectors."+op)
	payloadHash := sha256.Sum256(body)
	if err := c.signer.SignHTTP(context.Background(), c.creds, req,
		hex.EncodeToString(payloadHash[:]), "s3vectors", c.region, time.Now()); err != nil {
		t.Fatalf("sign %s: %v", op, err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", op, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", op, err)
	}
	var decoded map[string]interface{}
	if len(out) > 0 {
		if err := json.Unmarshal(out, &decoded); err != nil {
			decoded = map[string]interface{}{"raw": string(out)}
		}
	}
	return resp.StatusCode, decoded
}

func (c *vectorsClient) want(t *testing.T, op string, payload interface{}, code int) map[string]interface{} {
	t.Helper()
	got, body := c.call(t, op, payload)
	if got != code {
		t.Fatalf("%s = %d %v, want %d", op, got, body, code)
	}
	return body
}

// The catalog-side surface needs no vector engine: bucket CRUD, policy ops,
// and the validation that runs before any engine call all resolve through
// the shared table-bucket storage.
func TestVectorBucketCatalogSurface(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	env := newTestEnvironment(t)
	defer env.cleanup(t)
	env.start(t)

	c := env.client()

	// Bucket lifecycle.
	c.want(t, "CreateVectorBucket", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
		"encryptionConfiguration": map[string]interface{}{
			"sseType": "AES256",
		},
	}, http.StatusOK)
	got := c.want(t, "GetVectorBucket", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
	if enc, _ := got["vectorBucket"].(map[string]interface{})["encryptionConfiguration"].(map[string]interface{}); enc["sseType"] != "AES256" {
		t.Fatalf("encryptionConfiguration did not round-trip: %v", got)
	}
	list := c.want(t, "ListVectorBuckets", map[string]interface{}{}, http.StatusOK)
	if n := len(list["vectorBuckets"].([]interface{})); n != 1 {
		t.Fatalf("ListVectorBuckets returned %d buckets, want 1", n)
	}

	// Policy lifecycle: put accepts s3vectors actions, get returns the doc
	// the caller wrote, delete clears it.
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectors","Resource":"arn:aws:s3vectors:us-east-1:000000000000:bucket/vec-bucket/*"}]}`
	c.want(t, "PutVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
		"policy":           policy,
	}, http.StatusOK)
	stored := c.want(t, "GetVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
	if stored["policy"] != policy {
		t.Fatalf("stored policy does not round-trip verbatim: %v", stored)
	}
	c.want(t, "DeleteVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
	c.want(t, "GetVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusNotFound)

	// Rejected shapes fail as ValidationException before any engine work.
	c.want(t, "PutVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
		"policy":           `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}]}`,
	}, http.StatusBadRequest)
	c.want(t, "CreateIndex", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
		"dataType": "float64", "dimension": 3,
	}, http.StatusBadRequest)

	c.want(t, "DeleteVectorBucket", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
}

// The data plane needs the Rust engine: weed-vector registers on the master,
// the gateway discovers it there, and every index/vector op crosses the gRPC
// boundary. The suite skips when the binary is not on the host.
func TestVectorDataPlaneLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	bin := vectorEngineBinary(t)

	env := newTestEnvironment(t)
	defer env.cleanup(t)
	env.start(t)
	env.startVectorEngine(t, bin)

	c := env.client()
	c.want(t, "CreateVectorBucket", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
	env.waitForEngineDiscovery(t, c)
	indexes := c.want(t, "ListIndexes", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
	if n := len(indexes["indexes"].([]interface{})); n != 1 {
		t.Fatalf("ListIndexes returned %d indexes, want 1", n)
	}

	// Write and read the first vectors.
	c.want(t, "PutVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
		"vectors": []map[string]interface{}{
			{"key": "a", "data": map[string]interface{}{"float32": []float64{1, 0, 0}}, "metadata": map[string]interface{}{"color": "red"}},
			{"key": "b", "data": map[string]interface{}{"float32": []float64{0.9, 0.1, 0}}, "metadata": map[string]interface{}{"color": "blue"}},
			{"key": "c", "data": map[string]interface{}{"float32": []float64{0, 0, 1}}, "metadata": map[string]interface{}{"color": "red"}},
		},
	}, http.StatusOK)
	got := c.want(t, "GetVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
		"keys": []string{"a"}, "returnData": true, "returnMetadata": true,
	}, http.StatusOK)
	vec := got["vectors"].([]interface{})[0].(map[string]interface{})
	if vec["key"] != "a" || vec["metadata"].(map[string]interface{})["color"] != "red" {
		t.Fatalf("GetVectors returned %v, want key a with color red", vec)
	}
	data, _ := vec["data"].(map[string]interface{})["float32"].([]interface{})
	if len(data) != 3 || data[0] != 1.0 || data[1] != 0.0 || data[2] != 0.0 {
		t.Fatalf("GetVectors returned data %v, want float32 [1 0 0]", vec["data"])
	}
	listed := c.want(t, "ListVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
	}, http.StatusOK)
	if n := len(listed["vectors"].([]interface{})); n != 3 {
		t.Fatalf("ListVectors returned %d vectors, want 3", n)
	}

	// A filtered query excludes the nearest non-matching vector: b is closer
	// than c but fails the filter.
	hits := c.want(t, "QueryVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
		"queryVector":    map[string]interface{}{"float32": []float64{1, 0, 0}},
		"topK":           2,
		"filter":         map[string]interface{}{"color": map[string]interface{}{"$eq": "red"}},
		"returnDistance": true,
	}, http.StatusOK)
	keys := map[string]bool{}
	for _, h := range hits["vectors"].([]interface{}) {
		keys[h.(map[string]interface{})["key"].(string)] = true
	}
	if !keys["a"] || !keys["c"] || keys["b"] {
		t.Fatalf("filtered query returned %v, want a and c", hits["vectors"])
	}

	c.want(t, "DeleteVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx", "keys": []string{"b"},
	}, http.StatusOK)
	listed = c.want(t, "ListVectors", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
	}, http.StatusOK)
	remaining := map[string]bool{}
	for _, v := range listed["vectors"].([]interface{}) {
		remaining[v.(map[string]interface{})["key"].(string)] = true
	}
	if len(remaining) != 2 || !remaining["a"] || !remaining["c"] {
		t.Fatalf("ListVectors after delete returned %v, want keys a and c", listed["vectors"])
	}

	c.want(t, "DeleteIndex", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
	}, http.StatusOK)
	c.want(t, "DeleteVectorBucket", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)
}
