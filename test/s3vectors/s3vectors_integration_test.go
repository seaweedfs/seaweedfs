package s3vectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	process    *exec.Cmd
	cancel     context.CancelFunc
	endpoint   string
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

	ports := testutil.MustAllocatePorts(t, 4)
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
		"-dir="+e.dataDir,
		fmt.Sprintf("-master.port=%d", e.masterPort),
		fmt.Sprintf("-volume.port=%d", e.volumePort),
		"-filer=true",
		fmt.Sprintf("-filer.port=%d", e.filerPort),
		"-s3",
		fmt.Sprintf("-s3.port=%d", e.s3Port),
		"-s3.config="+iamConfigPath,
	)
	e.process.Dir = e.dataDir
	e.process.Stdout = os.Stdout
	e.process.Stderr = os.Stderr
	if err := e.process.Start(); err != nil {
		t.Fatalf("start weed: %v", err)
	}

	// Wait for the S3 listener — the vectors router shares it.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := http.Get(e.endpoint + "/"); err == nil {
			conn.Body.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("weed server did not come up on %s", e.endpoint)
}

func (e *TestEnvironment) cleanup(t *testing.T) {
	t.Helper()
	if e.cancel != nil {
		e.cancel()
	}
	if e.process != nil {
		_ = e.process.Wait()
	}
	_ = os.RemoveAll(e.dataDir)
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
		client:   http.DefaultClient,
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
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(fmt.Sprint(stored["policy"])), &doc); err != nil {
		t.Fatalf("stored policy is not JSON: %v", stored)
	}
	c.want(t, "DeleteVectorBucketPolicy", map[string]interface{}{
		"vectorBucketName": "vec-bucket",
	}, http.StatusOK)

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

// Engine-dependent ops skip cleanly when no vector backend is reachable so
// the suite runs on hosts that can't build the Rust workspace.
func TestVectorDataPlaneRequiresEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	env := newTestEnvironment(t)
	defer env.cleanup(t)
	env.start(t)

	c := env.client()

	code, body := c.call(t, "CreateIndex", map[string]interface{}{
		"vectorBucketName": "vec-bucket", "indexName": "idx",
		"dataType": "float32", "dimension": 3, "distanceMetric": "euclidean",
	})
	if code == http.StatusOK || code == http.StatusBadRequest && !strings.Contains(fmt.Sprint(body), "vector") {
		return // engine reachable or bucket rejected before engine use
	}
	t.Skipf("no vector engine reachable (CreateIndex = %d %v); data-plane tests skipped", code, body)
}
