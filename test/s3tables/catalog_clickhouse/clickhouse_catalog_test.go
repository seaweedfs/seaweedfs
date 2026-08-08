// Package catalog_clickhouse provides integration tests for ClickHouse with
// the SeaweedFS Iceberg REST Catalog.
package catalog_clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	clickhouseImage        = "clickhouse/clickhouse-server:25.8"
	clickhouseDatabase     = "iceberg_catalog"
	clickhouseHTTPPort     = 8123
	clickhouseStartTimeout = 2 * time.Minute

	// The image's passwordless `default` user only accepts connections from
	// localhost, and our queries arrive via the mapped port from the Docker
	// gateway. The entrypoint grants a CLICKHOUSE_USER/CLICKHOUSE_PASSWORD
	// user access from any host.
	clickhouseUser     = "seaweed"
	clickhousePassword = "seaweedtest"
)

type TestEnvironment struct {
	seaweedDir          string
	weedBinary          string
	dataDir             string
	bindIP              string
	s3Port              int
	s3GrpcPort          int
	icebergPort         int
	masterPort          int
	masterGrpcPort      int
	filerPort           int
	filerGrpcPort       int
	volumePort          int
	volumeGrpcPort      int
	weedProcess         *exec.Cmd
	weedCancel          context.CancelFunc
	clickhouseContainer string
	clickhouseHostPort  int
	accessKey           string
	secretKey           string
}

// TestClickHouseIcebergCatalog brings up SeaweedFS + ClickHouse and validates
// that ClickHouse's DataLakeCatalog database engine can discover catalog
// metadata served by SeaweedFS's Iceberg REST API and read both empty and
// populated tables through the standard S3 data path.
//
// Subtests:
//   - BasicSelect: ClickHouse is alive and answering SQL.
//   - DatabaseVisible: the DataLakeCatalog database exists.
//   - TableVisible: seeded tables appear as `namespace.table` entries.
//   - DescribeTable: ClickHouse mapped the Iceberg schema (long -> Int64,
//     optional string -> Nullable(String)).
//   - CountEmptyTable: ClickHouse resolves the table and scans an empty
//     Iceberg snapshot.
//   - ReadWrittenDataCount / ReadWrittenDataValues: a separate table is
//     populated by a PyIceberg writer container before ClickHouse connects;
//     ClickHouse then reads the three rows back, exercising the actual data
//     path (parquet over S3), not just metadata.
func TestClickHouseIcebergCatalog(t *testing.T) {
	requireClickHouseRuntime(t)

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	tableBucket := "iceberg-tables"
	fmt.Printf(">>> Creating table bucket: %s\n", tableBucket)
	createTableBucket(t, env, tableBucket)
	fmt.Printf(">>> Table bucket created.\n")

	testIcebergRestAPI(t, env)

	namespace := "clickhouse_" + randomString(6)
	tableName := "smoke_" + randomString(6)
	icebergToken := requestIcebergOAuthToken(t, env)
	createIcebergNamespace(t, env, icebergToken, tableBucket, namespace)
	createIcebergTable(t, env, icebergToken, tableBucket, namespace, tableName)

	// Seed a populated table by creating an empty one through the REST API
	// and then appending three rows via PyIceberg, so the snapshot exists
	// before ClickHouse's first scan.
	populatedTable := "populated_" + randomString(6)
	createIcebergTable(t, env, icebergToken, tableBucket, namespace, populatedTable)
	buildClickHouseWriterImage(t)
	writeIcebergRows(t, env, tableBucket, []string{namespace}, populatedTable)

	env.startClickHouseContainer(t)
	env.waitForClickHouse(t, clickhouseStartTimeout)

	env.createClickHouseCatalogDatabase(t, tableBucket)

	t.Run("BasicSelect", func(t *testing.T) {
		out := env.mustQuery(t, "SELECT 1")
		if out != "1" {
			t.Fatalf("SELECT 1 = %q, want 1", out)
		}
	})

	t.Run("DatabaseVisible", func(t *testing.T) {
		out := env.mustQuery(t, "SHOW DATABASES")
		if !containsLine(out, clickhouseDatabase) {
			t.Fatalf("SHOW DATABASES did not list %s:\n%s", clickhouseDatabase, out)
		}
	})

	// DataLakeCatalog flattens the Iceberg namespace hierarchy into table
	// names of the form "namespace.table", queried with backtick quoting.
	emptyRef := fmt.Sprintf("%s.`%s.%s`", clickhouseDatabase, namespace, tableName)
	populatedRef := fmt.Sprintf("%s.`%s.%s`", clickhouseDatabase, namespace, populatedTable)

	t.Run("TableVisible", func(t *testing.T) {
		out := env.mustQuery(t, fmt.Sprintf("SHOW TABLES FROM %s", clickhouseDatabase))
		for _, want := range []string{namespace + "." + tableName, namespace + "." + populatedTable} {
			if !containsLine(out, want) {
				t.Fatalf("SHOW TABLES FROM %s did not list %s:\n%s", clickhouseDatabase, want, out)
			}
		}
	})

	t.Run("DescribeTable", func(t *testing.T) {
		out := env.mustQuery(t, fmt.Sprintf("DESCRIBE TABLE %s", emptyRef))
		if !strings.Contains(out, "id\tInt64") {
			t.Fatalf("DESCRIBE %s missing id Int64:\n%s", emptyRef, out)
		}
		if !strings.Contains(out, "label\tNullable(String)") {
			t.Fatalf("DESCRIBE %s missing label Nullable(String):\n%s", emptyRef, out)
		}
	})

	t.Run("CountEmptyTable", func(t *testing.T) {
		out := env.mustQuery(t, fmt.Sprintf("SELECT count() FROM %s", emptyRef))
		if out != "0" {
			t.Fatalf("count(%s) = %q, want 0", emptyRef, out)
		}
	})

	t.Run("ReadWrittenDataCount", func(t *testing.T) {
		out := env.mustQuery(t, fmt.Sprintf("SELECT count() FROM %s", populatedRef))
		if out != "3" {
			t.Fatalf("count(%s) = %q, want 3", populatedRef, out)
		}
	})

	t.Run("ReadWrittenDataValues", func(t *testing.T) {
		out := env.mustQuery(t, fmt.Sprintf("SELECT id, label FROM %s ORDER BY id", populatedRef))
		want := "1\tone\n2\ttwo\n3\tthree"
		if out != want {
			t.Fatalf("SELECT id, label FROM %s = %q, want %q", populatedRef, out, want)
		}
	})
}

// NewTestEnvironment allocates ports and returns an environment for the test.
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
		weedBinary = filepath.Join(seaweedDir, "weed", "weed", "weed")
		info, err = os.Stat(weedBinary)
		if err != nil || info.IsDir() {
			weedBinary = "weed"
			if _, err := exec.LookPath(weedBinary); err != nil {
				t.Skip("weed binary not found, skipping integration test")
			}
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-clickhouse-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()
	// 9 ports for the seaweed mini cluster, plus one for the ClickHouse HTTP
	// interface mapped on the host.
	ports := testutil.MustAllocatePorts(t, 10)

	env := &TestEnvironment{
		seaweedDir:         seaweedDir,
		weedBinary:         weedBinary,
		dataDir:            dataDir,
		bindIP:             bindIP,
		masterPort:         ports[0],
		masterGrpcPort:     ports[1],
		volumePort:         ports[2],
		volumeGrpcPort:     ports[3],
		filerPort:          ports[4],
		filerGrpcPort:      ports[5],
		s3Port:             ports[6],
		s3GrpcPort:         ports[7],
		icebergPort:        ports[8],
		clickhouseHostPort: ports[9],
	}

	env.accessKey = "AKIAIOSFODNN7EXAMPLE"
	env.secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	return env
}

// StartSeaweedFS starts a SeaweedFS mini instance with the Iceberg REST API.
func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

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

	icebergURL := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	if !env.waitForService(icebergURL, 30*time.Second) {
		t.Fatalf("Iceberg REST API did not become ready at %s", icebergURL)
	}
}

// Cleanup stops ClickHouse, SeaweedFS, and removes temporary state.
func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.clickhouseContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.clickhouseContainer).Run()
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

// waitForService polls a URL until it returns a 2xx/401/403 status or timeout.
func (env *TestEnvironment) waitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		statusCode := resp.StatusCode
		resp.Body.Close()
		if statusCode >= 200 && statusCode < 300 {
			return true
		}
		if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// testIcebergRestAPI verifies the Iceberg REST endpoint is reachable.
func testIcebergRestAPI(t *testing.T, env *TestEnvironment) {
	t.Helper()

	url := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to connect to Iceberg REST API at %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 OK from /v1/config, got %d, body: %s", resp.StatusCode, body)
	}
}

// startClickHouseContainer launches the ClickHouse server image and exposes
// only the HTTP interface to the host. The Iceberg REST and S3 endpoints are
// reached via host.docker.internal, matching the Doris/Trino paths.
func (env *TestEnvironment) startClickHouseContainer(t *testing.T) {
	t.Helper()

	containerName := "seaweed-clickhouse-" + randomString(8)
	env.clickhouseContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:%d", env.clickhouseHostPort, clickhouseHTTPPort),
		"--ulimit", "nofile=262144:262144",
		"-e", "CLICKHOUSE_USER="+clickhouseUser,
		"-e", "CLICKHOUSE_PASSWORD="+clickhousePassword,
		clickhouseImage,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start ClickHouse container: %v\n%s", err, string(output))
	}
}

// clickhouseContainerLogs returns the tail of the container logs for diagnostics.
func clickhouseContainerLogs(containerName string) string {
	cmd := exec.Command("docker", "logs", "--tail", "200", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(failed to fetch docker logs: %v)\n%s", err, string(output))
	}
	return string(output)
}

// containerRunning returns true if the named container is in `running` state.
func containerRunning(containerName string) bool {
	cmd := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", containerName)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}

// waitForClickHouse polls the HTTP /ping endpoint until it answers.
func (env *TestEnvironment) waitForClickHouse(t *testing.T, timeout time.Duration) {
	t.Helper()

	pingURL := fmt.Sprintf("http://127.0.0.1:%d/ping", env.clickhouseHostPort)
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !containerRunning(env.clickhouseContainer) {
			t.Fatalf("ClickHouse container exited before becoming ready\nContainer logs:\n%s",
				clickhouseContainerLogs(env.clickhouseContainer))
		}
		resp, err := client.Get(pingURL)
		if err == nil {
			statusCode := resp.StatusCode
			resp.Body.Close()
			if statusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("Timed out waiting for ClickHouse to be ready\nContainer logs:\n%s",
		clickhouseContainerLogs(env.clickhouseContainer))
}

// query sends one SQL statement to ClickHouse's HTTP interface as the default
// user and returns the TabSeparated response body with trailing whitespace
// trimmed. Settings ride along as URL parameters because SET does not persist
// across stateless HTTP requests.
func (env *TestEnvironment) query(sqlText string, settings map[string]string) (string, error) {
	params := url.Values{}
	params.Set("user", clickhouseUser)
	params.Set("password", clickhousePassword)
	params.Set("default_format", "TabSeparated")
	for k, v := range settings {
		params.Set(k, v)
	}
	queryURL := fmt.Sprintf("http://127.0.0.1:%d/?%s", env.clickhouseHostPort, params.Encode())

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Post(queryURL, "text/plain", strings.NewReader(sqlText))
	if err != nil {
		return "", fmt.Errorf("POST query: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("query returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return strings.TrimRight(string(body), "\n"), nil
}

// mustQuery runs a query and fails the test with container logs on error.
func (env *TestEnvironment) mustQuery(t *testing.T, sqlText string) string {
	t.Helper()

	out, err := env.query(sqlText, nil)
	if err != nil {
		t.Fatalf("%s: %v\nContainer logs:\n%s", sqlText, err, clickhouseContainerLogs(env.clickhouseContainer))
	}
	return out
}

// createClickHouseCatalogDatabase attaches the SeaweedFS Iceberg REST catalog
// as a DataLakeCatalog database. The engine arguments carry the S3 storage
// credentials; catalog authentication uses the OAuth2 client_credentials flow
// against the SeaweedFS token endpoint, matching the PyIceberg writer.
func (env *TestEnvironment) createClickHouseCatalogDatabase(t *testing.T, warehouseBucket string) {
	t.Helper()

	icebergURI := fmt.Sprintf("http://host.docker.internal:%d/v1", env.icebergPort)
	storageEndpoint := fmt.Sprintf("http://host.docker.internal:%d/%s", env.s3Port, warehouseBucket)
	oauthURI := icebergURI + "/oauth/tokens"

	createSQL := fmt.Sprintf(`CREATE DATABASE %s
ENGINE = DataLakeCatalog('%s', '%s', '%s')
SETTINGS catalog_type = 'rest',
	warehouse = 's3://%s',
	storage_endpoint = '%s',
	catalog_credential = '%s:%s',
	oauth_server_uri = '%s'`,
		clickhouseDatabase,
		icebergURI, env.accessKey, env.secretKey,
		warehouseBucket,
		storageEndpoint,
		env.accessKey, env.secretKey,
		oauthURI,
	)

	if _, err := env.query(createSQL, map[string]string{"allow_experimental_database_iceberg": "1"}); err != nil {
		t.Fatalf("CREATE DATABASE %s failed: %v\nContainer logs:\n%s",
			clickhouseDatabase, err, clickhouseContainerLogs(env.clickhouseContainer))
	}
}

// containsLine reports whether any line of a TabSeparated result equals want.
func containsLine(out, want string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// requestIcebergOAuthToken requests an OAuth2 client_credentials token from
// the SeaweedFS Iceberg REST catalog. Used to seed the catalog with a
// namespace and tables directly through the REST API before ClickHouse connects.
func requestIcebergOAuthToken(t *testing.T, env *TestEnvironment) string {
	t.Helper()

	resp, err := http.PostForm(fmt.Sprintf("http://%s:%d/v1/oauth/tokens", env.bindIP, env.icebergPort), url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {env.accessKey},
		"client_secret": {env.secretKey},
	})
	if err != nil {
		t.Fatalf("POST /v1/oauth/tokens: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("OAuth token request failed: status=%d body=%s", resp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		t.Fatalf("decode token response: %v", err)
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("got empty access_token")
	}
	return tokenResp.AccessToken
}

// createIcebergNamespace creates a single-level Iceberg namespace through
// the REST catalog.
func createIcebergNamespace(t *testing.T, env *TestEnvironment, token, bucketName, namespace string) {
	t.Helper()

	doIcebergJSONRequest(t, env, token, http.MethodPost, fmt.Sprintf("/v1/%s/namespaces", url.PathEscape(bucketName)), map[string]any{
		"namespace": []string{namespace},
	}, http.StatusOK, http.StatusConflict)
}

// createIcebergTable creates a table inside a single-level namespace through
// the REST catalog. The table is created with the canonical
// (id long not null, label string nullable) schema used by all subtests.
func createIcebergTable(t *testing.T, env *TestEnvironment, token, bucketName, namespace, tableName string) {
	t.Helper()

	doIcebergJSONRequest(t, env, token, http.MethodPost,
		fmt.Sprintf("/v1/%s/namespaces/%s/tables", url.PathEscape(bucketName), url.PathEscape(namespace)),
		map[string]any{
			"name": tableName,
			"schema": map[string]any{
				"type":      "struct",
				"schema-id": 0,
				"fields": []map[string]any{
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "label", "required": false, "type": "string"},
				},
			},
		}, http.StatusOK)
}

const clickhouseWriterImage = "seaweedfs-clickhouse-writer"

// buildClickHouseWriterImage builds the local PyIceberg writer image. Layer
// caching makes repeat invocations cheap; the first build pulls
// python:3.11-slim and pip-installs pyiceberg+pyarrow (~1-2 min in CI).
func buildClickHouseWriterImage(t *testing.T) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	cmd := exec.Command("docker", "build",
		"-t", clickhouseWriterImage,
		"-f", filepath.Join(wd, "Dockerfile.writer"),
		wd,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build %s image: %v\n%s", clickhouseWriterImage, err, out)
	}
}

// writeIcebergRows runs the PyIceberg writer container, which loads the
// already-created table and appends three rows.
func writeIcebergRows(t *testing.T, env *TestEnvironment, bucketName string, namespace []string, tableName string) {
	t.Helper()

	args := []string{
		"run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		clickhouseWriterImage,
		"--catalog-url", fmt.Sprintf("http://host.docker.internal:%d", env.icebergPort),
		"--warehouse", "s3://" + bucketName,
		"--prefix", bucketName,
		"--s3-endpoint", fmt.Sprintf("http://host.docker.internal:%d", env.s3Port),
		"--access-key", env.accessKey,
		"--secret-key", env.secretKey,
		"--region", "us-west-2",
		"--table", tableName,
	}
	for _, level := range namespace {
		args = append(args, "--namespace", level)
	}

	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PyIceberg writer failed: %v\n%s", err, out)
	}
	t.Logf("PyIceberg writer output: %s", strings.TrimSpace(string(out)))
}

// doIcebergJSONRequest issues an authenticated JSON request to the Iceberg
// REST endpoint and returns the response body. It fails the test unless the
// response status matches one of expectedStatuses.
func doIcebergJSONRequest(t *testing.T, env *TestEnvironment, token, method, path string, payload any, expectedStatuses ...int) string {
	t.Helper()

	var body io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal Iceberg request: %v", err)
		}
		body = bytes.NewReader(payloadBytes)
	}

	req, err := http.NewRequest(method, fmt.Sprintf("http://%s:%d%s", env.bindIP, env.icebergPort, path), body)
	if err != nil {
		t.Fatalf("create Iceberg request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Iceberg request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	for _, expectedStatus := range expectedStatuses {
		if resp.StatusCode == expectedStatus {
			return string(respBody)
		}
	}
	t.Fatalf("Iceberg request returned unexpected status %d, want %v\nPath: %s\nBody: %s",
		resp.StatusCode, expectedStatuses, path, respBody)
	return ""
}

// createTableBucket creates an S3 table bucket using `weed shell`, which
// talks to the master over gRPC and bypasses the S3 SigV4 path. The `-master`
// flag uses SeaweedFS's canonical `host:port.grpcPort` ServerAddress format
// produced by pb.NewServerAddress — the dot separates the HTTP port from the
// gRPC port and is required, not a typo.
func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, env.weedBinary, "shell",
		fmt.Sprintf("-master=%s:%d.%d", env.bindIP, env.masterPort, env.masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("s3tables.bucket -create -name %s -account 000000000000\nexit\n", bucketName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create table bucket %s via weed shell: %v\nOutput: %s", bucketName, err, string(output))
	}
	t.Logf("Created table bucket: %s", bucketName)
}

// requireClickHouseRuntime skips the test in `-short` mode or when Docker
// isn't available, since the test cannot run without the ClickHouse container.
func requireClickHouseRuntime(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping ClickHouse integration test")
	}
}

// hasDocker reports whether `docker version` can run, which we treat as a
// sufficient signal that a Docker daemon is reachable from this process.
func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// randomString returns a lowercase alphanumeric string of the given length.
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
