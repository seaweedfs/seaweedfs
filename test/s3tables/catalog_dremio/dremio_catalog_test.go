// Package catalog_dremio provides integration tests for Dremio with SeaweedFS Iceberg REST Catalog.
package catalog_dremio

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	dremioImage         = "dremio/dremio-oss:25.2.0"
	dremioSourceName    = "iceberg"
	dremioAdminUser     = "seaweed-admin"
	dremioAdminPassword = "SeaweedFS123!"
)

type TestEnvironment struct {
	seaweedDir      string
	weedBinary      string
	dataDir         string
	bindIP          string
	s3Port          int
	s3GrpcPort      int
	icebergPort     int
	masterPort      int
	masterGrpcPort  int
	filerPort       int
	filerGrpcPort   int
	volumePort      int
	volumeGrpcPort  int
	weedProcess     *exec.Cmd
	weedCancel      context.CancelFunc
	dremioContainer string
	dremioToken     string
	accessKey       string
	secretKey       string
}

// TestDremioIcebergCatalog starts Dremio, registers SeaweedFS as an Iceberg
// REST catalog source, and runs Dremio SQL against tables served by the
// SeaweedFS catalog. The table and namespace are seeded via the Iceberg REST
// API before Dremio bootstraps so they are visible on the source's first scan.
//
// Subtests cover:
//   - BasicSelect: Dremio is alive and answering SQL.
//   - CountEmptyTable: catalog→table resolution and a scan of an empty table.
//   - ColumnProjection: the column names from the SeaweedFS-issued schema are
//     usable in Dremio (failure here means Dremio could not parse the schema).
//   - InformationSchemaColumns: the table's columns are exposed through
//     Dremio's metadata layer with the expected name and ordinal positions.
//   - InformationSchemaTables: the table is registered in Dremio's INFORMATION_SCHEMA.
//   - MultiLevelNamespace: a 2-level Iceberg namespace is surfaced as nested
//     folders and a table inside it is queryable with dot-separated identifiers.
func TestDremioIcebergCatalog(t *testing.T) {
	requireDremioRuntime(t)

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

	namespace := "dremio_" + randomString(6)
	tableName := "smoke_" + randomString(6)
	icebergToken := requestIcebergOAuthToken(t, env)
	createIcebergNamespace(t, env, icebergToken, tableBucket, namespace)
	createIcebergTable(t, env, icebergToken, tableBucket, namespace, tableName)

	// Seed a true multi-level namespace and a table inside it. Created before
	// Dremio bootstraps so the source's first scan with
	// isRecursiveAllowedNamespaces=true discovers both levels.
	multiLevelNs := []string{
		"ml_parent_" + randomString(4),
		"ml_child_" + randomString(4),
	}
	multiLevelTable := "nested_" + randomString(6)
	createIcebergNamespaceLevels(t, env, icebergToken, tableBucket, multiLevelNs[:1])
	createIcebergNamespaceLevels(t, env, icebergToken, tableBucket, multiLevelNs)
	createIcebergTableInLevels(t, env, icebergToken, tableBucket, multiLevelNs, multiLevelTable)

	configDir := env.writeDremioConfig(t, tableBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 180*time.Second)
	env.bootstrapDremio(t, tableBucket)

	tableRef := dremioObjectName(dremioSourceName, namespace, tableName)

	t.Run("BasicSelect", func(t *testing.T) {
		out := runDremioSQL(t, env, "SELECT 1 AS ok")
		assertSingleNumericValue(t, out, 1)
	})

	t.Run("CountEmptyTable", func(t *testing.T) {
		out := runDremioSQL(t, env, fmt.Sprintf("SELECT COUNT(*) AS row_count FROM %s", tableRef))
		assertSingleNumericValue(t, out, 0)
	})

	t.Run("ColumnProjection", func(t *testing.T) {
		// SELECT COUNT(*) does not exercise the schema. A projection by
		// column name fails fast with "column not found" if the schema
		// from the SeaweedFS catalog response was not parsed.
		out := runDremioSQL(t, env, fmt.Sprintf("SELECT id, label FROM %s", tableRef))
		schema, rows := parseDremioResponseSchemaRows(t, out)
		if len(rows) != 0 {
			t.Fatalf("Expected empty result set, got %d rows: %v", len(rows), rows)
		}
		assertSchemaContainsAll(t, schema, "id", "label")
	})

	t.Run("InformationSchemaColumns", func(t *testing.T) {
		// Filter only by TABLE_NAME (which is randomized and globally unique
		// in this run) to avoid depending on the exact TABLE_SCHEMA path
		// Dremio synthesizes for a nested REST-catalog folder.
		query := fmt.Sprintf(
			"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
			tableName,
		)
		out := runDremioSQL(t, env, query)
		_, rows := parseDremioResponseSchemaRows(t, out)
		columnNames := extractColumnNames(t, rows)
		expected := []string{"id", "label"}
		if !equalStringSlices(columnNames, expected) {
			t.Fatalf("INFORMATION_SCHEMA.COLUMNS for table %s = %v, want %v",
				tableName, columnNames, expected)
		}
	})

	t.Run("InformationSchemaTables", func(t *testing.T) {
		query := fmt.Sprintf(
			"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_NAME = '%s'",
			tableName,
		)
		out := runDremioSQL(t, env, query)
		_, rows := parseDremioResponseSchemaRows(t, out)
		if len(rows) == 0 {
			t.Fatalf("INFORMATION_SCHEMA.TABLES did not list %s; raw response: %s", tableName, out)
		}
	})

	t.Run("MultiLevelNamespace", func(t *testing.T) {
		// Reference is "iceberg"."<level1>"."<level2>"."<table>", relying on
		// isRecursiveAllowedNamespaces=true in the Dremio source config to
		// surface nested namespaces as nested folders.
		parts := append([]string{dremioSourceName}, append(append([]string{}, multiLevelNs...), multiLevelTable)...)
		ref := dremioObjectName(parts...)
		out := runDremioSQL(t, env, fmt.Sprintf("SELECT COUNT(*) AS row_count FROM %s", ref))
		assertSingleNumericValue(t, out, 0)
	})
}

// NewTestEnvironment creates a new test environment with allocated ports and configuration.
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

	dataDir, err := os.MkdirTemp("", "seaweed-dremio-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()
	ports := testutil.MustAllocatePorts(t, 9)

	env := &TestEnvironment{
		seaweedDir:     seaweedDir,
		weedBinary:     weedBinary,
		dataDir:        dataDir,
		bindIP:         bindIP,
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		s3Port:         ports[6],
		s3GrpcPort:     ports[7],
		icebergPort:    ports[8],
	}

	env.accessKey = "AKIAIOSFODNN7EXAMPLE"
	env.secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	return env
}

// StartSeaweedFS starts a SeaweedFS mini instance with all necessary services.
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
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(icebergURL)
		if err != nil {
			t.Logf("WARNING: Could not connect to Iceberg service at %s: %v", icebergURL, err)
		} else {
			t.Logf("WARNING: Iceberg service returned status %d at %s", resp.StatusCode, icebergURL)
			resp.Body.Close()
		}
		t.Fatalf("Iceberg REST API did not become ready")
	}
}

// Cleanup stops all processes and removes temporary resources.
func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.dremioContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.dremioContainer).Run()
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

// waitForService polls a URL until it responds with a success status or timeout is reached.
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

// testIcebergRestAPI verifies that the Iceberg REST API endpoint is responding.
func testIcebergRestAPI(t *testing.T, env *TestEnvironment) {
	t.Helper()
	fmt.Printf(">>> Testing Iceberg REST API directly...\n")

	addr := net.JoinHostPort(env.bindIP, fmt.Sprintf("%d", env.icebergPort))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Cannot connect to Iceberg service at %s: %v", addr, err)
	}
	conn.Close()
	t.Logf("Successfully connected to Iceberg service at %s", addr)

	url := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	t.Logf("Testing Iceberg REST API at %s", url)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to connect to Iceberg REST API at %s: %v", url, err)
	}
	defer resp.Body.Close()

	t.Logf("Iceberg REST API response status: %d", resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	t.Logf("Iceberg REST API response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK from /v1/config, got %d", resp.StatusCode)
	}
}

// writeDremioConfig writes a minimal dremio.conf. Iceberg REST catalog sources
// are registered after Dremio starts via POST /api/v3/catalog.
func (env *TestEnvironment) writeDremioConfig(t *testing.T, warehouseBucket string) string {
	t.Helper()
	_ = warehouseBucket

	configDir := filepath.Join(env.dataDir, "dremio")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create Dremio config dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(configDir, "dremio.conf"), []byte("{}\n"), 0644); err != nil {
		t.Fatalf("Failed to write Dremio config: %v", err)
	}

	return configDir
}

// startDremioContainer starts a Dremio Docker container with the given configuration.
// configDir's dremio.conf is bind-mounted as a single file so Dremio's default
// log4j2.properties, dremio-env, and distrib.conf in /opt/dremio/conf remain
// in place.
func (env *TestEnvironment) startDremioContainer(t *testing.T, configDir string) {
	t.Helper()

	containerName := "seaweed-dremio-" + randomString(8)
	env.dremioContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s/dremio.conf:/opt/dremio/conf/dremio.conf:ro", configDir),
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-west-2",
		"-e", "DREMIO_MAX_HEAP_MEMORY_SIZE_MB=2048",
		"-e", "DREMIO_MAX_DIRECT_MEMORY_SIZE_MB=2048",
		"-e", "DREMIO_JAVA_EXTRA_OPTS=-Ddremio.debug.sysopt.plugins.restcatalog.enabled=true",
		dremioImage,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Dremio container: %v\n%s", err, string(output))
	}
}

// dremioContainerLogs returns up to ~200 tail lines from the Dremio container,
// useful for diagnosing startup crashes.
func dremioContainerLogs(containerName string) string {
	cmd := exec.Command("docker", "logs", "--tail", "200", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(failed to fetch docker logs: %v)\n%s", err, string(output))
	}
	return string(output)
}

// waitForDremio waits for Dremio container to be ready by polling its health endpoint.
func waitForDremio(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastOutput []byte
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", containerName,
			"curl", "-fsS", "-o", "/dev/null", "http://localhost:9047/api/v2/ping",
		)
		if output, err := cmd.CombinedOutput(); err == nil {
			return
		} else {
			lastOutput = output
			outputStr := string(output)
			if strings.Contains(outputStr, "No such container") ||
				strings.Contains(outputStr, "is not running") {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	cmd := exec.Command("docker", "exec", containerName, "curl", "-I", "http://localhost:9047")
	if err := cmd.Run(); err == nil {
		time.Sleep(5 * time.Second)
		return
	}

	t.Fatalf("Timed out waiting for Dremio to be ready\nLast output:\n%s\nContainer logs:\n%s",
		string(lastOutput), dremioContainerLogs(containerName))
}

func (env *TestEnvironment) bootstrapDremio(t *testing.T, warehouseBucket string) {
	t.Helper()

	env.createDremioAdminUser(t)
	env.dremioToken = env.loginDremio(t)
	env.createDremioIcebergSource(t, warehouseBucket)
}

func (env *TestEnvironment) createDremioAdminUser(t *testing.T) {
	t.Helper()

	payload := map[string]any{
		"userName":  dremioAdminUser,
		"firstName": "Seaweed",
		"lastName":  "Admin",
		"email":     "seaweed-admin@example.com",
		"createdAt": time.Now().UnixMilli(),
		"password":  dremioAdminPassword,
	}

	deadline := time.Now().Add(90 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		status, body, err := env.dremioRequest(http.MethodPut, "/apiv2/bootstrap/firstuser", "_dremionull", payload)
		if err == nil && (status == http.StatusOK || status == http.StatusCreated || status == http.StatusConflict ||
			(status == http.StatusBadRequest && strings.Contains(strings.ToLower(body), "already"))) {
			return
		}
		last = fmt.Sprintf("status=%d err=%v body=%s", status, err, body)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("Failed to create Dremio admin user\nLast response: %s\nContainer logs:\n%s",
		last, dremioContainerLogs(env.dremioContainer))
}

func (env *TestEnvironment) loginDremio(t *testing.T) string {
	t.Helper()

	payload := map[string]string{
		"userName": dremioAdminUser,
		"password": dremioAdminPassword,
	}

	deadline := time.Now().Add(90 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		status, body, err := env.dremioRequest(http.MethodPost, "/apiv2/login", "", payload)
		if err == nil && status == http.StatusOK {
			var response struct {
				Token string `json:"token"`
			}
			if err := json.Unmarshal([]byte(body), &response); err != nil {
				t.Fatalf("Failed to decode Dremio login response: %v\nBody: %s", err, body)
			}
			if response.Token == "" {
				t.Fatalf("Dremio login returned empty token\nBody: %s", body)
			}
			return response.Token
		}
		last = fmt.Sprintf("status=%d err=%v body=%s", status, err, body)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("Failed to log in to Dremio\nLast response: %s\nContainer logs:\n%s",
		last, dremioContainerLogs(env.dremioContainer))
	return ""
}

func (env *TestEnvironment) createDremioIcebergSource(t *testing.T, warehouseBucket string) {
	t.Helper()

	s3Endpoint := fmt.Sprintf("host.docker.internal:%d", env.s3Port)
	source := map[string]any{
		"entityType": "source",
		"name":       dremioSourceName,
		"type":       "RESTCATALOG",
		"config": map[string]any{
			"restEndpointUri":              fmt.Sprintf("http://host.docker.internal:%d", env.icebergPort),
			"enableAsync":                  true,
			"isCachingEnabled":             false,
			"maxCacheSpacePct":             100,
			"isRecursiveAllowedNamespaces": true,
			"propertyList": []map[string]string{
				dremioProperty("warehouse", "s3://"+warehouseBucket),
				dremioProperty("scope", "PRINCIPAL_ROLE:ALL"),
				dremioProperty("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
				dremioProperty("fs.s3a.endpoint", s3Endpoint),
				dremioProperty("fs.s3a.path.style.access", "true"),
				dremioProperty("fs.s3a.connection.ssl.enabled", "false"),
				dremioProperty("fs.s3a.endpoint.region", "us-west-2"),
				dremioProperty("dremio.s3.compat", "true"),
				dremioProperty("dremio.s3.region", "us-west-2"),
				dremioProperty("dremio.bucket.discovery.enabled", "false"),
				dremioProperty("fs.s3a.audit.enabled", "false"),
				dremioProperty("fs.s3a.create.file-status-check", "false"),
			},
			"secretPropertyList": []map[string]string{
				dremioProperty("fs.s3a.access.key", env.accessKey),
				dremioProperty("fs.s3a.secret.key", env.secretKey),
				dremioProperty("credential", env.accessKey+":"+env.secretKey),
			},
		},
	}

	status, body, err := env.dremioRequest(http.MethodPost, "/api/v3/catalog", env.dremioAuthHeader(), source)
	if err != nil {
		t.Fatalf("Failed to create Dremio Iceberg source: %v\nBody: %s", err, body)
	}
	if status == http.StatusConflict {
		return
	}
	if status != http.StatusOK {
		t.Fatalf("Unexpected status creating Dremio Iceberg source: status=%d body=%s\nContainer logs:\n%s",
			status, body, dremioContainerLogs(env.dremioContainer))
	}

	var response struct {
		State struct {
			Status   string   `json:"status"`
			Messages []string `json:"messages"`
		} `json:"state"`
	}
	if err := json.Unmarshal([]byte(body), &response); err == nil && strings.EqualFold(response.State.Status, "bad") {
		t.Fatalf("Dremio Iceberg source was created in bad state: %v\nBody: %s\nContainer logs:\n%s",
			response.State.Messages, body, dremioContainerLogs(env.dremioContainer))
	}
}

func dremioProperty(name, value string) map[string]string {
	return map[string]string{"name": name, "value": value}
}

func (env *TestEnvironment) dremioAuthHeader() string {
	return "_dremio" + env.dremioToken
}

func (env *TestEnvironment) dremioRequest(method, path, authHeader string, payload any) (int, string, error) {
	args := []string{"exec", "-i", env.dremioContainer,
		"curl", "-sS", "--max-time", "60", "-X", method,
		"-H", "Accept: application/json",
		"-w", "\n%{http_code}",
	}
	if authHeader != "" {
		args = append(args, "-H", "Authorization: "+authHeader)
	}

	var payloadBytes []byte
	var err error
	if payload != nil {
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			return 0, "", fmt.Errorf("marshal payload: %w", err)
		}
		args = append(args, "-H", "Content-Type: application/json", "--data-binary", "@-")
	}
	args = append(args, "http://localhost:9047"+path)

	cmd := exec.Command("docker", args...)
	if payload != nil {
		cmd.Stdin = bytes.NewReader(payloadBytes)
	}

	outputBytes, err := cmd.CombinedOutput()
	output := strings.TrimRight(string(outputBytes), "\n")
	idx := strings.LastIndex(output, "\n")
	if idx < 0 {
		if err != nil {
			return 0, output, err
		}
		return 0, output, fmt.Errorf("curl output did not include HTTP status")
	}

	status, parseErr := strconv.Atoi(strings.TrimSpace(output[idx+1:]))
	body := output[:idx]
	if parseErr != nil {
		return 0, body, fmt.Errorf("parse HTTP status %q: %w", output[idx+1:], parseErr)
	}
	if err != nil {
		return status, body, err
	}
	return status, body, nil
}

// runDremioSQL submits SQL, polls the Dremio job until completion, and returns
// the job results JSON.
func runDremioSQL(t *testing.T, env *TestEnvironment, sql string) string {
	t.Helper()

	status, body, err := env.dremioRequest(http.MethodPost, "/api/v3/sql", env.dremioAuthHeader(), map[string]string{"sql": sql})
	if err != nil {
		t.Fatalf("Dremio SQL submit failed: %v\nSQL: %s\nBody: %s", err, sql, body)
	}
	if status != http.StatusOK {
		t.Fatalf("Dremio SQL submit returned status %d\nSQL: %s\nBody: %s\nContainer logs:\n%s",
			status, sql, body, dremioContainerLogs(env.dremioContainer))
	}

	var response struct {
		ID           string `json:"id"`
		ErrorMessage string `json:"errorMessage"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("Failed to decode Dremio SQL response: %v\nSQL: %s\nBody: %s", err, sql, body)
	}
	if response.ErrorMessage != "" {
		t.Fatalf("Dremio SQL submit returned error: %s\nSQL: %s\nBody: %s", response.ErrorMessage, sql, body)
	}
	if response.ID == "" {
		t.Fatalf("Dremio SQL response did not include a job id\nSQL: %s\nBody: %s", sql, body)
	}

	env.waitForDremioJob(t, response.ID, sql)

	resultsPath := fmt.Sprintf("/api/v3/job/%s/results?limit=500", url.PathEscape(response.ID))
	status, body, err = env.dremioRequest(http.MethodGet, resultsPath, env.dremioAuthHeader(), nil)
	if err != nil {
		t.Fatalf("Dremio job results request failed: %v\nSQL: %s\nBody: %s", err, sql, body)
	}
	if status != http.StatusOK {
		t.Fatalf("Dremio job results returned status %d\nSQL: %s\nBody: %s", status, sql, body)
	}
	return strings.TrimSpace(body)
}

func (env *TestEnvironment) waitForDremioJob(t *testing.T, jobID, sql string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		status, body, err := env.dremioRequest(http.MethodGet, "/api/v3/job/"+url.PathEscape(jobID), env.dremioAuthHeader(), nil)
		if err != nil {
			last = fmt.Sprintf("status=%d err=%v body=%s", status, err, body)
			time.Sleep(1 * time.Second)
			continue
		}
		if status != http.StatusOK {
			last = fmt.Sprintf("status=%d body=%s", status, body)
			time.Sleep(1 * time.Second)
			continue
		}

		var job struct {
			JobState     string `json:"jobState"`
			ErrorMessage string `json:"errorMessage"`
		}
		if err := json.Unmarshal([]byte(body), &job); err != nil {
			t.Fatalf("Failed to decode Dremio job response: %v\nSQL: %s\nBody: %s", err, sql, body)
		}

		switch job.JobState {
		case "COMPLETED":
			return
		case "FAILED", "CANCELED":
			t.Fatalf("Dremio job %s ended in %s\nSQL: %s\nError: %s\nBody: %s\nContainer logs:\n%s",
				jobID, job.JobState, sql, job.ErrorMessage, body, dremioContainerLogs(env.dremioContainer))
		default:
			last = body
			time.Sleep(1 * time.Second)
		}
	}

	t.Fatalf("Timed out waiting for Dremio job %s\nSQL: %s\nLast response: %s\nContainer logs:\n%s",
		jobID, sql, last, dremioContainerLogs(env.dremioContainer))
}

// parseDremioResponse parses the JSON response from Dremio and extracts rows.
func parseDremioResponse(t *testing.T, output string) [][]interface{} {
	t.Helper()
	_, rows := parseDremioResponseSchemaRows(t, output)
	return rows
}

// parseDremioResponseSchemaRows parses Dremio's job-results JSON and returns
// both the response schema (column names in declaration order) and the rows.
// Tests that need to assert on column metadata (column projection,
// INFORMATION_SCHEMA queries) use the schema; tests that only need values use
// parseDremioResponse.
func parseDremioResponseSchemaRows(t *testing.T, output string) ([]string, [][]interface{}) {
	t.Helper()

	var response map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		t.Fatalf("Failed to parse Dremio response as JSON: %v\nResponse: %s", err, output)
	}

	if errMsg, ok := response["errorMessage"]; ok && errMsg != "" {
		t.Fatalf("Dremio returned an error: %v", errMsg)
	}

	var schemaNames []string
	if schema, ok := response["schema"].([]interface{}); ok {
		for _, field := range schema {
			fieldMap, ok := field.(map[string]interface{})
			if !ok {
				continue
			}
			name, ok := fieldMap["name"].(string)
			if ok {
				schemaNames = append(schemaNames, name)
			}
		}
	}

	rows, ok := response["rows"].([]interface{})
	if !ok {
		t.Fatalf("Dremio response does not contain 'rows' field: %s", output)
	}

	var result [][]interface{}
	for _, row := range rows {
		switch rowData := row.(type) {
		case []interface{}:
			result = append(result, rowData)
		case map[string]interface{}:
			values := make([]interface{}, 0, len(rowData))
			if len(schemaNames) > 0 {
				for _, name := range schemaNames {
					values = append(values, rowData[name])
				}
			} else {
				keys := make([]string, 0, len(rowData))
				for key := range rowData {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					values = append(values, rowData[key])
				}
			}
			result = append(result, values)
		}
	}
	return schemaNames, result
}

// assertSchemaContainsAll fails the test if any expected column name is
// missing from the response schema. Order is not checked.
func assertSchemaContainsAll(t *testing.T, schema []string, expected ...string) {
	t.Helper()

	present := make(map[string]bool, len(schema))
	for _, name := range schema {
		present[strings.ToLower(name)] = true
	}
	var missing []string
	for _, name := range expected {
		if !present[strings.ToLower(name)] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("Dremio response schema %v missing expected columns %v", schema, missing)
	}
}

// extractColumnNames pulls the first value of each row as a string. Used to
// turn a `SELECT some_name FROM ...` result into a flat slice.
func extractColumnNames(t *testing.T, rows [][]interface{}) []string {
	t.Helper()

	names := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			t.Fatalf("Dremio result row is empty: %v", rows)
		}
		switch v := row[0].(type) {
		case string:
			names = append(names, v)
		default:
			names = append(names, fmt.Sprintf("%v", v))
		}
	}
	return names
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !strings.EqualFold(got[i], want[i]) {
			return false
		}
	}
	return true
}

func assertSingleNumericValue(t *testing.T, output string, expected float64) {
	t.Helper()

	rows := parseDremioResponse(t, output)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected one row with one value, got: %v\nOutput: %s", rows, output)
	}

	var got float64
	switch value := rows[0][0].(type) {
	case float64:
		got = value
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			t.Fatalf("Expected numeric value, got %v", rows[0][0])
		}
		got = parsed
	default:
		t.Fatalf("Expected numeric value, got %T: %v", rows[0][0], rows[0][0])
	}
	if got != expected {
		t.Fatalf("Expected numeric value %v, got %v\nOutput: %s", expected, got, output)
	}
}

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
	if tokenResp.TokenType != "bearer" {
		t.Fatalf("expected token_type=bearer, got %s", tokenResp.TokenType)
	}
	return tokenResp.AccessToken
}

func createIcebergNamespace(t *testing.T, env *TestEnvironment, token, bucketName, namespace string) {
	t.Helper()
	createIcebergNamespaceLevels(t, env, token, bucketName, []string{namespace})
}

func createIcebergTable(t *testing.T, env *TestEnvironment, token, bucketName, namespace, tableName string) {
	t.Helper()
	createIcebergTableInLevels(t, env, token, bucketName, []string{namespace}, tableName)
}

// createIcebergNamespaceLevels creates a multi-level Iceberg namespace via the
// REST catalog. Single-element levels create a flat namespace; multi-element
// levels create the nested form (e.g. ["analytics", "daily"]).
func createIcebergNamespaceLevels(t *testing.T, env *TestEnvironment, token, bucketName string, levels []string) {
	t.Helper()

	doIcebergJSONRequest(t, env, token, http.MethodPost, fmt.Sprintf("/v1/%s/namespaces", url.PathEscape(bucketName)), map[string]any{
		"namespace": levels,
	}, http.StatusOK, http.StatusConflict)
}

// createIcebergTableInLevels creates a table in a (possibly multi-level)
// namespace. The namespace path component is encoded with the unit-separator
// (0x1F) convention used by SeaweedFS's Iceberg REST API.
func createIcebergTableInLevels(t *testing.T, env *TestEnvironment, token, bucketName string, levels []string, tableName string) {
	t.Helper()

	encodedNs := strings.Join(levels, "\x1F")
	doIcebergJSONRequest(t, env, token, http.MethodPost,
		fmt.Sprintf("/v1/%s/namespaces/%s/tables", url.PathEscape(bucketName), url.PathEscape(encodedNs)),
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

func dremioObjectName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

// createTableBucket creates an S3 table bucket using `weed shell`, which
// talks to the filer over gRPC and bypasses S3 SigV4 auth (the test runs
// with IAM enabled). The master address must use `host:port.grpcPort`
// (dot, not colon).
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

func requireDremioRuntime(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping Dremio integration test")
	}
}

// hasDocker checks if Docker is available in the system.
func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// randomString generates a random string of the specified length.
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
