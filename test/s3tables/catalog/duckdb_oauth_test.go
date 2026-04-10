package catalog

import (
	"context"
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

// oauthTestEnv holds a weed mini instance started with IAM credentials
// so that the OAuth token endpoint is functional.
type oauthTestEnv struct {
	seaweedDir     string
	weedBinary     string
	dataDir        string
	bindIP         string
	s3Port         int
	s3GrpcPort     int
	icebergPort    int
	masterPort     int
	masterGrpcPort int
	filerPort      int
	filerGrpcPort  int
	volumePort     int
	volumeGrpcPort int
	webdavPort     int
	weedProcess    *exec.Cmd
	weedCancel     context.CancelFunc
	accessKey      string
	secretKey      string
}

func newOAuthTestEnv(t *testing.T) *oauthTestEnv {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	seaweedDir := wd
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

	dataDir, err := os.MkdirTemp("", "seaweed-oauth-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()
	ports := testutil.MustAllocatePorts(t, 10)

	return &oauthTestEnv{
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
		webdavPort:     ports[9],
		accessKey:      "AKIAIOSFODNN7EXAMPLE",
		secretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
}

func (env *oauthTestEnv) start(t *testing.T) {
	t.Helper()

	iamConfigPath, err := testutil.WriteIAMConfig(env.dataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("write IAM config: %v", err)
	}

	securityToml := filepath.Join(env.dataDir, "security.toml")
	if err := os.WriteFile(securityToml, []byte("# Empty security config for testing\n"), 0644); err != nil {
		t.Fatalf("write security.toml: %v", err)
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
		"-webdav.port", fmt.Sprintf("%d", env.webdavPort),
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
		cancel()
		t.Fatalf("start weed mini: %v", err)
	}
	env.weedProcess = cmd

	icebergURL := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	if !testutil.WaitForService(icebergURL, 30*time.Second) {
		cancel()
		cmd.Wait()
		t.Fatalf("Iceberg REST API did not become ready at %s", icebergURL)
	}
}

func (env *oauthTestEnv) cleanup(t *testing.T) {
	t.Helper()
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedProcess != nil {
		env.weedProcess.Wait()
	}
	if env.dataDir != "" {
		os.RemoveAll(env.dataDir)
	}
}

func (env *oauthTestEnv) icebergURL() string {
	return fmt.Sprintf("http://%s:%d", env.bindIP, env.icebergPort)
}

// TestOAuthTokenEndpoint tests the /v1/oauth/tokens endpoint directly via HTTP.
func TestOAuthTokenEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := newOAuthTestEnv(t)
	defer env.cleanup(t)
	env.start(t)

	t.Run("valid credentials", func(t *testing.T) {
		token := requestOAuthToken(t, env, env.accessKey, env.secretKey)
		if token == "" {
			t.Fatal("expected non-empty token")
		}
	})

	t.Run("invalid secret", func(t *testing.T) {
		resp, err := http.PostForm(env.icebergURL()+"/v1/oauth/tokens", url.Values{
			"grant_type":    {"client_credentials"},
			"client_id":     {env.accessKey},
			"client_secret": {"wrong-secret"},
		})
		if err != nil {
			t.Fatalf("POST /v1/oauth/tokens: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 401, got %d: %s", resp.StatusCode, body)
		}
	})

	t.Run("missing grant_type", func(t *testing.T) {
		resp, err := http.PostForm(env.icebergURL()+"/v1/oauth/tokens", url.Values{
			"client_id":     {env.accessKey},
			"client_secret": {env.secretKey},
		})
		if err != nil {
			t.Fatalf("POST /v1/oauth/tokens: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
		}
	})

	t.Run("bearer token auth on catalog endpoint", func(t *testing.T) {
		token := requestOAuthToken(t, env, env.accessKey, env.secretKey)

		// Use the token to call the catalog
		req, err := http.NewRequest(http.MethodGet, env.icebergURL()+"/v1/namespaces", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /v1/namespaces with Bearer: %v", err)
		}
		defer resp.Body.Close()

		// Auth should pass. We accept 200 (success) or 500 (missing warehouse bucket
		// is an internal error, not an auth error). Reject 401/403/404/405.
		body, _ := io.ReadAll(resp.Body)
		switch resp.StatusCode {
		case http.StatusOK, http.StatusInternalServerError:
			t.Logf("Bearer token auth succeeded, status=%d", resp.StatusCode)
		default:
			t.Fatalf("Bearer auth failed unexpectedly: status=%d body=%s", resp.StatusCode, body)
		}
	})
}

// TestDuckDBOAuthIntegration tests that DuckDB can connect to the Iceberg REST
// catalog using the OAuth2 client_credentials flow (CREATE SECRET with client_id
// and client_secret). This is the scenario reported in issue #9015.
func TestDuckDBOAuthIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping DuckDB OAuth integration test")
	}

	env := newOAuthTestEnv(t)
	defer env.cleanup(t)
	env.start(t)

	// Create a table bucket and namespace so DuckDB has something to query
	bucketName := "duckdb-oauth-" + randomSuffix()
	createTableBucketViaShell(t, env, bucketName)

	// Create a namespace via the Iceberg REST API using OAuth token
	token := requestOAuthToken(t, env, env.accessKey, env.secretKey)
	createNamespaceWithToken(t, env, token, bucketName, "testns")

	sqlContent := fmt.Sprintf(`
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET iceberg_secret (
    TYPE ICEBERG,
    ENDPOINT 'http://host.docker.internal:%d',
    CLIENT_ID '%s',
    CLIENT_SECRET '%s',
    SCOPE 's3://%s/'
);

CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID '%s',
    SECRET '%s',
    ENDPOINT 'host.docker.internal:%d',
    URL_STYLE 'path',
    USE_SSL false,
    SCOPE 's3://%s/'
);

SELECT 'OAuth token obtained successfully' as status;

-- Try listing namespaces via the Iceberg catalog
SELECT * FROM iceberg_scan('iceberg_secret', ALLOW_MOVED_PATHS => TRUE) LIMIT 0;
`, env.icebergPort, env.accessKey, env.secretKey, bucketName,
		env.accessKey, env.secretKey, env.s3Port, bucketName)

	sqlFile := filepath.Join(env.dataDir, "duckdb_oauth_test.sql")
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0644); err != nil {
		t.Fatalf("write SQL file: %v", err)
	}

	// Run DuckDB in Docker.
	// We use a simple test: get a token, create a secret, and verify the
	// Iceberg extension can communicate with the catalog.
	// The simpler fallback test just verifies CREATE SECRET succeeds (no 404).
	fallbackSQL := fmt.Sprintf(`
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET (
    TYPE ICEBERG,
    ENDPOINT 'http://host.docker.internal:%d',
    CLIENT_ID '%s',
    CLIENT_SECRET '%s'
);

SELECT 'DuckDB Iceberg OAuth secret created successfully' as result;
`, env.icebergPort, env.accessKey, env.secretKey)

	fallbackFile := filepath.Join(env.dataDir, "duckdb_oauth_fallback.sql")
	if err := os.WriteFile(fallbackFile, []byte(fallbackSQL), 0644); err != nil {
		t.Fatalf("write fallback SQL file: %v", err)
	}

	// Try the full SQL script first (CREATE SECRET + catalog access).
	// Fall back to the simple CREATE SECRET test if iceberg_scan isn't supported.
	cmd := exec.Command("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb",
		"duckdb/duckdb:latest",
		"-init", "/test/duckdb_oauth_test.sql",
		"-c", "SELECT 1",
	)

	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	t.Logf("DuckDB output:\n%s", outputStr)

	if err != nil {
		if strings.Contains(outputStr, "iceberg extension is not available") ||
			strings.Contains(outputStr, "Failed to load") {
			t.Skip("Skipping: Iceberg extension not available in DuckDB Docker image")
		}
		// The key check: the old error was "HTTP NotFound_404" on /v1/oauth/tokens.
		// With our fix, this should no longer happen.
		if strings.Contains(outputStr, "NotFound_404") && strings.Contains(outputStr, "/v1/oauth/tokens") {
			t.Fatal("OAuth token endpoint returned 404 - the fix is not working")
		}
		// If iceberg_scan failed but CREATE SECRET worked, fall back to simpler test
		if strings.Contains(outputStr, "OAuth token obtained successfully") {
			t.Logf("Full SQL had partial success (token obtained), iceberg_scan may not be supported. Continuing.")
		} else {
			// Try the fallback script that only tests CREATE SECRET
			t.Logf("Full SQL failed, trying fallback CREATE SECRET test...")
			fallbackCmd := exec.Command("docker", "run", "--rm",
				"-v", fmt.Sprintf("%s:/test", env.dataDir),
				"--add-host", "host.docker.internal:host-gateway",
				"--entrypoint", "duckdb",
				"duckdb/duckdb:latest",
				"-init", "/test/duckdb_oauth_fallback.sql",
				"-c", "SELECT 1",
			)
			fallbackOutput, fallbackErr := fallbackCmd.CombinedOutput()
			fallbackStr := string(fallbackOutput)
			t.Logf("DuckDB fallback output:\n%s", fallbackStr)

			if fallbackErr != nil {
				if strings.Contains(fallbackStr, "NotFound_404") && strings.Contains(fallbackStr, "/v1/oauth/tokens") {
					t.Fatal("OAuth token endpoint returned 404 - the fix is not working")
				}
				t.Fatalf("DuckDB fallback also failed: %v\nOutput: %s", fallbackErr, fallbackStr)
			}
			if !strings.Contains(fallbackStr, "DuckDB Iceberg OAuth secret created successfully") {
				t.Errorf("expected success message in fallback output, got:\n%s", fallbackStr)
			}
			return
		}
	}

	if strings.Contains(outputStr, "OAuth token obtained successfully") {
		t.Logf("DuckDB OAuth CREATE SECRET succeeded")
	} else {
		t.Errorf("expected OAuth success message in output, got:\n%s", outputStr)
	}
}

// requestOAuthToken obtains a bearer token from the OAuth endpoint.
func requestOAuthToken(t *testing.T, env *oauthTestEnv, accessKey, secretKey string) string {
	t.Helper()

	resp, err := http.PostForm(env.icebergURL()+"/v1/oauth/tokens", url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {accessKey},
		"client_secret": {secretKey},
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
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		t.Fatalf("decode token response: %v", err)
	}

	if tokenResp.AccessToken == "" {
		t.Fatal("got empty access_token")
	}
	if tokenResp.TokenType != "bearer" {
		t.Errorf("expected token_type=bearer, got %s", tokenResp.TokenType)
	}
	return tokenResp.AccessToken
}

// createTableBucketViaShell creates a table bucket using weed shell,
// which bypasses S3 auth. This is the same approach used by the Trino tests.
func createTableBucketViaShell(t *testing.T, env *oauthTestEnv, bucketName string) {
	t.Helper()

	cmd := exec.Command(env.weedBinary, "shell",
		fmt.Sprintf("-master=%s:%d.%d", env.bindIP, env.masterPort, env.masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf("s3tables.bucket -create -name %s -account 000000000000\nexit\n", bucketName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("create table bucket %s via weed shell: %v\nOutput: %s", bucketName, err, output)
	}
	t.Logf("Created table bucket %s", bucketName)
}

// createNamespaceWithToken creates a namespace using a Bearer token.
func createNamespaceWithToken(t *testing.T, env *oauthTestEnv, token, bucketName, namespace string) {
	t.Helper()

	path := fmt.Sprintf("/v1/%s/namespaces", bucketName)
	body := fmt.Sprintf(`{"namespace":["%s"]}`, namespace)
	req, err := http.NewRequest(http.MethodPost, env.icebergURL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create namespace failed: status=%d body=%s", resp.StatusCode, respBody)
	}
	t.Logf("Created namespace %s in bucket %s", namespace, bucketName)
}
