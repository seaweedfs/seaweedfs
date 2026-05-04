package catalog

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

// TestDuckDBDeterministicLocationRead validates that DuckDB's iceberg
// extension can resolve and scan a table that lives at the deterministic
// <bucket>/<namespace>/<tableName> path produced when no client-side UUID
// is appended to the table location. This is the layout produced by:
//
//   - iceberg-spark (no concept of UUID-suffixed locations)
//   - pyiceberg (no concept of UUID-suffixed locations)
//   - Trino with iceberg.unique-table-location=false
//
// Post-#9246, REST clients can resolve table locations under a namespace
// because GetNamespace advertises a default `location` property. This
// test creates a namespace + table via the REST API directly (so no
// client-side UUID is added), then asks DuckDB to ATTACH the catalog and
// describe the table — verifying DuckDB walks the catalog → metadata →
// schema chain successfully against a deterministic on-disk path.
//
// The test does not insert data files — DuckDB scanning an empty table
// is sufficient to validate the catalog/metadata resolution path. If
// DuckDB's iceberg extension is unavailable or the build is too old to
// support the ATTACH syntax, the test skips rather than fails.
func TestDuckDBDeterministicLocationRead(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping DuckDB integration test")
	}

	env := newOAuthTestEnv(t)
	defer env.cleanup(t)
	env.start(t)

	bucketName := "duckdb-detloc-" + randomSuffix()
	namespace := "detloc_ns_" + randomSuffix()
	tableName := "detloc_tbl_" + randomSuffix()

	createTableBucketViaShell(t, env, bucketName)

	token := requestOAuthToken(t, env, env.accessKey, env.secretKey)
	createNamespaceWithToken(t, env, token, bucketName, namespace)
	createIcebergTableWithToken(t, env, token, bucketName, namespace, tableName)

	// Sanity-check the catalog row exposes a deterministic location.
	tableLoc := getTableLocation(t, env, token, bucketName, namespace, tableName)
	wantPrefix := fmt.Sprintf("s3://%s/%s/%s", bucketName, namespace, tableName)
	if !strings.HasPrefix(tableLoc, wantPrefix) {
		t.Fatalf("table location %q should start with %q (deterministic, no UUID suffix)", tableLoc, wantPrefix)
	}
	if strings.Contains(strings.TrimPrefix(tableLoc, wantPrefix), "-") {
		t.Fatalf("table location %q should not have a -<uuid> suffix after %q", tableLoc, wantPrefix)
	}

	sqlContent := fmt.Sprintf(`
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET iceberg_secret (
    TYPE ICEBERG,
    ENDPOINT 'http://host.docker.internal:%d',
    CLIENT_ID '%s',
    CLIENT_SECRET '%s'
);

CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID '%s',
    SECRET '%s',
    ENDPOINT 'host.docker.internal:%d',
    URL_STYLE 'path',
    USE_SSL false
);

ATTACH 's3://%s/' AS detloc_cat (TYPE 'ICEBERG', SECRET iceberg_secret);

DESCRIBE detloc_cat.%s.%s;

SELECT 'duckdb deterministic-location describe ok' AS marker;
`,
		env.icebergPort, env.accessKey, env.secretKey,
		env.accessKey, env.secretKey, env.s3Port,
		bucketName,
		namespace, tableName,
	)

	sqlFile := filepath.Join(env.dataDir, "duckdb_detloc.sql")
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0644); err != nil {
		t.Fatalf("write SQL file: %v", err)
	}

	cmd := exec.Command("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb",
		"duckdb/duckdb:latest",
		"-init", "/test/duckdb_detloc.sql",
		"-c", "SELECT 1",
	)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	t.Logf("DuckDB output:\n%s", outputStr)

	if err != nil {
		// Tolerate environments where the Iceberg extension/version is too
		// limited to exercise this code path. The intent is to gain
		// coverage where it's available, not to require it everywhere.
		if strings.Contains(outputStr, "iceberg extension is not available") ||
			strings.Contains(outputStr, "Failed to load") ||
			strings.Contains(outputStr, "syntax error") ||
			strings.Contains(outputStr, "Unknown extension") {
			t.Skipf("DuckDB image lacks the required iceberg extension or syntax: %v", err)
		}
		t.Fatalf("DuckDB run failed: %v\nOutput:\n%s", err, outputStr)
	}

	if !strings.Contains(outputStr, "duckdb deterministic-location describe ok") {
		t.Fatalf("DuckDB DESCRIBE on deterministic-location table did not complete successfully:\n%s", outputStr)
	}
}

// createIcebergTableWithToken creates a minimal Iceberg table via the REST
// catalog using the provided OAuth bearer token. The table is created
// without an explicit location, so the server picks the deterministic
// <bucket>/<namespace>/<tableName> path.
func createIcebergTableWithToken(t *testing.T, env *oauthTestEnv, token, bucketName, namespace, tableName string) {
	t.Helper()

	body := fmt.Sprintf(`{
  "name": %q,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": true, "type": "long"},
      {"id": 2, "name": "label", "required": false, "type": "string"}
    ]
  }
}`, tableName)

	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables", env.icebergURL(), bucketName, namespace)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create table request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create table failed: status=%d body=%s", resp.StatusCode, respBody)
	}
	t.Logf("Created table %s.%s in bucket %s", namespace, tableName, bucketName)
}

// getTableLocation fetches the table's metadata via the REST catalog and
// returns the location field reported in the response.
func getTableLocation(t *testing.T, env *oauthTestEnv, token, bucketName, namespace, tableName string) string {
	t.Helper()

	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", env.icebergURL(), bucketName, namespace, tableName)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("load table request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("load table failed: status=%d body=%s", resp.StatusCode, respBody)
	}
	// The metadata.location field is nested. A naive substring match is
	// sufficient for asserting the expected path shape.
	body := string(respBody)
	const key = `"location":"`
	idx := strings.Index(body, key)
	if idx < 0 {
		t.Fatalf("LoadTable response missing location field: %s", body)
	}
	rest := body[idx+len(key):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("malformed location field: %s", body)
	}
	return rest[:end]
}
