// Reproduction tests for https://github.com/seaweedfs/seaweedfs/issues/9103
//
// The issue reports two distinct failure modes when using DuckDB against the
// SeaweedFS Iceberg REST catalog:
//
//  1. `ATTACH 's3://test/' AS cat (TYPE 'ICEBERG', ...); SELECT * FROM cat.ovirt.disk;`
//     fails with "Table with name 'ovirt.disk' does not exist because schema
//     'ovirt' does not exist." The namespace "ovirt" does exist in the bucket.
//
//  2. `SELECT * FROM iceberg_scan('s3://test/ovirt/disk');` fails with HTTP 403
//     AccessDenied when DuckDB tries to glob `s3://test/ovirt/disk/metadata/v*`
//     because the LoadTable response does not vend S3 file-io credentials back
//     to the client.
//
// These tests reproduce the underlying catalog-level misbehavior at the REST
// protocol layer so they run quickly and deterministically. A DuckDB-based
// end-to-end reproduction is also included (gated on Docker availability).
package catalog

import (
	"bytes"
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

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

// TestIssue9103_ConfigDoesNotVendWarehousePrefix reproduces failure mode #2.
//
// Per the Iceberg REST spec, a client attaching with warehouse=s3://<bucket>/
// calls GET /v1/config?warehouse=s3://<bucket>/ and expects the server to
// return an `overrides.prefix` that identifies the catalog namespace for
// subsequent requests. Without it the client falls back to unprefixed paths
// like /v1/namespaces, which on SeaweedFS resolve to the hard-coded default
// bucket ("warehouse") and therefore do not list the user's namespaces.
//
// Currently the handler ignores the warehouse query parameter and returns
// empty defaults/overrides. That is exactly what makes DuckDB's ATTACH flow
// report `schema "ovirt" does not exist`.
func TestIssue9103_ConfigDoesNotVendWarehousePrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := sharedEnv
	bucketName := "warehouse-9103cfg-" + randomSuffix()
	createTableBucket(t, env, bucketName)

	warehouse := fmt.Sprintf("s3://%s/", bucketName)
	u := fmt.Sprintf("%s/v1/config?warehouse=%s", env.IcebergURL(), url.QueryEscape(warehouse))

	status, body, err := doIcebergJSONRequest(env, http.MethodGet, fmt.Sprintf("/v1/config?warehouse=%s", url.QueryEscape(warehouse)), nil)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	if status != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", u, status)
	}

	overrides, _ := body["overrides"].(map[string]any)
	gotPrefix, _ := overrides["prefix"].(string)
	if gotPrefix != bucketName {
		t.Fatalf("GET /v1/config?warehouse=%s: overrides.prefix = %q, want %q (catalog must echo the warehouse's table bucket so clients like DuckDB know which /v1/{prefix}/namespaces to use)",
			warehouse, gotPrefix, bucketName)
	}
}

// TestIssue9103_BareNamespacesListMissesNamespaceInAttachedBucket
// demonstrates the downstream effect of the /v1/config bug.
//
// The client created the namespace "ovirt" inside bucket "test" (via
// /v1/{bucket}/namespaces), but when it later issues GET /v1/namespaces (no
// prefix, because /v1/config didn't give it one), that request resolves to
// the default bucket and returns no namespaces. DuckDB surfaces this as
// `schema "ovirt" does not exist`.
func TestIssue9103_BareNamespacesListMissesNamespaceInAttachedBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := sharedEnv
	bucketName := "warehouse-9103ns-" + randomSuffix()
	createTableBucket(t, env, bucketName)

	namespace := "ovirt"
	status, _, err := doIcebergJSONRequest(env, http.MethodPost,
		icebergPath(bucketName, "/v1/namespaces"),
		map[string]any{"namespace": []string{namespace}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if status != http.StatusOK && status != http.StatusConflict {
		t.Fatalf("create namespace status = %d, want 200 or 409", status)
	}

	// Sanity: prefixed listing finds the namespace.
	status, prefixedBody, err := doIcebergJSONRequest(env, http.MethodGet,
		icebergPath(bucketName, "/v1/namespaces"), nil)
	if err != nil {
		t.Fatalf("list namespaces with prefix: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("prefixed list status = %d, want 200", status)
	}
	if !containsNamespace(prefixedBody, namespace) {
		t.Fatalf("prefixed list missing namespace %q: %v", namespace, prefixedBody)
	}

	// Simulate what a DuckDB-style client does when /v1/config did not vend a
	// prefix: it falls back to the unprefixed listing. It should still be
	// able to discover the namespace (e.g. via a warehouse query parameter),
	// but today it cannot, which is the user-visible bug.
	bareWarehouse := "s3://" + bucketName + "/"
	status, bareBody, err := doIcebergJSONRequest(env, http.MethodGet,
		fmt.Sprintf("/v1/namespaces?warehouse=%s", url.QueryEscape(bareWarehouse)), nil)
	if err != nil {
		t.Fatalf("bare list namespaces: %v", err)
	}
	switch {
	case status != http.StatusOK:
		t.Fatalf("GET /v1/namespaces?warehouse=%s status = %d, want 200 (server ignores the warehouse query parameter and falls back to the default %q bucket, causing DuckDB to see no schemas)",
			bareWarehouse, status, "warehouse")
	case !containsNamespace(bareBody, namespace):
		t.Fatalf("GET /v1/namespaces?warehouse=%s did not return %q: %v (server must honor the warehouse query parameter here or vend overrides.prefix from /v1/config so the client can route to the right bucket)",
			bareWarehouse, namespace, bareBody)
	}
}

// TestIssue9103_LoadTableDoesNotVendS3FileIOCredentials reproduces failure
// mode #1 at the catalog level.
//
// When a client loads a table it expects the response `config` map to
// include FileIO properties (at minimum s3.access-key-id,
// s3.secret-access-key, s3.endpoint, s3.path-style-access) so it can read
// the table's data files directly from S3 without separately configuring a
// second S3 credential. SeaweedFS currently returns `config: {}`, forcing
// the user to know and configure the S3 endpoint out-of-band; when they
// don't, DuckDB's iceberg_scan fails with HTTP 403 on the metadata glob
// exactly as the issue describes.
func TestIssue9103_LoadTableDoesNotVendS3FileIOCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := sharedEnv
	bucketName := "warehouse-9103cfg2-" + randomSuffix()
	createTableBucket(t, env, bucketName)

	namespace := "ovirt"
	tableName := "disk"

	status, _, err := doIcebergJSONRequest(env, http.MethodPost,
		icebergPath(bucketName, "/v1/namespaces"),
		map[string]any{"namespace": []string{namespace}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if status != http.StatusOK && status != http.StatusConflict {
		t.Fatalf("create namespace status = %d, want 200 or 409", status)
	}

	status, _, err = doIcebergJSONRequest(env, http.MethodPost,
		icebergPath(bucketName, fmt.Sprintf("/v1/namespaces/%s/tables", namespace)),
		map[string]any{
			"name": tableName,
			"schema": map[string]any{
				"type":      "struct",
				"schema-id": 0,
				"fields": []map[string]any{
					{"id": 1, "name": "id", "required": true, "type": "long"},
				},
			},
		})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("create table status = %d, want 200", status)
	}

	status, loadResp, err := doIcebergJSONRequest(env, http.MethodGet,
		icebergPath(bucketName, fmt.Sprintf("/v1/namespaces/%s/tables/%s", namespace, tableName)), nil)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("load table status = %d, want 200", status)
	}

	config, _ := loadResp["config"].(map[string]any)
	required := []string{"s3.endpoint", "s3.path-style-access"}
	var missing []string
	for _, key := range required {
		if v, ok := config[key].(string); !ok || v == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("LoadTable response config missing FileIO keys %v (got %v). Without vended credentials DuckDB falls back to its own S3 config and fails with 403 when reading %s metadata.",
			missing, config, fmt.Sprintf("s3://%s/%s/%s/metadata/v*", bucketName, namespace, tableName))
	}
	if got := config["s3.path-style-access"]; got != "true" {
		t.Fatalf("LoadTable response s3.path-style-access = %v, want %q (SeaweedFS is path-style only)", got, "true")
	}
}

// TestIssue9103_DuckDBAttachCannotResolveNamespace is the end-to-end
// reproduction of the issue using DuckDB in Docker, mirroring the exact
// sequence of commands from the bug report. It runs under its own weed
// mini with IAM configured so the OAuth2 client_credentials flow that
// DuckDB's iceberg extension requires actually works (the shared env has
// no credentials registered). Gated on Docker availability.
func TestIssue9103_DuckDBAttachCannotResolveNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !testutil.HasDocker() {
		t.Skip("Docker not available, skipping DuckDB integration reproduction")
	}

	env := newOAuthTestEnv(t)
	defer env.cleanup(t)
	env.start(t)

	bucketName := "test9103-" + randomSuffix()
	createTableBucketViaShell(t, env, bucketName)

	namespace := "ovirt"
	tableName := "disk"

	token := requestOAuthToken(t, env, env.accessKey, env.secretKey)
	createNamespaceWithToken(t, env, token, bucketName, namespace)
	createTableWithToken(t, env, token, bucketName, namespace, tableName)

	sql := fmt.Sprintf(`
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET test_berg (
    TYPE ICEBERG,
    ENDPOINT 'http://host.docker.internal:%d',
    SCOPE 's3://%s/',
    CLIENT_ID '%s',
    CLIENT_SECRET '%s'
);

CREATE SECRET s3_berg (
    TYPE S3,
    KEY_ID '%s',
    SECRET '%s',
    ENDPOINT 'host.docker.internal:%d',
    URL_STYLE 'path',
    USE_SSL false,
    SCOPE 's3://%s/'
);

ATTACH 's3://%s/' AS test_catalog (TYPE 'ICEBERG', secret test_berg);
SELECT * FROM test_catalog.%s.%s LIMIT 0;
`, env.icebergPort, bucketName,
		env.accessKey, env.secretKey,
		env.accessKey, env.secretKey, env.s3Port, bucketName,
		bucketName, namespace, tableName)

	sqlFile := filepath.Join(env.dataDir, "issue_9103.sql")
	if err := os.WriteFile(sqlFile, []byte(sql), 0644); err != nil {
		t.Fatalf("write SQL: %v", err)
	}

	cmd := exec.Command("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb",
		"duckdb/duckdb:latest",
		"-init", "/test/issue_9103.sql",
		"-c", "SELECT 1",
	)

	out, runErr := cmd.CombinedOutput()
	outStr := string(out)
	t.Logf("DuckDB output:\n%s", outStr)

	if strings.Contains(outStr, "iceberg extension is not available") ||
		strings.Contains(outStr, "Failed to load") ||
		strings.Contains(outStr, "could not fetch extension") ||
		strings.Contains(outStr, "failed to download") {
		t.Skip("Iceberg extension not available in DuckDB Docker image")
	}

	if runErr != nil {
		if strings.Contains(outStr, "does not exist because schema") &&
			strings.Contains(outStr, namespace) {
			t.Fatalf("reproduced issue #9103: DuckDB cannot see namespace %q after ATTACH 's3://%s/'; see output above",
				namespace, bucketName)
		}
		t.Fatalf("DuckDB run failed for an unexpected reason: %v", runErr)
	}
}

// createTableWithToken creates an Iceberg table using an OAuth bearer token.
func createTableWithToken(t *testing.T, env *oauthTestEnv, token, bucketName, namespace, tableName string) {
	t.Helper()

	payload := map[string]any{
		"name": tableName,
		"schema": map[string]any{
			"type":      "struct",
			"schema-id": 0,
			"fields": []map[string]any{
				{"id": 1, "name": "id", "required": true, "type": "long"},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal create-table payload: %v", err)
	}

	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables", env.icebergURL(), bucketName, namespace)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
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

// containsNamespace checks whether an Iceberg REST ListNamespaces response
// contains the given single-level namespace.
func containsNamespace(body map[string]any, name string) bool {
	arr, _ := body["namespaces"].([]any)
	for _, item := range arr {
		parts, _ := item.([]any)
		if len(parts) == 1 {
			if s, ok := parts[0].(string); ok && s == name {
				return true
			}
		}
	}
	return false
}
