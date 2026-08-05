// Tests for the credential-vending access pattern.
//
// DuckDB attaches a catalog with "X-Iceberg-Access-Delegation: vended-credentials"
// and rebuilds its S3 credential out of whatever LoadTable returns in `config`,
// dropping the S3 secret it was configured with. A catalog that advertises an
// endpoint there but vends no credentials therefore leaves DuckDB sending
// unsigned requests, and every metadata and data file comes back 403.
//
// These run against a weed mini started with -s3.externalUrl, which is what
// makes the catalog advertise an endpoint at all; with a wildcard bind and no
// external URL the config is empty and the bug cannot appear.
package catalog

import (
	"encoding/json"
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

// duckDBImage tracks whatever DuckDB users are actually running, matching the
// other DuckDB tests here: a client that changes how it handles vended
// credentials is exactly what this suite exists to catch.
const duckDBImage = "duckdb/duckdb:latest"

// requireDuckDBIceberg skips when the image cannot install the iceberg
// extension -- no egress, or a build without it. Probing separately keeps the
// round trip below free to fail on any error instead of having to guess which
// DuckDB messages mean "no extension" and which mean "the catalog is broken".
func requireDuckDBIceberg(t *testing.T) {
	t.Helper()

	const ready = "iceberg extension ready"
	cmd := exec.Command("docker", "run", "--rm",
		"--entrypoint", "duckdb",
		duckDBImage,
		"-c", fmt.Sprintf("INSTALL iceberg; LOAD iceberg; SELECT '%s' AS marker;", ready),
	)
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), ready) {
		t.Skipf("DuckDB image cannot load the iceberg extension: %v\n%s", err, output)
	}
}

func TestIcebergVendedCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := newOAuthTestEnv(t)
	// The DuckDB container reaches S3 through host.docker.internal, so advertise
	// the endpoint under the name the client can actually resolve.
	env.s3ExternalURL = fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
	defer env.cleanup(t)
	env.start(t)

	bucketName := "vendcreds-" + randomSuffix()
	namespace := "vendcreds_ns_" + randomSuffix()
	tableName := "vendcreds_tbl_" + randomSuffix()

	createTableBucketViaShell(t, env, bucketName)
	token := requestOAuthToken(t, env, env.accessKey, env.secretKey)
	createNamespaceWithToken(t, env, token, bucketName, namespace)
	createTableWithToken(t, env, token, bucketName, namespace, tableName)

	t.Run("plain load table advertises the endpoint", func(t *testing.T) {
		config := loadTableConfig(t, env, token, bucketName, namespace, tableName, "")
		if config["s3.endpoint"] != env.s3ExternalURL {
			t.Fatalf("s3.endpoint = %q, want %q (clients that bring their own credentials still need the endpoint)",
				config["s3.endpoint"], env.s3ExternalURL)
		}
	})

	t.Run("vended credentials request gets no half-configured storage", func(t *testing.T) {
		config := loadTableConfig(t, env, token, bucketName, namespace, tableName, "vended-credentials")
		if config["s3.endpoint"] == "" {
			return
		}
		// If the catalog ever does vend credentials, the endpoint may come back
		// -- but only alongside the credentials that make it usable.
		if config["s3.access-key-id"] == "" || config["s3.secret-access-key"] == "" {
			t.Fatalf("LoadTable config = %v: a client that asked for vended credentials drops its own and signs with what it gets, so an endpoint without credentials makes every data file read 403", config)
		}
	})

	t.Run("duckdb writes and reads back through the catalog", func(t *testing.T) {
		if !testutil.HasDocker() {
			t.Skip("Docker not available, skipping DuckDB round trip")
		}
		requireDuckDBIceberg(t)
		duckDBRoundTrip(t, env, bucketName, namespace)
	})
}

// duckDBRoundTrip runs the reporter's flow: attach the catalog, create a table
// from a query, then read it back. Both halves need signed S3 access to the
// table's metadata and data files.
func duckDBRoundTrip(t *testing.T, env *oauthTestEnv, bucketName, namespace string) {
	t.Helper()

	const marker = "vended-credentials round trip ok"
	sql := fmt.Sprintf(`
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

ATTACH 's3://%s' AS vend_cat (
    TYPE ICEBERG,
    SECRET iceberg_secret,
    ENDPOINT 'http://host.docker.internal:%d',
    READ_ONLY false
);

CREATE TABLE vend_cat.%s.round_trip AS SELECT 42 AS answer;

SELECT CASE WHEN sum(answer) = 42 THEN '%s' ELSE 'wrong answer' END AS marker
FROM vend_cat.%s.round_trip;
`,
		env.icebergPort, env.accessKey, env.secretKey,
		env.accessKey, env.secretKey, env.s3Port,
		bucketName, env.icebergPort,
		namespace, marker, namespace,
	)

	sqlFile := filepath.Join(env.dataDir, "duckdb_vended_credentials.sql")
	if err := os.WriteFile(sqlFile, []byte(sql), 0644); err != nil {
		t.Fatalf("write SQL file: %v", err)
	}

	cmd := exec.Command("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"-e", "AWS_REGION=us-east-1",
		"--entrypoint", "duckdb",
		duckDBImage,
		"-init", "/test/duckdb_vended_credentials.sql",
		"-c", "SELECT 1",
	)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	t.Logf("DuckDB output:\n%s", outputStr)

	// Nothing below skips: requireDuckDBIceberg already established that the
	// extension loads, so any failure from here is the catalog's.
	if strings.Contains(outputStr, "trying to refresh secret") {
		t.Fatalf("DuckDB fell back to refreshing the credential the catalog vended, which no stage-created table can satisfy:\n%s", outputStr)
	}
	// Match the phrasings rather than a bare "403", which a random port could
	// carry.
	for _, denial := range []string{"AccessDenied", "403 Forbidden", "code 403"} {
		if strings.Contains(outputStr, denial) {
			t.Fatalf("DuckDB signed its S3 requests with the catalog's credential-less config:\n%s", outputStr)
		}
	}
	if !strings.Contains(outputStr, marker) {
		t.Fatalf("DuckDB did not write and read the table back (err=%v):\n%s", err, outputStr)
	}
}

// loadTableConfig loads a table and returns the FileIO config the catalog
// advertises, optionally asking for an access delegation mechanism.
func loadTableConfig(t *testing.T, env *oauthTestEnv, token, bucketName, namespace, tableName, delegation string) map[string]string {
	t.Helper()

	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", env.icebergURL(), bucketName, namespace, tableName)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if delegation != "" {
		req.Header.Set("X-Iceberg-Access-Delegation", delegation)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("load table failed: status=%d body=%s", resp.StatusCode, body)
	}

	var result struct {
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode LoadTableResult: %v", err)
	}
	return result.Config
}
