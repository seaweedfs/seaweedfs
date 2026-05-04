// Package catalog_doris provides integration tests for Apache Doris with the
// SeaweedFS Iceberg REST Catalog.
package catalog_doris

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	dorisImage        = "apache/doris:doris-all-in-one-2.1.0"
	dorisCatalogName  = "iceberg_catalog"
	dorisQueryPort    = 9030
	dorisStartTimeout = 4 * time.Minute
)

type TestEnvironment struct {
	seaweedDir         string
	weedBinary         string
	dataDir            string
	bindIP             string
	s3Port             int
	s3GrpcPort         int
	icebergPort        int
	masterPort         int
	masterGrpcPort     int
	filerPort          int
	filerGrpcPort      int
	volumePort         int
	volumeGrpcPort     int
	weedProcess        *exec.Cmd
	weedCancel         context.CancelFunc
	dorisContainer     string
	dorisHostQueryPort int
	accessKey          string
	secretKey          string
}

// TestDorisIcebergCatalog brings up SeaweedFS + Doris and validates that
// Doris can discover catalog metadata served by SeaweedFS's Iceberg REST API
// and read both empty and populated tables through the standard data path.
//
// Subtests:
//   - BasicSelect: Doris is alive and answering SQL.
//   - CatalogVisible: SHOW CATALOGS lists the SeaweedFS-backed catalog.
//   - DatabaseVisible: the seeded namespace is visible as a database.
//   - TableVisible: the seeded table is listed under the namespace.
//   - CountEmptyTable: Doris resolves the table and scans an empty Iceberg snapshot.
//   - ColumnProjection: Doris parsed the schema and accepts column-name projection
//     (a column-not-found here means the schema returned by SeaweedFS was rejected).
//   - ReadWrittenDataCount / ReadWrittenDataValues: a separate table is populated
//     by a PyIceberg writer container before Doris connects; Doris then reads the
//     three rows back, exercising the actual data path (not just metadata).
func TestDorisIcebergCatalog(t *testing.T) {
	requireDorisRuntime(t)

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

	namespace := "doris_" + randomString(6)
	tableName := "smoke_" + randomString(6)
	icebergToken := requestIcebergOAuthToken(t, env)
	createIcebergNamespace(t, env, icebergToken, tableBucket, namespace)
	createIcebergTable(t, env, icebergToken, tableBucket, namespace, tableName)

	// Seed a populated table by creating an empty one through the REST API
	// and then appending three rows via PyIceberg. Done before Doris bootstrap
	// so the snapshot is part of the catalog's first scan.
	populatedTable := "populated_" + randomString(6)
	createIcebergTable(t, env, icebergToken, tableBucket, namespace, populatedTable)
	buildDorisWriterImage(t)
	writeIcebergRows(t, env, tableBucket, []string{namespace}, populatedTable)

	env.startDorisContainer(t)
	env.waitForDoris(t, dorisStartTimeout)
	db := env.connectDoris(t)
	defer db.Close()

	env.createDorisIcebergCatalog(t, db, tableBucket)

	t.Run("BasicSelect", func(t *testing.T) {
		var v int
		if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("SELECT 1: %v", err)
		}
		if v != 1 {
			t.Fatalf("SELECT 1 = %d, want 1", v)
		}
	})

	t.Run("CatalogVisible", func(t *testing.T) {
		// Doris's SHOW CATALOGS columns vary between versions. The catalog
		// name appears in the first column for all releases that ship the
		// Iceberg REST connector, so a per-row substring check is enough.
		if !rowsContain(t, db, "SHOW CATALOGS", dorisCatalogName) {
			t.Fatalf("SHOW CATALOGS did not list %s", dorisCatalogName)
		}
	})

	t.Run("DatabaseVisible", func(t *testing.T) {
		query := fmt.Sprintf("SHOW DATABASES FROM %s", quoteDorisIdent(dorisCatalogName))
		if !rowsContain(t, db, query, namespace) {
			t.Fatalf("SHOW DATABASES from %s did not list namespace %s", dorisCatalogName, namespace)
		}
	})

	t.Run("TableVisible", func(t *testing.T) {
		query := fmt.Sprintf("SHOW TABLES FROM %s.%s",
			quoteDorisIdent(dorisCatalogName), quoteDorisIdent(namespace))
		if !rowsContain(t, db, query, tableName) {
			t.Fatalf("SHOW TABLES from %s.%s did not list %s", dorisCatalogName, namespace, tableName)
		}
	})

	tableRef := dorisObjectName(dorisCatalogName, namespace, tableName)

	t.Run("CountEmptyTable", func(t *testing.T) {
		var count int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableRef)
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != 0 {
			t.Fatalf("count(%s) = %d, want 0", tableRef, count)
		}
	})

	t.Run("ColumnProjection", func(t *testing.T) {
		// SELECT COUNT(*) doesn't exercise the schema. A projection by column
		// name fails fast with "column not found" if the catalog's schema
		// response wasn't parsed by Doris.
		query := fmt.Sprintf("SELECT id, label FROM %s", tableRef)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Columns(): %v", err)
		}
		if !equalStringSlicesIgnoreCase(cols, []string{"id", "label"}) {
			t.Fatalf("projected columns = %v, want [id label]", cols)
		}
		if rows.Next() {
			t.Fatalf("expected empty result set, got at least one row")
		}
	})

	populatedRef := dorisObjectName(dorisCatalogName, namespace, populatedTable)

	t.Run("ReadWrittenDataCount", func(t *testing.T) {
		var count int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", populatedRef)
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != 3 {
			t.Fatalf("count(%s) = %d, want 3", populatedRef, count)
		}
	})

	t.Run("ReadWrittenDataValues", func(t *testing.T) {
		query := fmt.Sprintf("SELECT id, label FROM %s ORDER BY id", populatedRef)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close()
		expected := [][2]string{{"1", "one"}, {"2", "two"}, {"3", "three"}}
		i := 0
		for rows.Next() {
			var id int64
			var label sql.NullString
			if err := rows.Scan(&id, &label); err != nil {
				t.Fatalf("Scan row %d: %v", i, err)
			}
			if i >= len(expected) {
				t.Fatalf("got more than %d rows from %s", len(expected), populatedRef)
			}
			gotID := fmt.Sprintf("%d", id)
			gotLabel := label.String
			if gotID != expected[i][0] || gotLabel != expected[i][1] {
				t.Errorf("row %d = (%s, %s), want (%s, %s)",
					i, gotID, gotLabel, expected[i][0], expected[i][1])
			}
			i++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if i != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), i)
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

	dataDir, err := os.MkdirTemp("", "seaweed-doris-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	bindIP := testutil.FindBindIP()
	// 9 ports for the seaweed mini cluster, plus one for the Doris MySQL
	// query port mapped on the host.
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
		dorisHostQueryPort: ports[9],
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

// Cleanup stops Doris, SeaweedFS, and removes temporary state.
func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.dorisContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.dorisContainer).Run()
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

// waitForService polls a URL until it returns a 2xx/4xx status or timeout.
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 OK from /v1/config, got %d, body: %s", resp.StatusCode, body)
	}
}

// startDorisContainer launches the Apache Doris all-in-one image and exposes
// only the FE MySQL/SQL port to the host. The Iceberg REST and S3 endpoints
// are reached via host.docker.internal, matching the Dremio/Trino paths.
func (env *TestEnvironment) startDorisContainer(t *testing.T) {
	t.Helper()

	containerName := "seaweed-doris-" + randomString(8)
	env.dorisContainer = containerName

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:%d", env.dorisHostQueryPort, dorisQueryPort),
		"-e", "AWS_ACCESS_KEY_ID="+env.accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY="+env.secretKey,
		"-e", "AWS_REGION=us-west-2",
		dorisImage,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Doris container: %v\n%s", err, string(output))
	}
}

// dorisContainerLogs returns the tail of Doris's container logs for diagnostics.
func dorisContainerLogs(containerName string) string {
	cmd := exec.Command("docker", "logs", "--tail", "200", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(failed to fetch docker logs: %v)\n%s", err, string(output))
	}
	return string(output)
}

// waitForDoris polls Doris's MySQL query port until it accepts SELECT 1.
// The all-in-one image takes ~30-60s to boot the FE and BE; we poll long
// enough to absorb cold-start variance in CI.
func (env *TestEnvironment) waitForDoris(t *testing.T, timeout time.Duration) {
	t.Helper()

	dsn := fmt.Sprintf("root:@tcp(127.0.0.1:%d)/", env.dorisHostQueryPort)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if !containerRunning(env.dorisContainer) {
			t.Fatalf("Doris container exited before becoming ready\nContainer logs:\n%s",
				dorisContainerLogs(env.dorisContainer))
		}

		db, err := sql.Open("mysql", dsn+"?timeout=2s&readTimeout=5s&writeTimeout=5s")
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var v int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&v)
		cancel()
		db.Close()
		if err == nil && v == 1 {
			// Doris reports the FE is up before the BE has registered. A
			// catalog SHOW CATALOGS still works without BEs, but a REFRESH
			// CATALOG against an external source needs at least one live
			// BE. Wait for one to appear so subsequent subtests don't race.
			if env.waitForDorisBackend(t, 60*time.Second) {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("Timed out waiting for Doris to be ready\nLast error: %v\nContainer logs:\n%s",
		lastErr, dorisContainerLogs(env.dorisContainer))
}

// waitForDorisBackend polls SHOW BACKENDS until at least one BE reports Alive=true.
func (env *TestEnvironment) waitForDorisBackend(t *testing.T, timeout time.Duration) bool {
	t.Helper()

	dsn := fmt.Sprintf("root:@tcp(127.0.0.1:%d)/?timeout=2s&readTimeout=5s&writeTimeout=5s",
		env.dorisHostQueryPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false
	}
	defer db.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		alive, err := dorisHasAliveBackend(db)
		if err == nil && alive {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

// dorisHasAliveBackend returns true if any row in SHOW BACKENDS has Alive="true".
func dorisHasAliveBackend(db *sql.DB) (bool, error) {
	rows, err := db.Query("SHOW BACKENDS")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	aliveIdx := -1
	for i, c := range cols {
		if strings.EqualFold(c, "Alive") {
			aliveIdx = i
			break
		}
	}
	if aliveIdx < 0 {
		return false, fmt.Errorf("SHOW BACKENDS has no Alive column: %v", cols)
	}

	values := make([]sql.NullString, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return false, err
		}
		if values[aliveIdx].Valid && strings.EqualFold(values[aliveIdx].String, "true") {
			return true, nil
		}
	}
	return false, rows.Err()
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

// connectDoris opens a long-lived MySQL-protocol connection to Doris.
func (env *TestEnvironment) connectDoris(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("root:@tcp(127.0.0.1:%d)/?parseTime=true&timeout=10s&readTimeout=60s&writeTimeout=60s",
		env.dorisHostQueryPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open(mysql): %v", err)
	}
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("Doris ping failed: %v\nContainer logs:\n%s", err, dorisContainerLogs(env.dorisContainer))
	}
	return db
}

// createDorisIcebergCatalog registers a Doris EXTERNAL CATALOG of type=iceberg
// pointing at the SeaweedFS REST endpoint. Doris reuses Iceberg's standard
// REST client, so OAuth2 client credentials are passed via "credential".
func (env *TestEnvironment) createDorisIcebergCatalog(t *testing.T, db *sql.DB, warehouseBucket string) {
	t.Helper()

	icebergURI := fmt.Sprintf("http://host.docker.internal:%d", env.icebergPort)
	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
	credential := env.accessKey + ":" + env.secretKey

	// Drop first so a stale state from a previous run does not poison
	// metadata caching. Doris versions before 2.1 use IF EXISTS at the end.
	if _, err := db.Exec(fmt.Sprintf("DROP CATALOG IF EXISTS %s", quoteDorisIdent(dorisCatalogName))); err != nil {
		t.Logf("DROP CATALOG IF EXISTS %s: %v (continuing)", dorisCatalogName, err)
	}

	createSQL := fmt.Sprintf(`CREATE CATALOG %s PROPERTIES (
		"type" = "iceberg",
		"iceberg.catalog.type" = "rest",
		"uri" = %q,
		"warehouse" = %q,
		"credential" = %q,
		"s3.endpoint" = %q,
		"s3.access_key" = %q,
		"s3.secret_key" = %q,
		"s3.region" = "us-west-2",
		"use_path_style" = "true"
	)`,
		quoteDorisIdent(dorisCatalogName),
		icebergURI,
		"s3://"+warehouseBucket,
		credential,
		s3Endpoint,
		env.accessKey,
		env.secretKey,
	)
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("CREATE CATALOG %s failed: %v\nContainer logs:\n%s",
			dorisCatalogName, err, dorisContainerLogs(env.dorisContainer))
	}

	// Refresh so namespaces/tables seeded via the REST API right before this
	// step are visible to the first SHOW DATABASES query.
	if _, err := db.Exec(fmt.Sprintf("REFRESH CATALOG %s", quoteDorisIdent(dorisCatalogName))); err != nil {
		t.Logf("REFRESH CATALOG %s: %v (continuing)", dorisCatalogName, err)
	}
}

// rowsContain runs query and returns true if any value of any column equals
// (case-insensitively) the wanted token. Doris's SHOW family returns
// version-dependent column counts, so we don't pin to a specific column.
func rowsContain(t *testing.T, db *sql.DB, query, want string) bool {
	t.Helper()

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns(): %v", err)
	}
	values := make([]sql.NullString, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	wantLower := strings.ToLower(want)
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, v := range values {
			if v.Valid && strings.Contains(strings.ToLower(v.String), wantLower) {
				return true
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return false
}

// equalStringSlicesIgnoreCase compares two []string for equality, ignoring case.
func equalStringSlicesIgnoreCase(got, want []string) bool {
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

// quoteDorisIdent wraps an identifier in backticks (Doris uses MySQL syntax),
// escaping any embedded backticks.
func quoteDorisIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// dorisObjectName produces a backtick-quoted dotted reference like
// `catalog`.`namespace`.`table` so identifiers with hyphens or random
// suffixes parse without quoting issues.
func dorisObjectName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteDorisIdent(part))
	}
	return strings.Join(quoted, ".")
}

// requestIcebergOAuthToken requests an OAuth2 client_credentials token from
// the SeaweedFS Iceberg REST catalog. Used to seed the catalog with a
// namespace and table directly through the REST API before Doris connects.
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
// the REST catalog. Wrapper around createIcebergNamespaceLevels for the
// common single-level case.
func createIcebergNamespace(t *testing.T, env *TestEnvironment, token, bucketName, namespace string) {
	t.Helper()
	createIcebergNamespaceLevels(t, env, token, bucketName, []string{namespace})
}

// createIcebergTable creates a table inside a single-level namespace through
// the REST catalog. The table is created with the canonical
// (id long not null, label string nullable) schema used by all subtests.
func createIcebergTable(t *testing.T, env *TestEnvironment, token, bucketName, namespace, tableName string) {
	t.Helper()
	createIcebergTableInLevels(t, env, token, bucketName, []string{namespace}, tableName)
}

// createIcebergNamespaceLevels creates a (possibly multi-level) Iceberg
// namespace via the REST catalog.
func createIcebergNamespaceLevels(t *testing.T, env *TestEnvironment, token, bucketName string, levels []string) {
	t.Helper()

	doIcebergJSONRequest(t, env, token, http.MethodPost, fmt.Sprintf("/v1/%s/namespaces", url.PathEscape(bucketName)), map[string]any{
		"namespace": levels,
	}, http.StatusOK, http.StatusConflict)
}

// createIcebergTableInLevels creates a table inside the given namespace levels.
// The namespace path component is encoded with the unit-separator (0x1F)
// convention used by the SeaweedFS Iceberg REST API.
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

const dorisWriterImage = "seaweedfs-doris-writer"

// buildDorisWriterImage builds the local PyIceberg writer image. Layer caching
// makes repeat invocations cheap; the first build pulls python:3.11-slim and
// pip-installs pyiceberg+pyarrow (~1-2 min in CI).
func buildDorisWriterImage(t *testing.T) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	cmd := exec.Command("docker", "build",
		"-t", dorisWriterImage,
		"-f", filepath.Join(wd, "Dockerfile.writer"),
		wd,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build %s image: %v\n%s", dorisWriterImage, err, out)
	}
}

// writeIcebergRows runs the PyIceberg writer container, which loads the
// already-created table and appends three rows.
func writeIcebergRows(t *testing.T, env *TestEnvironment, bucketName string, namespace []string, tableName string) {
	t.Helper()

	args := []string{
		"run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		dorisWriterImage,
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
// response status matches one of expectedStatuses. Used by namespace and
// table seeding so the tests can fail fast with a useful error if the REST
// API rejects a setup call.
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
// talks to the master over gRPC and bypasses the S3 SigV4 path so the test
// works whether IAM is enabled or not. The `-master` flag uses SeaweedFS's
// canonical `host:port.grpcPort` ServerAddress format produced by
// pb.NewServerAddress (see weed/pb/server_address.go) — the dot is the
// separator between the HTTP port and the gRPC port and is required, not
// a typo. Replacing it with a colon would make the parser treat the gRPC
// port as the HTTP port and synthesize the wrong gRPC port.
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

// requireDorisRuntime skips the test in `-short` mode or when Docker isn't
// available, since the test cannot run without the Doris container.
func requireDorisRuntime(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !hasDocker() {
		t.Skip("Docker not available, skipping Doris integration test")
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
